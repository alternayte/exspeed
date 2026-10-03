//! `mssql_cdc`: SQL Server Change Data Capture.
//!
//! Reads `[cdc].[fn_cdc_get_all_changes_<capture_instance>]` and emits one
//! record per change with the same envelope as `postgres_cdc`:
//!
//! ```json
//! {"op": "c" | "u" | "d",
//!  "before": null | {...},          // the deleted row for "d"
//!  "after":  {...} | null,
//!  "source": {"connector": "...", "capture_instance": "dbo_orders",
//!             "lsn": "0000002A000007F80003", "seqval": "..."}}
//! ```
//!
//! Prerequisites on the server:
//! ```sql
//! EXEC sys.sp_cdc_enable_db;
//! EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'orders', @role_name = NULL;
//! ```
//!
//! **Cursor.** The checkpoint is the `(__$start_lsn, __$seqval)` of the last
//! emitted change (`"<lsn>:<seqval>"`, hex). The next poll reads from that
//! LSN and skips rows at or before the pair, so a transaction larger than
//! `batch_size` is paged through instead of re-read forever. Once a poll
//! catches up (fewer rows than requested), the cursor moves to
//! `sys.fn_cdc_increment_lsn(<lsn>)` (stored as `"<lsn>"`) so the finished
//! transaction is not scanned again.
//!
//! Columns are cast in SQL according to the `schema` DSL; NULL becomes JSON
//! null. Each record carries `x-idempotency-key = mssqlcdc:<ci>:<lsn>:<seqval>`,
//! so replays inside the stream's dedup window are dropped.
//!
//! Guarantee: at-least-once (effectively-once inside the dedup window). If
//! the CDC cleanup job removed changes past the stored cursor, the connector
//! fails instead of silently skipping them.

use async_trait::async_trait;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tiberius::Query;
use tracing::info;

use crate::builtin::jdbc::dialect::{ColumnSpec, DialectKind, JsonType};
use crate::builtin::jdbc::errors;
use crate::builtin::jdbc::schema::{is_valid_ident, parse_schema};
use crate::builtin::jdbc_poll::parse_mssql_url;
use crate::registry::PluginInit;
use crate::settings::{self, de};
use crate::traits::{ConnectorError, SourceBatch, SourceConnector, SourceRecord};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MssqlCdcSettings {
    pub connection: String,
    pub capture_instance: String,
    /// Column DSL for the business columns: `"id:bigint, name:text"`.
    pub schema: String,
    /// Columns that form the record key (default: none).
    #[serde(default, deserialize_with = "de::string_list")]
    pub key_columns: Vec<String>,
}

/// Position in the change table.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Cursor {
    /// Read from this LSN (inclusive); nothing at it was emitted yet.
    From(Vec<u8>),
    /// The last emitted change; skip everything up to and including it.
    After { lsn: Vec<u8>, seqval: Vec<u8> },
}

impl Cursor {
    fn parse(s: &str) -> Option<Self> {
        match s.split_once(':') {
            Some((l, q)) => Some(Self::After {
                lsn: hex_to_bytes(l)?,
                seqval: hex_to_bytes(q)?,
            }),
            None => Some(Self::From(hex_to_bytes(s)?)),
        }
    }

    fn encode(&self) -> String {
        match self {
            Self::From(l) => to_hex(l),
            Self::After { lsn, seqval } => format!("{}:{}", to_hex(lsn), to_hex(seqval)),
        }
    }

    fn lsn(&self) -> &[u8] {
        match self {
            Self::From(l) => l,
            Self::After { lsn, .. } => lsn,
        }
    }
}

pub struct MssqlCdcSource {
    connector: String,
    settings: MssqlCdcSettings,
    schema_cols: Vec<ColumnSpec>,
    subject_template: String,
    pool: Option<Pool<ConnectionManager>>,
    cursor: Option<Cursor>,
}

impl MssqlCdcSource {
    pub fn new(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: MssqlCdcSettings = settings::parse("mssql_cdc", &init.settings)?;
        let lower = s.connection.to_ascii_lowercase();
        if !(lower.starts_with("mssql://") || lower.starts_with("sqlserver://")) {
            return Err(ConnectorError::config(
                "mssql_cdc: connection URL must start with mssql:// or sqlserver://",
            ));
        }
        if !is_valid_ident(&s.capture_instance) {
            return Err(ConnectorError::config(format!(
                "mssql_cdc: invalid capture_instance '{}' (must match [A-Za-z_][A-Za-z0-9_]*)",
                s.capture_instance
            )));
        }
        if s.schema.trim().is_empty() {
            return Err(ConnectorError::config(
                "mssql_cdc: 'schema' is required — declares the business columns to extract",
            ));
        }
        let schema_cols = parse_schema(&s.schema)
            .map_err(|e| ConnectorError::config(format!("mssql_cdc: schema DSL error: {e}")))?;
        for k in &s.key_columns {
            if !schema_cols.iter().any(|c| &c.name == k) {
                return Err(ConnectorError::config(format!(
                    "mssql_cdc: key column '{k}' is not in the schema"
                )));
            }
        }
        Ok(Self {
            connector: init.config.name.clone(),
            settings: s,
            schema_cols,
            subject_template: if init.config.subject_template.is_empty() {
                "mssql_cdc.{capture_instance}.{op}".into()
            } else {
                init.config.subject_template.clone()
            },
            pool: None,
            cursor: None,
        })
    }

    fn fetch_sql(&self, limit: usize, filtered: bool) -> String {
        let cols: Vec<String> = self
            .schema_cols
            .iter()
            .map(|c| {
                let q = format!("[{}]", c.name);
                let expr = match c.json_type {
                    JsonType::Bigint => format!("CAST({q} AS BIGINT)"),
                    JsonType::Double => format!("CAST({q} AS FLOAT)"),
                    JsonType::Boolean => format!("CAST({q} AS BIT)"),
                    JsonType::Timestamptz => format!("CONVERT(NVARCHAR(40), {q}, 127)"),
                    JsonType::Text | JsonType::Jsonb => format!("CAST({q} AS NVARCHAR(MAX))"),
                };
                format!("{expr} AS {q}")
            })
            .collect();
        let filter = if filtered {
            " WHERE [__$start_lsn] > @P3 OR ([__$start_lsn] = @P3 AND [__$seqval] > @P4)"
        } else {
            ""
        };
        format!(
            "SELECT TOP ({limit}) [__$start_lsn], [__$seqval], [__$operation], {cols} \
             FROM [cdc].[fn_cdc_get_all_changes_{ci}](@P1, @P2, N'all'){filter} \
             ORDER BY [__$start_lsn], [__$seqval]",
            limit = limit.max(1),
            cols = cols.join(", "),
            ci = self.settings.capture_instance,
        )
    }

    fn record(&self, row: &tiberius::Row) -> Option<(SourceRecord, Vec<u8>, Vec<u8>)> {
        let lsn = row.get::<&[u8], _>("__$start_lsn")?.to_vec();
        let seqval = row.get::<&[u8], _>("__$seqval")?.to_vec();
        let operation: i32 = row.get::<i32, _>("__$operation").unwrap_or(0);
        let (op, op_name) = match operation {
            1 => ("d", "delete"),
            2 => ("c", "insert"),
            4 => ("u", "update"),
            _ => return None, // 3 = update before-image, not requested with N'all'
        };
        let mut obj = Map::with_capacity(self.schema_cols.len());
        for col in &self.schema_cols {
            obj.insert(col.name.clone(), decode_cell(row, &col.name, col.json_type));
        }
        let key = if self.settings.key_columns.is_empty() {
            None
        } else if self.settings.key_columns.len() == 1 {
            Some(match obj.get(&self.settings.key_columns[0]) {
                Some(Value::String(s)) => s.clone(),
                Some(v) => v.to_string(),
                None => String::new(),
            })
        } else {
            let parts: Vec<Value> = self
                .settings
                .key_columns
                .iter()
                .map(|k| obj.get(k).cloned().unwrap_or(Value::Null))
                .collect();
            Some(Value::Array(parts).to_string())
        };
        let row_value = Value::Object(obj);
        let (before, after) = if op == "d" {
            (row_value, Value::Null)
        } else {
            (Value::Null, row_value)
        };
        let lsn_hex = to_hex(&lsn);
        let seq_hex = to_hex(&seqval);
        let envelope = json!({
            "op": op,
            "before": before,
            "after": after,
            "source": {
                "connector": self.connector,
                "capture_instance": self.settings.capture_instance,
                "lsn": lsn_hex,
                "seqval": seq_hex,
            }
        });
        let subject = crate::subject::render(
            &self.subject_template,
            &[
                ("capture_instance", &self.settings.capture_instance),
                ("op", op_name),
            ],
            Some(&envelope),
        );
        let rec = SourceRecord {
            key: key.map(|k| Bytes::from(k.into_bytes())),
            value: Bytes::from(envelope.to_string().into_bytes()),
            subject,
            headers: vec![
                (
                    "x-idempotency-key".into(),
                    format!(
                        "mssqlcdc:{}:{lsn_hex}:{seq_hex}",
                        self.settings.capture_instance
                    ),
                ),
                ("exspeed-mssql-op".into(), op_name.into()),
            ],
        };
        Some((rec, lsn, seqval))
    }
}

fn decode_cell(row: &tiberius::Row, col: &str, ty: JsonType) -> Value {
    let v = match ty {
        JsonType::Bigint => row.try_get::<i64, _>(col).ok().flatten().map(Value::from),
        JsonType::Double => row
            .try_get::<f64, _>(col)
            .ok()
            .flatten()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number),
        JsonType::Boolean => row.try_get::<bool, _>(col).ok().flatten().map(Value::Bool),
        JsonType::Text | JsonType::Timestamptz => row
            .try_get::<&str, _>(col)
            .ok()
            .flatten()
            .map(|s| Value::String(s.to_string())),
        JsonType::Jsonb => row
            .try_get::<&str, _>(col)
            .ok()
            .flatten()
            .map(|s| serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string()))),
    };
    v.unwrap_or(Value::Null)
}

fn tib_err(e: tiberius::error::Error, what: &str) -> ConnectorError {
    match errors::to_connector_error(DialectKind::Mssql, &errors::from_tiberius(e), what) {
        // A source's own query can't be "poison".
        ConnectorError::Poison(r) => ConnectorError::fatal(r.detail()),
        other => other,
    }
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02X}")).collect()
}

fn hex_to_bytes(s: &str) -> Option<Vec<u8>> {
    if s.is_empty() || !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[async_trait]
impl SourceConnector for MssqlCdcSource {
    async fn start(&mut self, checkpoint: Option<String>) -> Result<(), ConnectorError> {
        self.cursor = match &checkpoint {
            Some(s) => Some(Cursor::parse(s).ok_or_else(|| {
                ConnectorError::fatal(format!(
                    "mssql_cdc: stored checkpoint '{s}' is not a valid LSN cursor"
                ))
            })?),
            None => None,
        };
        let cfg = parse_mssql_url(&self.settings.connection)?;
        let mgr = ConnectionManager::build(cfg)
            .map_err(|e| ConnectorError::config(format!("mssql_cdc: {e}")))?;
        let pool = Pool::builder()
            .max_size(2)
            .build(mgr)
            .await
            .map_err(|e| ConnectorError::connection(format!("mssql_cdc pool: {e}")))?;
        info!(
            capture_instance = %self.settings.capture_instance,
            cursor = ?checkpoint,
            "mssql_cdc source started"
        );
        self.pool = Some(pool);
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| ConnectorError::connection("mssql_cdc: not started"))?;
        let mut conn = pool
            .get()
            .await
            .map_err(|e| ConnectorError::connection(format!("mssql_cdc pool get: {e}")))?;

        let bounds_sql = format!(
            "SELECT sys.fn_cdc_get_max_lsn() AS max_lsn, sys.fn_cdc_get_min_lsn(N'{}') AS min_lsn",
            self.settings.capture_instance
        );
        let rows = conn
            .simple_query(bounds_sql)
            .await
            .map_err(|e| tib_err(e, "mssql_cdc bounds"))?
            .into_first_result()
            .await
            .map_err(|e| tib_err(e, "mssql_cdc bounds"))?;
        let (max_lsn, min_lsn) = match rows.first() {
            Some(r) => (
                r.get::<&[u8], _>(0).map(<[u8]>::to_vec),
                r.get::<&[u8], _>(1).map(<[u8]>::to_vec),
            ),
            None => (None, None),
        };
        let (Some(max_lsn), Some(min_lsn)) = (max_lsn, min_lsn) else {
            return Ok(SourceBatch::empty());
        };
        // An all-zero min LSN means the capture instance has no data yet.
        if min_lsn.iter().all(|b| *b == 0) {
            return Ok(SourceBatch::empty());
        }

        let (from, filter) = match &self.cursor {
            None => (min_lsn.clone(), None),
            Some(c) => {
                if c.lsn() < min_lsn.as_slice() {
                    return Err(ConnectorError::fatal(format!(
                        "mssql_cdc: changes after the stored cursor {} were removed by CDC cleanup \
                         (min LSN is now {}); reset the connector's offset to continue",
                        c.encode(),
                        to_hex(&min_lsn)
                    )));
                }
                match c {
                    Cursor::From(l) => (l.clone(), None),
                    Cursor::After { lsn, seqval } => {
                        (lsn.clone(), Some((lsn.clone(), seqval.clone())))
                    }
                }
            }
        };
        if from > max_lsn {
            return Ok(SourceBatch::empty());
        }

        let limit = max_batch.max(1);
        let mut q = Query::new(self.fetch_sql(limit, filter.is_some()));
        q.bind(from);
        q.bind(max_lsn);
        if let Some((l, s)) = &filter {
            q.bind(l.clone());
            q.bind(s.clone());
        }
        let rows = q
            .query(&mut *conn)
            .await
            .map_err(|e| tib_err(e, "mssql_cdc fetch"))?
            .into_first_result()
            .await
            .map_err(|e| tib_err(e, "mssql_cdc fetch"))?;

        let mut records = Vec::with_capacity(rows.len());
        let mut last: Option<(Vec<u8>, Vec<u8>)> = None;
        for row in &rows {
            if let Some((rec, lsn, seq)) = self.record(row) {
                records.push(rec);
                last = Some((lsn, seq));
            } else if let (Some(l), Some(s)) = (row.get::<&[u8], _>(0), row.get::<&[u8], _>(1)) {
                last = Some((l.to_vec(), s.to_vec()));
            }
        }
        let Some((lsn, seqval)) = last else {
            return Ok(SourceBatch::empty());
        };

        let next = if rows.len() < limit {
            // Caught up: the transaction at `lsn` is complete.
            let mut q = Query::new("SELECT sys.fn_cdc_increment_lsn(@P1)");
            q.bind(lsn.clone());
            let inc = q
                .query(&mut *conn)
                .await
                .map_err(|e| tib_err(e, "mssql_cdc increment_lsn"))?
                .into_row()
                .await
                .map_err(|e| tib_err(e, "mssql_cdc increment_lsn"))?
                .and_then(|r| r.get::<&[u8], _>(0).map(<[u8]>::to_vec));
            match inc {
                Some(l) => Cursor::From(l),
                None => Cursor::After { lsn, seqval },
            }
        } else {
            Cursor::After { lsn, seqval }
        };
        let checkpoint = next.encode();
        self.cursor = Some(next);
        Ok(SourceBatch {
            records,
            checkpoint: Some(checkpoint),
        })
    }

    async fn ack(&mut self, _checkpoint: Option<&str>) -> Result<(), ConnectorError> {
        // Nothing to acknowledge server-side; the cursor is the checkpoint.
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.pool = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectorConfig, ConnectorType};

    fn init(settings: serde_json::Value) -> PluginInit {
        let (m, _) = exspeed_common::Metrics::new();
        PluginInit {
            config: ConnectorConfig::new(
                "test-cdc",
                ConnectorType::Source,
                "mssql_cdc",
                "cdc-events",
            ),
            settings: settings.as_object().unwrap().clone(),
            metrics: std::sync::Arc::new(m),
        }
    }

    #[test]
    fn settings_validation() {
        let ok = json!({"connection": "mssql://sa:pw@h/db", "capture_instance": "dbo_orders",
                        "schema": "id:bigint, name:text", "key_columns": ["id"]});
        assert!(MssqlCdcSource::new(&init(ok)).is_ok());
        for bad in [
            json!({"connection": "postgres://x/y", "capture_instance": "dbo_orders", "schema": "id:bigint"}),
            json!({"connection": "mssql://sa:pw@h/db"}),
            json!({"connection": "mssql://sa:pw@h/db", "capture_instance": "dbo_orders"}),
            json!({"connection": "mssql://sa:pw@h/db", "capture_instance": "dbo; DROP TABLE x", "schema": "id:bigint"}),
            json!({"connection": "mssql://sa:pw@h/db", "capture_instance": "c", "schema": "id:bigint", "key_columns": "nope"}),
            json!({"connection": "mssql://sa:pw@h/db", "capture_instance": "c", "schema": "id:bigint", "tabel": "x"}),
        ] {
            assert!(MssqlCdcSource::new(&init(bad.clone())).is_err(), "{bad}");
        }
    }

    #[test]
    fn cursor_roundtrip() {
        let lsn = vec![0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x07, 0xF8, 0x00, 0x03];
        let from = Cursor::From(lsn.clone());
        assert_eq!(from.encode(), "0000002A000007F80003");
        assert_eq!(Cursor::parse(&from.encode()), Some(from));
        let after = Cursor::After {
            lsn: lsn.clone(),
            seqval: vec![0, 1],
        };
        assert_eq!(after.encode(), "0000002A000007F80003:0001");
        assert_eq!(Cursor::parse(&after.encode()), Some(after));
        assert_eq!(Cursor::parse("ABC"), None);
        assert_eq!(Cursor::parse("GG"), None);
        assert_eq!(Cursor::parse(""), None);
    }

    #[test]
    fn fetch_sql_pages_with_seqval() {
        let src = MssqlCdcSource::new(&init(json!({
            "connection": "mssql://sa:pw@h/db", "capture_instance": "dbo_orders",
            "schema": "id:bigint, at:timestamptz, doc:jsonb"
        })))
        .unwrap();
        let sql = src.fetch_sql(10, true);
        assert!(sql.starts_with("SELECT TOP (10) [__$start_lsn], [__$seqval], [__$operation]"));
        assert!(sql.contains("CAST([id] AS BIGINT) AS [id]"));
        assert!(sql.contains("CONVERT(NVARCHAR(40), [at], 127) AS [at]"));
        assert!(sql.contains("fn_cdc_get_all_changes_dbo_orders](@P1, @P2, N'all')"));
        assert!(sql.contains("[__$start_lsn] = @P3 AND [__$seqval] > @P4"));
        assert!(!src.fetch_sql(10, false).contains("@P3"));
    }
}
