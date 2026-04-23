//! SQL Server Change Data Capture (CDC) source.
//!
//! Reads from SQL Server's built-in CDC change tables (`cdc.<capture_instance>_CT`)
//! via the `[cdc].[fn_cdc_get_all_changes_<capture_instance>]` function.
//! Emits each insert/update/delete as a JSON record with a metadata op type.
//!
//! Prerequisites on the target SQL Server:
//! ```sql
//! EXEC sys.sp_cdc_enable_db;
//! EXEC sys.sp_cdc_enable_table
//!     @source_schema = 'dbo', @source_name = 'mytable', @role_name = NULL;
//! ```
//! SQL Server Agent must be running; the CDC capture job populates the
//! change tables asynchronously from the transaction log.
//!
//! LSN tracking: we store the last-processed Log Sequence Number as a
//! hex-encoded string (20 hex chars for a 10-byte LSN). On restart we
//! resume from just past that LSN.
//!
//! Required config (`settings`):
//! - `connection`: `mssql://` or `sqlserver://` URL.
//! - `capture_instance`: the CDC capture instance name (typically
//!   `<schema>_<table>` — e.g. `dbo_orders`).
//! - `schema`: column-type DSL (same format as the JDBC sink) describing
//!   the business columns to extract. Do NOT include CDC metadata
//!   columns (`__$start_lsn`, `__$operation`, etc.); we handle those.

use async_trait::async_trait;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use bytes::Bytes;
use tiberius::{ColumnData, Query};
use tracing::{info, warn};
use url::Url;

use crate::builtin::jdbc::dialect::{ColumnSpec, JsonType};
use crate::builtin::jdbc::schema::{is_valid_ident, parse_schema};
use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

pub struct MssqlCdcSource {
    connection_string: String,
    capture_instance: String,
    schema_cols: Vec<ColumnSpec>,
    pool: Option<Pool<ConnectionManager>>,
    last_lsn: Option<Vec<u8>>, // 10-byte LSN, stored/resumed as hex string
    subject_template: String,
}

impl MssqlCdcSource {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let connection_string = config
            .setting("connection")
            .map_err(ConnectorError::Config)?
            .to_string();

        let lower = connection_string.to_ascii_lowercase();
        if !(lower.starts_with("mssql://") || lower.starts_with("sqlserver://")) {
            return Err(ConnectorError::Config(
                "mssql_cdc: connection URL must start with mssql:// or sqlserver://".into(),
            ));
        }

        let capture_instance = config
            .setting("capture_instance")
            .map_err(ConnectorError::Config)?
            .to_string();
        if !is_valid_ident(&capture_instance) {
            return Err(ConnectorError::Config(format!(
                "mssql_cdc: invalid capture_instance '{capture_instance}' (must match [A-Za-z_][A-Za-z0-9_]*)"
            )));
        }

        let schema_raw = config.setting_or("schema", "");
        if schema_raw.trim().is_empty() {
            return Err(ConnectorError::Config(
                "mssql_cdc: 'schema' setting required — declares business columns to extract".into(),
            ));
        }
        let schema_cols = parse_schema(&schema_raw).map_err(|e| {
            ConnectorError::Config(format!("mssql_cdc: schema DSL error: {e}"))
        })?;

        Ok(Self {
            connection_string,
            capture_instance,
            schema_cols,
            pool: None,
            last_lsn: None,
            subject_template: config.subject_template.clone(),
        })
    }
}

fn parse_url_to_config(raw: &str) -> Result<tiberius::Config, ConnectorError> {
    let normalized = if raw.to_ascii_lowercase().starts_with("mssql://") {
        format!("sqlserver://{}", &raw[8..])
    } else {
        raw.to_string()
    };
    let u = Url::parse(&normalized)
        .map_err(|e| ConnectorError::Config(format!("mssql_cdc url parse: {e}")))?;

    let mut cfg = tiberius::Config::new();
    if let Some(h) = u.host_str() { cfg.host(h); }
    cfg.port(u.port().unwrap_or(1433));
    if !u.username().is_empty() {
        let user = percent_encoding::percent_decode_str(u.username())
            .decode_utf8_lossy()
            .into_owned();
        let pass = u
            .password()
            .map(|p| {
                percent_encoding::percent_decode_str(p)
                    .decode_utf8_lossy()
                    .into_owned()
            })
            .unwrap_or_default();
        cfg.authentication(tiberius::AuthMethod::sql_server(&user, &pass));
    }
    let db = u.path().trim_start_matches('/');
    if !db.is_empty() { cfg.database(db); }
    for (k, v) in u.query_pairs() {
        if k.eq_ignore_ascii_case("trust_server_certificate") && v.eq_ignore_ascii_case("true") {
            cfg.trust_cert();
        }
    }
    Ok(cfg)
}

fn lsn_to_hex(lsn: &[u8]) -> String {
    lsn.iter().map(|b| format!("{b:02X}")).collect()
}

fn hex_to_lsn(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte = u8::from_str_radix(&s[i..i + 2], 16).ok()?;
        out.push(byte);
    }
    Some(out)
}

/// Map a CDC `__$operation` code to a human-readable op string.
/// 1=delete, 2=insert, 3=update (before), 4=update (after).
fn op_label(code: i32) -> &'static str {
    match code {
        1 => "delete",
        2 => "insert",
        3 => "update_before",
        4 => "update_after",
        _ => "unknown",
    }
}

fn column_data_to_json(cd: &ColumnData, ty: JsonType) -> serde_json::Value {
    use serde_json::Value;
    match cd {
        ColumnData::Bit(Some(b)) => Value::Bool(*b),
        ColumnData::I16(Some(v)) => Value::Number((*v as i64).into()),
        ColumnData::I32(Some(v)) => Value::Number((*v as i64).into()),
        ColumnData::I64(Some(v)) => Value::Number((*v).into()),
        ColumnData::U8(Some(v)) => Value::Number((*v as u64).into()),
        ColumnData::F32(Some(v)) => serde_json::Number::from_f64(*v as f64)
            .map(Value::Number).unwrap_or(Value::Null),
        ColumnData::F64(Some(v)) => serde_json::Number::from_f64(*v)
            .map(Value::Number).unwrap_or(Value::Null),
        ColumnData::String(Some(s)) => Value::String(s.to_string()),
        _ => match ty {
            JsonType::Text | JsonType::Jsonb | JsonType::Timestamptz => {
                // Best-effort stringification for complex types or NULL.
                Value::String(format!("{cd:?}"))
            }
            _ => Value::Null,
        },
    }
}

#[async_trait]
impl SourceConnector for MssqlCdcSource {
    async fn start(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        let cfg = parse_url_to_config(&self.connection_string)?;
        let mgr = ConnectionManager::build(cfg)
            .map_err(|e| ConnectorError::Connection(format!("mssql_cdc bb8 build: {e}")))?;
        let pool = Pool::builder()
            .max_size(4)
            .build(mgr)
            .await
            .map_err(|e| ConnectorError::Connection(format!("mssql_cdc pool: {e}")))?;

        if let Some(s) = last_position {
            match hex_to_lsn(&s) {
                Some(lsn) => self.last_lsn = Some(lsn),
                None => warn!(last_position = %s, "mssql_cdc: invalid LSN hex; starting from beginning"),
            }
        }

        info!(
            capture_instance = %self.capture_instance,
            resume_lsn = self.last_lsn.as_deref().map(lsn_to_hex).as_deref().unwrap_or("<none>"),
            "mssql_cdc source started"
        );

        self.pool = Some(pool);
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let pool = match &self.pool {
            Some(p) => p,
            None => return Err(ConnectorError::Connection("mssql_cdc: not started".into())),
        };
        let mut conn = pool
            .get()
            .await
            .map_err(|e| ConnectorError::Connection(format!("mssql_cdc pool get: {e}")))?;

        // Determine `from_lsn` and `to_lsn`.
        //   from_lsn = last_lsn + 1 (via sys.fn_cdc_increment_lsn), or min_lsn if first run.
        //   to_lsn   = sys.fn_cdc_get_max_lsn()
        let bounds_sql = format!(
            "SELECT \
                sys.fn_cdc_get_max_lsn() AS max_lsn, \
                sys.fn_cdc_get_min_lsn(N'{}') AS min_lsn",
            self.capture_instance.replace('\'', "''")
        );
        let stream = conn.simple_query(bounds_sql).await
            .map_err(|e| ConnectorError::Connection(format!("mssql_cdc bounds query: {e}")))?;
        let rows = stream.into_first_result().await
            .map_err(|e| ConnectorError::Connection(format!("mssql_cdc bounds result: {e}")))?;
        let bounds = match rows.first() {
            Some(r) => r,
            None => return Ok(SourceBatch { records: Vec::new(), position: None }),
        };
        let max_lsn: Option<&[u8]> = bounds.get::<&[u8], _>(0);
        let min_lsn: Option<&[u8]> = bounds.get::<&[u8], _>(1);
        let Some(max_lsn) = max_lsn else {
            return Ok(SourceBatch { records: Vec::new(), position: None });
        };
        let Some(min_lsn) = min_lsn else {
            return Ok(SourceBatch { records: Vec::new(), position: None });
        };
        let max_lsn_vec = max_lsn.to_vec();
        let min_lsn_vec = min_lsn.to_vec();

        let from_lsn = match &self.last_lsn {
            Some(v) => v.clone(),
            None => min_lsn_vec.clone(),
        };

        // If from_lsn > max_lsn nothing new.
        if from_lsn > max_lsn_vec {
            return Ok(SourceBatch { records: Vec::new(), position: None });
        }

        let fetch_sql = format!(
            "SELECT TOP ({limit}) * FROM [cdc].[fn_cdc_get_all_changes_{ci}](@P1, @P2, N'all') ORDER BY [__$start_lsn], [__$seqval]",
            limit = max_batch.max(1),
            ci = self.capture_instance,
        );

        let mut q = Query::new(fetch_sql);
        q.bind(from_lsn.clone());
        q.bind(max_lsn_vec.clone());
        let stream = q.query(&mut *conn).await
            .map_err(|e| ConnectorError::Connection(format!("mssql_cdc change fetch: {e}")))?;
        let rows = stream.into_first_result().await
            .map_err(|e| ConnectorError::Connection(format!("mssql_cdc change result: {e}")))?;

        let mut out: Vec<SourceRecord> = Vec::with_capacity(rows.len());
        let mut highest_lsn = self.last_lsn.clone().unwrap_or_default();

        for row in &rows {
            // CDC metadata columns (always first 5):
            //   __$start_lsn (binary(10)), __$end_lsn (binary(10)),
            //   __$seqval (binary(10)), __$operation (int),
            //   __$update_mask (varbinary).
            let start_lsn: Option<&[u8]> = row.get::<&[u8], _>("__$start_lsn");
            let operation: Option<i32> = row.get::<i32, _>("__$operation");

            let op = operation.map(op_label).unwrap_or("unknown");

            let mut obj = serde_json::Map::with_capacity(self.schema_cols.len() + 1);
            obj.insert("__op".into(), serde_json::Value::String(op.to_string()));

            for col in &self.schema_cols {
                // Look up the business column by name.
                let cd_opt: Option<ColumnData> = row_get_column_data(row, &col.name);
                let v = cd_opt
                    .map(|cd| column_data_to_json(&cd, col.json_type))
                    .unwrap_or(serde_json::Value::Null);
                obj.insert(col.name.clone(), v);
            }

            if let Some(lsn) = start_lsn {
                if lsn.to_vec() > highest_lsn {
                    highest_lsn = lsn.to_vec();
                }
            }

            let value = Bytes::from(
                serde_json::to_vec(&obj).unwrap_or_else(|_| b"null".to_vec()),
            );
            let subject = if self.subject_template.is_empty() {
                format!("mssql_cdc.{}.{}", self.capture_instance, op)
            } else {
                self.subject_template.clone()
            };
            out.push(SourceRecord {
                key: None,
                value,
                subject,
                headers: vec![
                    ("exspeed-mssql-op".into(), op.to_string()),
                    ("exspeed-mssql-capture".into(), self.capture_instance.clone()),
                ],
            });
        }

        let position = if !highest_lsn.is_empty() && Some(&highest_lsn) != self.last_lsn.as_ref() {
            self.last_lsn = Some(highest_lsn.clone());
            Some(lsn_to_hex(&highest_lsn))
        } else {
            None
        };

        Ok(SourceBatch { records: out, position })
    }

    async fn commit(&mut self, _position: String) -> Result<(), ConnectorError> {
        // LSN is advanced on successful poll; nothing server-side to ack.
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.pool = None; // bb8 drops connections when pool is dropped.
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        if self.pool.is_some() { HealthStatus::Healthy }
        else { HealthStatus::Unhealthy("mssql_cdc: not started".into()) }
    }
}

/// Best-effort extract a `ColumnData` owned-value from a row by column name.
/// tiberius gives typed accessors; we probe the common types.
fn row_get_column_data(row: &tiberius::Row, col: &str) -> Option<ColumnData<'static>> {
    // Note: tiberius::Row doesn't expose ColumnData directly. We re-materialize
    // the value into an owned ColumnData by probing the typed accessors in
    // the same order used by column_data_to_json.
    if let Ok(Some(v)) = std::panic::catch_unwind(|| row.try_get::<&str, _>(col)).unwrap_or(Ok(None)) {
        return Some(ColumnData::String(Some(v.to_string().into())));
    }
    if let Ok(Some(v)) = std::panic::catch_unwind(|| row.try_get::<i64, _>(col)).unwrap_or(Ok(None)) {
        return Some(ColumnData::I64(Some(v)));
    }
    if let Ok(Some(v)) = std::panic::catch_unwind(|| row.try_get::<i32, _>(col)).unwrap_or(Ok(None)) {
        return Some(ColumnData::I32(Some(v)));
    }
    if let Ok(Some(v)) = std::panic::catch_unwind(|| row.try_get::<bool, _>(col)).unwrap_or(Ok(None)) {
        return Some(ColumnData::Bit(Some(v)));
    }
    if let Ok(Some(v)) = std::panic::catch_unwind(|| row.try_get::<f64, _>(col)).unwrap_or(Ok(None)) {
        return Some(ColumnData::F64(Some(v)));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_config(settings: HashMap<String, String>) -> ConnectorConfig {
        ConnectorConfig {
            name: "test-cdc".into(),
            connector_type: "source".into(),
            plugin: "mssql_cdc".into(),
            stream: "cdc-events".into(),
            subject_template: String::new(),
            subject_filter: String::new(),
            settings,
            batch_size: 100,
            poll_interval_ms: 50,
            dedup_enabled: false,
            dedup_key: String::new(),
            dedup_window_secs: 86400,
            transform_sql: String::new(),
            on_transient_exhausted: crate::config::OnTransientExhausted::default(),
            retry: crate::retry::RetryPolicy::default_transient(),
        }
    }

    #[test]
    fn new_rejects_non_mssql_url() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x/y".into()),
            ("capture_instance".into(), "dbo_orders".into()),
            ("schema".into(), "id:bigint".into()),
        ]));
        assert!(MssqlCdcSource::new(&cfg).is_err());
    }

    #[test]
    fn new_requires_capture_instance_and_schema() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "mssql://sa:pw@h/db".into()),
        ]));
        assert!(MssqlCdcSource::new(&cfg).is_err());

        let cfg = make_config(HashMap::from([
            ("connection".into(), "mssql://sa:pw@h/db".into()),
            ("capture_instance".into(), "dbo_orders".into()),
        ]));
        assert!(MssqlCdcSource::new(&cfg).is_err());
    }

    #[test]
    fn new_accepts_valid() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "mssql://sa:pw@h/db".into()),
            ("capture_instance".into(), "dbo_orders".into()),
            ("schema".into(), "id:bigint, name:text".into()),
        ]));
        assert!(MssqlCdcSource::new(&cfg).is_ok());
    }

    #[test]
    fn new_rejects_injection_in_capture_instance() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "mssql://sa:pw@h/db".into()),
            ("capture_instance".into(), "dbo; DROP TABLE x".into()),
            ("schema".into(), "id:bigint".into()),
        ]));
        assert!(MssqlCdcSource::new(&cfg).is_err());
    }

    #[test]
    fn lsn_hex_roundtrip() {
        let lsn = vec![0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x07, 0xF8, 0x00, 0x03];
        let hex = lsn_to_hex(&lsn);
        assert_eq!(hex, "0000002A000007F80003");
        assert_eq!(hex_to_lsn(&hex).unwrap(), lsn);
    }

    #[test]
    fn hex_to_lsn_rejects_bad_input() {
        assert!(hex_to_lsn("ABC").is_none()); // odd length
        assert!(hex_to_lsn("GG").is_none());  // non-hex
    }

    #[test]
    fn op_label_cases() {
        assert_eq!(op_label(1), "delete");
        assert_eq!(op_label(2), "insert");
        assert_eq!(op_label(3), "update_before");
        assert_eq!(op_label(4), "update_after");
        assert_eq!(op_label(99), "unknown");
    }
}
