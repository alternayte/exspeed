//! `jdbc` sink: write records to Postgres, MySQL, SQLite or SQL Server.
//!
//! - **Typed mode** (`schema` set): each JSON field maps to a column.
//! - **Blob mode**: `(offset, subject, key, value)` rows.
//!
//! Rows are written with multi-row `INSERT`/upsert statements (bounded by
//! the dialect's parameter limit). In upsert mode a key repeated within one
//! statement keeps its last occurrence. When a multi-row statement hits a
//! data error, the chunk is retried row by row to isolate the poison row.
//!
//! Errors follow the per-dialect tables in [`errors`]. Guarantee:
//! effectively-once in upsert mode (a replay rewrites the same rows);
//! at-least-once in insert mode, where a duplicate-key error on replay is
//! treated as "already written".

pub mod backend;
pub mod dialect;
pub mod errors;
pub mod mssql;
pub mod mysql;
pub mod postgres;
pub mod schema;
pub mod sqlite;
pub(super) mod sqlx_backend;
pub(super) mod tiberius_backend;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use tracing::{info, warn};

use crate::registry::PluginInit;
use crate::settings::{self, de};
use crate::traits::{ConnectorError, PoisonReason, SinkConnector, SinkRecord, WriteResult};

use self::backend::{BackendError, Param, SinkBackend};
use self::dialect::{dialect_for, ColumnSpec, Dialect, DialectKind, JsonType};
use self::errors::{classify, SqlClass};
use self::schema::{bind_json_as_type, is_valid_ident, parse_schema, BindError};
use self::sqlx_backend::SqlxBackend;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkMode {
    Upsert,
    Insert,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JdbcSinkSettings {
    pub connection: String,
    pub table: String,
    #[serde(default = "default_mode")]
    pub mode: SinkMode,
    #[serde(default, deserialize_with = "de::string_list")]
    pub upsert_keys: Vec<String>,
    #[serde(default, deserialize_with = "de::bool")]
    pub auto_create_table: bool,
    /// Column DSL: `"id:bigint, name:text?, at:timestamptz"`.
    #[serde(default)]
    pub schema: Option<String>,
    /// Upper bound on rows per statement (the dialect's parameter limit
    /// may lower it).
    #[serde(default = "default_max_rows", deserialize_with = "de::usize")]
    pub max_rows_per_statement: usize,
}

fn default_mode() -> SinkMode {
    SinkMode::Upsert
}
fn default_max_rows() -> usize {
    500
}

const BLOB_COLS: [&str; 4] = ["offset", "subject", "key", "value"];
const BLOB_TYPES: [JsonType; 4] = [
    JsonType::Bigint,
    JsonType::Text,
    JsonType::Text,
    JsonType::Jsonb,
];

pub struct JdbcSinkConnector {
    s: JdbcSinkSettings,
    schema_cols: Option<Vec<ColumnSpec>>,
    dialect: Box<dyn Dialect>,
    kind: DialectKind,
    backend: Option<Box<dyn SinkBackend>>,
    connector_name: String,
    stream_name: String,
    metrics: Arc<exspeed_common::Metrics>,
}

/// One record bound to statement parameters.
struct BoundRow {
    /// Index in the `write()` input.
    index: usize,
    params: Vec<Param>,
    /// Upsert key (for de-duplication within a statement).
    key: Option<String>,
}

enum StepOutcome {
    Done,
    Poison {
        index: usize,
        reason: PoisonReason,
    },
    Failed {
        accepted: usize,
        error: ConnectorError,
    },
}

impl JdbcSinkConnector {
    pub fn new(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: JdbcSinkSettings = settings::parse("jdbc", &init.settings)?;
        if !is_valid_ident(&s.table) {
            return Err(ConnectorError::config(format!(
                "jdbc sink: invalid table name '{}' (must match [A-Za-z_][A-Za-z0-9_]*)",
                s.table
            )));
        }
        for k in &s.upsert_keys {
            if !is_valid_ident(k) {
                return Err(ConnectorError::config(format!(
                    "jdbc sink: invalid upsert_keys entry '{k}'"
                )));
            }
        }
        let schema_cols = match s.schema.as_deref().map(str::trim) {
            None | Some("") => None,
            Some(dsl) => Some(parse_schema(dsl).map_err(|e| {
                ConnectorError::config(format!("jdbc sink: schema DSL error: {e}"))
            })?),
        };
        if let Some(cols) = &schema_cols {
            if s.mode == SinkMode::Upsert && s.upsert_keys.is_empty() {
                return Err(ConnectorError::config(
                    "jdbc sink: upsert_keys must be set when mode=upsert and schema is declared",
                ));
            }
            for k in &s.upsert_keys {
                if !cols.iter().any(|c| &c.name == k) {
                    return Err(ConnectorError::config(format!(
                        "jdbc sink: upsert_keys entry '{k}' is not in declared schema"
                    )));
                }
            }
        }
        if s.max_rows_per_statement == 0 {
            return Err(ConnectorError::config(
                "jdbc sink: max_rows_per_statement must be >= 1",
            ));
        }
        let kind = DialectKind::from_url(&s.connection)?;
        Ok(Self {
            dialect: dialect_for(kind),
            kind,
            s,
            schema_cols,
            backend: None,
            connector_name: init.config.name.clone(),
            stream_name: init.config.stream.clone(),
            metrics: init.metrics.clone(),
        })
    }

    fn record_skip(&self, reason: &'static str) {
        self.metrics.connector_records_skipped_total.add(
            1,
            &[
                opentelemetry::KeyValue::new("connector", self.connector_name.clone()),
                opentelemetry::KeyValue::new("stream", self.stream_name.clone()),
                opentelemetry::KeyValue::new("reason", reason),
            ],
        );
    }

    fn record_write_error(&self, e: &BackendError) {
        let code = match e {
            BackendError::Sql { code, .. } => code.clone(),
            _ => String::new(),
        };
        self.metrics.connector_write_errors_total.add(
            1,
            &[
                opentelemetry::KeyValue::new("connector", self.connector_name.clone()),
                opentelemetry::KeyValue::new("stream", self.stream_name.clone()),
                opentelemetry::KeyValue::new("sqlstate", code),
            ],
        );
    }

    fn columns(&self) -> (Vec<&str>, Vec<JsonType>, Vec<&str>) {
        match &self.schema_cols {
            Some(cols) => (
                cols.iter().map(|c| c.name.as_str()).collect(),
                cols.iter().map(|c| c.json_type).collect(),
                self.s.upsert_keys.iter().map(|s| s.as_str()).collect(),
            ),
            None => (BLOB_COLS.to_vec(), BLOB_TYPES.to_vec(), vec!["offset"]),
        }
    }

    /// Bind one record, or explain why it is poison.
    fn bind(&self, index: usize, record: &SinkRecord) -> Result<BoundRow, PoisonReason> {
        let json: serde_json::Value =
            serde_json::from_slice(&record.value).map_err(|_| PoisonReason::NonJsonRecord)?;
        match &self.schema_cols {
            None => {
                let params = vec![
                    Param::I64(record.offset as i64),
                    if record.subject.is_empty() {
                        Param::Null
                    } else {
                        Param::Text(record.subject.clone())
                    },
                    match &record.key {
                        Some(b) => Param::Text(String::from_utf8_lossy(b).into_owned()),
                        None => Param::Null,
                    },
                    Param::JsonText(json.to_string()),
                ];
                Ok(BoundRow {
                    index,
                    params,
                    key: Some(record.offset.to_string()),
                })
            }
            Some(cols) => {
                let obj = json.as_object().ok_or(PoisonReason::NonJsonRecord)?;
                let mut params = Vec::with_capacity(cols.len());
                for c in cols {
                    let p = bind_json_as_type(c, obj.get(&c.name)).map_err(|e| match e {
                        BindError::TypeMismatch {
                            field,
                            expected,
                            got,
                        } => PoisonReason::TypeMismatch {
                            field,
                            expected: expected.to_string(),
                            got: got.to_string(),
                        },
                        BindError::TimestampParse { field, .. } => {
                            PoisonReason::TimestampParseFailed { field }
                        }
                        BindError::MissingRequired { field } => {
                            PoisonReason::MissingRequiredField { field }
                        }
                    })?;
                    params.push(p);
                }
                let key = (self.s.mode == SinkMode::Upsert).then(|| {
                    self.s
                        .upsert_keys
                        .iter()
                        .map(|k| obj.get(k).map(|v| v.to_string()).unwrap_or_default())
                        .collect::<Vec<_>>()
                        .join("\u{1f}")
                });
                Ok(BoundRow { index, params, key })
            }
        }
    }

    fn statement(&self, rows: usize) -> String {
        let (cols, types, keys) = self.columns();
        let sql = match self.s.mode {
            SinkMode::Upsert => self
                .dialect
                .upsert_rows_sql(&self.s.table, &cols, &keys, rows),
            SinkMode::Insert => self.dialect.insert_rows_sql(&self.s.table, &cols, rows),
        };
        let all_types: Vec<JsonType> = (0..rows).flat_map(|_| types.iter().copied()).collect();
        self.dialect.cast_placeholders(sql, &all_types)
    }

    fn rows_per_statement(&self) -> usize {
        let ncols = self.columns().0.len().max(1);
        (self.dialect.max_params() / ncols).clamp(1, self.s.max_rows_per_statement)
    }

    /// Classify a failed statement for row `index`.
    fn row_error(&self, e: BackendError, index: usize, accepted: usize) -> Result<(), StepOutcome> {
        let class = classify(self.kind, &e);
        match class {
            // Insert mode: the row is already there (a replay). Upsert mode:
            // a different unique constraint rejected the row.
            SqlClass::Duplicate if self.s.mode == SinkMode::Insert => Ok(()),
            SqlClass::Duplicate | SqlClass::Poison => {
                self.record_write_error(&e);
                Err(StepOutcome::Poison {
                    index,
                    reason: PoisonReason::SinkRejected {
                        detail: e.to_string(),
                    },
                })
            }
            _ => {
                self.record_write_error(&e);
                Err(StepOutcome::Failed {
                    accepted,
                    error: errors::to_connector_error(self.kind, &e, "jdbc sink"),
                })
            }
        }
    }

    async fn write_rows(&self, backend: &dyn SinkBackend, rows: &[BoundRow]) -> StepOutcome {
        let per = self.rows_per_statement();
        for chunk in rows.chunks(per) {
            // Keep the last occurrence of each upsert key in this statement.
            let deduped: Vec<&BoundRow> = if self.s.mode == SinkMode::Upsert {
                let mut last: HashMap<&str, usize> = HashMap::new();
                for (i, r) in chunk.iter().enumerate() {
                    if let Some(k) = &r.key {
                        last.insert(k.as_str(), i);
                    }
                }
                chunk
                    .iter()
                    .enumerate()
                    .filter(|(i, r)| r.key.as_deref().is_none_or(|k| last.get(k) == Some(i)))
                    .map(|(_, r)| r)
                    .collect()
            } else {
                chunk.iter().collect()
            };
            let sql = self.statement(deduped.len());
            let params: Vec<Param> = deduped
                .iter()
                .flat_map(|r| r.params.iter().cloned())
                .collect();
            let first = chunk[0].index;
            match backend.execute_row(&sql, &params).await {
                Ok(()) => {}
                Err(e) => match classify(self.kind, &e) {
                    SqlClass::Duplicate | SqlClass::Poison => {
                        // Isolate the offending row.
                        if let Some(o) = self.write_one_by_one(backend, chunk).await {
                            return o;
                        }
                    }
                    _ => {
                        self.record_write_error(&e);
                        return StepOutcome::Failed {
                            accepted: first,
                            error: errors::to_connector_error(self.kind, &e, "jdbc sink"),
                        };
                    }
                },
            }
        }
        StepOutcome::Done
    }

    async fn write_one_by_one(
        &self,
        backend: &dyn SinkBackend,
        rows: &[BoundRow],
    ) -> Option<StepOutcome> {
        let sql = self.statement(1);
        for r in rows {
            if let Err(e) = backend.execute_row(&sql, &r.params).await {
                if let Err(o) = self.row_error(e, r.index, r.index) {
                    return Some(o);
                }
            }
        }
        None
    }

    async fn build_backend(&self) -> Result<Box<dyn SinkBackend>, ConnectorError> {
        let map = |e: BackendError| errors::to_connector_error(self.kind, &e, "jdbc sink connect");
        match self.kind {
            DialectKind::Postgres | DialectKind::MySql | DialectKind::Sqlite => Ok(Box::new(
                SqlxBackend::connect(&self.s.connection)
                    .await
                    .map_err(map)?,
            )),
            DialectKind::Mssql => Ok(Box::new(
                tiberius_backend::TiberiusBackend::connect(&self.s.connection)
                    .await
                    .map_err(map)?,
            )),
        }
    }
}

#[async_trait]
impl SinkConnector for JdbcSinkConnector {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        let backend = self.build_backend().await.inspect_err(|_| {
            self.metrics.connector_start_errors_total.add(
                1,
                &[
                    opentelemetry::KeyValue::new("connector", self.connector_name.clone()),
                    opentelemetry::KeyValue::new("stream", self.stream_name.clone()),
                ],
            );
        })?;
        if self.s.auto_create_table {
            let sql = match &self.schema_cols {
                Some(cols) => {
                    let pk: Vec<&str> = self.s.upsert_keys.iter().map(|s| s.as_str()).collect();
                    self.dialect
                        .create_table_typed_sql(&self.s.table, cols, &pk)
                }
                None => self.dialect.create_table_blob_sql(&self.s.table),
            };
            backend.execute_ddl(&sql).await.map_err(|e| {
                errors::to_connector_error(self.kind, &e, &format!("auto_create_table ({sql})"))
            })?;
        }
        info!(
            table = %self.s.table,
            mode = ?self.s.mode,
            schema_mode = if self.schema_cols.is_some() { "typed" } else { "blob" },
            "jdbc sink started"
        );
        self.backend = Some(backend);
        Ok(())
    }

    async fn write(&mut self, records: &[SinkRecord]) -> Result<WriteResult, ConnectorError> {
        let backend = self
            .backend
            .as_deref()
            .ok_or_else(|| ConnectorError::connection("jdbc sink: not started"))?;

        // Bind up to the first poison record; write those rows first so
        // `Poison { index }` means "everything before index is written".
        let mut rows = Vec::with_capacity(records.len());
        let mut poison: Option<(usize, PoisonReason)> = None;
        for (i, r) in records.iter().enumerate() {
            match self.bind(i, r) {
                Ok(b) => rows.push(b),
                Err(reason) => {
                    warn!(
                        offset = r.offset,
                        reason = reason.label(),
                        "jdbc sink: poison record"
                    );
                    self.record_skip(reason.label());
                    poison = Some((i, reason));
                    break;
                }
            }
        }
        match self.write_rows(backend, &rows).await {
            StepOutcome::Done => {}
            StepOutcome::Poison { index, reason } => {
                return Ok(WriteResult::Poison { index, reason })
            }
            StepOutcome::Failed { accepted, error } => {
                return Ok(WriteResult::Failed { accepted, error })
            }
        }
        Ok(match poison {
            Some((index, reason)) => WriteResult::Poison { index, reason },
            None => WriteResult::Accepted,
        })
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        if let Some(b) = self.backend.take() {
            b.close().await;
        }
        Ok(())
    }

    fn default_flush_interval(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectorConfig, ConnectorType};
    use bytes::Bytes;
    use serde_json::json;

    fn init(settings: serde_json::Value) -> PluginInit {
        let (m, _) = exspeed_common::Metrics::new();
        PluginInit {
            config: ConnectorConfig::new("j", ConnectorType::Sink, "jdbc", "events"),
            settings: settings.as_object().unwrap().clone(),
            metrics: Arc::new(m),
        }
    }

    fn rec(offset: u64, v: serde_json::Value) -> SinkRecord {
        SinkRecord {
            offset,
            timestamp: 0,
            subject: "s".into(),
            key: None,
            value: Bytes::from(v.to_string()),
            headers: vec![],
        }
    }

    #[test]
    fn config_validation() {
        assert!(JdbcSinkConnector::new(&init(json!({}))).is_err());
        assert!(JdbcSinkConnector::new(&init(json!({"connection": "postgres://h/d"}))).is_err());
        let c = JdbcSinkConnector::new(&init(
            json!({"connection": "postgres://h/d", "table": "events"}),
        ))
        .unwrap();
        assert_eq!(c.s.mode, SinkMode::Upsert);
        assert!(
            JdbcSinkConnector::new(&init(json!({"connection": "oracle://h/d", "table": "t"})))
                .is_err()
        );
        assert!(JdbcSinkConnector::new(&init(
            json!({"connection": "postgres://h/d", "table": "a; DROP"})
        ))
        .is_err());
        assert!(
            JdbcSinkConnector::new(&init(json!({
                "connection": "postgres://h/d", "table": "t", "schema": "id:bigint"
            })))
            .is_err(),
            "typed upsert needs upsert_keys"
        );
        assert!(JdbcSinkConnector::new(&init(json!({
            "connection": "postgres://h/d", "table": "t", "schema": "id:bigint", "upsert_keys": "nope"
        })))
        .is_err());
        assert!(JdbcSinkConnector::new(&init(json!({
            "connection": "postgres://h/d", "table": "t", "mode": "insert", "schema": "id:bigint"
        })))
        .is_ok());
        assert!(
            JdbcSinkConnector::new(&init(json!({
                "connection": "postgres://h/d", "table": "t", "auto_create": true
            })))
            .is_err(),
            "misspelled key"
        );
        let c = JdbcSinkConnector::new(&init(json!({
            "connection": "mysql://h/d", "table": "t", "mode": "insert",
            "upsert_keys": ["id", "tenant"], "auto_create_table": true
        })))
        .unwrap();
        assert_eq!(c.s.upsert_keys, vec!["id", "tenant"]);
    }

    #[test]
    fn rows_per_statement_respects_param_limits() {
        let c = JdbcSinkConnector::new(&init(json!({
            "connection": "mssql://h/d", "table": "t", "max_rows_per_statement": 10000
        })))
        .unwrap();
        assert_eq!(c.rows_per_statement(), 2000 / 4);
    }

    async fn sqlite_sink(dir: &tempfile::TempDir, extra: serde_json::Value) -> JdbcSinkConnector {
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("t.db").display());
        let mut settings = json!({
            "connection": url, "table": "items", "auto_create_table": true,
            "schema": "id:bigint, name:text", "upsert_keys": "id", "max_rows_per_statement": 3
        });
        for (k, v) in extra.as_object().unwrap() {
            settings[k] = v.clone();
        }
        let mut s = JdbcSinkConnector::new(&init(settings)).unwrap();
        s.start().await.unwrap();
        s
    }

    async fn rows(dir: &tempfile::TempDir) -> Vec<(i64, String)> {
        let url = format!("sqlite://{}", dir.path().join("t.db").display());
        let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
        sqlx::query_as("SELECT id, name FROM items ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn multi_row_upsert_dedups_keys_within_a_statement() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = sqlite_sink(&dir, json!({})).await;
        let batch: Vec<_> = (0..7)
            .map(|i| rec(i, json!({"id": i % 4, "name": format!("v{i}")})))
            .collect();
        assert!(matches!(
            s.write(&batch).await.unwrap(),
            WriteResult::Accepted
        ));
        // Replay is idempotent.
        assert!(matches!(
            s.write(&batch).await.unwrap(),
            WriteResult::Accepted
        ));
        assert_eq!(
            rows(&dir).await,
            vec![
                (0, "v4".into()),
                (1, "v5".into()),
                (2, "v6".into()),
                (3, "v3".into())
            ]
        );
    }

    #[tokio::test]
    async fn poison_rows_are_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = sqlite_sink(&dir, json!({})).await;
        let batch = vec![
            rec(0, json!({"id": 1, "name": "a"})),
            rec(1, json!({"id": "not-a-number", "name": "b"})),
            rec(2, json!({"id": 3, "name": "c"})),
        ];
        match s.write(&batch).await.unwrap() {
            WriteResult::Poison { index, reason } => {
                assert_eq!(index, 1);
                assert_eq!(reason.label(), "type_mismatch");
            }
            other => panic!("expected poison, got {other:?}"),
        }
        assert_eq!(
            rows(&dir).await,
            vec![(1, "a".into())],
            "rows before the poison are written"
        );
        assert!(matches!(
            s.write(&batch[2..]).await.unwrap(),
            WriteResult::Accepted
        ));
        assert_eq!(rows(&dir).await.len(), 2);
    }

    #[tokio::test]
    async fn constraint_violation_in_multi_row_insert_is_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = sqlite_sink(
            &dir,
            json!({"mode": "insert", "schema": "id:bigint, name:text?"}),
        )
        .await;
        // Make `name` NOT NULL at the database level only.
        let url = format!("sqlite://{}", dir.path().join("t.db").display());
        let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("DROP TABLE items")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
            .execute(&pool)
            .await
            .unwrap();
        let batch = vec![
            rec(0, json!({"id": 1, "name": "a"})),
            rec(1, json!({"id": 2})),
            rec(2, json!({"id": 3, "name": "c"})),
        ];
        match s.write(&batch).await.unwrap() {
            WriteResult::Poison { index, .. } => assert_eq!(index, 1),
            other => panic!("expected poison, got {other:?}"),
        }
        // Insert mode: replaying an existing row is not an error.
        let replay = vec![
            rec(0, json!({"id": 1, "name": "a"})),
            rec(2, json!({"id": 3, "name": "c"})),
        ];
        assert!(matches!(
            s.write(&replay).await.unwrap(),
            WriteResult::Accepted
        ));
        assert_eq!(rows(&dir).await, vec![(1, "a".into()), (3, "c".into())]);
    }

    #[tokio::test]
    async fn missing_table_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = sqlite_sink(&dir, json!({"auto_create_table": false, "table": "nope"})).await;
        match s
            .write(&[rec(0, json!({"id": 1, "name": "a"}))])
            .await
            .unwrap()
        {
            WriteResult::Failed { accepted: 0, error } => {
                assert_eq!(error.kind(), crate::traits::ErrorKind::Fatal, "{error}")
            }
            other => panic!("expected failure, got {other:?}"),
        }
    }
}
