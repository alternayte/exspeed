pub mod backend;
pub(super) mod sqlx_backend;
pub(super) mod tiberius_backend;
pub mod dialect;
pub mod postgres;
pub mod mysql;
pub mod mssql;
pub mod sqlite;
pub mod schema;

use async_trait::async_trait;
use tracing::{error, info, warn};

use crate::config::ConnectorConfig;
use crate::traits::{
    ConnectorError, HealthStatus, PoisonReason, SinkBatch, SinkConnector, SinkRecord, WriteResult,
};

use self::backend::{BackendError, Param, SinkBackend};
use self::dialect::{dialect_for, ColumnSpec, Dialect, DialectKind};
use self::schema::{is_valid_ident, parse_schema};
use self::sqlx_backend::SqlxBackend;

/// Outcome of one sink step (one record, DDL, etc.) before it is turned
/// into a `WriteResult` by the main `write` loop.
enum StepError {
    /// Record is fundamentally unfit for this sink — route to DLQ.
    Poison { reason: PoisonReason },
    /// Transient failure — whole batch retry is appropriate.
    Transient { msg: String },
    /// Duplicate primary key on upsert — treat as success and advance past.
    DuplicateKeyIgnored,
}

/// Classification of a backend error for DLQ/retry routing.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ErrClass {
    DuplicateKey,
    Poison { detail: String },
    Transient,
}

fn classify_backend_err(err: &BackendError) -> ErrClass {
    match err {
        BackendError::Pool(_) | BackendError::Io(_) => ErrClass::Transient,
        BackendError::Sql { sqlstate, message } => match sqlstate.as_str() {
            // PK violation: Postgres 23505, MySQL 1062, MSSQL 2627.
            "23505" | "1062" | "2627" => ErrClass::DuplicateKey,
            // NOT NULL violation.
            "23502" => ErrClass::Poison { detail: format!("not-null violation: {message}") },
            // Numeric value out of range.
            "22003" => ErrClass::Poison { detail: format!("numeric overflow: {message}") },
            // Invalid text representation (e.g. bad timestamp parsed by DB).
            "22P02" => ErrClass::Poison { detail: format!("invalid text: {message}") },
            // Anything else — unknown, default to transient.
            _ => ErrClass::Transient,
        },
    }
}

fn step_error_from_backend(e: &BackendError) -> StepError {
    match classify_backend_err(e) {
        ErrClass::DuplicateKey => StepError::DuplicateKeyIgnored,
        ErrClass::Poison { detail } =>
            StepError::Poison { reason: PoisonReason::SinkRejected { detail } },
        ErrClass::Transient => StepError::Transient { msg: format!("execute failed: {e}") },
    }
}

pub struct JdbcSinkConnector {
    connection_string: String,
    table: String,
    mode: String,
    upsert_keys: Vec<String>,
    auto_create_table: bool,
    schema_cols: Option<Vec<ColumnSpec>>,
    dialect: Box<dyn Dialect>,
    kind: DialectKind,
    backend: Option<Box<dyn SinkBackend>>,
    connector_name: String,
    stream_name: String,
    metrics: Option<std::sync::Arc<exspeed_common::Metrics>>,
}

impl JdbcSinkConnector {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let connection_string = config
            .setting("connection")
            .map_err(ConnectorError::Config)?
            .to_string();

        let table = config
            .setting("table")
            .map_err(ConnectorError::Config)?
            .to_string();

        if !is_valid_ident(&table) {
            return Err(ConnectorError::Config(format!(
                "jdbc sink: invalid table name '{table}' (must match [A-Za-z_][A-Za-z0-9_]*)"
            )));
        }

        let mode = config.setting_or("mode", "upsert");

        let upsert_keys_str = config.setting_or("upsert_keys", "");
        let upsert_keys: Vec<String> = if upsert_keys_str.is_empty() {
            Vec::new()
        } else {
            upsert_keys_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        };
        for k in &upsert_keys {
            if !is_valid_ident(k) {
                return Err(ConnectorError::Config(format!(
                    "jdbc sink: invalid upsert_keys entry '{k}'"
                )));
            }
        }

        let auto_create_table = config
            .setting_or("auto_create_table", "false")
            .eq_ignore_ascii_case("true");

        let schema_raw = config.setting_or("schema", "");
        let schema_cols = if schema_raw.trim().is_empty() {
            None
        } else {
            Some(parse_schema(&schema_raw).map_err(|e| {
                ConnectorError::Config(format!("jdbc sink: schema DSL error: {e}"))
            })?)
        };

        if schema_cols.is_some() && mode == "upsert" && upsert_keys.is_empty() {
            return Err(ConnectorError::Config(
                "jdbc sink: upsert_keys must be set when mode=upsert and schema is declared".into(),
            ));
        }

        if let Some(cols) = &schema_cols {
            for k in &upsert_keys {
                if !cols.iter().any(|c| &c.name == k) {
                    return Err(ConnectorError::Config(format!(
                        "jdbc sink: upsert_keys entry '{k}' is not in declared schema"
                    )));
                }
            }
        }

        let kind = DialectKind::from_url(&connection_string)?;
        let dialect = dialect_for(kind);

        Ok(Self {
            connection_string,
            table,
            mode,
            upsert_keys,
            auto_create_table,
            schema_cols,
            dialect,
            kind,
            backend: None,
            connector_name: config.name.clone(),
            stream_name: config.stream.clone(),
            metrics: None,
        })
    }

    pub fn with_metrics(mut self, m: std::sync::Arc<exspeed_common::Metrics>) -> Self {
        self.metrics = Some(m);
        self
    }

    fn record_skip(&self, reason: &'static str) {
        if let Some(m) = &self.metrics {
            m.connector_records_skipped_total.add(
                1,
                &[
                    opentelemetry::KeyValue::new("connector", self.connector_name.clone()),
                    opentelemetry::KeyValue::new("stream", self.stream_name.clone()),
                    opentelemetry::KeyValue::new("reason", reason),
                ],
            );
        }
    }

    fn record_write_error(&self, sqlstate: &str) {
        if let Some(m) = &self.metrics {
            m.connector_write_errors_total.add(
                1,
                &[
                    opentelemetry::KeyValue::new("connector", self.connector_name.clone()),
                    opentelemetry::KeyValue::new("stream", self.stream_name.clone()),
                    opentelemetry::KeyValue::new("sqlstate", sqlstate.to_string()),
                ],
            );
        }
    }

    fn record_start_error(&self) {
        if let Some(m) = &self.metrics {
            m.connector_start_errors_total.add(
                1,
                &[
                    opentelemetry::KeyValue::new("connector", self.connector_name.clone()),
                    opentelemetry::KeyValue::new("stream", self.stream_name.clone()),
                ],
            );
        }
    }

    async fn write_blob(
        &self,
        backend: &dyn SinkBackend,
        record: &SinkRecord,
        json: &serde_json::Value,
    ) -> Result<(), StepError> {
        let cols = ["offset", "subject", "key", "value"];
        let sql = if self.mode == "upsert" {
            self.dialect.upsert_sql(&self.table, &cols, &["offset"])
        } else {
            self.dialect.insert_sql(&self.table, &cols)
        };

        let subject_param = if record.subject.is_empty() {
            Param::Null
        } else {
            Param::Text(record.subject.clone())
        };
        let key_param = match &record.key {
            Some(b) => Param::Text(String::from_utf8_lossy(b).into_owned()),
            None => Param::Null,
        };
        let value_param = Param::JsonText(
            serde_json::to_string(json).unwrap_or_else(|_| "null".to_string()),
        );

        let params = vec![
            Param::I64(record.offset as i64),
            subject_param,
            key_param,
            value_param,
        ];

        match backend.execute_row(&sql, &params).await {
            Ok(()) => Ok(()),
            Err(e) => Err(step_error_from_backend(&e)),
        }
    }

    async fn write_typed(
        &self,
        backend: &dyn SinkBackend,
        record: &SinkRecord,
        json: &serde_json::Value,
        cols: &[ColumnSpec],
    ) -> Result<(), StepError> {
        use self::schema::{bind_json_as_type, BindError};

        let obj = match json.as_object() {
            Some(o) => o,
            None => return Err(StepError::Poison { reason: PoisonReason::NonJsonRecord }),
        };

        for c in cols {
            if !c.nullable && !obj.contains_key(&c.name) {
                warn!(offset = record.offset, field = %c.name, "jdbc sink: missing required field");
                return Err(StepError::Poison {
                    reason: PoisonReason::MissingRequiredField { field: c.name.clone() },
                });
            }
        }

        let col_names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        let sql = if self.mode == "upsert" {
            let key_refs: Vec<&str> = self.upsert_keys.iter().map(|s| s.as_str()).collect();
            self.dialect.upsert_sql(&self.table, &col_names, &key_refs)
        } else {
            self.dialect.insert_sql(&self.table, &col_names)
        };

        let mut params: Vec<Param> = Vec::with_capacity(cols.len());
        for c in cols {
            let value = obj.get(&c.name);
            match bind_json_as_type(c, value) {
                Ok(p) => params.push(p),
                Err(e) => {
                    let reason = match e {
                        BindError::TypeMismatch { field, expected, got } => {
                            warn!(offset = record.offset, %field, %expected, %got,
                                  "jdbc sink: type mismatch");
                            PoisonReason::TypeMismatch {
                                field,
                                expected: expected.to_string(),
                                got: got.to_string(),
                            }
                        }
                        BindError::TimestampParse { field, .. } => {
                            warn!(offset = record.offset, %field,
                                  "jdbc sink: unparseable timestamp");
                            PoisonReason::TimestampParseFailed { field }
                        }
                        BindError::MissingRequired { field } => {
                            warn!(offset = record.offset, %field, "jdbc sink: missing required");
                            PoisonReason::MissingRequiredField { field }
                        }
                    };
                    return Err(StepError::Poison { reason });
                }
            }
        }

        match backend.execute_row(&sql, &params).await {
            Ok(()) => Ok(()),
            Err(e) => Err(step_error_from_backend(&e)),
        }
    }

    async fn build_backend(&self) -> Result<Box<dyn SinkBackend>, ConnectorError> {
        match self.kind {
            DialectKind::Postgres | DialectKind::MySql | DialectKind::Sqlite => {
                let b = SqlxBackend::connect(&self.connection_string)
                    .await
                    .map_err(|e| ConnectorError::Connection(format!("jdbc sink: {e}")))?;
                Ok(Box::new(b))
            }
            DialectKind::Mssql => {
                let b = self::tiberius_backend::TiberiusBackend::connect(&self.connection_string)
                    .await
                    .map_err(|e| ConnectorError::Connection(format!("jdbc sink: {e}")))?;
                Ok(Box::new(b))
            }
        }
    }
}

#[async_trait]
impl SinkConnector for JdbcSinkConnector {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        let backend = match self.build_backend().await {
            Ok(b) => b,
            Err(e) => {
                self.record_start_error();
                return Err(e);
            }
        };

        if self.auto_create_table {
            let sql = match &self.schema_cols {
                Some(cols) => {
                    let pk_refs: Vec<&str> = self.upsert_keys.iter().map(|s| s.as_str()).collect();
                    self.dialect
                        .create_table_typed_sql(&self.table, cols, &pk_refs)
                }
                None => self.dialect.create_table_blob_sql(&self.table),
            };
            if let Err(e) = backend.execute_ddl(&sql).await {
                self.record_start_error();
                return Err(ConnectorError::Connection(format!(
                    "jdbc sink: auto_create_table failed: {e}\nSQL: {sql}"
                )));
            }
        }

        info!(
            table = %self.table,
            mode = %self.mode,
            auto_create_table = self.auto_create_table,
            schema_mode = if self.schema_cols.is_some() { "typed" } else { "blob" },
            columns = self.schema_cols.as_ref().map(|c| c.len()).unwrap_or(0),
            "jdbc sink started"
        );

        self.backend = Some(backend);
        Ok(())
    }

    async fn write(&mut self, batch: SinkBatch) -> Result<WriteResult, ConnectorError> {
        let backend = match self.backend.as_deref() {
            Some(b) => b,
            None => {
                return Ok(WriteResult::TransientFailure {
                    last_successful_offset: None,
                    error: "jdbc sink: backend not initialised — call start() first".into(),
                });
            }
        };

        let mut last_successful: Option<u64> = None;

        for record in &batch.records {
            let json: serde_json::Value = match serde_json::from_slice(&record.value) {
                Ok(v) => v,
                Err(_) => {
                    warn!(offset = record.offset, "jdbc sink: non-JSON record");
                    return Ok(WriteResult::Poison {
                        last_successful_offset: last_successful,
                        poison_offset: record.offset,
                        reason: PoisonReason::NonJsonRecord,
                        record: record.clone(),
                    });
                }
            };

            let step_result = if let Some(cols) = self.schema_cols.clone() {
                self.write_typed(backend, record, &json, &cols).await
            } else {
                self.write_blob(backend, record, &json).await
            };

            match step_result {
                Ok(()) | Err(StepError::DuplicateKeyIgnored) => {
                    last_successful = Some(record.offset);
                }
                Err(StepError::Poison { reason }) => {
                    self.record_skip(reason.label());
                    return Ok(WriteResult::Poison {
                        last_successful_offset: last_successful,
                        poison_offset: record.offset,
                        reason,
                        record: record.clone(),
                    });
                }
                Err(StepError::Transient { msg }) => {
                    error!(offset = record.offset, "jdbc sink: {msg}");
                    self.record_write_error("");
                    return Ok(WriteResult::TransientFailure {
                        last_successful_offset: last_successful,
                        error: msg,
                    });
                }
            }
        }

        Ok(WriteResult::AllSuccess)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        if let Some(backend) = self.backend.take() {
            backend.close().await;
        }
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        if self.backend.is_some() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy("jdbc sink: backend not initialised".into())
        }
    }
}

#[cfg(test)]
mod classify_tests {
    use super::*;

    fn sql(code: &str) -> BackendError {
        BackendError::Sql { sqlstate: code.into(), message: "msg".into() }
    }

    #[test]
    fn pg_pk_violation_is_duplicate() {
        assert_eq!(classify_backend_err(&sql("23505")), ErrClass::DuplicateKey);
    }
    #[test]
    fn mysql_pk_violation_is_duplicate() {
        assert_eq!(classify_backend_err(&sql("1062")), ErrClass::DuplicateKey);
    }
    #[test]
    fn mssql_pk_violation_is_duplicate() {
        assert_eq!(classify_backend_err(&sql("2627")), ErrClass::DuplicateKey);
    }
    #[test]
    fn not_null_is_poison() {
        assert!(matches!(classify_backend_err(&sql("23502")), ErrClass::Poison { .. }));
    }
    #[test]
    fn numeric_overflow_is_poison() {
        assert!(matches!(classify_backend_err(&sql("22003")), ErrClass::Poison { .. }));
    }
    #[test]
    fn invalid_text_is_poison() {
        assert!(matches!(classify_backend_err(&sql("22P02")), ErrClass::Poison { .. }));
    }
    #[test]
    fn unknown_code_is_transient() {
        assert_eq!(classify_backend_err(&sql("42601")), ErrClass::Transient);
    }
    #[test]
    fn pool_err_is_transient() {
        assert_eq!(
            classify_backend_err(&BackendError::Pool("boom".into())),
            ErrClass::Transient
        );
    }
    #[test]
    fn io_err_is_transient() {
        assert_eq!(
            classify_backend_err(&BackendError::Io("boom".into())),
            ErrClass::Transient
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_config(settings: HashMap<String, String>) -> ConnectorConfig {
        ConnectorConfig {
            name: "test-jdbc".into(),
            connector_type: "sink".into(),
            plugin: "jdbc".into(),
            stream: "events".into(),
            subject_template: "".into(),
            subject_filter: "".into(),
            settings,
            batch_size: 100,
            poll_interval_ms: 50,
            dedup_enabled: true,
            dedup_key: String::new(),
            dedup_window_secs: 86400,
            transform_sql: String::new(),
            on_transient_exhausted: crate::config::OnTransientExhausted::default(),
            retry: crate::retry::RetryPolicy::default_transient(),
        }
    }

    #[test]
    fn config_required_fields() {
        assert!(JdbcSinkConnector::new(&make_config(HashMap::new())).is_err());

        let cfg = make_config(HashMap::from([(
            "connection".into(),
            "postgres://localhost/test".into(),
        )]));
        assert!(JdbcSinkConnector::new(&cfg).is_err());

        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/test".into()),
            ("table".into(), "events".into()),
        ]));
        let c = JdbcSinkConnector::new(&cfg).unwrap();
        assert_eq!(c.table, "events");
        assert_eq!(c.mode, "upsert");
        assert!(!c.auto_create_table);
        assert!(c.upsert_keys.is_empty());
        assert!(c.schema_cols.is_none());
    }

    #[test]
    fn config_rejects_unsupported_scheme() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "oracle://u:p@h/db".into()),
            ("table".into(), "events".into()),
        ]));
        assert!(JdbcSinkConnector::new(&cfg).is_err());
    }

    #[test]
    fn config_rejects_table_name_injection() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/test".into()),
            ("table".into(), "foo; DROP TABLE users".into()),
        ]));
        assert!(JdbcSinkConnector::new(&cfg).is_err());
    }

    #[test]
    fn config_typed_upsert_requires_upsert_keys() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/test".into()),
            ("table".into(), "events".into()),
            ("mode".into(), "upsert".into()),
            ("schema".into(), "id:bigint, email:text".into()),
        ]));
        match JdbcSinkConnector::new(&cfg) {
            Err(ConnectorError::Config(msg)) => assert!(msg.contains("upsert_keys")),
            Err(e) => panic!("expected Config error, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn config_typed_upsert_keys_must_be_in_schema() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/test".into()),
            ("table".into(), "events".into()),
            ("mode".into(), "upsert".into()),
            ("upsert_keys".into(), "missing_col".into()),
            ("schema".into(), "id:bigint, email:text".into()),
        ]));
        assert!(JdbcSinkConnector::new(&cfg).is_err());
    }

    #[test]
    fn config_typed_mode_insert_without_upsert_keys_is_ok() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/test".into()),
            ("table".into(), "events".into()),
            ("mode".into(), "insert".into()),
            ("schema".into(), "id:bigint, email:text".into()),
        ]));
        let c = JdbcSinkConnector::new(&cfg).unwrap();
        assert_eq!(c.schema_cols.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn config_optional_fields_roundtrip() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "mysql://localhost/test".into()),
            ("table".into(), "orders".into()),
            ("mode".into(), "insert".into()),
            ("upsert_keys".into(), "id, tenant_id".into()),
            ("auto_create_table".into(), "true".into()),
        ]));
        let c = JdbcSinkConnector::new(&cfg).unwrap();
        assert_eq!(c.mode, "insert");
        assert_eq!(c.upsert_keys, vec!["id", "tenant_id"]);
        assert!(c.auto_create_table);
    }
}
