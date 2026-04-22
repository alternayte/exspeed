pub mod backend;
pub mod dialect;
pub mod postgres;
pub mod mysql;
pub mod schema;

use async_trait::async_trait;
use tracing::{error, info, warn};

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SinkBatch, SinkConnector, SinkRecord, WriteResult};

use self::dialect::{dialect_for, ColumnSpec, Dialect, DialectKind};
use self::schema::{is_valid_ident, parse_schema};

enum WriteStepError {
    Skip { reason: &'static str },
    Halt { msg: String },
}

pub struct JdbcSinkConnector {
    connection_string: String,
    table: String,
    mode: String,
    upsert_keys: Vec<String>,
    auto_create_table: bool,
    schema_cols: Option<Vec<ColumnSpec>>,
    dialect: Box<dyn Dialect>,
    pool: Option<sqlx::AnyPool>,
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
            pool: None,
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

    fn record_write_error(&self) {
        if let Some(m) = &self.metrics {
            m.connector_write_errors_total.add(
                1,
                &[
                    opentelemetry::KeyValue::new("connector", self.connector_name.clone()),
                    opentelemetry::KeyValue::new("stream", self.stream_name.clone()),
                    opentelemetry::KeyValue::new("sqlstate", ""),
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
        pool: &sqlx::AnyPool,
        record: &SinkRecord,
        json: &serde_json::Value,
    ) -> Result<(), WriteStepError> {
        let cols = ["offset", "subject", "key", "value"];
        let sql = if self.mode == "upsert" {
            self.dialect.upsert_sql(&self.table, &cols, &["offset"])
        } else {
            self.dialect.insert_sql(&self.table, &cols)
        };

        let subject_opt = if record.subject.is_empty() { None } else { Some(record.subject.clone()) };
        let key_opt = record.key.as_ref().map(|b| String::from_utf8_lossy(b).into_owned());
        let value_str = serde_json::to_string(json)
            .unwrap_or_else(|_| "null".to_string());

        let q = sqlx::query(&sql)
            .bind(record.offset as i64)
            .bind(subject_opt)
            .bind(key_opt)
            .bind(value_str);

        q.execute(pool).await.map_err(|e| WriteStepError::Halt {
            msg: format!("execute failed: {e}"),
        })?;
        Ok(())
    }

    async fn write_typed(
        &self,
        pool: &sqlx::AnyPool,
        record: &SinkRecord,
        json: &serde_json::Value,
        cols: &[ColumnSpec],
    ) -> Result<(), WriteStepError> {
        use self::schema::{bind_json_as_type, BindError};

        let obj = match json.as_object() {
            Some(o) => o,
            None => return Err(WriteStepError::Skip { reason: "non_object_json" }),
        };

        for c in cols {
            if !c.nullable && !obj.contains_key(&c.name) {
                warn!(offset = record.offset, field = %c.name, "jdbc sink: missing required field");
                return Err(WriteStepError::Skip { reason: "missing_required_field" });
            }
        }

        let col_names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        let sql = if self.mode == "upsert" {
            let key_refs: Vec<&str> = self.upsert_keys.iter().map(|s| s.as_str()).collect();
            self.dialect.upsert_sql(&self.table, &col_names, &key_refs)
        } else {
            self.dialect.insert_sql(&self.table, &col_names)
        };

        let mut q = sqlx::query(&sql);
        for c in cols {
            let value = obj.get(&c.name);
            match bind_json_as_type(q, c, value) {
                Ok(next) => q = next,
                Err(e) => {
                    match &e {
                        BindError::TypeMismatch { field, expected, got } => {
                            warn!(offset = record.offset, %field, %expected, %got, "jdbc sink: type mismatch");
                            return Err(WriteStepError::Skip { reason: "type_mismatch" });
                        }
                        BindError::TimestampParse { field, .. } => {
                            warn!(offset = record.offset, %field, "jdbc sink: unparseable timestamp");
                            return Err(WriteStepError::Skip { reason: "timestamp_parse" });
                        }
                        BindError::MissingRequired { field } => {
                            warn!(offset = record.offset, %field, "jdbc sink: missing required");
                            return Err(WriteStepError::Skip { reason: "missing_required_field" });
                        }
                    }
                }
            }
        }

        q.execute(pool).await.map_err(|e| WriteStepError::Halt {
            msg: format!("execute failed: {e}"),
        })?;
        Ok(())
    }
}

#[async_trait]
impl SinkConnector for JdbcSinkConnector {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        sqlx::any::install_default_drivers();

        let pool = match sqlx::AnyPool::connect(&self.connection_string).await {
            Ok(p) => p,
            Err(e) => {
                self.record_start_error();
                return Err(ConnectorError::Connection(format!(
                    "jdbc sink: failed to connect: {e}"
                )));
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
            if let Err(e) = sqlx::query(&sql).execute(&pool).await {
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

        self.pool = Some(pool);
        Ok(())
    }

    async fn write(&mut self, batch: SinkBatch) -> Result<WriteResult, ConnectorError> {
        let pool = match &self.pool {
            Some(p) => p,
            None => {
                return Ok(WriteResult::AllFailed(
                    "jdbc sink: pool not initialised — call start() first".into(),
                ))
            }
        };

        let mut last_successful: Option<u64> = None;
        let mut any_success = false;
        let mut first_fail: Option<String> = None;

        'records: for record in &batch.records {
            let json: serde_json::Value = match serde_json::from_slice(&record.value) {
                Ok(v) => v,
                Err(_) => {
                    warn!(offset = record.offset, reason = "non_json", "jdbc sink: skipping record");
                    self.record_skip("non_json_object");
                    last_successful = Some(record.offset);
                    any_success = true;
                    continue 'records;
                }
            };

            let result = if let Some(cols) = self.schema_cols.clone() {
                self.write_typed(pool, record, &json, &cols).await
            } else {
                self.write_blob(pool, record, &json).await
            };

            match result {
                Ok(()) => {
                    last_successful = Some(record.offset);
                    any_success = true;
                }
                Err(WriteStepError::Skip { reason }) => {
                    warn!(offset = record.offset, %reason, "jdbc sink: skipping record");
                    self.record_skip(reason);
                    last_successful = Some(record.offset);
                    any_success = true;
                }
                Err(WriteStepError::Halt { msg }) => {
                    error!(offset = record.offset, "jdbc sink: {msg}");
                    self.record_write_error();
                    first_fail = Some(msg);
                    break 'records;
                }
            }
        }

        if first_fail.is_some() {
            if let Some(offset) = last_successful {
                Ok(WriteResult::PartialSuccess { last_successful_offset: offset })
            } else {
                Ok(WriteResult::AllFailed(first_fail.unwrap_or_default()))
            }
        } else if any_success || batch.records.is_empty() {
            Ok(WriteResult::AllSuccess)
        } else {
            Ok(WriteResult::AllFailed("no records processed".into()))
        }
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        if self.pool.is_some() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy("jdbc sink: pool not initialised".into())
        }
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
            ("connection".into(), "sqlite:///tmp/test.db".into()),
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
