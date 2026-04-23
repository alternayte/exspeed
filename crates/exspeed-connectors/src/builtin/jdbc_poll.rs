//! Generic JDBC polling source.
//!
//! Periodically polls a relational table for rows whose `tracking_column`
//! exceeds the last-seen value, emitting each row as a JSON record.
//!
//! Backed by `sqlx::AnyPool` — supports Postgres, MySQL, and SQLite. For
//! SQL Server, prefer a separate CDC-based source (uses tiberius).
//!
//! Required config (`settings`):
//! - `connection`: the DB URL (dialect inferred from scheme).
//! - `table`: the table to poll (must match `[A-Za-z_][A-Za-z0-9_]*`).
//! - `tracking_column`: integer column to use as monotonic cursor.
//!   Typically an auto-increment `id` or a `bigint` epoch/microsecond.
//! - `schema`: column-type DSL (same format as the JDBC sink) describing
//!   the columns to SELECT and decode into JSON.
//!
//! Offset: the last-seen `tracking_column` value, stored as a decimal
//! string via the standard `OffsetStore`. Restart resumes from that value.

use async_trait::async_trait;
use bytes::Bytes;
use sqlx::{any::AnyRow, AnyPool, Row};
use tracing::{info, warn};

use crate::builtin::jdbc::dialect::{ColumnSpec, DialectKind, JsonType};
use crate::builtin::jdbc::schema::{is_valid_ident, parse_schema};
use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

pub struct JdbcPollSource {
    connection_string: String,
    table: String,
    tracking_column: String,
    schema_cols: Vec<ColumnSpec>,
    pool: Option<AnyPool>,
    last_value: i64,
    kind: DialectKind,
    subject_template: String,
}

impl JdbcPollSource {
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
                "jdbc_poll: invalid table name '{table}'"
            )));
        }

        let tracking_column = config
            .setting("tracking_column")
            .map_err(ConnectorError::Config)?
            .to_string();
        if !is_valid_ident(&tracking_column) {
            return Err(ConnectorError::Config(format!(
                "jdbc_poll: invalid tracking_column '{tracking_column}'"
            )));
        }

        let schema_raw = config.setting_or("schema", "");
        if schema_raw.trim().is_empty() {
            return Err(ConnectorError::Config(
                "jdbc_poll: 'schema' setting is required — declares columns to SELECT".into(),
            ));
        }
        let schema_cols = parse_schema(&schema_raw).map_err(|e| {
            ConnectorError::Config(format!("jdbc_poll: schema DSL error: {e}"))
        })?;

        if !schema_cols.iter().any(|c| c.name == tracking_column) {
            return Err(ConnectorError::Config(format!(
                "jdbc_poll: tracking_column '{tracking_column}' must be present in the schema"
            )));
        }

        let kind = DialectKind::from_url(&connection_string)
            .map_err(|e| ConnectorError::Config(format!("jdbc_poll: {e:?}")))?;

        // MSSQL uses a different driver (tiberius); polling via sqlx::AnyPool
        // won't work. Reject clearly rather than fail at connect time.
        if matches!(kind, DialectKind::Mssql) {
            return Err(ConnectorError::Config(
                "jdbc_poll: SQL Server not supported — use a dedicated MSSQL CDC source".into(),
            ));
        }

        Ok(Self {
            connection_string,
            table,
            tracking_column,
            schema_cols,
            pool: None,
            last_value: 0,
            kind,
            subject_template: config.subject_template.clone(),
        })
    }

    fn build_select_sql(&self, batch_size: usize) -> String {
        let cols_sql: Vec<String> = self
            .schema_cols
            .iter()
            .map(|c| quote_ident(self.kind, &c.name))
            .collect();
        let table_q = quote_ident(self.kind, &self.table);
        let tc_q = quote_ident(self.kind, &self.tracking_column);
        let placeholder = match self.kind {
            DialectKind::Postgres => "$1".to_string(),
            DialectKind::MySql | DialectKind::Sqlite => "?".to_string(),
            DialectKind::Mssql => unreachable!("MSSQL rejected in new()"),
        };
        format!(
            "SELECT {} FROM {} WHERE {} > {} ORDER BY {} ASC LIMIT {}",
            cols_sql.join(", "),
            table_q,
            tc_q,
            placeholder,
            tc_q,
            batch_size.max(1),
        )
    }
}

fn quote_ident(kind: DialectKind, name: &str) -> String {
    match kind {
        DialectKind::Postgres | DialectKind::Sqlite => format!("\"{name}\""),
        DialectKind::MySql => format!("`{name}`"),
        DialectKind::Mssql => format!("[{name}]"),
    }
}

/// Decode a single row column into a `serde_json::Value` based on the
/// declared `JsonType`. Errors/missing decode as `Value::Null`.
fn decode_cell(row: &AnyRow, idx: usize, ty: JsonType) -> serde_json::Value {
    use serde_json::Value;
    match ty {
        JsonType::Bigint => row
            .try_get::<Option<i64>, _>(idx)
            .ok()
            .flatten()
            .map(|v| Value::Number(v.into()))
            .unwrap_or(Value::Null),
        JsonType::Double => row
            .try_get::<Option<f64>, _>(idx)
            .ok()
            .flatten()
            .and_then(|v| serde_json::Number::from_f64(v).map(Value::Number))
            .unwrap_or(Value::Null),
        JsonType::Boolean => row
            .try_get::<Option<bool>, _>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        JsonType::Text | JsonType::Timestamptz => row
            .try_get::<Option<String>, _>(idx)
            .ok()
            .flatten()
            .map(Value::String)
            .unwrap_or(Value::Null),
        JsonType::Jsonb => {
            // Try String first (MySQL + SQLite), then fall through to Null.
            let s: Option<String> = row.try_get::<Option<String>, _>(idx).ok().flatten();
            match s {
                Some(s) => serde_json::from_str::<Value>(&s).unwrap_or(Value::String(s)),
                None => Value::Null,
            }
        }
    }
}

#[async_trait]
impl SourceConnector for JdbcPollSource {
    async fn start(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        sqlx::any::install_default_drivers();
        let pool = AnyPool::connect(&self.connection_string)
            .await
            .map_err(|e| ConnectorError::Connection(format!("jdbc_poll connect: {e}")))?;

        // Resume from persisted offset if provided.
        if let Some(s) = last_position {
            if let Ok(v) = s.parse::<i64>() {
                self.last_value = v;
            } else {
                warn!(
                    last_position = %s,
                    "jdbc_poll: could not parse last_position as i64; starting from 0"
                );
            }
        }

        info!(
            table = %self.table,
            tracking_column = %self.tracking_column,
            resume_from = self.last_value,
            dialect = ?self.kind,
            "jdbc_poll source started"
        );

        self.pool = Some(pool);
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let pool = match &self.pool {
            Some(p) => p,
            None => return Err(ConnectorError::Connection("jdbc_poll: not started".into())),
        };
        let sql = self.build_select_sql(max_batch);
        let rows = sqlx::query(&sql)
            .bind(self.last_value)
            .fetch_all(pool)
            .await
            .map_err(|e| ConnectorError::Connection(format!("jdbc_poll query: {e}")))?;

        let mut out: Vec<SourceRecord> = Vec::with_capacity(rows.len());
        let mut high_water = self.last_value;

        for row in &rows {
            let mut obj = serde_json::Map::with_capacity(self.schema_cols.len());
            for (idx, col) in self.schema_cols.iter().enumerate() {
                let v = decode_cell(row, idx, col.json_type);
                obj.insert(col.name.clone(), v);
            }

            // Extract the tracking column value to update our cursor.
            if let Some(tc_idx) = self.schema_cols.iter().position(|c| c.name == self.tracking_column) {
                if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(tc_idx) {
                    if v > high_water {
                        high_water = v;
                    }
                }
            }

            let value = Bytes::from(
                serde_json::to_vec(&obj).unwrap_or_else(|_| b"null".to_vec()),
            );
            let subject = if self.subject_template.is_empty() {
                format!("jdbc_poll.{}", self.table)
            } else {
                self.subject_template.clone()
            };
            out.push(SourceRecord {
                key: None,
                value,
                subject,
                headers: Vec::new(),
            });
        }

        let position = if high_water != self.last_value {
            self.last_value = high_water;
            Some(high_water.to_string())
        } else {
            None
        };

        Ok(SourceBatch { records: out, position })
    }

    async fn commit(&mut self, _position: String) -> Result<(), ConnectorError> {
        // No server-side ack — sqlx polling is idempotent given a monotone
        // tracking column. Offset persistence is handled by the manager.
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
            HealthStatus::Unhealthy("jdbc_poll: not started".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_config(settings: HashMap<String, String>) -> ConnectorConfig {
        ConnectorConfig {
            name: "test-jdbc-poll".into(),
            connector_type: "source".into(),
            plugin: "jdbc_poll".into(),
            stream: "events".into(),
            subject_template: String::new(),
            subject_filter: String::new(),
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
    fn new_requires_connection_and_table_and_tracking_column_and_schema() {
        // Missing everything.
        assert!(JdbcPollSource::new(&make_config(HashMap::new())).is_err());

        // Missing tracking_column.
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x".into()),
            ("table".into(), "events".into()),
            ("schema".into(), "id:bigint".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_err());

        // Missing schema.
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_err());

        // Tracking column not in schema.
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "name:text".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_err());

        // Happy path.
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "id:bigint, name:text".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_ok());
    }

    #[test]
    fn new_rejects_mssql() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "mssql://sa:pw@host/db".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "id:bigint".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_err());
    }

    #[test]
    fn new_rejects_invalid_idents() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x".into()),
            ("table".into(), "foo; DROP TABLE bar".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "id:bigint".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_err());

        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id; DROP".into()),
            ("schema".into(), "id:bigint".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_err());
    }

    #[test]
    fn build_select_sql_postgres_shape() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/test".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "id:bigint, name:text".into()),
        ]));
        let src = JdbcPollSource::new(&cfg).unwrap();
        let sql = src.build_select_sql(50);
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"events\" WHERE \"id\" > $1 ORDER BY \"id\" ASC LIMIT 50"
        );
    }

    #[test]
    fn build_select_sql_mysql_shape() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "mysql://localhost/test".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "id:bigint, name:text".into()),
        ]));
        let src = JdbcPollSource::new(&cfg).unwrap();
        let sql = src.build_select_sql(50);
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `events` WHERE `id` > ? ORDER BY `id` ASC LIMIT 50"
        );
    }

    #[test]
    fn build_select_sql_sqlite_shape() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "sqlite::memory:".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "id:bigint".into()),
        ]));
        let src = JdbcPollSource::new(&cfg).unwrap();
        let sql = src.build_select_sql(10);
        assert!(sql.contains("\"events\""));
        assert!(sql.contains("> ?"));
        assert!(sql.contains("LIMIT 10"));
    }
}
