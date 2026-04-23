//! Generic JDBC polling source.
//!
//! Periodically polls a relational table for rows whose `tracking_column`
//! exceeds the last-seen value, emitting each row as a JSON record.
//!
//! Supports Postgres, MySQL, SQLite (via `sqlx::AnyPool`) and SQL Server
//! (via `tiberius` + `bb8-tiberius`). For SQL Server CDC semantics
//! (insert/update/delete capture with operation type) use `mssql_cdc`
//! instead.
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
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use bytes::Bytes;
use sqlx::{any::AnyRow, AnyPool, Row};
use tiberius::Query;
use tracing::{info, warn};
use url::Url;

use crate::builtin::jdbc::dialect::{ColumnSpec, DialectKind, JsonType};
use crate::builtin::jdbc::schema::{is_valid_ident, parse_schema};
use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

enum Backend {
    Sqlx(AnyPool),
    Tiberius(Pool<ConnectionManager>),
}

pub struct JdbcPollSource {
    connection_string: String,
    table: String,
    tracking_column: String,
    schema_cols: Vec<ColumnSpec>,
    backend: Option<Backend>,
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

        Ok(Self {
            connection_string,
            table,
            tracking_column,
            schema_cols,
            backend: None,
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
        let batch = batch_size.max(1);
        match self.kind {
            DialectKind::Postgres => format!(
                "SELECT {} FROM {} WHERE {} > $1 ORDER BY {} ASC LIMIT {}",
                cols_sql.join(", "), table_q, tc_q, tc_q, batch
            ),
            DialectKind::MySql | DialectKind::Sqlite => format!(
                "SELECT {} FROM {} WHERE {} > ? ORDER BY {} ASC LIMIT {}",
                cols_sql.join(", "), table_q, tc_q, tc_q, batch
            ),
            DialectKind::Mssql => format!(
                "SELECT TOP ({}) {} FROM {} WHERE {} > @P1 ORDER BY {} ASC",
                batch, cols_sql.join(", "), table_q, tc_q, tc_q
            ),
        }
    }
}

fn quote_ident(kind: DialectKind, name: &str) -> String {
    match kind {
        DialectKind::Postgres | DialectKind::Sqlite => format!("\"{name}\""),
        DialectKind::MySql => format!("`{name}`"),
        DialectKind::Mssql => format!("[{name}]"),
    }
}

/// Decode a sqlx::AnyRow column into `serde_json::Value`.
fn decode_sqlx_cell(row: &AnyRow, idx: usize, ty: JsonType) -> serde_json::Value {
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
            let s: Option<String> = row.try_get::<Option<String>, _>(idx).ok().flatten();
            match s {
                Some(s) => serde_json::from_str::<Value>(&s).unwrap_or(Value::String(s)),
                None => Value::Null,
            }
        }
    }
}

/// Decode a tiberius row column by name.
fn decode_tiberius_cell(row: &tiberius::Row, col: &str, ty: JsonType) -> serde_json::Value {
    use serde_json::Value;
    match ty {
        JsonType::Bigint => row
            .try_get::<i64, _>(col)
            .ok()
            .flatten()
            .map(|v| Value::Number(v.into()))
            .or_else(|| {
                row.try_get::<i32, _>(col)
                    .ok()
                    .flatten()
                    .map(|v| Value::Number((v as i64).into()))
            })
            .unwrap_or(Value::Null),
        JsonType::Double => row
            .try_get::<f64, _>(col)
            .ok()
            .flatten()
            .and_then(|v| serde_json::Number::from_f64(v).map(Value::Number))
            .unwrap_or(Value::Null),
        JsonType::Boolean => row
            .try_get::<bool, _>(col)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        JsonType::Text | JsonType::Timestamptz | JsonType::Jsonb => row
            .try_get::<&str, _>(col)
            .ok()
            .flatten()
            .map(|s| {
                if matches!(ty, JsonType::Jsonb) {
                    serde_json::from_str::<Value>(s).unwrap_or(Value::String(s.to_string()))
                } else {
                    Value::String(s.to_string())
                }
            })
            .unwrap_or(Value::Null),
    }
}

fn parse_mssql_url(raw: &str) -> Result<tiberius::Config, ConnectorError> {
    let normalized = if raw.to_ascii_lowercase().starts_with("mssql://") {
        format!("sqlserver://{}", &raw[8..])
    } else {
        raw.to_string()
    };
    let u = Url::parse(&normalized)
        .map_err(|e| ConnectorError::Config(format!("jdbc_poll url parse: {e}")))?;

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

#[async_trait]
impl SourceConnector for JdbcPollSource {
    async fn start(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        let backend = match self.kind {
            DialectKind::Postgres | DialectKind::MySql | DialectKind::Sqlite => {
                sqlx::any::install_default_drivers();
                let pool = AnyPool::connect(&self.connection_string)
                    .await
                    .map_err(|e| ConnectorError::Connection(format!("jdbc_poll connect: {e}")))?;
                Backend::Sqlx(pool)
            }
            DialectKind::Mssql => {
                let cfg = parse_mssql_url(&self.connection_string)?;
                let mgr = ConnectionManager::build(cfg)
                    .map_err(|e| ConnectorError::Connection(format!("jdbc_poll bb8 build: {e}")))?;
                let pool = Pool::builder()
                    .max_size(4)
                    .build(mgr)
                    .await
                    .map_err(|e| ConnectorError::Connection(format!("jdbc_poll pool: {e}")))?;
                Backend::Tiberius(pool)
            }
        };

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

        self.backend = Some(backend);
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let sql = self.build_select_sql(max_batch);
        let backend = match &self.backend {
            Some(b) => b,
            None => return Err(ConnectorError::Connection("jdbc_poll: not started".into())),
        };

        let mut out: Vec<SourceRecord> = Vec::with_capacity(max_batch);
        let mut high_water = self.last_value;

        match backend {
            Backend::Sqlx(pool) => {
                let rows = sqlx::query(&sql)
                    .bind(self.last_value)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| ConnectorError::Connection(format!("jdbc_poll sqlx query: {e}")))?;
                for row in &rows {
                    let mut obj = serde_json::Map::with_capacity(self.schema_cols.len());
                    for (idx, col) in self.schema_cols.iter().enumerate() {
                        obj.insert(col.name.clone(), decode_sqlx_cell(row, idx, col.json_type));
                    }
                    if let Some(tc_idx) = self
                        .schema_cols
                        .iter()
                        .position(|c| c.name == self.tracking_column)
                    {
                        if let Ok(Some(v)) = row.try_get::<Option<i64>, _>(tc_idx) {
                            if v > high_water {
                                high_water = v;
                            }
                        }
                    }
                    out.push(self.build_record(obj));
                }
            }
            Backend::Tiberius(pool) => {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| ConnectorError::Connection(format!("jdbc_poll pool get: {e}")))?;
                let mut q = Query::new(sql);
                q.bind(self.last_value);
                let stream = q.query(&mut *conn).await.map_err(|e| {
                    ConnectorError::Connection(format!("jdbc_poll tiberius query: {e}"))
                })?;
                let rows = stream.into_first_result().await.map_err(|e| {
                    ConnectorError::Connection(format!("jdbc_poll tiberius result: {e}"))
                })?;
                for row in &rows {
                    let mut obj = serde_json::Map::with_capacity(self.schema_cols.len());
                    for col in &self.schema_cols {
                        obj.insert(
                            col.name.clone(),
                            decode_tiberius_cell(row, &col.name, col.json_type),
                        );
                    }
                    // Update high-water mark from the tracking column.
                    if let Ok(Some(v)) = row.try_get::<i64, _>(self.tracking_column.as_str()) {
                        if v > high_water {
                            high_water = v;
                        }
                    } else if let Ok(Some(v)) = row.try_get::<i32, _>(self.tracking_column.as_str()) {
                        let v = v as i64;
                        if v > high_water {
                            high_water = v;
                        }
                    }
                    out.push(self.build_record(obj));
                }
            }
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
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        match self.backend.take() {
            Some(Backend::Sqlx(pool)) => pool.close().await,
            Some(Backend::Tiberius(_)) => {
                // bb8 pool drops connections when the pool is dropped.
            }
            None => {}
        }
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        if self.backend.is_some() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy("jdbc_poll: not started".into())
        }
    }
}

impl JdbcPollSource {
    fn build_record(&self, obj: serde_json::Map<String, serde_json::Value>) -> SourceRecord {
        let value = Bytes::from(serde_json::to_vec(&obj).unwrap_or_else(|_| b"null".to_vec()));
        let subject = if self.subject_template.is_empty() {
            format!("jdbc_poll.{}", self.table)
        } else {
            self.subject_template.clone()
        };
        SourceRecord {
            key: None,
            value,
            subject,
            headers: Vec::new(),
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
        assert!(JdbcPollSource::new(&make_config(HashMap::new())).is_err());

        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x".into()),
            ("table".into(), "events".into()),
            ("schema".into(), "id:bigint".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_err()); // missing tracking_column

        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_err()); // missing schema

        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "name:text".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_err()); // tc not in schema

        let cfg = make_config(HashMap::from([
            ("connection".into(), "postgres://x".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "id:bigint, name:text".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_ok());
    }

    #[test]
    fn new_accepts_mssql() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "mssql://sa:pw@host/db".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "id:bigint".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_ok());
    }

    #[test]
    fn new_accepts_sqlserver_scheme() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "sqlserver://sa:pw@host/db".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "id:bigint".into()),
        ]));
        assert!(JdbcPollSource::new(&cfg).is_ok());
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

    #[test]
    fn build_select_sql_mssql_shape() {
        let cfg = make_config(HashMap::from([
            ("connection".into(), "mssql://sa:pw@h/db".into()),
            ("table".into(), "events".into()),
            ("tracking_column".into(), "id".into()),
            ("schema".into(), "id:bigint, name:text".into()),
        ]));
        let src = JdbcPollSource::new(&cfg).unwrap();
        let sql = src.build_select_sql(50);
        assert_eq!(
            sql,
            "SELECT TOP (50) [id], [name] FROM [events] WHERE [id] > @P1 ORDER BY [id] ASC"
        );
    }
}
