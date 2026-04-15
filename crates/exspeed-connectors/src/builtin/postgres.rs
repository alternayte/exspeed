use async_trait::async_trait;
use bytes::Bytes;
use tokio_postgres::NoTls;
use tracing::error;

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

use super::postgres_outbox::sanitize_pg_name;

#[derive(Debug, Clone, PartialEq)]
enum PgMode {
    Poll,
    Cdc,
}

#[allow(dead_code)] // mode, slot_name, publication_name, operations used by future CDC mode
pub struct PostgresSource {
    // Note: tokio_postgres::Client does not implement Debug, so we impl Debug manually below.
    connection_string: String,
    tables: Vec<String>,
    timestamp_column: String,
    subject_template: String,
    mode: PgMode,
    slot_name: String,
    publication_name: String,
    operations: Vec<String>,
    client: Option<tokio_postgres::Client>,
    last_timestamp: Option<String>,
}

impl std::fmt::Debug for PostgresSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSource")
            .field("connection_string", &"[redacted]")
            .field("tables", &self.tables)
            .field("timestamp_column", &self.timestamp_column)
            .field("subject_template", &self.subject_template)
            .field("mode", &self.mode)
            .field("connected", &self.client.is_some())
            .field("last_timestamp", &self.last_timestamp)
            .finish()
    }
}

/// Extract (schema, bare_table) from a possibly-qualified table name like "public.orders".
fn split_table_name(table: &str) -> (&str, &str) {
    if let Some(dot) = table.find('.') {
        (&table[..dot], &table[dot + 1..])
    } else {
        ("public", table)
    }
}

/// Attempt to read a column value as a String, trying several common Postgres types.
fn col_to_string(row: &tokio_postgres::Row, idx: usize) -> String {
    if let Ok(v) = row.try_get::<_, String>(idx) {
        return v;
    }
    if let Ok(v) = row.try_get::<_, i64>(idx) {
        return v.to_string();
    }
    if let Ok(v) = row.try_get::<_, i32>(idx) {
        return v.to_string();
    }
    if let Ok(v) = row.try_get::<_, f64>(idx) {
        return v.to_string();
    }
    if let Ok(v) = row.try_get::<_, bool>(idx) {
        return v.to_string();
    }
    "<null>".to_string()
}

impl PostgresSource {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let connection_string = config
            .setting("connection")
            .map_err(ConnectorError::Config)?
            .to_string();

        let tables_raw = config
            .setting("tables")
            .map_err(ConnectorError::Config)?
            .to_string();

        let tables: Vec<String> = tables_raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if tables.is_empty() {
            return Err(ConnectorError::Config(
                "setting 'tables' must contain at least one table name".to_string(),
            ));
        }

        let timestamp_column = config.setting_or("timestamp_column", "updated_at");
        let subject_template = config.subject_template.clone();

        let mode = match config.setting_or("mode", "poll").as_str() {
            "cdc" => PgMode::Cdc,
            _ => PgMode::Poll,
        };

        let sanitized = sanitize_pg_name(&config.name);
        let slot_name = {
            let configured = config.setting_or("slot_name", "");
            if configured.is_empty() {
                format!("exspeed_{sanitized}_slot")
            } else {
                configured
            }
        };
        let publication_name = {
            let configured = config.setting_or("publication_name", "");
            if configured.is_empty() {
                format!("exspeed_{sanitized}_pub")
            } else {
                configured
            }
        };

        let operations: Vec<String> = config
            .setting_or("operations", "INSERT,UPDATE,DELETE")
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .collect();

        if mode == PgMode::Cdc {
            return Err(ConnectorError::Config(
                "CDC mode not yet implemented — use mode=poll".to_string(),
            ));
        }

        Ok(Self {
            connection_string,
            tables,
            timestamp_column,
            subject_template,
            mode,
            slot_name,
            publication_name,
            operations,
            client: None,
            last_timestamp: None,
        })
    }
}

#[async_trait]
impl SourceConnector for PostgresSource {
    async fn start(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres source connection error: {}", e);
            }
        });

        self.client = Some(client);
        self.last_timestamp = last_position;
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let client = match &self.client {
            Some(c) => c,
            None => return Err(ConnectorError::Connection("not connected".to_string())),
        };

        let mut all_records: Vec<SourceRecord> = Vec::new();
        let mut max_ts: Option<String> = self.last_timestamp.clone();

        for table in &self.tables.clone() {
            let (schema, bare_table) = split_table_name(table);

            // Use parameterized timestamp comparison instead of string comparison
            let rows = match &self.last_timestamp {
                Some(ts) => {
                    let query = format!(
                        "SELECT * FROM {table} WHERE {ts_col}::text > $1 ORDER BY {ts_col} LIMIT $2",
                        table = table,
                        ts_col = self.timestamp_column,
                    );
                    client
                        .query(&query, &[ts, &(max_batch as i64)])
                        .await
                        .map_err(|e| ConnectorError::Data(e.to_string()))?
                }
                None => {
                    let query = format!(
                        "SELECT * FROM {table} ORDER BY {ts_col} LIMIT $1",
                        table = table,
                        ts_col = self.timestamp_column,
                    );
                    client
                        .query(&query, &[&(max_batch as i64)])
                        .await
                        .map_err(|e| ConnectorError::Data(e.to_string()))?
                }
            };

            for row in &rows {
                let columns = row.columns();

                // Build a JSON object from all columns.
                let mut map = serde_json::Map::new();
                let mut row_ts: Option<String> = None;
                let mut pk_value = String::new();

                for (idx, col) in columns.iter().enumerate() {
                    let val_str = col_to_string(row, idx);
                    map.insert(
                        col.name().to_string(),
                        serde_json::Value::String(val_str.clone()),
                    );

                    // Track max timestamp from the timestamp column.
                    if col.name() == self.timestamp_column.as_str() {
                        row_ts = Some(val_str.clone());
                        let update = match &max_ts {
                            None => true,
                            Some(prev) => val_str > *prev,
                        };
                        if update {
                            max_ts = Some(val_str.clone());
                        }
                    }

                    // First column is treated as the primary key
                    if idx == 0 {
                        pk_value = val_str;
                    }
                }

                let value_json = serde_json::Value::Object(map);
                let value_bytes = Bytes::from(
                    serde_json::to_vec(&value_json)
                        .map_err(|e| ConnectorError::Data(format!("json serialization: {e}")))?,
                );

                let subject = if !self.subject_template.is_empty() {
                    self.subject_template
                        .replace("{schema}", schema)
                        .replace("{table}", bare_table)
                } else {
                    format!("{schema}.{bare_table}.change")
                };

                let key = if pk_value.is_empty() {
                    None
                } else {
                    Some(Bytes::from(pk_value.clone().into_bytes()))
                };

                // Build idempotency key: table:pk:timestamp
                let idemp_key = format!(
                    "{}:{}:{}",
                    table,
                    pk_value,
                    row_ts.as_deref().unwrap_or("")
                );

                let record = SourceRecord {
                    key,
                    value: value_bytes,
                    subject,
                    headers: vec![
                        ("x-idempotency-key".to_string(), idemp_key),
                        ("x-exspeed-source".to_string(), "postgres".to_string()),
                        ("x-table".to_string(), table.clone()),
                    ],
                };

                all_records.push(record);
            }
        }

        if max_ts != self.last_timestamp {
            self.last_timestamp = max_ts.clone();
        }

        Ok(SourceBatch {
            records: all_records,
            position: max_ts,
        })
    }

    async fn commit(&mut self, position: String) -> Result<(), ConnectorError> {
        self.last_timestamp = Some(position);
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.client = None;
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        if self.client.is_some() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy("not connected".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_config(settings: HashMap<String, String>) -> ConnectorConfig {
        ConnectorConfig {
            name: "test-pg".into(),
            connector_type: "source".into(),
            plugin: "postgres".into(),
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
    fn rejects_missing_connection() {
        let config = make_config(HashMap::from([("tables".into(), "public.orders".into())]));
        let err = PostgresSource::new(&config).unwrap_err();
        assert!(err.to_string().contains("connection"));
    }

    #[test]
    fn rejects_missing_tables() {
        let config = make_config(HashMap::from([(
            "connection".into(),
            "postgres://localhost/db".into(),
        )]));
        let err = PostgresSource::new(&config).unwrap_err();
        assert!(err.to_string().contains("tables"));
    }

    #[test]
    fn parses_valid_config() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "public.orders, public.customers".into()),
            ("timestamp_column".into(), "modified_at".into()),
        ]));
        let src = PostgresSource::new(&config).unwrap();
        assert_eq!(src.tables, vec!["public.orders", "public.customers"]);
        assert_eq!(src.timestamp_column, "modified_at");
    }

    #[test]
    fn default_timestamp_column() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "orders".into()),
        ]));
        let src = PostgresSource::new(&config).unwrap();
        assert_eq!(src.timestamp_column, "updated_at");
    }

    #[test]
    fn split_table_name_qualified() {
        assert_eq!(split_table_name("myschema.mytable"), ("myschema", "mytable"));
    }

    #[test]
    fn split_table_name_unqualified() {
        assert_eq!(split_table_name("mytable"), ("public", "mytable"));
    }

    #[test]
    fn auto_generates_slot_and_publication_names() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "orders".into()),
        ]));
        let src = PostgresSource::new(&config).unwrap();
        assert_eq!(src.slot_name, "exspeed_test_pg_slot");
        assert_eq!(src.publication_name, "exspeed_test_pg_pub");
    }
}
