use async_trait::async_trait;
use bytes::Bytes;
use tokio_postgres::NoTls;
use tracing::error;

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

pub struct PostgresCdcSource {
    connection_string: String,
    tables: Vec<String>,
    timestamp_column: String,
    subject_template: String,
    client: Option<tokio_postgres::Client>,
    last_timestamp: Option<String>,
}

// tokio_postgres::Client does not implement Debug, so we implement it manually.
impl std::fmt::Debug for PostgresCdcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresCdcSource")
            .field("connection_string", &"[redacted]")
            .field("tables", &self.tables)
            .field("timestamp_column", &self.timestamp_column)
            .field("subject_template", &self.subject_template)
            .field("connected", &self.client.is_some())
            .field("last_timestamp", &self.last_timestamp)
            .finish()
    }
}

impl PostgresCdcSource {
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

        Ok(Self {
            connection_string,
            tables,
            timestamp_column,
            subject_template,
            client: None,
            last_timestamp: None,
        })
    }
}

/// Extract (schema, bare_table) from a possibly-qualified table name like "public.orders".
/// Falls back to ("public", table) when no schema prefix is present.
fn split_table_name(table: &str) -> (&str, &str) {
    if let Some(dot) = table.find('.') {
        (&table[..dot], &table[dot + 1..])
    } else {
        ("public", table)
    }
}

/// Attempt to read a column value as a String, trying several common Postgres types.
fn col_to_string(row: &tokio_postgres::Row, idx: usize) -> String {
    // Try the most common types in order.
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

#[async_trait]
impl SourceConnector for PostgresCdcSource {
    async fn start(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres_cdc connection error: {}", e);
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

            let rows = match &self.last_timestamp {
                Some(ts) => {
                    let query = format!(
                        "SELECT * FROM {table} WHERE {ts_col} > $1 ORDER BY {ts_col} LIMIT $2",
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
                for (idx, col) in columns.iter().enumerate() {
                    let val_str = col_to_string(row, idx);
                    map.insert(
                        col.name().to_string(),
                        serde_json::Value::String(val_str.clone()),
                    );

                    // Track max timestamp from the timestamp column.
                    if col.name() == self.timestamp_column.as_str() {
                        let update = match &max_ts {
                            None => true,
                            Some(prev) => val_str > *prev,
                        };
                        if update {
                            max_ts = Some(val_str);
                        }
                    }
                }

                let value_json = serde_json::Value::Object(map);
                let value_bytes = Bytes::from(
                    serde_json::to_vec(&value_json)
                        .map_err(|e| ConnectorError::Data(format!("json serialization: {e}")))?,
                );

                // Subject: use template if set, otherwise derive from table name.
                let subject = if !self.subject_template.is_empty() {
                    self.subject_template
                        .replace("{schema}", schema)
                        .replace("{table}", bare_table)
                } else {
                    format!("{schema}.{bare_table}.change")
                };

                // Key: first column value.
                let key = if columns.is_empty() {
                    None
                } else {
                    Some(Bytes::from(col_to_string(row, 0).into_bytes()))
                };

                let record = SourceRecord {
                    key,
                    value: value_bytes,
                    subject,
                    headers: vec![
                        ("x-exspeed-source".to_string(), "postgres_cdc".to_string()),
                        ("x-table".to_string(), table.clone()),
                    ],
                };

                all_records.push(record);
            }
        }

        // Advance cursor only when we saw new rows.
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_config(settings: HashMap<String, String>) -> ConnectorConfig {
        ConnectorConfig {
            name: "test-cdc".into(),
            connector_type: "source".into(),
            plugin: "postgres_cdc".into(),
            stream: "events".into(),
            subject_template: "".into(),
            subject_filter: "".into(),
            settings,
            batch_size: 100,
            poll_interval_ms: 50,
        }
    }

    #[test]
    fn rejects_missing_connection() {
        let config = make_config(HashMap::from([("tables".into(), "public.orders".into())]));
        let err = PostgresCdcSource::new(&config).unwrap_err();
        assert!(
            err.to_string().contains("connection"),
            "expected 'connection' in error, got: {err}"
        );
    }

    #[test]
    fn rejects_missing_tables() {
        let config = make_config(HashMap::from([(
            "connection".into(),
            "postgres://localhost/db".into(),
        )]));
        let err = PostgresCdcSource::new(&config).unwrap_err();
        assert!(
            err.to_string().contains("tables"),
            "expected 'tables' in error, got: {err}"
        );
    }

    #[test]
    fn rejects_empty_tables_value() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "  ,  ".into()),
        ]));
        let err = PostgresCdcSource::new(&config).unwrap_err();
        assert!(
            err.to_string().contains("tables"),
            "expected 'tables' in error, got: {err}"
        );
    }

    #[test]
    fn parses_valid_config() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "public.orders, public.customers".into()),
            ("timestamp_column".into(), "modified_at".into()),
        ]));
        let src = PostgresCdcSource::new(&config).unwrap();
        assert_eq!(src.tables, vec!["public.orders", "public.customers"]);
        assert_eq!(src.timestamp_column, "modified_at");
    }

    #[test]
    fn default_timestamp_column() {
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/db".into()),
            ("tables".into(), "orders".into()),
        ]));
        let src = PostgresCdcSource::new(&config).unwrap();
        assert_eq!(src.timestamp_column, "updated_at");
    }

    #[test]
    fn split_table_name_qualified() {
        assert_eq!(
            split_table_name("myschema.mytable"),
            ("myschema", "mytable")
        );
    }

    #[test]
    fn split_table_name_unqualified() {
        assert_eq!(split_table_name("mytable"), ("public", "mytable"));
    }
}
