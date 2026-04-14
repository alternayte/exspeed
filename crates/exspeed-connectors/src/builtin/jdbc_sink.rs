use async_trait::async_trait;
use tracing::{info, warn};

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SinkBatch, SinkConnector, WriteResult};

pub struct JdbcSinkConnector {
    connection_string: String,
    table: String,
    mode: String,
    upsert_keys: Vec<String>,
    auto_create_table: bool,
    pool: Option<sqlx::AnyPool>,
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

        let auto_create_table = config
            .setting_or("auto_create_table", "false")
            .eq_ignore_ascii_case("true");

        Ok(Self {
            connection_string,
            table,
            mode,
            upsert_keys,
            auto_create_table,
            pool: None,
        })
    }
}

#[async_trait]
impl SinkConnector for JdbcSinkConnector {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        // Must be called before creating Any pool — registers Postgres + MySQL drivers.
        sqlx::any::install_default_drivers();

        let pool = sqlx::AnyPool::connect(&self.connection_string)
            .await
            .map_err(|e| {
                ConnectorError::Connection(format!("JDBC sink: failed to connect: {e}"))
            })?;

        info!(
            connection = %self.connection_string,
            table = %self.table,
            mode = %self.mode,
            auto_create_table = self.auto_create_table,
            "JDBC sink connected"
        );

        self.pool = Some(pool);
        Ok(())
    }

    async fn write(&mut self, batch: SinkBatch) -> Result<WriteResult, ConnectorError> {
        let pool = match &self.pool {
            Some(p) => p,
            None => {
                return Ok(WriteResult::AllFailed(
                    "JDBC sink: pool not initialised — call start() first".into(),
                ))
            }
        };

        let mut last_successful_offset: Option<u64> = None;
        let mut any_success = false;
        let mut first_fail: Option<String> = None;

        'records: for record in &batch.records {
            // Parse value as JSON object.
            let json: serde_json::Value = match serde_json::from_slice(&record.value) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        offset = record.offset,
                        "JDBC sink: skipping non-JSON record: {e}"
                    );
                    // Treat parse failures as "advanced past" (poisoned data).
                    last_successful_offset = Some(record.offset);
                    any_success = true;
                    continue 'records;
                }
            };

            let obj = match json.as_object() {
                Some(o) => o,
                None => {
                    warn!(
                        offset = record.offset,
                        "JDBC sink: skipping non-object JSON record"
                    );
                    last_successful_offset = Some(record.offset);
                    any_success = true;
                    continue 'records;
                }
            };

            // Extract ordered (col, value) pairs.
            let pairs: Vec<(String, String)> = obj
                .iter()
                .map(|(k, v)| {
                    let s = match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), s)
                })
                .collect();

            if pairs.is_empty() {
                last_successful_offset = Some(record.offset);
                any_success = true;
                continue 'records;
            }

            let cols: Vec<&str> = pairs.iter().map(|(c, _)| c.as_str()).collect();
            let vals: Vec<&str> = pairs.iter().map(|(_, v)| v.as_str()).collect();

            // sqlx::Any uses `?` placeholders for all drivers.
            let placeholders: Vec<String> = (0..cols.len()).map(|_| "?".to_string()).collect();

            let sql = match self.mode.as_str() {
                "insert" => {
                    format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        self.table,
                        cols.join(", "),
                        placeholders.join(", ")
                    )
                }
                "upsert" => {
                    let keys = if self.upsert_keys.is_empty() {
                        // Fall back to first column if no keys configured.
                        vec![cols[0].to_string()]
                    } else {
                        self.upsert_keys.clone()
                    };
                    let update_set: Vec<String> = cols
                        .iter()
                        .filter(|c| !keys.contains(&c.to_string()))
                        .map(|c| format!("{c} = EXCLUDED.{c}"))
                        .collect();
                    if update_set.is_empty() {
                        // All columns are key columns — nothing to update; treat as INSERT IGNORE.
                        format!(
                            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                            self.table,
                            cols.join(", "),
                            placeholders.join(", "),
                            keys.join(", ")
                        )
                    } else {
                        format!(
                            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
                            self.table,
                            cols.join(", "),
                            placeholders.join(", "),
                            keys.join(", "),
                            update_set.join(", ")
                        )
                    }
                }
                "update" => {
                    let keys = if self.upsert_keys.is_empty() {
                        vec![cols[0].to_string()]
                    } else {
                        self.upsert_keys.clone()
                    };
                    let set_parts: Vec<String> = cols
                        .iter()
                        .filter(|c| !keys.contains(&c.to_string()))
                        .map(|c| format!("{c} = ?"))
                        .collect();
                    let where_parts: Vec<String> =
                        keys.iter().map(|k| format!("{k} = ?")).collect();

                    if set_parts.is_empty() {
                        // Nothing to update — skip.
                        last_successful_offset = Some(record.offset);
                        any_success = true;
                        continue 'records;
                    }

                    format!(
                        "UPDATE {} SET {} WHERE {}",
                        self.table,
                        set_parts.join(", "),
                        where_parts.join(" AND ")
                    )
                }
                unknown => {
                    let msg = format!("JDBC sink: unknown mode '{unknown}'");
                    warn!("{}", msg);
                    first_fail = Some(msg);
                    break 'records;
                }
            };

            // Build the bind sequence.
            // For "update" the bind order is: non-key values first, then key values.
            let bind_vals: Vec<&str> = if self.mode == "update" {
                let keys = if self.upsert_keys.is_empty() {
                    vec![cols[0].to_string()]
                } else {
                    self.upsert_keys.clone()
                };
                let mut non_key: Vec<&str> = pairs
                    .iter()
                    .filter(|(c, _)| !keys.contains(c))
                    .map(|(_, v)| v.as_str())
                    .collect();
                let key_vals: Vec<&str> = pairs
                    .iter()
                    .filter(|(c, _)| keys.contains(c))
                    .map(|(_, v)| v.as_str())
                    .collect();
                non_key.extend(key_vals);
                non_key
            } else {
                vals.clone()
            };

            // Dynamically bind all values as strings.
            let mut query = sqlx::query(&sql);
            for v in &bind_vals {
                query = query.bind(*v);
            }

            match query.execute(pool).await {
                Ok(_) => {
                    last_successful_offset = Some(record.offset);
                    any_success = true;
                }
                Err(e) => {
                    let msg = format!(
                        "JDBC sink: execute failed for offset {}: {e}",
                        record.offset
                    );
                    warn!("{}", msg);
                    first_fail = Some(msg);
                    break 'records;
                }
            }
        }

        if first_fail.is_some() {
            if let Some(offset) = last_successful_offset {
                Ok(WriteResult::PartialSuccess {
                    last_successful_offset: offset,
                })
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
            HealthStatus::Unhealthy("JDBC sink: pool not initialised".into())
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
        // Missing both required fields.
        let config = make_config(HashMap::new());
        assert!(JdbcSinkConnector::new(&config).is_err());

        // Missing table.
        let config = make_config(HashMap::from([(
            "connection".into(),
            "postgres://localhost/test".into(),
        )]));
        assert!(JdbcSinkConnector::new(&config).is_err());

        // Both required fields present.
        let config = make_config(HashMap::from([
            ("connection".into(), "postgres://localhost/test".into()),
            ("table".into(), "events".into()),
        ]));
        let connector = JdbcSinkConnector::new(&config).unwrap();
        assert_eq!(connector.table, "events");
        assert_eq!(connector.mode, "upsert");
        assert!(!connector.auto_create_table);
        assert!(connector.upsert_keys.is_empty());
    }

    #[test]
    fn config_optional_fields() {
        let config = make_config(HashMap::from([
            ("connection".into(), "mysql://localhost/test".into()),
            ("table".into(), "orders".into()),
            ("mode".into(), "insert".into()),
            ("upsert_keys".into(), "id, tenant_id".into()),
            ("auto_create_table".into(), "true".into()),
        ]));
        let connector = JdbcSinkConnector::new(&config).unwrap();
        assert_eq!(connector.mode, "insert");
        assert_eq!(connector.upsert_keys, vec!["id", "tenant_id"]);
        assert!(connector.auto_create_table);
    }
}
