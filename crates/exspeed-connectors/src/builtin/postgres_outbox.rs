use async_trait::async_trait;
use bytes::Bytes;
use tokio_postgres::NoTls;
use tracing::{error, warn};

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

#[derive(Debug, Clone, PartialEq)]
enum OutboxMode {
    Poll,
    Cdc,
}

#[derive(Debug, Clone, PartialEq)]
enum CleanupMode {
    Delete,
    None,
}

#[allow(dead_code)] // mode, slot_name, publication_name used by future CDC mode
pub struct PostgresOutboxSource {
    connection_string: String,
    outbox_table: String,
    id_column: String,
    aggregate_type_column: String,
    event_type_column: String,
    payload_column: String,
    key_column: String,
    subject_template: String,
    mode: OutboxMode,
    cleanup_mode: CleanupMode,
    slot_name: String,
    publication_name: String,
    client: Option<tokio_postgres::Client>,
    last_position: Option<String>,
    /// Row IDs from the last poll batch, used for DELETE on commit (CDC mode).
    pending_ids: Vec<String>,
}

/// Sanitize connector name to valid Postgres identifier chars (alphanumeric + underscore).
pub fn sanitize_pg_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

impl PostgresOutboxSource {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let connection_string = config
            .setting("connection")
            .map_err(ConnectorError::Config)?
            .to_string();

        let outbox_table = config.setting_or("outbox_table", "outbox_events");
        let id_column = config.setting_or("id_column", "id");
        let aggregate_type_column = config.setting_or("aggregate_type_column", "aggregate_type");
        let event_type_column = config.setting_or("event_type_column", "event_type");
        let payload_column = config.setting_or("payload_column", "payload");
        let key_column = config.setting_or("key_column", "aggregate_id");
        let subject_template = config.subject_template.clone();

        let mode = match config.setting_or("mode", "poll").as_str() {
            "cdc" => OutboxMode::Cdc,
            _ => OutboxMode::Poll,
        };

        let cleanup_mode = match config.setting_or("cleanup_mode", "delete").as_str() {
            "none" => CleanupMode::None,
            _ => CleanupMode::Delete,
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

        if mode == OutboxMode::Cdc {
            // CDC mode will be implemented in a later task
            return Err(ConnectorError::Config(
                "CDC mode not yet implemented — use mode=poll".to_string(),
            ));
        }

        Ok(Self {
            connection_string,
            outbox_table,
            id_column,
            aggregate_type_column,
            event_type_column,
            payload_column,
            key_column,
            subject_template,
            mode,
            cleanup_mode,
            slot_name,
            publication_name,
            client: None,
            last_position: None,
            pending_ids: Vec::new(),
        })
    }
}

#[async_trait]
impl SourceConnector for PostgresOutboxSource {
    async fn start(&mut self, last_position: Option<String>) -> Result<(), ConnectorError> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres connection error: {}", e);
            }
        });

        self.client = Some(client);
        self.last_position = last_position;
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let client = match &self.client {
            Some(c) => c,
            None => return Err(ConnectorError::Connection("not connected".to_string())),
        };

        let (query, params): (String, Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>) =
            match &self.last_position {
                Some(pos) => {
                    // Parse position as i64 for typed comparison
                    let pos_i64: i64 = pos
                        .parse()
                        .map_err(|e| ConnectorError::Data(format!("invalid position: {e}")))?;
                    (
                        format!(
                            "SELECT {id}, {agg_type}, {evt_type}, {payload}, {key} FROM {table} WHERE {id} > $1 ORDER BY {id} LIMIT $2",
                            id = self.id_column,
                            agg_type = self.aggregate_type_column,
                            evt_type = self.event_type_column,
                            payload = self.payload_column,
                            key = self.key_column,
                            table = self.outbox_table,
                        ),
                        vec![Box::new(pos_i64), Box::new(max_batch as i64)],
                    )
                }
                None => (
                    format!(
                        "SELECT {id}, {agg_type}, {evt_type}, {payload}, {key} FROM {table} ORDER BY {id} LIMIT $1",
                        id = self.id_column,
                        agg_type = self.aggregate_type_column,
                        evt_type = self.event_type_column,
                        payload = self.payload_column,
                        key = self.key_column,
                        table = self.outbox_table,
                    ),
                    vec![Box::new(max_batch as i64)],
                ),
            };

        let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| &**p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

        let rows = client
            .query(&query, &params_ref)
            .await
            .map_err(|e| ConnectorError::Data(e.to_string()))?;

        let mut records = Vec::with_capacity(rows.len());
        let mut last_id: Option<String> = None;
        self.pending_ids.clear();

        for row in &rows {
            let id: String = row
                .try_get::<_, String>(0)
                .or_else(|_| row.try_get::<_, i64>(0).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("id column: {e}")))?;

            let agg_type: String = row
                .try_get::<_, String>(1)
                .or_else(|_| row.try_get::<_, i64>(1).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("aggregate_type column: {e}")))?;

            let evt_type: String = row
                .try_get::<_, String>(2)
                .or_else(|_| row.try_get::<_, i64>(2).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("event_type column: {e}")))?;

            let payload_bytes: Bytes = row
                .try_get::<_, String>(3)
                .map(|s| Bytes::from(s.into_bytes()))
                .or_else(|_| row.try_get::<_, Vec<u8>>(3).map(Bytes::from))
                .map_err(|e| ConnectorError::Data(format!("payload column: {e}")))?;

            let key_str: String = row
                .try_get::<_, String>(4)
                .or_else(|_| row.try_get::<_, i64>(4).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("key column: {e}")))?;

            let subject = self
                .subject_template
                .replace("{aggregate_type}", &agg_type)
                .replace("{event_type}", &evt_type);

            let record = SourceRecord {
                key: Some(Bytes::from(key_str.into_bytes())),
                value: payload_bytes,
                subject,
                headers: vec![
                    ("x-idempotency-key".to_string(), id.clone()),
                    ("x-aggregate-type".to_string(), agg_type),
                    ("x-event-type".to_string(), evt_type),
                ],
            };

            self.pending_ids.push(id.clone());
            last_id = Some(id);
            records.push(record);
        }

        Ok(SourceBatch {
            records,
            position: last_id,
        })
    }

    async fn commit(&mut self, position: String) -> Result<(), ConnectorError> {
        self.last_position = Some(position.clone());

        if self.cleanup_mode == CleanupMode::Delete && !self.pending_ids.is_empty() {
            let client = match &self.client {
                Some(c) => c,
                None => return Ok(()), // Can't delete if not connected
            };

            // DELETE processed rows by ID
            // For poll mode, we can use WHERE id <= position (simpler, single query)
            let pos_i64: i64 = position
                .parse()
                .map_err(|e| ConnectorError::Data(format!("invalid position for delete: {e}")))?;

            let delete_sql = format!(
                "DELETE FROM {} WHERE {} <= $1",
                self.outbox_table, self.id_column
            );

            if let Err(e) = client.execute(&delete_sql, &[&pos_i64]).await {
                // Log but don't fail -- broker dedup is the safety net
                warn!(
                    table = self.outbox_table.as_str(),
                    error = %e,
                    "failed to delete processed outbox rows"
                );
            }

            self.pending_ids.clear();
        }

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
