use async_trait::async_trait;
use bytes::Bytes;
use tokio_postgres::NoTls;
use tracing::error;

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

pub struct PostgresOutboxSource {
    connection_string: String,
    outbox_table: String,
    id_column: String,
    aggregate_type_column: String,
    event_type_column: String,
    payload_column: String,
    key_column: String,
    subject_template: String,
    client: Option<tokio_postgres::Client>,
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

        Ok(Self {
            connection_string,
            outbox_table,
            id_column,
            aggregate_type_column,
            event_type_column,
            payload_column,
            key_column,
            subject_template,
            client: None,
        })
    }
}

#[async_trait]
impl SourceConnector for PostgresOutboxSource {
    async fn start(&mut self, _last_position: Option<String>) -> Result<(), ConnectorError> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres connection error: {}", e);
            }
        });

        self.client = Some(client);
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        let client = match &self.client {
            Some(c) => c,
            None => return Err(ConnectorError::Connection("not connected".to_string())),
        };

        let query = format!(
            "SELECT {id}, {agg_type}, {evt_type}, {payload}, {key} FROM {table} ORDER BY {id} LIMIT $1",
            id = self.id_column,
            agg_type = self.aggregate_type_column,
            evt_type = self.event_type_column,
            payload = self.payload_column,
            key = self.key_column,
            table = self.outbox_table,
        );

        let rows = client
            .query(&query, &[&(max_batch as i64)])
            .await
            .map_err(|e| ConnectorError::Data(e.to_string()))?;

        let mut records = Vec::with_capacity(rows.len());
        let mut last_id: Option<String> = None;

        for row in &rows {
            // id (column 0)
            let id: String = row
                .try_get::<_, String>(0)
                .or_else(|_| row.try_get::<_, i64>(0).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("id column: {e}")))?;

            // aggregate_type (column 1)
            let agg_type: String = row
                .try_get::<_, String>(1)
                .or_else(|_| row.try_get::<_, i64>(1).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("aggregate_type column: {e}")))?;

            // event_type (column 2)
            let evt_type: String = row
                .try_get::<_, String>(2)
                .or_else(|_| row.try_get::<_, i64>(2).map(|v| v.to_string()))
                .map_err(|e| ConnectorError::Data(format!("event_type column: {e}")))?;

            // payload (column 3) — try String then raw bytes
            let payload_bytes: Bytes = row
                .try_get::<_, String>(3)
                .map(|s| Bytes::from(s.into_bytes()))
                .or_else(|_| row.try_get::<_, Vec<u8>>(3).map(Bytes::from))
                .map_err(|e| ConnectorError::Data(format!("payload column: {e}")))?;

            // key (column 4)
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

            last_id = Some(id);
            records.push(record);
        }

        Ok(SourceBatch {
            records,
            position: last_id,
        })
    }

    async fn commit(&mut self, _position: String) -> Result<(), ConnectorError> {
        // No-op for polling mode. Delete-after-publish is Phase 5b.
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
