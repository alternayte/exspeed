use std::env;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use crate::consumer_state::ConsumerConfig;
use super::{ConsumerStore, ConsumerStoreError};

pub struct PostgresConsumerStore {
    client: Mutex<Client>,
    schema: String,
    table: String,
}

impl PostgresConsumerStore {
    /// Build from environment variables. Reuses the offset store's Postgres connection.
    pub async fn from_env() -> Result<Self, ConsumerStoreError> {
        let url = env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").map_err(|_| {
            ConsumerStoreError::Connection(
                "EXSPEED_OFFSET_STORE_POSTGRES_URL is required".to_string(),
            )
        })?;
        let schema = env::var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA")
            .unwrap_or_else(|_| "public".to_string());
        let table = "exspeed_consumers".to_string();

        let (client, connection) = tokio_postgres::connect(&url, NoTls)
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("postgres connect failed: {e}")))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "postgres consumer store connection error");
            }
        });

        let store = Self {
            client: Mutex::new(client),
            schema,
            table,
        };

        store.ensure_table().await?;
        Ok(store)
    }

    async fn ensure_table(&self) -> Result<(), ConsumerStoreError> {
        let client = self.client.lock().await;
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (
                name            TEXT PRIMARY KEY,
                stream          TEXT NOT NULL,
                consumer_group  TEXT NOT NULL DEFAULT '',
                subject_filter  TEXT NOT NULL DEFAULT '',
                consumer_offset BIGINT NOT NULL DEFAULT 0
            )",
            self.schema, self.table
        );
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("failed to create table: {e}")))?;
        Ok(())
    }

    fn upsert_sql(&self) -> String {
        format!(
            "INSERT INTO {}.{} (name, stream, consumer_group, subject_filter, consumer_offset)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (name) DO UPDATE
             SET stream = $2, consumer_group = $3, subject_filter = $4, consumer_offset = $5",
            self.schema, self.table
        )
    }

    fn select_sql(&self) -> String {
        format!(
            "SELECT name, stream, consumer_group, subject_filter, consumer_offset
             FROM {}.{} WHERE name = $1",
            self.schema, self.table
        )
    }

    fn select_all_sql(&self) -> String {
        format!(
            "SELECT name, stream, consumer_group, subject_filter, consumer_offset
             FROM {}.{} ORDER BY name",
            self.schema, self.table
        )
    }

    fn delete_sql(&self) -> String {
        format!("DELETE FROM {}.{} WHERE name = $1", self.schema, self.table)
    }

    fn row_to_config(row: &tokio_postgres::Row) -> ConsumerConfig {
        let offset: i64 = row.get("consumer_offset");
        ConsumerConfig {
            name: row.get("name"),
            stream: row.get("stream"),
            group: row.get("consumer_group"),
            subject_filter: row.get("subject_filter"),
            offset: offset as u64,
        }
    }
}

#[async_trait]
impl ConsumerStore for PostgresConsumerStore {
    async fn save(&self, config: &ConsumerConfig) -> Result<(), ConsumerStoreError> {
        let client = self.client.lock().await;
        client
            .execute(
                &self.upsert_sql(),
                &[
                    &config.name,
                    &config.stream,
                    &config.group,
                    &config.subject_filter,
                    &(config.offset as i64),
                ],
            )
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("save failed: {e}")))?;
        Ok(())
    }

    async fn load(&self, name: &str) -> Result<Option<ConsumerConfig>, ConsumerStoreError> {
        let client = self.client.lock().await;
        let rows = client
            .query(&self.select_sql(), &[&name])
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("load failed: {e}")))?;
        Ok(rows.first().map(Self::row_to_config))
    }

    async fn load_all(&self) -> Result<Vec<ConsumerConfig>, ConsumerStoreError> {
        let client = self.client.lock().await;
        let rows = client
            .query(&self.select_all_sql(), &[])
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("load_all failed: {e}")))?;
        Ok(rows.iter().map(Self::row_to_config).collect())
    }

    async fn delete(&self, name: &str) -> Result<(), ConsumerStoreError> {
        let client = self.client.lock().await;
        client
            .execute(&self.delete_sql(), &[&name])
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("delete failed: {e}")))?;
        Ok(())
    }
}
