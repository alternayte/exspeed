use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use super::{OffsetStore, OffsetStoreError};

pub struct PostgresOffsetStore {
    client: Mutex<Client>,
    schema: String,
    table: String,
}

impl PostgresOffsetStore {
    pub async fn from_env() -> Result<Self, OffsetStoreError> {
        let url = std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").map_err(|_| {
            OffsetStoreError::Connection(
                "EXSPEED_OFFSET_STORE_POSTGRES_URL environment variable is required".to_string(),
            )
        })?;

        let schema = std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA")
            .unwrap_or_else(|_| "public".to_string());

        let table = std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_TABLE")
            .unwrap_or_else(|_| "exspeed_offsets".to_string());

        let (client, connection) = tokio_postgres::connect(&url, NoTls)
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres offset store connection error: {}", e);
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

    async fn ensure_table(&self) -> Result<(), OffsetStoreError> {
        let sql = format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.{table} (
                connector_name TEXT PRIMARY KEY,
                offset_type    TEXT NOT NULL,
                position       TEXT,
                sink_offset    BIGINT,
                updated_at     TIMESTAMPTZ DEFAULT NOW()
            )"#,
            schema = self.schema,
            table = self.table,
        );

        let client = self.client.lock().await;
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;

        Ok(())
    }

    fn upsert_sql(&self) -> String {
        format!(
            r#"INSERT INTO {schema}.{table} (connector_name, offset_type, position, sink_offset, updated_at)
               VALUES ($1, $2, $3, $4, NOW())
               ON CONFLICT (connector_name) DO UPDATE SET
                   offset_type  = EXCLUDED.offset_type,
                   position     = EXCLUDED.position,
                   sink_offset  = EXCLUDED.sink_offset,
                   updated_at   = EXCLUDED.updated_at"#,
            schema = self.schema,
            table = self.table,
        )
    }

    fn select_sql(&self) -> String {
        format!(
            "SELECT offset_type, position, sink_offset FROM {schema}.{table} WHERE connector_name = $1",
            schema = self.schema,
            table = self.table,
        )
    }

    fn delete_sql(&self) -> String {
        format!(
            "DELETE FROM {schema}.{table} WHERE connector_name = $1",
            schema = self.schema,
            table = self.table,
        )
    }
}

#[async_trait]
impl OffsetStore for PostgresOffsetStore {
    async fn save_source_offset(
        &self,
        connector: &str,
        position: &str,
    ) -> Result<(), OffsetStoreError> {
        let sql = self.upsert_sql();
        let client = self.client.lock().await;
        client
            .execute(
                &sql,
                &[
                    &connector,
                    &"source",
                    &Some(position),
                    &(None::<i64>),
                ],
            )
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn load_source_offset(
        &self,
        connector: &str,
    ) -> Result<Option<String>, OffsetStoreError> {
        let sql = self.select_sql();
        let client = self.client.lock().await;
        let row = client
            .query_opt(&sql, &[&connector])
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;

        match row {
            None => Ok(None),
            Some(r) => {
                let position: Option<String> = r.get("position");
                Ok(position)
            }
        }
    }

    async fn save_sink_offset(
        &self,
        connector: &str,
        offset: u64,
    ) -> Result<(), OffsetStoreError> {
        let sql = self.upsert_sql();
        let sink_offset = offset as i64;
        let client = self.client.lock().await;
        client
            .execute(
                &sql,
                &[
                    &connector,
                    &"sink",
                    &(None::<String>),
                    &Some(sink_offset),
                ],
            )
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn load_sink_offset(&self, connector: &str) -> Result<u64, OffsetStoreError> {
        let sql = self.select_sql();
        let client = self.client.lock().await;
        let row = client
            .query_opt(&sql, &[&connector])
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;

        match row {
            None => Ok(0),
            Some(r) => {
                let sink_offset: Option<i64> = r.get("sink_offset");
                Ok(sink_offset.unwrap_or(0) as u64)
            }
        }
    }

    async fn delete(&self, connector: &str) -> Result<(), OffsetStoreError> {
        let sql = self.delete_sql();
        let client = self.client.lock().await;
        client
            .execute(&sql, &[&connector])
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;
        Ok(())
    }
}
