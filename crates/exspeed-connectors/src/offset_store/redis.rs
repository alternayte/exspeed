use async_trait::async_trait;
use redis::AsyncCommands;
use tokio::sync::Mutex;

use super::{OffsetStore, OffsetStoreError};

pub struct RedisOffsetStore {
    conn: Mutex<redis::aio::MultiplexedConnection>,
    key_prefix: String,
}

impl RedisOffsetStore {
    pub async fn from_env() -> Result<Self, OffsetStoreError> {
        let url = std::env::var("EXSPEED_OFFSET_STORE_REDIS_URL").map_err(|_| {
            OffsetStoreError::Connection(
                "EXSPEED_OFFSET_STORE_REDIS_URL environment variable is required".to_string(),
            )
        })?;

        let key_prefix = std::env::var("EXSPEED_OFFSET_STORE_REDIS_KEY_PREFIX")
            .unwrap_or_else(|_| "exspeed:offsets:".to_string());

        let client = redis::Client::open(url)
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;

        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;

        Ok(Self {
            conn: Mutex::new(conn),
            key_prefix,
        })
    }

    fn key(&self, connector: &str) -> String {
        format!("{}{}", self.key_prefix, connector)
    }
}

#[async_trait]
impl OffsetStore for RedisOffsetStore {
    async fn save_source_offset(
        &self,
        connector: &str,
        position: &str,
    ) -> Result<(), OffsetStoreError> {
        let key = self.key(connector);
        let mut conn = self.conn.lock().await;
        conn.hset_multiple::<_, _, _, ()>(&key, &[("type", "source"), ("position", position)])
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn load_source_offset(
        &self,
        connector: &str,
    ) -> Result<Option<String>, OffsetStoreError> {
        let key = self.key(connector);
        let mut conn = self.conn.lock().await;
        let value: Option<String> = conn
            .hget(&key, "position")
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;
        Ok(value)
    }

    async fn save_sink_offset(
        &self,
        connector: &str,
        offset: u64,
    ) -> Result<(), OffsetStoreError> {
        let key = self.key(connector);
        let offset_str = offset.to_string();
        let mut conn = self.conn.lock().await;
        conn.hset_multiple::<_, _, _, ()>(&key, &[("type", "sink"), ("sink_offset", offset_str.as_str())])
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn load_sink_offset(&self, connector: &str) -> Result<u64, OffsetStoreError> {
        let key = self.key(connector);
        let mut conn = self.conn.lock().await;
        let value: Option<String> = conn
            .hget(&key, "sink_offset")
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;
        match value {
            None => Ok(0),
            Some(s) => s.parse::<u64>().map_err(|e| {
                OffsetStoreError::Serialization(format!("invalid sink_offset: {}", e))
            }),
        }
    }

    async fn delete(&self, connector: &str) -> Result<(), OffsetStoreError> {
        let key = self.key(connector);
        let mut conn = self.conn.lock().await;
        conn.del::<_, ()>(&key)
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;
        Ok(())
    }
}
