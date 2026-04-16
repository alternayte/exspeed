use std::env;

use async_trait::async_trait;
use redis::AsyncCommands;
use tokio::sync::Mutex;

use crate::consumer_state::ConsumerConfig;
use super::{ConsumerStore, ConsumerStoreError};

pub struct RedisConsumerStore {
    conn: Mutex<redis::aio::MultiplexedConnection>,
    key_prefix: String,
}

impl RedisConsumerStore {
    /// Build from environment variables. Reuses the offset store's Redis connection.
    pub async fn from_env() -> Result<Self, ConsumerStoreError> {
        let url = env::var("EXSPEED_OFFSET_STORE_REDIS_URL").map_err(|_| {
            ConsumerStoreError::Connection("EXSPEED_OFFSET_STORE_REDIS_URL is required".to_string())
        })?;
        let key_prefix = env::var("EXSPEED_CONSUMER_STORE_REDIS_KEY_PREFIX")
            .unwrap_or_else(|_| "exspeed:consumers:".to_string());

        let client = redis::Client::open(url.as_str())
            .map_err(|e| ConsumerStoreError::Connection(format!("redis client error: {e}")))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("redis connect failed: {e}")))?;

        Ok(Self {
            conn: Mutex::new(conn),
            key_prefix,
        })
    }

    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.key_prefix)
    }

    fn scan_pattern(&self) -> String {
        format!("{}*", self.key_prefix)
    }
}

#[async_trait]
impl ConsumerStore for RedisConsumerStore {
    async fn save(&self, config: &ConsumerConfig) -> Result<(), ConsumerStoreError> {
        let mut conn = self.conn.lock().await;
        let key = self.key(&config.name);
        conn.hset_multiple::<_, _, _, ()>(
            &key,
            &[
                ("stream".to_string(), config.stream.clone()),
                ("group".to_string(), config.group.clone()),
                ("subject_filter".to_string(), config.subject_filter.clone()),
                ("offset".to_string(), config.offset.to_string()),
            ],
        )
        .await
        .map_err(|e| ConsumerStoreError::Connection(format!("redis hset failed: {e}")))?;
        Ok(())
    }

    async fn load(&self, name: &str) -> Result<Option<ConsumerConfig>, ConsumerStoreError> {
        let mut conn = self.conn.lock().await;
        let key = self.key(name);
        let fields: std::collections::HashMap<String, String> = conn
            .hgetall(&key)
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("redis hgetall failed: {e}")))?;

        if fields.is_empty() {
            return Ok(None);
        }

        Ok(Some(ConsumerConfig {
            name: name.to_string(),
            stream: fields.get("stream").cloned().unwrap_or_default(),
            group: fields.get("group").cloned().unwrap_or_default(),
            subject_filter: fields.get("subject_filter").cloned().unwrap_or_default(),
            offset: fields
                .get("offset")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
        }))
    }

    async fn load_all(&self) -> Result<Vec<ConsumerConfig>, ConsumerStoreError> {
        let pattern = self.scan_pattern();
        let keys: Vec<String> = {
            let mut conn = self.conn.lock().await;
            let mut iter = conn
                .scan_match::<_, String>(&pattern)
                .await
                .map_err(|e| ConsumerStoreError::Connection(format!("redis scan failed: {e}")))?;
            let mut keys = Vec::new();
            while let Some(k) = iter.next_item().await {
                keys.push(k);
            }
            keys
        };

        let mut configs = Vec::new();
        for key in keys {
            let name = key
                .strip_prefix(&self.key_prefix)
                .unwrap_or(&key)
                .to_string();
            if let Some(cfg) = self.load(&name).await? {
                configs.push(cfg);
            }
        }
        configs.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(configs)
    }

    async fn delete(&self, name: &str) -> Result<(), ConsumerStoreError> {
        let mut conn = self.conn.lock().await;
        let key = self.key(name);
        conn.del::<_, ()>(&key)
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("redis del failed: {e}")))?;
        Ok(())
    }
}
