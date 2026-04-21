pub mod file;
pub mod postgres;
pub mod redis;
pub mod s3;

use async_trait::async_trait;

use crate::consumer_state::ConsumerConfig;

#[derive(Debug, thiserror::Error)]
pub enum ConsumerStoreError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
}

#[async_trait]
pub trait ConsumerStore: Send + Sync {
    /// Save a consumer config (insert or update). Returns after the write
    /// reaches disk. Use this at create/delete time where the client expects
    /// a synchronous commit.
    async fn save(&self, config: &ConsumerConfig) -> Result<(), ConsumerStoreError>;

    /// Persist a consumer config, potentially deferred for coalescing. Unlike
    /// `save`, this may return before the write reaches disk — implementations
    /// are free to coalesce rapid successive calls for the same consumer name
    /// into a single disk write on a timer.
    ///
    /// Used for the Ack/Nack hot path where per-record `save` would cost a
    /// disk syscall per message. The coalescing window is implementation-
    /// defined; the `FileConsumerStore` default is 100 ms. Data acked within
    /// that window is lost on crash — symmetric with the async-storage-mode
    /// tradeoff we already ship.
    ///
    /// Default impl just forwards to `save` so backends without explicit
    /// batching (Postgres, Redis, S3) preserve synchronous semantics.
    async fn save_debounced(&self, config: ConsumerConfig) -> Result<(), ConsumerStoreError> {
        self.save(&config).await
    }

    /// Load a single consumer by name. Returns None if not found.
    async fn load(&self, name: &str) -> Result<Option<ConsumerConfig>, ConsumerStoreError>;

    /// Load all persisted consumers (used at broker startup).
    async fn load_all(&self) -> Result<Vec<ConsumerConfig>, ConsumerStoreError>;

    /// Delete a consumer by name. No error if already absent.
    async fn delete(&self, name: &str) -> Result<(), ConsumerStoreError>;
}

use std::sync::Arc;

/// Build a ConsumerStore from the `EXSPEED_CONSUMER_STORE` env var.
/// Defaults to the same backend as `EXSPEED_OFFSET_STORE`, then to `file`.
pub async fn from_env(
    data_dir: &std::path::Path,
) -> Result<Arc<dyn ConsumerStore>, ConsumerStoreError> {
    let backend = std::env::var("EXSPEED_CONSUMER_STORE")
        .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE"))
        .unwrap_or_else(|_| "file".to_string());

    match backend.as_str() {
        "postgres" => {
            let store = postgres::PostgresConsumerStore::from_env().await?;
            Ok(Arc::new(store))
        }
        "redis" => {
            let store = redis::RedisConsumerStore::from_env().await?;
            Ok(Arc::new(store))
        }
        "s3" => {
            let store = s3::S3ConsumerStore::from_env()?;
            Ok(Arc::new(store))
        }
        _ => {
            let store = file::FileConsumerStore::new(data_dir.to_path_buf());
            Ok(Arc::new(store))
        }
    }
}
