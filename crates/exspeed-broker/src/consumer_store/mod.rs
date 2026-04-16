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
    /// Save a consumer config (insert or update).
    async fn save(&self, config: &ConsumerConfig) -> Result<(), ConsumerStoreError>;

    /// Load a single consumer by name. Returns None if not found.
    async fn load(&self, name: &str) -> Result<Option<ConsumerConfig>, ConsumerStoreError>;

    /// Load all persisted consumers (used at broker startup).
    async fn load_all(&self) -> Result<Vec<ConsumerConfig>, ConsumerStoreError>;

    /// Delete a consumer by name. No error if already absent.
    async fn delete(&self, name: &str) -> Result<(), ConsumerStoreError>;
}
