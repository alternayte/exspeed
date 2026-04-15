pub mod file;
pub mod postgres;
pub mod redis;
pub mod s3;
pub mod stream;

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum OffsetStoreError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
}

#[async_trait]
pub trait OffsetStore: Send + Sync {
    /// Save a source connector's opaque position string.
    async fn save_source_offset(
        &self,
        connector: &str,
        position: &str,
    ) -> Result<(), OffsetStoreError>;

    /// Load a source connector's last saved position. None = fresh start.
    async fn load_source_offset(
        &self,
        connector: &str,
    ) -> Result<Option<String>, OffsetStoreError>;

    /// Save a sink connector's stream offset.
    async fn save_sink_offset(
        &self,
        connector: &str,
        offset: u64,
    ) -> Result<(), OffsetStoreError>;

    /// Load a sink connector's last saved offset. 0 = start of stream.
    async fn load_sink_offset(&self, connector: &str) -> Result<u64, OffsetStoreError>;

    /// Delete all offsets for a connector.
    async fn delete(&self, connector: &str) -> Result<(), OffsetStoreError>;
}
