use async_trait::async_trait;
use crate::error::StorageError;
use crate::record::{Record, StoredRecord};
use exspeed_common::{Offset, StreamName};

#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn create_stream(
        &self,
        stream: &StreamName,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<(), StorageError>;

    async fn append(&self, stream: &StreamName, record: &Record) -> Result<Offset, StorageError>;

    async fn read(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError>;

    /// Find the offset of the first record at or after the given timestamp.
    async fn seek_by_time(&self, stream: &StreamName, timestamp: u64) -> Result<Offset, StorageError>;

    /// List all stream names known to this storage engine.
    async fn list_streams(&self) -> Result<Vec<StreamName>, StorageError>;
}
