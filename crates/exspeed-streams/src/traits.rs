use crate::error::StorageError;
use crate::record::{Record, StoredRecord};
use exspeed_common::{Offset, StreamName};

pub trait StorageEngine: Send + Sync {
    fn create_stream(&self, stream: &StreamName, max_age_secs: u64, max_bytes: u64) -> Result<(), StorageError>;

    fn append(&self, stream: &StreamName, record: &Record) -> Result<Offset, StorageError>;

    fn read(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError>;

    /// Find the offset of the first record at or after the given timestamp.
    fn seek_by_time(
        &self,
        stream: &StreamName,
        timestamp: u64,
    ) -> Result<Offset, StorageError>;
}
