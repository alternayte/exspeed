use crate::error::StorageError;
use crate::record::{Record, StoredRecord};
use exspeed_common::{Offset, StreamName};

pub trait StorageEngine: Send + Sync {
    fn create_stream(&self, stream: &StreamName) -> Result<(), StorageError>;

    fn append(&self, stream: &StreamName, record: &Record) -> Result<Offset, StorageError>;

    fn read(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError>;
}
