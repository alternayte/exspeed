use crate::error::StorageError;
use crate::record::{Record, StoredRecord};
use exspeed_common::{Offset, PartitionId, StreamName};

pub trait StorageEngine: Send + Sync {
    fn create_stream(&self, stream: &StreamName, partition_count: u32) -> Result<(), StorageError>;

    fn append(
        &self,
        stream: &StreamName,
        partition: PartitionId,
        record: &Record,
    ) -> Result<Offset, StorageError>;

    fn read(
        &self,
        stream: &StreamName,
        partition: PartitionId,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError>;
}
