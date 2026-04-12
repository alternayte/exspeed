use thiserror::Error;
use exspeed_common::{PartitionId, StreamName};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("stream not found: {0}")]
    StreamNotFound(StreamName),

    #[error("partition not found: stream={stream}, partition={partition:?}")]
    PartitionNotFound {
        stream: StreamName,
        partition: PartitionId,
    },

    #[error("stream already exists: {0}")]
    StreamAlreadyExists(StreamName),

    #[error("corrupted record at offset {offset}: {reason}")]
    CorruptedRecord { offset: u64, reason: String },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
