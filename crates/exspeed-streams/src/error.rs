use exspeed_common::StreamName;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("stream not found: {0}")]
    StreamNotFound(StreamName),

    #[error("stream already exists: {0}")]
    StreamAlreadyExists(StreamName),

    #[error("corrupted record at offset {offset}: {reason}")]
    CorruptedRecord { offset: u64, reason: String },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
