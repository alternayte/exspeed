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

    #[error("key collision: body differs from stored record at offset {stored_offset}")]
    KeyCollision { stored_offset: u64 },

    #[error("dedup map full for this stream; retry after {retry_after_secs}s")]
    DedupMapFull { retry_after_secs: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_collision_display() {
        let err = StorageError::KeyCollision { stored_offset: 42 };
        assert_eq!(err.to_string(), "key collision: body differs from stored record at offset 42");
    }

    #[test]
    fn dedup_map_full_display() {
        let err = StorageError::DedupMapFull { retry_after_secs: 30 };
        assert_eq!(err.to_string(), "dedup map full for this stream; retry after 30s");
    }
}
