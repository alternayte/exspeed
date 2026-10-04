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

    #[error(
        "offset {requested} is below earliest retained offset {earliest} (records trimmed by retention)"
    )]
    OffsetOutOfRange { requested: u64, earliest: u64 },

    #[error("internal channel closed (writer task exited unexpectedly)")]
    ChannelClosed,

    /// The record can't be represented in the storage format (a field is
    /// longer than its length prefix allows). Never retryable.
    #[error("invalid record: {0}")]
    InvalidRecord(String),

    /// An explicit-offset append (`append_at`) would overlap existing data or
    /// isn't strictly increasing.
    #[error(
        "offset {offset} conflicts with the log: the lowest acceptable offset is {min_allowed}"
    )]
    OffsetConflict { offset: u64, min_allowed: u64 },

    /// The partition hit an IO error it could not roll back from and is now
    /// read-only. Reads still work; writes fail until the process restarts
    /// and recovery runs.
    #[error("stream {stream} is read-only after an unrecoverable storage error: {reason}")]
    PartitionFailed { stream: String, reason: String },

    /// The stream is at its `max_msgs` / `max_bytes` limit and its discard
    /// policy is `new`: nothing was written. Retryable once records leave.
    #[error("stream {stream} is full ({limit}); nothing was written (discard policy: new)")]
    StreamFull { stream: String, limit: String },

    /// The engine doesn't implement this operation.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_collision_display() {
        let err = StorageError::KeyCollision { stored_offset: 42 };
        assert_eq!(
            err.to_string(),
            "key collision: body differs from stored record at offset 42"
        );
    }

    #[test]
    fn dedup_map_full_display() {
        let err = StorageError::DedupMapFull {
            retry_after_secs: 30,
        };
        assert_eq!(
            err.to_string(),
            "dedup map full for this stream; retry after 30s"
        );
    }

    #[test]
    fn offset_out_of_range_display() {
        let err = StorageError::OffsetOutOfRange {
            requested: 50,
            earliest: 100,
        };
        assert_eq!(
            err.to_string(),
            "offset 50 is below earliest retained offset 100 (records trimmed by retention)"
        );
    }
}
