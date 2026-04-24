use std::path::PathBuf;

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

    /// Append a record and return `(offset, timestamp_ns)` — the offset the
    /// record was assigned and the nanosecond-precision wall-clock timestamp
    /// the storage engine stamped it with. Returning the timestamp alongside
    /// the offset lets callers (e.g. the replication fan-out) propagate the
    /// leader-assigned timestamp to followers without a round-trip read.
    async fn append(
        &self,
        stream: &StreamName,
        record: &Record,
    ) -> Result<(Offset, u64), StorageError>;

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

    /// Delete all records in `stream` with offset strictly less than
    /// `keep_from`. Safe to call with `keep_from` pointing mid-segment —
    /// the segment containing `keep_from` is preserved; earlier segments
    /// are removed. Also updates any offset / time indexes to reflect the
    /// new earliest offset.
    async fn trim_up_to(
        &self,
        stream: &StreamName,
        keep_from: Offset,
    ) -> Result<(), StorageError>;

    /// Remove the stream entirely — all segments, indexes, and stream
    /// configuration. Idempotent: deleting a non-existent stream returns
    /// `Ok(())` (the caller's intent is "make sure it's gone").
    async fn delete_stream(&self, stream: &StreamName) -> Result<(), StorageError>;

    /// Return `(earliest, next)` for a stream — the offset of the first
    /// retained record and the offset the NEXT append will write to.
    /// `earliest == next` means the stream is empty.
    ///
    /// Implementations return the tightest available view: local storage
    /// first, falling back to a remote/tiered manifest when the backend
    /// has one. No backend returns `(0, 0)` for a stream it knows nothing
    /// about — that case is always `StorageError::StreamNotFound`.
    async fn stream_bounds(
        &self,
        stream: &StreamName,
    ) -> Result<(Offset, Offset), StorageError>;

    /// Drop records at offsets `>= drop_from`. Complement to
    /// [`StorageEngine::trim_up_to`]. Used by the follower's
    /// divergent-history recovery path — after a leader failover the
    /// follower may have records that the new leader's log does not
    /// contain. This method removes those records and makes `drop_from`
    /// the new `next` offset for the stream.
    ///
    /// Contract: records at offsets `>= drop_from` are dropped; records
    /// at offsets `< drop_from` are preserved. After a successful call,
    /// `stream_bounds` returns `next == drop_from`, and the next
    /// `append` on this stream assigns exactly `drop_from`. A `drop_from`
    /// at or past the current `next` offset is a no-op.
    async fn truncate_from(
        &self,
        stream: &StreamName,
        drop_from: Offset,
    ) -> Result<(), StorageError>;

    /// Register a secondary index on the given stream's partition so that
    /// `.sidx.{name}` files are built when segments are sealed.
    ///
    /// Default is a no-op for non-file-backed implementations.
    async fn register_secondary_index(
        &self,
        _stream: &StreamName,
        _name: String,
        _field_path: String,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    /// Return the filesystem path for a given stream + partition, if the
    /// implementation is backed by local files.
    ///
    /// Default returns `None` for non-file-backed implementations.
    fn partition_dir_path(&self, _stream: &str, _partition: u32) -> Option<PathBuf> {
        None
    }

    /// Read records with optional hints that allow storage-level optimisations.
    ///
    /// When `key_filter` is `Some`, the engine MAY skip segments whose bloom
    /// filter proves the key is absent.  The default implementation ignores
    /// the hint and delegates to [`read`].
    async fn read_with_hints(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
        key_filter: Option<&str>,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let _ = key_filter;
        self.read(stream, from, max_records).await
    }

    /// Append N records. Default implementation serializes via `append`;
    /// FileStorage overrides this to use a single WAL batch.
    async fn append_batch(
        &self,
        stream: &StreamName,
        records: Vec<Record>,
    ) -> Result<Vec<(Offset, u64)>, StorageError> {
        let mut out = Vec::with_capacity(records.len());
        for record in records {
            let (offset, ts) = self.append(stream, &record).await?;
            out.push((offset, ts));
        }
        Ok(out)
    }
}
