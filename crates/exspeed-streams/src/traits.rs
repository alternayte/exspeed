use std::path::PathBuf;

use crate::config::StreamConfig;
use crate::error::StorageError;
use crate::record::{Record, StoredRecord};
use async_trait::async_trait;
use bytes::BytesMut;
use exspeed_common::record_format;
use exspeed_common::{Offset, StreamName};

/// Bounds for a [`StorageEngine::read_batch`] call. A batch always contains
/// at least one record when one is available, even if it exceeds
/// `max_bytes`, so a single large record can't stall a reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            max_records: 500,
            max_bytes: 1024 * 1024,
        }
    }
}

/// Result of a [`StorageEngine::read_batch`] call.
#[derive(Debug, Clone)]
pub struct ReadBatch {
    pub records: Vec<StoredRecord>,
    /// Where the next read should start: one past the last returned record.
    /// When nothing was returned it is `from` clamped to the earliest
    /// retained offset, or the high watermark when every offset between
    /// `from` and the high watermark is a gap (compacted away).
    pub next_offset: Offset,
    /// Offset the next append will get (the visible end of the log). Only
    /// records below the high watermark are ever returned.
    /// `next_offset == high_watermark` means the reader is caught up.
    pub high_watermark: Offset,
}

/// Result of a [`StorageEngine::read_raw`] call: records in their stored
/// (= wire) encoding, back to back, ready to be copied into a response
/// frame. See [`exspeed_common::record_format`] for the byte layout.
#[derive(Debug, Clone)]
pub struct RawBatch {
    /// `count` encoded records with strictly increasing offsets. Their
    /// `delivery_count` is 0. Mutable so a consumer can patch the delivery
    /// count in place before sending.
    pub bytes: BytesMut,
    pub count: usize,
    /// Same meaning as [`ReadBatch::next_offset`].
    pub next_offset: Offset,
    /// Same meaning as [`ReadBatch::high_watermark`].
    pub high_watermark: Offset,
}

impl RawBatch {
    /// Iterate the records' positions (and offsets) in [`RawBatch::bytes`].
    pub fn records(&self) -> record_format::Iter<'_> {
        record_format::iter(&self.bytes)
    }

    /// Encode decoded records (for engines that don't store the wire
    /// format, and for tests).
    pub fn encode(
        records: &[StoredRecord],
        next_offset: Offset,
        high_watermark: Offset,
    ) -> Result<Self, StorageError> {
        let mut bytes = BytesMut::new();
        for r in records {
            record_format::encode(
                &mut bytes,
                &record_format::Fields {
                    offset: r.offset.0,
                    timestamp_ns: r.timestamp,
                    delivery_count: 0,
                    subject: &r.subject,
                    key: r.key.as_deref(),
                    value: &r.value,
                    headers: &r.headers,
                },
            )
            .map_err(|e| StorageError::InvalidRecord(e.0))?;
        }
        Ok(Self {
            bytes,
            count: records.len(),
            next_offset,
            high_watermark,
        })
    }
}

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
    async fn seek_by_time(
        &self,
        stream: &StreamName,
        timestamp: u64,
    ) -> Result<Offset, StorageError>;

    /// List all stream names known to this storage engine.
    async fn list_streams(&self) -> Result<Vec<StreamName>, StorageError>;

    /// Delete all records in `stream` with offset strictly less than
    /// `keep_from`. Safe to call with `keep_from` pointing mid-segment —
    /// the segment containing `keep_from` is preserved; earlier segments
    /// are removed. Also updates any offset / time indexes to reflect the
    /// new earliest offset.
    async fn trim_up_to(&self, stream: &StreamName, keep_from: Offset) -> Result<(), StorageError>;

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
    async fn stream_bounds(&self, stream: &StreamName) -> Result<(Offset, Offset), StorageError>;

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

    /// **Deprecated — will be removed.** Secondary indexes were dropped from
    /// the storage engine; this is a no-op kept only until the ExQL engine
    /// stops calling it.
    async fn register_secondary_index(
        &self,
        _stream: &StreamName,
        _name: String,
        _field_path: String,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    /// **Deprecated — will be removed.** No engine exposes its partition
    /// directory any more; always returns `None`.
    fn partition_dir_path(&self, _stream: &str, _partition: u32) -> Option<PathBuf> {
        None
    }

    /// **Deprecated — will be removed.** Bloom filters were dropped from the
    /// storage engine, so the hint is ignored and this is a plain
    /// [`StorageEngine::read`].
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
    /// FileStorage overrides this to write the batch with one group commit.
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

    /// Append records that already carry their offsets, timestamps and keys
    /// (the replication follower path). Rules:
    ///
    /// * offsets within one call must be strictly increasing;
    /// * the first offset must be `>= next_offset`; gaps are allowed
    ///   (compacted logs have them);
    /// * any record with offset `< next_offset` is a
    ///   [`StorageError::OffsetConflict`] — callers filter out records they
    ///   already have first.
    ///
    /// Afterwards `next_offset` is the last record's offset + 1. Nothing is
    /// written when the call fails validation. The default implementation
    /// returns [`StorageError::Unsupported`].
    async fn append_at(
        &self,
        stream: &StreamName,
        records: Vec<StoredRecord>,
    ) -> Result<(), StorageError> {
        let _ = (stream, records);
        Err(StorageError::Unsupported(
            "this storage engine does not support append_at".into(),
        ))
    }

    /// Create a stream with a full config (retention + dedup). The default
    /// implementation only applies retention.
    async fn create_stream_with(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), StorageError> {
        self.create_stream(stream, config.max_age_secs, config.max_bytes)
            .await
    }

    /// Current config of a stream.
    async fn stream_config(&self, stream: &StreamName) -> Result<StreamConfig, StorageError> {
        self.stream_bounds(stream).await?;
        Ok(StreamConfig::default())
    }

    /// Replace a stream's config.
    async fn update_stream_config(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), StorageError> {
        let _ = (stream, config);
        Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this storage engine does not support config updates",
        )))
    }

    /// Bounded read that also reports where to resume and the end of the
    /// log. Prefer this over [`StorageEngine::read`] in new code.
    async fn read_batch(
        &self,
        stream: &StreamName,
        from: Offset,
        limits: ReadLimits,
    ) -> Result<ReadBatch, StorageError> {
        let (earliest, high_watermark) = self.stream_bounds(stream).await?;
        let from = Offset(from.0.max(earliest.0));
        let mut records = self.read(stream, from, limits.max_records.max(1)).await?;
        let mut bytes = 0usize;
        let mut keep = 0usize;
        for r in &records {
            let size = r.value.len() + r.subject.len() + r.key.as_ref().map_or(0, |k| k.len());
            if keep > 0 && bytes + size > limits.max_bytes {
                break;
            }
            bytes += size;
            keep += 1;
        }
        records.truncate(keep);
        // An empty read below the high watermark means everything in
        // `[from, high_watermark)` is a gap; skip it so readers don't spin.
        let next_offset = records
            .last()
            .map(|r| Offset(r.offset.0 + 1))
            .unwrap_or(Offset(from.0.max(high_watermark.0)));
        Ok(ReadBatch {
            records,
            next_offset,
            high_watermark,
        })
    }

    /// Like [`StorageEngine::read_batch`], but returns the records in their
    /// wire encoding without decoding them. `limits.max_bytes` bounds the
    /// encoded size (at least one record is returned when one is
    /// available). A batch may stop early at a segment boundary; resume
    /// from `next_offset`. The default implementation encodes the result of
    /// `read_batch`; engines that store the wire format override it.
    async fn read_raw(
        &self,
        stream: &StreamName,
        from: Offset,
        limits: ReadLimits,
    ) -> Result<RawBatch, StorageError> {
        let b = self.read_batch(stream, from, limits).await?;
        RawBatch::encode(&b.records, b.next_offset, b.high_watermark)
    }

    /// Subscribe to the stream's high watermark (the offset the next append
    /// will get). Readers that are caught up can `changed().await` instead of
    /// polling. `None` means the engine doesn't support notifications.
    fn watch_appends(&self, stream: &StreamName) -> Option<tokio::sync::watch::Receiver<u64>> {
        let _ = stream;
        None
    }
}
