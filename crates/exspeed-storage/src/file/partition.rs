// Built in Task 8, updated in Task 4 (Phase 3)

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;

use exspeed_common::Offset;
use exspeed_streams::record::{Record, StoredRecord};
use tracing::{error, info};

use crate::file::io_errors::is_storage_full;

use crate::file::offset_index::OffsetIndex;
use crate::file::segment_reader::SegmentReader;
use crate::file::segment_writer::SegmentWriter;
use crate::file::time_index::{self, TimeIndex};
use crate::file::wal::{replay_wal, WalWriter};

/// Default maximum segment size before rolling: 256 MiB.
const DEFAULT_SEGMENT_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Stats from retention enforcement.
#[derive(Debug, Default)]
pub struct RetentionStats {
    pub segments_deleted: u32,
    pub bytes_reclaimed: u64,
}

/// Information about a segment that has just been sealed (rolled).
/// Sent via the notification channel so that background tasks (e.g. S3 upload)
/// can act on the newly sealed segment.
#[derive(Debug, Clone)]
pub struct SealedSegmentInfo {
    pub stream_name: String,
    pub partition_id: u32,
    pub seg_path: PathBuf,
    pub base_offset: u64,
    pub end_offset: u64,
    pub size_bytes: u64,
    pub record_count: u64,
    pub first_timestamp: u64,
    pub last_timestamp: u64,
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Emit a structured `error!` log for a partition write failure. Disk-full
/// errors are highlighted separately so operators can alert on them.
fn log_write_error(stream_name: &str, partition_id: u32, msg: &'static str, err: &io::Error) {
    if is_storage_full(err) {
        error!(
            stream = stream_name,
            partition = partition_id,
            error = %err,
            kind = "storage_full",
            "{msg}: storage full"
        );
    } else {
        error!(
            stream = stream_name,
            partition = partition_id,
            error = %err,
            kind = "other",
            "{msg}"
        );
    }
}

/// Manages a single partition's on-disk state: active segment writer,
/// sealed segment readers, and the write-ahead log.
pub struct Partition {
    dir: PathBuf,
    active_writer: SegmentWriter,
    sealed_readers: Vec<SegmentReader>,
    wal: WalWriter,
    next_offset: u64,
    stream_name: String,
    partition_id: u32,
    segment_max_bytes: u64,
    seal_tx: Option<mpsc::UnboundedSender<SealedSegmentInfo>>,
}

impl Partition {
    /// Create a brand-new partition directory with its first segment and WAL.
    pub fn create(dir: &Path, stream_name: &str, partition_id: u32) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let writer = SegmentWriter::create(dir, 0)?;
        let wal = WalWriter::open(&dir.join("wal.log"))?;

        Ok(Self {
            dir: dir.to_path_buf(),
            active_writer: writer,
            sealed_readers: Vec::new(),
            wal,
            next_offset: 0,
            stream_name: stream_name.to_string(),
            partition_id,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            seal_tx: None,
        })
    }

    /// Open an existing partition directory, recovering state from segment
    /// files and replaying the WAL.
    pub fn open(dir: &Path, stream_name: &str, partition_id: u32) -> io::Result<Self> {
        // Find all .seg files, sorted by name (which sorts by base_offset
        // due to zero-padded filenames).
        let mut seg_paths: Vec<PathBuf> = fs::read_dir(dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("seg") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();
        seg_paths.sort();

        if seg_paths.is_empty() {
            // No segments exist — treat as a fresh partition.
            return Self::create(dir, stream_name, partition_id);
        }

        // Open all but the last as sealed readers.
        let mut sealed_readers = Vec::new();
        for path in &seg_paths[..seg_paths.len() - 1] {
            sealed_readers.push(SegmentReader::open(path)?);
        }

        // Read metadata from the last segment to derive base_offset and next_offset.
        let last_seg_path = &seg_paths[seg_paths.len() - 1];
        let last_reader = SegmentReader::open(last_seg_path)?;
        let base_offset = last_reader.base_offset();

        // Derive next_offset from the last offset across all segments.
        let mut next_offset: u64 = 0;
        for reader in &sealed_readers {
            if let Some(last) = reader.last_offset()? {
                next_offset = next_offset.max(last + 1);
            }
        }
        if let Some(last) = last_reader.last_offset()? {
            next_offset = next_offset.max(last + 1);
        }

        // Open the WAL and replay any records that weren't flushed to segments.
        let wal_path = dir.join("wal.log");
        let wal_records = replay_wal(&wal_path)?;

        let mut wal = WalWriter::open(&wal_path)?;

        // Open the last segment as the active writer and replay WAL records
        // that are not yet in any segment.
        let replay_base = next_offset;
        let current_size = fs::metadata(last_seg_path)?.len();
        let mut active_writer =
            SegmentWriter::open_append(last_seg_path, base_offset, current_size)?;

        let mut replayed = 0usize;
        for wal_rec in &wal_records {
            if wal_rec.offset.0 >= replay_base {
                active_writer.append(wal_rec.offset, wal_rec.timestamp, &wal_rec.record)?;
                next_offset = wal_rec.offset.0 + 1;
                replayed += 1;
            }
        }

        if replayed > 0 {
            active_writer.sync()?;
            info!(
                stream = stream_name,
                partition = partition_id,
                replayed,
                "WAL replay complete"
            );
        }

        // Truncate the WAL now that all records are in segments.
        wal.truncate()?;

        Ok(Self {
            dir: dir.to_path_buf(),
            active_writer,
            sealed_readers,
            wal,
            next_offset,
            stream_name: stream_name.to_string(),
            partition_id,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            seal_tx: None,
        })
    }

    /// Append a record to this partition.
    ///
    /// The record is first written to the WAL (and synced), then to the
    /// active segment. If the segment exceeds `segment_max_bytes` after the
    /// write, a new segment is rolled.
    pub fn append(&mut self, record: &Record) -> io::Result<Offset> {
        let offset = Offset(self.next_offset);
        let timestamp = now_nanos();

        // WAL first — durable after sync_data inside WalWriter::append.
        if let Err(e) = self.wal.append(
            &self.stream_name,
            self.partition_id,
            offset,
            timestamp,
            record,
        ) {
            log_write_error(&self.stream_name, self.partition_id, "WAL write failed", &e);
            return Err(e);
        }

        // Then segment.
        if let Err(e) = self.active_writer.append(offset, timestamp, record) {
            log_write_error(
                &self.stream_name,
                self.partition_id,
                "segment write failed",
                &e,
            );
            return Err(e);
        }
        self.next_offset += 1;

        // Check if we need to roll the segment.
        if self.active_writer.bytes_written() >= self.segment_max_bytes {
            self.roll_segment()?;
        }

        Ok(offset)
    }

    /// Read records from this partition starting at `from_offset`.
    ///
    /// Reads from sealed segments first, then from the active segment.
    /// The active writer is synced before reading so that recently appended
    /// data is visible.
    pub fn read(&self, from: Offset, max_records: usize) -> io::Result<Vec<StoredRecord>> {
        let mut result = Vec::new();
        let mut remaining = max_records;

        // Read from sealed segments (they are ordered by base_offset).
        for reader in &self.sealed_readers {
            if remaining == 0 {
                break;
            }
            let records = reader.read_from(from.0, remaining)?;
            remaining -= records.len();
            result.extend(records);
        }

        // Read from the active segment.
        if remaining > 0 {
            // Sync so that buffered writes are visible to a new reader.
            self.active_writer.sync()?;

            let active_reader = SegmentReader::open(self.active_writer.path())?;
            let records = active_reader.read_from(from.0, remaining)?;
            result.extend(records);
        }

        Ok(result)
    }

    /// Find the offset of the first record at or after the given timestamp.
    ///
    /// Checks sealed segments (which have time indexes) in reverse order,
    /// then falls back to scanning the active segment.
    pub fn seek_by_time(&self, timestamp: u64) -> io::Result<Offset> {
        // Check sealed segments (they have time indexes)
        for reader in self.sealed_readers.iter().rev() {
            if let Some(first_ts) = reader.first_timestamp() {
                if timestamp >= first_ts {
                    if let Some(offset) = reader.seek_by_time(timestamp) {
                        return Ok(Offset(offset));
                    }
                }
            }
        }
        // Fall back: scan active segment
        self.active_writer.sync()?;
        let active_reader = SegmentReader::open(self.active_writer.path())?;
        let records = active_reader.read_all()?;
        for record in &records {
            if record.timestamp >= timestamp {
                return Ok(record.offset);
            }
        }
        Ok(Offset(self.next_offset))
    }

    /// Enforce retention: delete old sealed segments based on age and size limits.
    /// Never deletes the active segment.
    pub fn enforce_retention(
        &mut self,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> io::Result<RetentionStats> {
        let mut stats = RetentionStats::default();
        let now = now_nanos();
        let age_cutoff_nanos = now.saturating_sub(max_age_secs * 1_000_000_000);

        // Age-based deletion: remove sealed segments where all records are older than cutoff
        let mut indices_to_remove: Vec<usize> = Vec::new();
        for (i, reader) in self.sealed_readers.iter().enumerate() {
            let is_old = match reader.last_timestamp() {
                Some(ts) => ts < age_cutoff_nanos,
                None => {
                    // No time index -- try reading last record
                    match reader.last_offset() {
                        Ok(Some(_)) => false, // has records but no timestamp info -- keep it safe
                        _ => true,            // empty segment, ok to delete
                    }
                }
            };
            if is_old {
                indices_to_remove.push(i);
            }
        }

        // Remove in reverse order so indices stay valid
        for &i in indices_to_remove.iter().rev() {
            let reader = self.sealed_readers.remove(i);
            let seg_path = reader.path().to_path_buf();
            let size = reader.file_size();

            // Delete segment + index files
            let _ = fs::remove_file(&seg_path);
            let _ = fs::remove_file(seg_path.with_extension("idx"));
            let _ = fs::remove_file(seg_path.with_extension("tix"));

            stats.segments_deleted += 1;
            stats.bytes_reclaimed += size;
        }

        // Size-based deletion: remove oldest sealed segments until under limit
        loop {
            let total_size = self.total_bytes();
            if total_size <= max_bytes || self.sealed_readers.is_empty() {
                break;
            }
            let reader = self.sealed_readers.remove(0); // remove oldest
            let seg_path = reader.path().to_path_buf();
            let size = reader.file_size();

            let _ = fs::remove_file(&seg_path);
            let _ = fs::remove_file(seg_path.with_extension("idx"));
            let _ = fs::remove_file(seg_path.with_extension("tix"));

            stats.segments_deleted += 1;
            stats.bytes_reclaimed += size;
        }

        Ok(stats)
    }

    /// Return the next offset to be assigned.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Return the earliest retained offset across sealed and active segments.
    ///
    /// Returns 0 when no records have ever been written (or `next_offset == 0`).
    /// Returns `next_offset` when the stream exists but all records have been
    /// trimmed — callers treat `earliest == next` as empty.
    pub fn earliest_offset(&self) -> u64 {
        if let Some(first) = self.sealed_readers.first() {
            return first.base_offset();
        }
        self.active_writer.base_offset()
    }

    /// Delete sealed segments whose records are entirely below `keep_from`.
    ///
    /// The segment containing `keep_from` is preserved intact (sub-segment
    /// rewrite is out of scope). Safe to call with `keep_from` at or before
    /// the current earliest offset — such calls are no-ops.
    pub fn trim_up_to(&mut self, keep_from: u64) -> io::Result<RetentionStats> {
        let mut stats = RetentionStats::default();
        if keep_from == 0 {
            return Ok(stats);
        }

        // Remove any sealed segment whose last offset is strictly less than
        // `keep_from`. We walk from oldest to newest and stop at the first
        // segment that might contain `keep_from`.
        let mut to_remove: Vec<usize> = Vec::new();
        for (i, reader) in self.sealed_readers.iter().enumerate() {
            let last = match reader.last_offset()? {
                Some(o) => o,
                None => {
                    // Empty sealed segment — safe to drop.
                    to_remove.push(i);
                    continue;
                }
            };
            if last < keep_from {
                to_remove.push(i);
            } else {
                break;
            }
        }

        for &i in to_remove.iter().rev() {
            let reader = self.sealed_readers.remove(i);
            let seg_path = reader.path().to_path_buf();
            let size = reader.file_size();
            let _ = fs::remove_file(&seg_path);
            let _ = fs::remove_file(seg_path.with_extension("idx"));
            let _ = fs::remove_file(seg_path.with_extension("tix"));
            stats.segments_deleted += 1;
            stats.bytes_reclaimed += size;
        }

        Ok(stats)
    }

    /// Drop records at offsets `>= drop_from` at segment granularity.
    ///
    /// Sealed segments whose `base_offset >= drop_from` are deleted. The
    /// active segment is preserved when `drop_from` falls inside it; a
    /// future iteration may rewrite the active segment to strip the tail.
    /// After truncation, `next_offset` is lowered to the new high-water
    /// mark so subsequent appends resume at the correct offset.
    pub fn truncate_from(&mut self, drop_from: u64) -> io::Result<RetentionStats> {
        let mut stats = RetentionStats::default();
        if drop_from >= self.next_offset {
            return Ok(stats);
        }

        // Drop sealed segments whose base_offset >= drop_from.
        let mut to_remove: Vec<usize> = Vec::new();
        for (i, reader) in self.sealed_readers.iter().enumerate() {
            if reader.base_offset() >= drop_from {
                to_remove.push(i);
            }
        }
        for &i in to_remove.iter().rev() {
            let reader = self.sealed_readers.remove(i);
            let seg_path = reader.path().to_path_buf();
            let size = reader.file_size();
            let _ = fs::remove_file(&seg_path);
            let _ = fs::remove_file(seg_path.with_extension("idx"));
            let _ = fs::remove_file(seg_path.with_extension("tix"));
            stats.segments_deleted += 1;
            stats.bytes_reclaimed += size;
        }

        // Recompute next_offset from the remaining segments.
        let mut new_next: u64 = 0;
        for reader in &self.sealed_readers {
            if let Some(last) = reader.last_offset()? {
                new_next = new_next.max(last + 1);
            }
        }
        // If the active segment survives and it contains records at or past
        // `drop_from`, sub-segment truncation would require a rewrite. For
        // the trait contract we accept this as best-effort: leave the active
        // segment alone unless its base_offset is past drop_from, in which
        // case it's a fresh empty segment the next append will reuse.
        if self.active_writer.base_offset() >= drop_from {
            // Segment created but no records written: reset next_offset to
            // drop_from.
            new_next = drop_from;
        } else if let Some(last) = SegmentReader::open(self.active_writer.path())?.last_offset()? {
            new_next = new_next.max(last + 1);
        }

        self.next_offset = new_next.max(drop_from.min(self.next_offset));
        // Truncate WAL — its contents may reference offsets we just deleted.
        self.wal.truncate()?;
        Ok(stats)
    }

    /// Total bytes across all segments (sealed + active).
    pub fn total_bytes(&self) -> u64 {
        let sealed: u64 = self.sealed_readers.iter().map(|r| r.file_size()).sum();
        sealed + self.active_writer.bytes_written()
    }

    /// Set the notification sender for sealed segments.
    pub fn set_seal_notifier(&mut self, tx: mpsc::UnboundedSender<SealedSegmentInfo>) {
        self.seal_tx = Some(tx);
    }

    /// Clone the seal notification sender, if one has been set.
    pub fn seal_tx_clone(&self) -> Option<mpsc::UnboundedSender<SealedSegmentInfo>> {
        self.seal_tx.clone()
    }

    /// Override the segment max bytes threshold (for testing).
    #[cfg(test)]
    pub(crate) fn set_segment_max_bytes(&mut self, max: u64) {
        self.segment_max_bytes = max;
    }

    /// Roll the active segment: seal it, build indexes, open a reader for it,
    /// and create a new active segment starting at `next_offset`.
    fn roll_segment(&mut self) -> io::Result<()> {
        // Sync the current active writer.
        self.active_writer.sync()?;

        // Build indexes for the sealed segment.
        let seg_path = self.active_writer.path().to_path_buf();
        self.build_indexes(&seg_path)?;

        // Open the current segment as a sealed reader (will load the new index files).
        let sealed = SegmentReader::open(&seg_path)?;

        // Collect metadata for the sealed segment notification.
        let info = SealedSegmentInfo {
            stream_name: self.stream_name.clone(),
            partition_id: self.partition_id,
            seg_path: seg_path.clone(),
            base_offset: sealed.base_offset(),
            end_offset: self.next_offset.saturating_sub(1),
            size_bytes: sealed.file_size(),
            record_count: 0, // Not tracked per-segment currently
            first_timestamp: sealed.first_timestamp().unwrap_or(0),
            last_timestamp: sealed.last_timestamp().unwrap_or(0),
        };

        self.sealed_readers.push(sealed);

        // Create a new segment.
        self.active_writer = SegmentWriter::create(&self.dir, self.next_offset)?;

        // Truncate the WAL — all records prior to this point are in sealed segments.
        self.wal.truncate()?;

        // Send sealed-segment notification (non-blocking, best-effort).
        if let Some(ref tx) = self.seal_tx {
            let _ = tx.send(info);
        }

        Ok(())
    }

    /// Scan the segment at `seg_path` and build companion `.idx` and `.tix`
    /// index files alongside it.
    fn build_indexes(&self, seg_path: &Path) -> io::Result<()> {
        // Open a temporary reader to scan the data. Indexes don't exist yet,
        // so offset_index and time_index will be None — that's fine, we only
        // need sequential scanning here.
        let reader = SegmentReader::open(seg_path)?;
        let index_data = reader.scan_for_index_data()?;

        if index_data.is_empty() {
            return Ok(());
        }

        // Build offset index (.idx).
        let offset_entries: Vec<(u64, u32)> = index_data
            .iter()
            .map(|&(offset, file_pos, _)| (offset, file_pos))
            .collect();
        let idx_path = seg_path.with_extension("idx");
        OffsetIndex::build(&idx_path, &offset_entries)?;

        // Build timestamp index (.tix).
        let time_entries: Vec<(u64, u64)> = index_data
            .iter()
            .map(|&(offset, _, timestamp)| (timestamp, offset))
            .collect();
        let tix_path = seg_path.with_extension("tix");
        TimeIndex::build(&tix_path, &time_entries, time_index::DEFAULT_INTERVAL)?;

        Ok(())
    }
}
