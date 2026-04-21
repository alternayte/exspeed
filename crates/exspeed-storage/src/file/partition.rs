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

/// Pick a base_offset for a throw-away placeholder segment whose filename
/// is guaranteed not to collide with the old active segment, any doomed
/// sealed segment, or any file the truncate rewrite is about to create.
/// We walk down from `u64::MAX` — real segments never reach that range.
fn pick_placeholder_base(
    dir: &Path,
    active_path: &Path,
    doomed_sealed: &[PathBuf],
) -> io::Result<u64> {
    let mut candidate: u64 = u64::MAX;
    loop {
        let filename = format!("{:020}.seg", candidate);
        let path = dir.join(&filename);
        let collides = path == active_path
            || doomed_sealed.iter().any(|p| p == &path)
            || path.exists();
        if !collides {
            return Ok(candidate);
        }
        candidate = candidate
            .checked_sub(1)
            .ok_or_else(|| io::Error::other("no placeholder base_offset available"))?;
    }
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

/// Manages a single partition's on-disk state: active segment writer and
/// sealed segment readers.
///
/// v0.2.0 removed the separate WAL file — the active segment is the sole
/// durability journal. Every `append` / `append_batch` writes directly to
/// the segment and (in sync mode) fsyncs it. In async mode the
/// `SegmentSyncer` task fsyncs periodically on a cloned file handle.
pub struct Partition {
    dir: PathBuf,
    active_writer: SegmentWriter,
    sealed_readers: Vec<SegmentReader>,
    next_offset: u64,
    stream_name: String,
    partition_id: u32,
    segment_max_bytes: u64,
    seal_tx: Option<mpsc::UnboundedSender<SealedSegmentInfo>>,
}

impl Partition {
    /// Create a brand-new partition directory with its first segment.
    ///
    /// v0.2.0 removed the separate WAL file — the segment is the sole
    /// durability journal from the first write onwards.
    pub fn create(dir: &Path, stream_name: &str, partition_id: u32) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let writer = SegmentWriter::create(dir, 0)?;

        Ok(Self {
            dir: dir.to_path_buf(),
            active_writer: writer,
            sealed_readers: Vec::new(),
            next_offset: 0,
            stream_name: stream_name.to_string(),
            partition_id,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            seal_tx: None,
        })
    }

    /// Open an existing partition directory, recovering state via a
    /// CRC-validating tail scan of the active segment.
    ///
    /// v0.2.0 removed the separate `wal.log` file — recovery is now unified
    /// through `SegmentWriter::recover_tail`, which walks the active segment
    /// from the header forward and truncates any torn/corrupt tail. If a
    /// legacy `wal.log` is present the open fails fast rather than silently
    /// ignoring durable data the caller expected to be replayed.
    pub fn open(dir: &Path, stream_name: &str, partition_id: u32) -> io::Result<Self> {
        // Fail-fast if a legacy WAL is present. Pre-v0.2.0 data is unsupported.
        let legacy_wal = dir.join("wal.log");
        if legacy_wal.exists() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "legacy wal.log found at {}; v0.2.0 removed the separate \
                     WAL file. Either delete your data dir (fresh start) or \
                     downgrade to exspeed 0.1.1.",
                    legacy_wal.display()
                ),
            ));
        }

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

        // Derive next_offset from sealed segments first.
        let mut next_offset: u64 = 0;
        for reader in &sealed_readers {
            if let Some(last) = reader.last_offset()? {
                next_offset = next_offset.max(last + 1);
            }
        }

        // Tail-scan the active segment: validate every frame's CRC and
        // truncate at the first torn/corrupt record. Replaces WAL replay.
        let last_seg_path = &seg_paths[seg_paths.len() - 1];
        let (max_offset_in_active, _max_ts, current_size) =
            SegmentWriter::recover_tail(last_seg_path)?;

        if let Some(m) = max_offset_in_active {
            next_offset = next_offset.max(m + 1);
        }

        info!(
            stream = stream_name,
            partition = partition_id,
            segment = %last_seg_path.display(),
            current_size,
            next_offset,
            "partition recovery complete (tail-scan)"
        );

        // Open the active segment for append at the post-recovery length.
        let last_reader = SegmentReader::open(last_seg_path)?;
        let base_offset = last_reader.base_offset();
        let active_writer =
            SegmentWriter::open_append(last_seg_path, base_offset, current_size)?;

        Ok(Self {
            dir: dir.to_path_buf(),
            active_writer,
            sealed_readers,
            next_offset,
            stream_name: stream_name.to_string(),
            partition_id,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            seal_tx: None,
        })
    }

    /// Append a record to this partition.
    ///
    /// v0.2.0: the active segment is the sole durability journal. The record
    /// is written to the segment and `sync_data` is called directly (this
    /// used to happen via the WAL). If the segment exceeds `segment_max_bytes`
    /// after the write, a new segment is rolled.
    pub fn append(&mut self, record: &Record) -> io::Result<(Offset, u64)> {
        let offset = Offset(self.next_offset);
        // Honor the caller-supplied timestamp when present (replication
        // follower path preserves the leader's persisted timestamp); else
        // mint a fresh one from the wall clock.
        let timestamp = record.timestamp_ns.unwrap_or_else(now_nanos);

        // Write to the active segment (sole durability path as of v0.2.0).
        if let Err(e) = self.active_writer.append(offset, timestamp, record) {
            log_write_error(
                &self.stream_name,
                self.partition_id,
                "segment write failed",
                &e,
            );
            return Err(e);
        }
        // Fsync the segment directly (previously done via the WAL).
        self.active_writer.sync_data()?;

        self.next_offset += 1;

        // Check if we need to roll the segment.
        if self.active_writer.bytes_written() >= self.segment_max_bytes {
            self.roll_segment()?;
        }

        Ok((offset, timestamp))
    }

    /// Append N records in one shot via `SegmentWriter::append_batch`.
    ///
    /// v0.2.0: the active segment is the sole durability journal. A single
    /// `write_all` lands every record's framed bytes in the page cache; if
    /// `sync_now` is true the writer issues exactly one `sync_data` at the
    /// end (group-commit + fsync). In `Async` mode `sync_now` is false —
    /// the `SegmentSyncer` task handles the periodic fsync on a cloned file
    /// handle.
    ///
    /// On `Err`, the entire batch fails from the caller's perspective. If
    /// the write reached the page cache partially before failing, the next
    /// `Partition::open` will detect the torn tail via `recover_tail` and
    /// truncate to the last durable frame.
    pub fn append_batch(&mut self, records: &[Record], sync_now: bool) -> io::Result<Vec<(Offset, u64)>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 1: assign offsets + timestamps.
        let mut assignments: Vec<(Offset, u64, Record)> = Vec::with_capacity(records.len());
        let mut results: Vec<(Offset, u64)> = Vec::with_capacity(records.len());
        for record in records {
            let offset = Offset(self.next_offset);
            let timestamp = record.timestamp_ns.unwrap_or_else(now_nanos);
            assignments.push((offset, timestamp, record.clone()));
            results.push((offset, timestamp));
            self.next_offset += 1;
        }

        // Phase 2: ONE segment append + (conditional) sync_data. No WAL step.
        if let Err(e) = self.active_writer.append_batch(&assignments, sync_now) {
            log_write_error(
                &self.stream_name,
                self.partition_id,
                "segment batch write failed",
                &e,
            );
            self.next_offset -= records.len() as u64;
            return Err(e);
        }

        // Segment roll AFTER the batch completes.
        if self.active_writer.bytes_written() >= self.segment_max_bytes {
            self.roll_segment()?;
        }

        Ok(results)
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

    /// Remove the sealed segment at `i` from `self.sealed_readers`, delete
    /// its `.seg` / `.idx` / `.tix` files on disk, and account for the
    /// reclaimed space in `stats`. Errors from the file deletes are swallowed
    /// (they're best-effort) — the in-memory state is always updated.
    fn remove_sealed_segment(&mut self, i: usize, stats: &mut RetentionStats) {
        let reader = self.sealed_readers.remove(i);
        let seg_path = reader.path().to_path_buf();
        let size = reader.file_size();

        let _ = fs::remove_file(&seg_path);
        let _ = fs::remove_file(seg_path.with_extension("idx"));
        let _ = fs::remove_file(seg_path.with_extension("tix"));

        stats.segments_deleted += 1;
        stats.bytes_reclaimed += size;
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
            self.remove_sealed_segment(i, &mut stats);
        }

        // Size-based deletion: remove oldest sealed segments until under limit
        loop {
            let total_size = self.total_bytes();
            if total_size <= max_bytes || self.sealed_readers.is_empty() {
                break;
            }
            self.remove_sealed_segment(0, &mut stats);
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
            self.remove_sealed_segment(i, &mut stats);
        }

        Ok(stats)
    }

    /// Drop records at offsets `>= drop_from`. Record-exact truncation —
    /// the partition is rewritten so that subsequent `stream_bounds`
    /// returns `next == drop_from`, and the next `append` assigns exactly
    /// `drop_from`.
    ///
    /// Algorithm:
    ///   1. Classify the segment that straddles `drop_from` (the one whose
    ///      records include offsets `< drop_from` AND `>= drop_from`), if any.
    ///      It is either the last surviving sealed segment or the active
    ///      segment.
    ///   2. Read the surviving records (offset `< drop_from`) from the
    ///      straddled segment into memory.
    ///   3. Delete every segment whose records are entirely `>= drop_from`
    ///      (sealed and the active segment if it falls past drop_from).
    ///   4. Delete the straddled segment's file.
    ///   5. Create a fresh active segment at the straddled segment's
    ///      `base_offset` and replay the surviving records into it.
    ///   6. Set `next_offset = drop_from`.
    ///
    /// Called only on the follower's divergent-history recovery path, so
    /// the rewrite cost (bounded by `replication_lag`) is acceptable;
    /// correctness over speed.
    pub fn truncate_from(&mut self, drop_from: u64) -> io::Result<RetentionStats> {
        let mut stats = RetentionStats::default();
        if drop_from >= self.next_offset {
            return Ok(stats);
        }

        // ── Step 1: Classify segments. ────────────────────────────────────
        //
        // Sealed segments are ordered by base_offset. We split them into:
        //   - kept:    entirely below drop_from (base < drop_from AND
        //              last_offset < drop_from)
        //   - straddle: contains drop_from (some records below, some at/past)
        //   - gone:    base >= drop_from (entirely at or past drop_from)
        //
        // The active segment is either straddled, entirely gone, or entirely
        // below drop_from (which can't happen here because drop_from <
        // next_offset, and the active always contains next_offset - 1 when
        // the stream has records).

        enum StraddleSegment {
            Sealed(usize, PathBuf, u64), // (index in sealed_readers, path, base_offset)
            Active(PathBuf, u64),         // (path, base_offset)
            None,                          // no straddle (e.g. drop_from == 0)
        }

        let active_base = self.active_writer.base_offset();
        let active_path = self.active_writer.path().to_path_buf();

        let straddle = if drop_from == 0 {
            StraddleSegment::None
        } else if active_base < drop_from {
            // drop_from - 1 is at or past active_base → active is the straddle.
            StraddleSegment::Active(active_path.clone(), active_base)
        } else {
            // drop_from - 1 is in some sealed segment. Find the last sealed
            // segment whose base_offset < drop_from.
            match self
                .sealed_readers
                .iter()
                .rposition(|r| r.base_offset() < drop_from)
            {
                Some(i) => {
                    let r = &self.sealed_readers[i];
                    StraddleSegment::Sealed(i, r.path().to_path_buf(), r.base_offset())
                }
                None => StraddleSegment::None,
            }
        };

        // ── Step 2: Read surviving records from the straddle. ─────────────
        //
        // Do this BEFORE any deletion so a read error leaves the partition
        // intact.
        let (replay_records, replay_base_offset): (Vec<StoredRecord>, u64) = match &straddle {
            StraddleSegment::None => (Vec::new(), drop_from),
            StraddleSegment::Sealed(_i, path, base) => {
                let reader = SegmentReader::open(path)?;
                let records: Vec<StoredRecord> = reader
                    .read_all()?
                    .into_iter()
                    .filter(|r| r.offset.0 < drop_from)
                    .collect();
                (records, *base)
            }
            StraddleSegment::Active(_path, base) => {
                self.active_writer.sync()?;
                let reader = SegmentReader::open(&active_path)?;
                let records: Vec<StoredRecord> = reader
                    .read_all()?
                    .into_iter()
                    .filter(|r| r.offset.0 < drop_from)
                    .collect();
                (records, *base)
            }
        };

        // ── Step 3: Determine what to delete. ─────────────────────────────
        //
        // We'll delete the straddle (if any) + all sealed segments strictly
        // past the straddle + the old active segment (whether or not it's
        // the straddle). Kept sealed segments stay untouched.

        let kept_sealed_count = match &straddle {
            StraddleSegment::Sealed(i, _, _) => *i, // sealed below the straddle
            StraddleSegment::Active(_, _) => self.sealed_readers.len(),
            StraddleSegment::None => {
                // drop_from is 0 or earlier than every sealed segment.
                // Keep nothing.
                0
            }
        };

        // ── Step 4: Close and delete doomed segments. ─────────────────────
        //
        // Close the active writer first by replacing it with a placeholder.
        // The placeholder's base_offset is picked to avoid colliding with
        // any retained or doomed segment filename.
        let sealed_doomed_paths: Vec<PathBuf> = self
            .sealed_readers
            .iter()
            .skip(kept_sealed_count)
            .map(|r| r.path().to_path_buf())
            .collect();
        let placeholder_base =
            pick_placeholder_base(&self.dir, &active_path, &sealed_doomed_paths)?;
        let placeholder = SegmentWriter::create(&self.dir, placeholder_base)?;
        let placeholder_path = placeholder.path().to_path_buf();
        let old_active_size = self.active_writer.bytes_written();
        drop(std::mem::replace(&mut self.active_writer, placeholder));

        // Drop doomed sealed segments (straddle + everything past it) via
        // the shared helper. Walk in reverse so indices stay valid.
        while self.sealed_readers.len() > kept_sealed_count {
            self.remove_sealed_segment(self.sealed_readers.len() - 1, &mut stats);
        }

        // Drop the old active segment's files. It isn't a "sealed" segment
        // for stats purposes (no increment to `segments_deleted`), but its
        // bytes are reclaimed.
        let _ = fs::remove_file(&active_path);
        let _ = fs::remove_file(active_path.with_extension("idx"));
        let _ = fs::remove_file(active_path.with_extension("tix"));
        stats.bytes_reclaimed += old_active_size;

        // ── Step 5: Build the new active segment and replay. ──────────────
        let new_active = SegmentWriter::create(&self.dir, replay_base_offset)?;
        drop(std::mem::replace(&mut self.active_writer, new_active));
        // Remove the placeholder now that the real writer owns a different file.
        let _ = fs::remove_file(&placeholder_path);
        let _ = fs::remove_file(placeholder_path.with_extension("idx"));
        let _ = fs::remove_file(placeholder_path.with_extension("tix"));

        for stored in &replay_records {
            let record = Record {
                key: stored.key.clone(),
                value: stored.value.clone(),
                subject: stored.subject.clone(),
                headers: stored.headers.clone(),
                timestamp_ns: None,
            };
            self.active_writer
                .append(stored.offset, stored.timestamp, &record)?;
        }
        self.active_writer.sync()?;

        // ── Step 6: Update next_offset. ───────────────────────────────────
        self.next_offset = drop_from;

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

    /// Override the segment max bytes threshold. Intended for tests that
    /// need to force segment rolling without writing 256MB of data.
    pub fn set_segment_max_bytes(&mut self, max: u64) {
        self.segment_max_bytes = max;
    }

    /// Clone the active segment's file handle so `SegmentSyncer` can issue
    /// `sync_data` without holding the per-partition `Mutex<Partition>`
    /// that serializes writes. Both handles share the same kernel fd —
    /// concurrent fsync on the syncer's handle does not block writes
    /// through the `SegmentWriter`'s handle, which is exactly the
    /// async-storage-mode semantic.
    pub(crate) fn try_clone_active_segment_file(&self) -> io::Result<std::fs::File> {
        self.active_writer.try_clone_file()
    }

    /// Force a `sync_data` on the active segment. Called by `SegmentSyncer`
    /// in async mode as a last-resort direct sync, and whenever a caller
    /// needs to commit the current batch without waiting for the next timer
    /// tick.
    pub(crate) fn sync_active_segment_now(&mut self) -> io::Result<()> {
        self.active_writer.sync_data()
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
