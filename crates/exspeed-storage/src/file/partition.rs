//! One partition: the state readers share with the writer, recovery from
//! disk, and the lock-free read paths.
//!
//! Readers never take the writer's lock and never fsync. They load the high
//! watermark, then the segment list (an [`ArcSwap`]), then each segment's
//! committed length, binary-search the sparse index and `pread` forward.
//! The writer publishes in the opposite order (bytes, index entries,
//! segment length, high watermark), so a reader never sees a partial frame
//! or a record at or beyond the high watermark.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use exspeed_common::Offset;
use exspeed_streams::{ReadBatch, StorageError, StoredRecord, StreamConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::encoding::decode_payload;
use crate::file::fsutil::{atomic_write, fsync_dir, remove_if_exists};
use crate::file::segment::{
    create_segment_file, encode_index, header_bytes, idx_path, load_meta, meta_path,
    parse_seg_name, read_header, save_meta, scan_segment, seg_path, valid_frame_after, FrameError,
    FrameIter, IndexEntry, Segment, SegmentMeta, SegmentStats, INDEX_ENTRY_LEN, SEGMENT_HEADER_LEN,
};

/// Name of the truncation intent marker inside a partition directory.
pub const TRUNCATE_MARKER: &str = "truncate.json";

/// Health of a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionStatus {
    Healthy,
    /// An IO error could not be rolled back. The partition is read-only
    /// until the process restarts and recovery runs.
    Failed {
        reason: String,
    },
}

/// State shared between the writer thread and readers.
pub struct PartitionShared {
    pub stream: String,
    pub dir: PathBuf,
    segments: ArcSwap<Vec<Arc<Segment>>>,
    /// One past the last committed record (durable in sync mode, written in
    /// async mode).
    committed: AtomicU64,
    /// Replication floor: visible = min(committed, floor). `u64::MAX` when
    /// unset.
    floor: AtomicU64,
    /// The high watermark readers see.
    visible: AtomicU64,
    publish_lock: Mutex<()>,
    watch_tx: watch::Sender<u64>,
    failed: AtomicBool,
    failed_reason: Mutex<Option<String>>,
    /// Cached `stream.json`. `None` when the file exists but can't be parsed
    /// — retention and compaction then skip this stream instead of guessing.
    config: Mutex<Option<StreamConfig>>,
}

impl PartitionShared {
    pub fn new(
        stream: &str,
        dir: &Path,
        segments: Vec<Arc<Segment>>,
        next_offset: u64,
        config: Option<StreamConfig>,
    ) -> Self {
        let (watch_tx, _) = watch::channel(next_offset);
        Self {
            stream: stream.to_string(),
            dir: dir.to_path_buf(),
            segments: ArcSwap::from_pointee(segments),
            committed: AtomicU64::new(next_offset),
            floor: AtomicU64::new(u64::MAX),
            visible: AtomicU64::new(next_offset),
            publish_lock: Mutex::new(()),
            watch_tx,
            failed: AtomicBool::new(false),
            failed_reason: Mutex::new(None),
            config: Mutex::new(config),
        }
    }

    pub fn segments(&self) -> Arc<Vec<Arc<Segment>>> {
        self.segments.load_full()
    }

    /// Writer only.
    pub fn set_segments(&self, list: Vec<Arc<Segment>>) {
        self.segments.store(Arc::new(list));
    }

    /// The high watermark: one past the last record readers may see.
    pub fn high_watermark(&self) -> u64 {
        self.visible.load(Ordering::Acquire)
    }

    pub fn committed(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }

    /// Offset of the first retained record (the first segment's base),
    /// never above the high watermark.
    pub fn earliest(&self) -> u64 {
        let hwm = self.high_watermark();
        self.segments
            .load()
            .first()
            .map_or(hwm, |s| s.base_offset)
            .min(hwm)
    }

    pub fn total_bytes(&self) -> u64 {
        self.segments.load().iter().map(|s| s.len()).sum()
    }

    /// Writer only: committed data now ends at `next`.
    pub fn publish_committed(&self, next: u64) {
        let _g = self.publish_lock.lock().unwrap();
        self.committed.store(next, Ordering::Release);
        self.recompute_visible();
    }

    /// Hold the high watermark at or below `floor` (e.g. the offset a
    /// replication quorum has acknowledged); `None` removes the floor.
    pub fn set_floor(&self, floor: Option<u64>) {
        let _g = self.publish_lock.lock().unwrap();
        self.floor
            .store(floor.unwrap_or(u64::MAX), Ordering::Release);
        self.recompute_visible();
    }

    fn recompute_visible(&self) {
        let vis = self
            .committed
            .load(Ordering::Acquire)
            .min(self.floor.load(Ordering::Acquire));
        self.visible.store(vis, Ordering::Release);
        self.watch_tx.send_if_modified(|v| {
            if *v != vis {
                *v = vis;
                true
            } else {
                false
            }
        });
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.watch_tx.subscribe()
    }

    pub fn fail(&self, reason: String) {
        tracing::error!(
            stream = self.stream.as_str(),
            reason = reason.as_str(),
            "partition fenced read-only after an unrecoverable storage error"
        );
        *self.failed_reason.lock().unwrap() = Some(reason);
        self.failed.store(true, Ordering::Release);
    }

    pub fn failure(&self) -> Option<String> {
        if self.failed.load(Ordering::Acquire) {
            self.failed_reason.lock().unwrap().clone()
        } else {
            None
        }
    }

    pub fn status(&self) -> PartitionStatus {
        match self.failure() {
            None => PartitionStatus::Healthy,
            Some(reason) => PartitionStatus::Failed { reason },
        }
    }

    pub fn failed_error(&self) -> Option<StorageError> {
        self.failure().map(|reason| StorageError::PartitionFailed {
            stream: self.stream.clone(),
            reason,
        })
    }

    pub fn config(&self) -> Option<StreamConfig> {
        self.config.lock().unwrap().clone()
    }

    pub fn set_config(&self, cfg: StreamConfig) {
        *self.config.lock().unwrap() = Some(cfg);
    }

    /// Read records at offsets `>= from`, below the high watermark. `strict`
    /// reports `from` below the earliest retained offset as
    /// [`StorageError::OffsetOutOfRange`]; otherwise `from` is clamped.
    pub fn read(
        &self,
        from: u64,
        max_records: usize,
        max_bytes: usize,
        strict: bool,
    ) -> Result<ReadBatch, StorageError> {
        let hwm = self.high_watermark();
        let list = self.segments();
        let earliest = list.first().map_or(hwm, |s| s.base_offset).min(hwm);
        let mut from = from;
        if from < earliest {
            if strict && from < hwm {
                return Err(StorageError::OffsetOutOfRange {
                    requested: from,
                    earliest,
                });
            }
            from = earliest;
        }

        let mut out: Vec<StoredRecord> = Vec::new();
        let mut bytes = 0usize;
        if from < hwm && max_records > 0 {
            let start = list
                .partition_point(|s| s.base_offset <= from)
                .saturating_sub(1);
            // Initial read size: roughly what the limits ask for.
            let chunk = max_bytes
                .min(max_records.saturating_mul(256))
                .saturating_add(4096)
                .clamp(8 * 1024, 1 << 20);
            'segments: for seg in &list[start..] {
                if seg.base_offset >= hwm {
                    break;
                }
                let end = seg.len();
                let pos = if from > seg.base_offset {
                    seg.position_for_offset(from)?.min(end)
                } else {
                    SEGMENT_HEADER_LEN
                };
                let mut it = FrameIter::new(seg.file(), pos, end, chunk);
                loop {
                    let f = match it.next_frame() {
                        Ok(Some(f)) => f,
                        Ok(None) => break,
                        // Truncated underneath us (truncate_from): stop.
                        Err(FrameError::Short { .. }) => break,
                        Err(e) => {
                            return Err(StorageError::CorruptedRecord {
                                offset: from,
                                reason: e.into_io(&seg.path).to_string(),
                            })
                        }
                    };
                    if f.offset >= hwm {
                        break 'segments;
                    }
                    if f.offset < from {
                        continue;
                    }
                    let rec = decode_payload(&f.payload).map_err(|reason| {
                        StorageError::CorruptedRecord {
                            offset: f.offset,
                            reason,
                        }
                    })?;
                    let size = rec.value.len()
                        + rec.subject.len()
                        + rec.key.as_ref().map_or(0, |k| k.len());
                    if !out.is_empty() && bytes + size > max_bytes {
                        break 'segments;
                    }
                    bytes += size;
                    out.push(rec);
                    if out.len() >= max_records {
                        break 'segments;
                    }
                }
            }
        }
        // An empty read below the high watermark means every offset in
        // `[from, hwm)` is a gap (compaction); skip past it.
        let next_offset = out.last().map_or(from.max(hwm), |r| r.offset.0 + 1);
        Ok(ReadBatch {
            records: out,
            next_offset: Offset(next_offset),
            high_watermark: Offset(hwm),
        })
    }

    /// Offset of the first record with timestamp `>= ts`, or the high
    /// watermark when there is none.
    pub fn seek_by_time(&self, ts: u64) -> Result<u64, StorageError> {
        let hwm = self.high_watermark();
        let list = self.segments();
        for seg in list.iter() {
            if seg.base_offset >= hwm {
                break;
            }
            match seg.stats().max_ts {
                Some(max) if max >= ts => {}
                _ => continue,
            }
            let end = seg.len();
            let pos = seg.position_for_time(ts)?.min(end);
            let mut it = FrameIter::new(seg.file(), pos, end, 64 * 1024);
            loop {
                match it.next_frame() {
                    Ok(Some(f)) => {
                        if f.offset >= hwm {
                            return Ok(hwm);
                        }
                        if f.timestamp >= ts {
                            return Ok(f.offset);
                        }
                    }
                    Ok(None) | Err(FrameError::Short { .. }) => break,
                    Err(e) => {
                        return Err(StorageError::CorruptedRecord {
                            offset: seg.base_offset,
                            reason: e.into_io(&seg.path).to_string(),
                        })
                    }
                }
            }
        }
        Ok(hwm)
    }
}

/// Durability mode of a partition writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// fsync before acknowledging (and before data becomes visible).
    Sync,
    /// Acknowledge after the write; fsync in the background.
    Async,
}

/// What recovery found on disk: everything the writer needs to resume.
pub struct Recovered {
    pub segments: Vec<Arc<Segment>>,
    /// Append handle on the active segment.
    pub active_file: File,
    pub active_entries: Vec<IndexEntry>,
    pub active_stats: SegmentStats,
    pub next_offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct TruncateMarker {
    drop_from: u64,
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Segment base offsets present in `dir`, ascending.
pub fn list_segment_bases(dir: &Path) -> io::Result<Vec<u64>> {
    let mut bases: Vec<u64> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| parse_seg_name(e.file_name().to_str()?))
        .collect();
    bases.sort_unstable();
    Ok(bases)
}

/// Remove leftovers of interrupted operations: `*.tmp` sidecars,
/// `*.compacting` rewrites, and `.idx` / `.meta` files whose segment is gone.
fn cleanup_dir(dir: &Path, bases: &[u64]) -> io::Result<()> {
    let mut removed = false;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let orphan = |ext: &str| {
            name.strip_suffix(ext)
                .and_then(|stem| stem.parse::<u64>().ok())
                .is_some_and(|b| bases.binary_search(&b).is_err())
        };
        if name.ends_with(".tmp")
            || name.ends_with(".compacting")
            || orphan(".idx")
            || orphan(".meta")
        {
            remove_if_exists(&entry.path())?;
            removed = true;
        }
    }
    if removed {
        fsync_dir(dir)?;
    }
    Ok(())
}

/// Open a sealed segment from its `.meta`, rebuilding the index and metadata
/// with a scan when they are missing or don't match the file (a crash
/// during roll or compaction).
fn open_sealed(dir: &Path, base: u64) -> io::Result<Segment> {
    let path = seg_path(dir, base);
    let file_len = fs::metadata(&path)?.len();
    if let Some(meta) = load_meta(&meta_path(dir, base)) {
        let idx_len = fs::metadata(idx_path(dir, base)).map(|m| m.len()).ok();
        if meta.base_offset == base
            && meta.len == file_len
            && idx_len == Some(meta.index_entries * INDEX_ENTRY_LEN as u64)
        {
            let seg = Segment::new_sealed(dir, &meta)?;
            if read_header(seg.file(), &path)? != base {
                return Err(invalid(format!(
                    "{}: header base offset does not match file name",
                    path.display()
                )));
            }
            return Ok(seg);
        }
    }
    warn!(segment = %path.display(), "segment metadata missing or stale; rebuilding index");
    let file = File::open(&path)?;
    if read_header(&file, &path)? != base {
        return Err(invalid(format!(
            "{}: header base offset does not match file name",
            path.display()
        )));
    }
    let scan = scan_segment(&file, base, file_len)?;
    if let Some(stop) = scan.stop {
        // Sealed segments were fsynced in full before the roll completed,
        // so a bad frame here is real corruption.
        return Err(invalid(format!(
            "{}: corrupt sealed segment at byte {}: {}",
            path.display(),
            stop.pos,
            stop.reason
        )));
    }
    let meta = SegmentMeta {
        base_offset: base,
        len: scan.stats.len,
        end_offset: scan.stats.end_offset,
        first_ts: scan.stats.first_ts,
        max_ts: scan.stats.max_ts,
        records: scan.stats.records,
        index_entries: scan.entries.len() as u64,
    };
    atomic_write(&idx_path(dir, base), &encode_index(&scan.entries))?;
    save_meta(&meta_path(dir, base), &meta)?;
    Segment::new_sealed(dir, &meta)
}

/// Recover a partition directory: finish an interrupted truncation, clean
/// up leftovers, open sealed segments from their metadata and tail-scan the
/// active segment.
///
/// A bad frame in the active segment is a torn tail when nothing valid
/// follows it; it is truncated away. If valid frames follow it, the file is
/// corrupt in the middle: in [`Durability::Sync`] every acknowledged record
/// was fsynced, so this is real corruption and recovery fails loudly. In
/// [`Durability::Async`] the kernel may have written later pages before
/// earlier ones before a crash, so the tail is truncated at the first bad
/// frame (acknowledged-but-unsynced data is the documented async risk).
pub fn recover(dir: &Path, durability: Durability) -> io::Result<Recovered> {
    fs::create_dir_all(dir)?;
    let marker = dir.join(TRUNCATE_MARKER);
    if let Ok(data) = fs::read(&marker) {
        let m: TruncateMarker = serde_json::from_slice(&data)
            .map_err(|e| invalid(format!("{}: {e}", marker.display())))?;
        info!(dir = %dir.display(), drop_from = m.drop_from, "finishing interrupted truncation");
        apply_truncation(dir, m.drop_from)?;
        remove_if_exists(&marker)?;
        fsync_dir(dir)?;
    }

    let mut bases = list_segment_bases(dir)?;
    cleanup_dir(dir, &bases)?;
    if bases.is_empty() {
        drop(create_segment_file(dir, 0)?);
        bases.push(0);
    }

    let active_base = *bases.last().unwrap();
    let mut segments: Vec<Arc<Segment>> = Vec::with_capacity(bases.len());
    let mut prev_end: Option<u64> = None;
    for &base in &bases[..bases.len() - 1] {
        let seg = open_sealed(dir, base)?;
        if let Some(pe) = prev_end {
            if base < pe {
                return Err(invalid(format!(
                    "{}: segment base {base} overlaps the previous segment (ends at {pe})",
                    dir.display()
                )));
            }
        }
        prev_end = Some(seg.end_offset());
        segments.push(Arc::new(seg));
    }
    if let Some(pe) = prev_end {
        if active_base < pe {
            return Err(invalid(format!(
                "{}: active segment base {active_base} overlaps the previous segment (ends at {pe})",
                dir.display()
            )));
        }
    }

    // Active segment: CRC-validating tail scan.
    let path = seg_path(dir, active_base);
    let mut file = OpenOptions::new().read(true).append(true).open(&path)?;
    if file.metadata()?.len() < SEGMENT_HEADER_LEN {
        // Crash while the segment was being created: it never held data.
        warn!(segment = %path.display(), "rewriting torn segment header");
        file.set_len(0)?;
        std::io::Write::write_all(&mut file, &header_bytes(active_base))?;
        file.sync_all()?;
    }
    if read_header(&file, &path)? != active_base {
        return Err(invalid(format!(
            "{}: header base offset does not match file name",
            path.display()
        )));
    }
    let file_len = file.metadata()?.len();
    let scan = scan_segment(&file, active_base, file_len)?;
    if let Some(stop) = &scan.stop {
        let last = scan
            .stats
            .end_offset
            .checked_sub(1)
            .filter(|_| scan.stats.records > 0);
        if durability == Durability::Sync && valid_frame_after(&file, stop.pos, file_len, last)? {
            return Err(invalid(format!(
                "{}: corruption in the middle of the active segment at byte {} ({}); \
                 valid records follow it, so this is not a torn write. Refusing to \
                 truncate acknowledged data — restore the file or remove the damaged \
                 tail manually",
                path.display(),
                stop.pos,
                stop.reason
            )));
        }
        warn!(
            segment = %path.display(),
            at = stop.pos,
            dropped_bytes = file_len - scan.stats.len,
            reason = stop.reason.as_str(),
            "truncating torn tail of active segment"
        );
        file.set_len(scan.stats.len)?;
        file.sync_all()?;
    }
    atomic_write(&idx_path(dir, active_base), &encode_index(&scan.entries))?;
    remove_if_exists(&meta_path(dir, active_base))?;

    let next_offset = scan
        .stats
        .end_offset
        .max(active_base)
        .max(prev_end.unwrap_or(0));
    let active = Segment::new_active(dir, active_base, scan.stats, scan.entries.clone())?;
    segments.push(Arc::new(active));
    Ok(Recovered {
        segments,
        active_file: file,
        active_entries: scan.entries,
        active_stats: scan.stats,
        next_offset,
    })
}

/// Write the truncation intent marker (tmp + rename + dir fsync).
pub fn write_truncate_marker(dir: &Path, drop_from: u64) -> io::Result<()> {
    let json = serde_json::to_vec(&TruncateMarker { drop_from }).map_err(io::Error::other)?;
    atomic_write(&dir.join(TRUNCATE_MARKER), &json)
}

/// Drop every record at offset `>= drop_from` from the files in `dir`.
/// Idempotent, so recovery can re-run it after a crash:
///
/// 1. delete every segment whose base is `>= drop_from` (newest first);
/// 2. truncate the last remaining segment just before its first record
///    `>= drop_from`;
/// 3. if that segment now ends exactly at `drop_from` it becomes the active
///    segment (its `.meta` is removed); otherwise it stays sealed (fresh
///    `.idx` + `.meta`) and an empty segment with base `drop_from` is
///    created, so the next offset is `drop_from` after any restart.
pub fn apply_truncation(dir: &Path, drop_from: u64) -> io::Result<()> {
    let bases = list_segment_bases(dir)?;
    for &base in bases.iter().rev().filter(|&&b| b >= drop_from) {
        remove_if_exists(&seg_path(dir, base))?;
        remove_if_exists(&idx_path(dir, base))?;
        remove_if_exists(&meta_path(dir, base))?;
    }
    fsync_dir(dir)?;

    let Some(&base) = bases.iter().rev().find(|&&b| b < drop_from) else {
        drop(create_segment_file(dir, drop_from)?);
        return Ok(());
    };
    let path = seg_path(dir, base);
    let file = OpenOptions::new().read(true).write(true).open(&path)?;
    let file_len = file.metadata()?.len();
    let mut it = FrameIter::new(&file, SEGMENT_HEADER_LEN, file_len, 1 << 20);
    let mut cut = SEGMENT_HEADER_LEN;
    loop {
        match it.next_frame() {
            Ok(Some(f)) if f.offset < drop_from => cut = f.pos + f.size,
            Ok(_) => break,
            Err(FrameError::Io(e)) => return Err(e),
            // A torn tail only exists past the records we keep.
            Err(_) => break,
        }
    }
    file.set_len(cut)?;
    file.sync_all()?;
    let scan = scan_segment(&file, base, cut)?;
    if scan.stats.end_offset.max(base) == drop_from {
        remove_if_exists(&meta_path(dir, base))?;
        atomic_write(&idx_path(dir, base), &encode_index(&scan.entries))?;
    } else {
        let meta = SegmentMeta {
            base_offset: base,
            len: scan.stats.len,
            end_offset: scan.stats.end_offset,
            first_ts: scan.stats.first_ts,
            max_ts: scan.stats.max_ts,
            records: scan.stats.records,
            index_entries: scan.entries.len() as u64,
        };
        atomic_write(&idx_path(dir, base), &encode_index(&scan.entries))?;
        save_meta(&meta_path(dir, base), &meta)?;
        drop(create_segment_file(dir, drop_from)?);
    }
    fsync_dir(dir)
}
