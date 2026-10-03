//! The per-partition writer thread.
//!
//! Every mutation of a partition — appends, `append_at`, segment rolls,
//! retention, `trim_up_to`, `truncate_from`, installing a compacted segment
//! — runs on one dedicated OS thread, so file IO never blocks the tokio
//! runtime and readers never wait for it. Appends are group-committed: the
//! thread collects requests for up to `flush_window` (or until a threshold
//! is reached), writes them with one `write`, and in sync mode issues one
//! `fdatasync` before publishing the new high watermark and answering.
//!
//! **Fence on error.** If a write or fsync fails, the file is truncated back
//! to the last committed length; the batch's offsets were never visible or
//! acknowledged, so they are handed out again. If that truncation fails too,
//! bytes of unknown state may be on disk past the committed length, so the
//! partition is fenced read-only ([`PartitionShared::fail`]) and no offset is
//! ever assigned again by this process.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use exspeed_common::Offset;
use exspeed_streams::{Record, StorageError, StoredRecord};
use tracing::{error, info, warn};

use crate::encoding::{encode_frame, RecordRef};
use crate::file::fsutil::{fsync_dir, remove_if_exists};
use crate::file::io_errors::is_storage_full;
use crate::file::partition::{
    apply_truncation, recover, write_truncate_marker, Durability, PartitionShared, Recovered,
    TRUNCATE_MARKER,
};
use crate::file::segment::{
    create_segment_file, encode_index, idx_path, meta_path, save_meta, seg_path, IndexBuilder,
    IndexEntry, Segment, SegmentMeta, SegmentStats, INDEX_ENTRY_LEN, SEGMENT_HEADER_LEN,
};

/// Default maximum segment size before rolling: 256 MiB.
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Group-commit tunables. A commit happens when `flush_window` has passed
/// since the first queued request, or when the queued requests reach either
/// threshold, whichever comes first.
#[derive(Debug, Clone, Copy)]
pub struct AppenderConfig {
    pub flush_window: Duration,
    pub flush_threshold_records: usize,
    pub flush_threshold_bytes: usize,
}

impl Default for AppenderConfig {
    fn default() -> Self {
        Self {
            flush_window: Duration::from_micros(500),
            flush_threshold_records: 256,
            flush_threshold_bytes: 1024 * 1024,
        }
    }
}

/// Everything a writer needs besides its partition.
#[derive(Debug, Clone, Copy)]
pub struct WriterOptions {
    pub durability: Durability,
    /// Async mode: fsync at least this often while there is unsynced data.
    pub async_interval: Duration,
    /// Async mode: fsync early once this many bytes are unsynced.
    pub async_threshold_bytes: usize,
    pub appender: AppenderConfig,
    pub segment_max_bytes: u64,
}

/// Fault injection for tests: called before each file operation the writer
/// performs on the active segment.
pub trait IoHooks: Send + Sync {
    fn on_write(&self, _len: usize) -> Option<WriteFault> {
        None
    }
    fn on_sync(&self) -> io::Result<()> {
        Ok(())
    }
    fn on_truncate(&self) -> io::Result<()> {
        Ok(())
    }
}

/// An injected write failure.
pub enum WriteFault {
    /// Fail before writing anything.
    Fail(io::Error),
    /// Write the first `n` bytes, then fail.
    Partial(usize, io::Error),
}

/// Where the writer sends a result.
pub enum Reply<T> {
    Async(tokio::sync::oneshot::Sender<T>),
    Sync(crossbeam_channel::Sender<T>),
}

impl<T> Reply<T> {
    pub fn send(self, v: T) {
        match self {
            Reply::Async(tx) => {
                let _ = tx.send(v);
            }
            Reply::Sync(tx) => {
                let _ = tx.send(v);
            }
        }
    }
}

pub type AppendReply = Reply<Result<Vec<(Offset, u64)>, StorageError>>;

pub enum AppendReq {
    Records {
        records: Vec<Record>,
        reply: AppendReply,
    },
    At {
        records: Vec<StoredRecord>,
        reply: Reply<Result<(), StorageError>>,
    },
}

impl AppendReq {
    fn len(&self) -> usize {
        match self {
            AppendReq::Records { records, .. } => records.len(),
            AppendReq::At { records, .. } => records.len(),
        }
    }
    fn approx_bytes(&self) -> usize {
        match self {
            AppendReq::Records { records, .. } => records
                .iter()
                .map(|r| r.value.len() + r.subject.len() + 32)
                .sum(),
            AppendReq::At { records, .. } => records
                .iter()
                .map(|r| r.value.len() + r.subject.len() + 32)
                .sum(),
        }
    }
    fn fail(self, e: StorageError) {
        match self {
            AppendReq::Records { reply, .. } => reply.send(Err(e)),
            AppendReq::At { reply, .. } => reply.send(Err(e)),
        }
    }
    fn succeed(self, results: Vec<(Offset, u64)>) {
        match self {
            AppendReq::Records { reply, .. } => reply.send(Ok(results)),
            AppendReq::At { reply, .. } => reply.send(Ok(())),
        }
    }
}

/// Stats from retention enforcement or trimming.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionStats {
    pub segments_deleted: u32,
    pub bytes_reclaimed: u64,
}

/// A rewritten (compacted) sealed segment waiting to be installed.
pub struct CompactionJob {
    pub old: Arc<Segment>,
    pub tmp_seg: PathBuf,
    pub tmp_idx: PathBuf,
    pub meta: SegmentMeta,
}

pub enum Cmd {
    Append(AppendReq),
    Truncate {
        drop_from: u64,
        reply: Reply<Result<(), StorageError>>,
    },
    Trim {
        keep_from: u64,
        reply: Reply<Result<RetentionStats, StorageError>>,
    },
    Retention {
        max_age_secs: u64,
        max_bytes: u64,
        reply: Reply<Result<RetentionStats, StorageError>>,
    },
    InstallCompacted {
        job: CompactionJob,
        reply: Reply<Result<bool, StorageError>>,
    },
    SetSegmentMaxBytes(u64),
    #[allow(dead_code)] // only constructed by tests
    SetHooks(Option<Arc<dyn IoHooks>>),
    Shutdown {
        reply: Option<Reply<()>>,
    },
}

pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn io_copy(e: &io::Error) -> io::Error {
    io::Error::new(e.kind(), e.to_string())
}

struct Writer {
    shared: Arc<PartitionShared>,
    opts: WriterOptions,
    file: File,
    idx: Option<BufWriter<File>>,
    /// The on-disk `.idx` of the active segment may not match memory; it is
    /// rewritten from memory when the segment is sealed.
    idx_dirty: bool,
    active: Arc<Segment>,
    entries: u64,
    builder: IndexBuilder,
    stats: SegmentStats,
    next_offset: u64,
    synced_len: u64,
    last_sync: Instant,
    buf: Vec<u8>,
    hooks: Option<Arc<dyn IoHooks>>,
}

/// Start the writer thread for a recovered partition.
pub fn spawn(
    shared: Arc<PartitionShared>,
    rec: Recovered,
    opts: WriterOptions,
) -> io::Result<(Sender<Cmd>, std::thread::JoinHandle<()>)> {
    let (tx, rx) = crossbeam_channel::unbounded::<Cmd>();
    let mut w = Writer {
        file: rec.active_file.try_clone()?,
        idx: None,
        idx_dirty: false,
        active: rec.segments.last().unwrap().clone(),
        entries: 0,
        builder: IndexBuilder::default(),
        stats: rec.active_stats,
        next_offset: rec.next_offset,
        synced_len: rec.active_stats.len,
        last_sync: Instant::now(),
        buf: Vec::new(),
        hooks: None,
        shared,
        opts,
    };
    w.install(rec);
    let name = format!("exspeed-writer-{}", w.shared.stream);
    let handle = std::thread::Builder::new()
        .name(name)
        .stack_size(512 * 1024)
        .spawn(move || w.run(rx))?;
    Ok((tx, handle))
}

impl Writer {
    /// Adopt freshly recovered on-disk state (startup or after truncation).
    fn install(&mut self, rec: Recovered) {
        let dir = self.shared.dir.clone();
        let active = rec.segments.last().unwrap().clone();
        self.idx = OpenOptions::new()
            .append(true)
            .open(idx_path(&dir, active.base_offset))
            .map(|f| BufWriter::with_capacity(4096, f))
            .ok();
        self.idx_dirty = self.idx.is_none();
        self.entries = rec.active_entries.len() as u64;
        self.builder = IndexBuilder::resume(
            rec.active_entries.last().map(|e| e.pos),
            rec.active_stats.max_ts.unwrap_or(0),
        );
        self.file = rec.active_file;
        self.active = active;
        self.stats = rec.active_stats;
        self.next_offset = rec.next_offset;
        self.synced_len = rec.active_stats.len;
        self.shared.set_segments(rec.segments);
        self.shared.publish_committed(rec.next_offset);
    }

    fn run(mut self, rx: Receiver<Cmd>) {
        loop {
            let first = if self.opts.durability == Durability::Async && self.dirty() {
                let deadline = self.last_sync + self.opts.async_interval;
                match rx.recv_deadline(deadline) {
                    Ok(c) => c,
                    Err(RecvTimeoutError::Timeout) => {
                        self.async_sync();
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            } else {
                match rx.recv() {
                    Ok(c) => c,
                    Err(_) => break,
                }
            };
            match first {
                Cmd::Append(req) => {
                    let (group, deferred) = self.gather(&rx, req);
                    self.commit(group);
                    if let Some(cmd) = deferred {
                        if !self.handle(cmd) {
                            return;
                        }
                    }
                }
                other => {
                    if !self.handle(other) {
                        return;
                    }
                }
            }
        }
        self.shutdown();
    }

    fn dirty(&self) -> bool {
        self.stats.len > self.synced_len
    }

    /// Collect more append requests for one group commit. Single-record
    /// requests wait up to `flush_window` for company; once the group holds
    /// an explicit multi-record batch, only requests that are already queued
    /// are added (the caller already batched, so waiting only adds latency).
    fn gather(&self, rx: &Receiver<Cmd>, first: AppendReq) -> (Vec<AppendReq>, Option<Cmd>) {
        let cfg = self.opts.appender;
        let deadline = Instant::now() + cfg.flush_window;
        let mut records = first.len();
        let mut bytes = first.approx_bytes();
        let mut wait = first.len() <= 1;
        let mut group = vec![first];
        while records < cfg.flush_threshold_records && bytes < cfg.flush_threshold_bytes {
            let next = match rx.try_recv() {
                Ok(c) => Some(c),
                Err(TryRecvError::Disconnected) => None,
                Err(TryRecvError::Empty) if wait => rx.recv_deadline(deadline).ok(),
                Err(TryRecvError::Empty) => None,
            };
            match next {
                Some(Cmd::Append(req)) => {
                    records += req.len();
                    bytes += req.approx_bytes();
                    wait &= req.len() <= 1;
                    group.push(req);
                }
                Some(other) => return (group, Some(other)),
                None => break,
            }
        }
        (group, None)
    }

    /// Handle a non-append command. Returns `false` when the thread must exit.
    fn handle(&mut self, cmd: Cmd) -> bool {
        match cmd {
            Cmd::Append(req) => self.commit(vec![req]),
            Cmd::Truncate { drop_from, reply } => reply.send(self.truncate(drop_from)),
            Cmd::Trim { keep_from, reply } => reply.send(self.trim(keep_from)),
            Cmd::Retention {
                max_age_secs,
                max_bytes,
                reply,
            } => reply.send(self.retention(max_age_secs, max_bytes)),
            Cmd::InstallCompacted { job, reply } => reply.send(self.install_compacted(job)),
            Cmd::SetSegmentMaxBytes(n) => self.opts.segment_max_bytes = n.max(1),
            Cmd::SetHooks(h) => self.hooks = h,
            Cmd::Shutdown { reply } => {
                self.shutdown();
                if let Some(r) = reply {
                    r.send(());
                }
                return false;
            }
        }
        true
    }

    fn shutdown(&mut self) {
        if let Some(idx) = self.idx.as_mut() {
            let _ = idx.flush();
        }
        if self.dirty() {
            if let Err(e) = self.file.sync_data() {
                error!(stream = self.shared.stream.as_str(), error = %e, "final fsync failed");
            }
        }
    }

    fn async_sync(&mut self) {
        let len = self.stats.len;
        let r = self.sync_hooked();
        self.last_sync = Instant::now();
        match r {
            Ok(()) => self.synced_len = len,
            // Acknowledged data may now be lost; it is already visible, so it
            // can't be rolled back. Fence the partition.
            Err(e) => self
                .shared
                .fail(format!("background fsync failed (async mode): {e}")),
        }
    }

    fn write_hooked(&mut self, data: &[u8]) -> io::Result<()> {
        if let Some(h) = &self.hooks {
            match h.on_write(data.len()) {
                Some(WriteFault::Fail(e)) => return Err(e),
                Some(WriteFault::Partial(n, e)) => {
                    self.file.write_all(&data[..n.min(data.len())])?;
                    return Err(e);
                }
                None => {}
            }
        }
        self.file.write_all(data)
    }

    fn sync_hooked(&mut self) -> io::Result<()> {
        if let Some(h) = &self.hooks {
            h.on_sync()?;
        }
        self.file.sync_data()
    }

    fn truncate_hooked(&mut self, len: u64) -> io::Result<()> {
        if let Some(h) = &self.hooks {
            h.on_truncate()?;
        }
        self.file.set_len(len)?;
        self.file.sync_all()
    }

    fn log_write_error(&self, what: &str, e: &io::Error) {
        error!(
            stream = self.shared.stream.as_str(),
            error = %e,
            kind = if is_storage_full(e) { "storage_full" } else { "other" },
            "{what}"
        );
    }

    /// Encode one request into `buf`, assigning offsets from `next`.
    fn encode(
        buf: &mut Vec<u8>,
        req: &AppendReq,
        next: &mut u64,
        frames: &mut Vec<(u64, usize, u64)>,
    ) -> Result<Vec<(Offset, u64)>, StorageError> {
        match req {
            AppendReq::Records { records, .. } => {
                let mut out = Vec::with_capacity(records.len());
                for r in records {
                    let offset = *next;
                    let ts = r.timestamp_ns.unwrap_or_else(now_nanos);
                    let pos = buf.len();
                    encode_frame(
                        buf,
                        RecordRef {
                            offset,
                            timestamp: ts,
                            subject: &r.subject,
                            key: r.key.as_deref(),
                            value: &r.value,
                            headers: &r.headers,
                        },
                    )?;
                    frames.push((offset, pos, ts));
                    out.push((Offset(offset), ts));
                    *next += 1;
                }
                Ok(out)
            }
            AppendReq::At { records, .. } => {
                let mut min = *next;
                for r in records {
                    if r.offset.0 < min {
                        return Err(StorageError::OffsetConflict {
                            offset: r.offset.0,
                            min_allowed: min,
                        });
                    }
                    min = r.offset.0 + 1;
                }
                for r in records {
                    let pos = buf.len();
                    encode_frame(buf, RecordRef::from_stored(r))?;
                    frames.push((r.offset.0, pos, r.timestamp));
                }
                if let Some(last) = records.last() {
                    *next = last.offset.0 + 1;
                }
                Ok(Vec::new())
            }
        }
    }

    /// Group commit.
    fn commit(&mut self, group: Vec<AppendReq>) {
        if let Some(reason) = self.shared.failure() {
            for req in group {
                req.fail(StorageError::PartitionFailed {
                    stream: self.shared.stream.clone(),
                    reason: reason.clone(),
                });
            }
            return;
        }

        let mut buf = std::mem::take(&mut self.buf);
        buf.clear();
        let base_next = self.next_offset;
        let mut next = self.next_offset;
        let mut frames: Vec<(u64, usize, u64)> = Vec::new();
        let mut accepted: Vec<(AppendReq, Vec<(Offset, u64)>)> = Vec::with_capacity(group.len());
        for req in group {
            if req.len() == 0 {
                req.succeed(Vec::new());
                continue;
            }
            let (mark, mark_next, mark_frames) = (buf.len(), next, frames.len());
            match Self::encode(&mut buf, &req, &mut next, &mut frames) {
                Ok(results) => accepted.push((req, results)),
                Err(e) => {
                    buf.truncate(mark);
                    next = mark_next;
                    frames.truncate(mark_frames);
                    req.fail(e);
                }
            }
        }
        if accepted.is_empty() {
            self.buf = buf;
            return;
        }

        let good_len = self.stats.len;
        let sync = self.opts.durability == Durability::Sync;
        let result =
            self.write_hooked(&buf)
                .and_then(|()| if sync { self.sync_hooked() } else { Ok(()) });
        if let Err(e) = result {
            self.log_write_error("segment write failed; rolling back", &e);
            self.next_offset = base_next;
            if let Err(e2) = self.truncate_hooked(good_len) {
                self.shared.fail(format!(
                    "write failed ({e}) and truncating back to {good_len} bytes failed ({e2})"
                ));
            } else if !sync {
                // The truncate's sync_all also made earlier async data durable.
                self.synced_len = good_len;
                self.last_sync = Instant::now();
            }
            for (req, _) in accepted {
                req.fail(
                    self.shared
                        .failed_error()
                        .unwrap_or_else(|| StorageError::Io(io_copy(&e))),
                );
            }
            self.buf = buf;
            return;
        }

        // Committed: index, segment stats, high watermark, replies.
        let mut new_entries: Vec<IndexEntry> = Vec::new();
        for &(offset, pos, ts) in &frames {
            if let Some(e) = self.builder.observe(offset, good_len + pos as u64, ts) {
                new_entries.push(e);
            }
            self.stats.first_ts.get_or_insert(ts);
        }
        self.stats.records += frames.len() as u64;
        self.stats.max_ts = Some(self.builder.max_ts);
        self.stats.len = good_len + buf.len() as u64;
        self.stats.end_offset = next;
        self.next_offset = next;
        if sync {
            self.synced_len = self.stats.len;
        }
        self.active.publish(&new_entries, self.stats);
        self.append_index(&new_entries);
        self.shared.publish_committed(next);
        for (req, results) in accepted {
            req.succeed(results);
        }
        self.buf = buf;

        if !sync && self.stats.len - self.synced_len >= self.opts.async_threshold_bytes as u64 {
            self.async_sync();
        }
        if self.stats.len >= self.opts.segment_max_bytes {
            // The batch is already durable and acknowledged; a failed roll
            // is retried on the next commit and never fails an append.
            if let Err(e) = self.roll() {
                self.log_write_error("segment roll failed; will retry", &e);
            }
        }
    }

    fn append_index(&mut self, entries: &[IndexEntry]) {
        if entries.is_empty() {
            return;
        }
        self.entries += entries.len() as u64;
        let Some(idx) = self.idx.as_mut() else {
            self.idx_dirty = true;
            return;
        };
        let mut b = Vec::with_capacity(entries.len() * INDEX_ENTRY_LEN);
        for e in entries {
            e.encode(&mut b);
        }
        if idx.write_all(&b).is_err() {
            self.idx_dirty = true;
            self.idx = None;
        }
    }

    /// Seal the active segment and start a new one at `next_offset`.
    fn roll(&mut self) -> io::Result<()> {
        if self.next_offset <= self.active.base_offset || self.stats.records == 0 {
            return Ok(()); // never seal an empty segment
        }
        let dir = self.shared.dir.clone();
        let base = self.active.base_offset;
        if self.dirty() {
            self.sync_hooked().inspect_err(|e| {
                self.shared
                    .fail(format!("fsync before segment roll failed: {e}"));
            })?;
            self.synced_len = self.stats.len;
            self.last_sync = Instant::now();
        }

        // Make the on-disk index match memory, then make it durable.
        let ipath = idx_path(&dir, base);
        let mut idx_ok = !self.idx_dirty;
        if let Some(mut idx) = self.idx.take() {
            idx_ok &= idx.flush().is_ok();
        }
        idx_ok &= fs::metadata(&ipath).map(|m| m.len()).ok()
            == Some(self.entries * INDEX_ENTRY_LEN as u64);
        if !idx_ok {
            let entries = self.active.index_entries()?;
            crate::file::fsutil::atomic_write(&ipath, &encode_index(&entries))?;
            self.entries = entries.len() as u64;
        }
        File::open(&ipath)?.sync_all()?;
        self.idx_dirty = false;
        let mut meta = self.active.meta();
        meta.index_entries = self.entries;
        save_meta(&meta_path(&dir, base), &meta)?;

        // A previous failed roll may have left an empty file behind.
        let new_base = self.next_offset;
        let new_path = seg_path(&dir, new_base);
        if new_path.exists() {
            remove_if_exists(&new_path)?;
        }
        let new_file = create_segment_file(&dir, new_base)?;
        let new_idx = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(idx_path(&dir, new_base))?;
        let new_stats = SegmentStats {
            len: SEGMENT_HEADER_LEN,
            end_offset: new_base,
            first_ts: None,
            max_ts: None,
            records: 0,
        };
        let sealed = Arc::new(Segment::new_sealed(&dir, &meta)?);
        let active = Arc::new(Segment::new_active(&dir, new_base, new_stats, Vec::new())?);

        let mut list: Vec<Arc<Segment>> = self.shared.segments().as_ref().clone();
        let last = list.len() - 1;
        list[last] = sealed;
        list.push(active.clone());
        self.shared.set_segments(list);

        self.file = new_file;
        self.idx = Some(BufWriter::with_capacity(4096, new_idx));
        self.active = active;
        self.entries = 0;
        self.builder = IndexBuilder::default();
        self.stats = new_stats;
        self.synced_len = SEGMENT_HEADER_LEN;
        Ok(())
    }

    /// Remove sealed segments at `indexes` (into the current list): unpublish
    /// them first, then delete their files and fsync the directory. Readers
    /// that already hold a segment keep reading it through their open handle.
    fn remove_segments(&mut self, indexes: &[usize]) -> RetentionStats {
        let mut stats = RetentionStats::default();
        if indexes.is_empty() {
            return stats;
        }
        let list = self.shared.segments();
        let active_idx = list.len() - 1;
        let doomed: Vec<Arc<Segment>> = indexes
            .iter()
            .filter(|&&i| i < active_idx)
            .map(|&i| list[i].clone())
            .collect();
        let keep: Vec<Arc<Segment>> = list
            .iter()
            .enumerate()
            .filter(|(i, _)| !indexes.contains(i) || *i == active_idx)
            .map(|(_, s)| s.clone())
            .collect();
        self.shared.set_segments(keep);
        let dir = self.shared.dir.clone();
        for seg in doomed {
            stats.segments_deleted += 1;
            stats.bytes_reclaimed += seg.len();
            for p in [
                seg_path(&dir, seg.base_offset),
                idx_path(&dir, seg.base_offset),
                meta_path(&dir, seg.base_offset),
            ] {
                if let Err(e) = remove_if_exists(&p) {
                    warn!(path = %p.display(), error = %e, "failed to delete segment file");
                }
            }
        }
        if let Err(e) = fsync_dir(&dir) {
            warn!(dir = %dir.display(), error = %e, "directory fsync after segment delete failed");
        }
        stats
    }

    fn retention(
        &mut self,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<RetentionStats, StorageError> {
        let list = self.shared.segments();
        let sealed = list.len() - 1;
        let cutoff = now_nanos().saturating_sub(max_age_secs.saturating_mul(1_000_000_000));
        let mut doomed: Vec<usize> = Vec::new();
        // Age: a sealed segment goes once its newest record is older than
        // the cutoff (empty sealed segments go too). Only a prefix of the
        // log is ever removed: producer-supplied timestamps need not be
        // monotonic across segments, and deleting a middle segment would
        // punch a hole in the log.
        for (i, seg) in list[..sealed].iter().enumerate() {
            if !seg.stats().max_ts.is_none_or(|ts| ts < cutoff) {
                break;
            }
            doomed.push(i);
        }
        // Size: drop the oldest sealed segments until under the limit.
        let mut total: u64 = list
            .iter()
            .enumerate()
            .filter(|(i, _)| !doomed.contains(i))
            .map(|(_, s)| s.len())
            .sum();
        for (i, seg) in list[..sealed].iter().enumerate() {
            if total <= max_bytes {
                break;
            }
            if !doomed.contains(&i) {
                doomed.push(i);
                total -= seg.len();
            }
        }
        Ok(self.remove_segments(&doomed))
    }

    fn trim(&mut self, keep_from: u64) -> Result<RetentionStats, StorageError> {
        let list = self.shared.segments();
        // Segment i holds only offsets below segment i+1's base.
        let doomed: Vec<usize> = (0..list.len() - 1)
            .take_while(|&i| list[i + 1].base_offset <= keep_from)
            .collect();
        Ok(self.remove_segments(&doomed))
    }

    fn truncate(&mut self, drop_from: u64) -> Result<(), StorageError> {
        if let Some(e) = self.shared.failed_error() {
            return Err(e);
        }
        if drop_from >= self.next_offset {
            return Ok(());
        }
        // Hide the doomed records first.
        self.shared.publish_committed(drop_from);
        let dir = self.shared.dir.clone();
        if let Some(idx) = self.idx.as_mut() {
            let _ = idx.flush();
        }
        let durability = self.opts.durability;
        let result = (|| -> io::Result<Recovered> {
            write_truncate_marker(&dir, drop_from)?;
            apply_truncation(&dir, drop_from)?;
            remove_if_exists(&dir.join(TRUNCATE_MARKER))?;
            fsync_dir(&dir)?;
            recover(&dir, durability)
        })();
        match result {
            Ok(rec) => {
                info!(
                    stream = self.shared.stream.as_str(),
                    drop_from, "truncated partition"
                );
                self.install(rec);
                Ok(())
            }
            Err(e) => {
                // The marker (if written) finishes the job on restart.
                self.shared
                    .fail(format!("truncate_from({drop_from}) failed: {e}"));
                Err(StorageError::Io(e))
            }
        }
    }

    fn install_compacted(&mut self, job: CompactionJob) -> Result<bool, StorageError> {
        let list = self.shared.segments();
        let Some(i) = list
            .iter()
            .position(|s| Arc::ptr_eq(s, &job.old))
            .filter(|&i| i + 1 < list.len())
        else {
            // Deleted or truncated meanwhile.
            let _ = remove_if_exists(&job.tmp_seg);
            let _ = remove_if_exists(&job.tmp_idx);
            return Ok(false);
        };
        let dir = self.shared.dir.clone();
        let base = job.meta.base_offset;
        // Crash safety: if we crash after the first rename, `.meta` no longer
        // matches the segment length and recovery rebuilds index + meta.
        fs::rename(&job.tmp_seg, seg_path(&dir, base))?;
        fs::rename(&job.tmp_idx, idx_path(&dir, base))?;
        save_meta(&meta_path(&dir, base), &job.meta)?;
        let seg = Arc::new(Segment::new_sealed(&dir, &job.meta)?);
        let mut new_list = list.as_ref().clone();
        new_list[i] = seg;
        self.shared.set_segments(new_list);
        Ok(true)
    }
}
