//! File-backed storage engine.
//!
//! Directory layout:
//!
//! ```text
//! {data_dir}/streams/{stream}/stream.json            config (atomic writes)
//! {data_dir}/streams/{stream}/partitions/0/B.seg     segment (B = base offset)
//! {data_dir}/streams/{stream}/partitions/0/B.idx     sparse offset + time index
//! {data_dir}/streams/{stream}/partitions/0/B.meta    sealed-segment metadata
//! {data_dir}/streams/{stream}/partitions/0/truncate.json  truncation in progress
//! {data_dir}/.trash/                                 streams being deleted
//! ```
//!
//! See [`segment`] for the file formats, [`partition`] for recovery and the
//! lock-free read path, [`writer`] for group commit and fencing, and
//! [`compaction`] for log compaction.

pub mod backup;
pub mod compaction;
pub mod fsutil;
pub mod io_errors;
pub mod partition;
pub mod segment;
pub mod stream_config;
pub mod writer;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use async_trait::async_trait;
use crossbeam_channel::{RecvTimeoutError, Sender};
use dashmap::DashMap;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{
    ReadBatch, ReadLimits, Record, StorageEngine, StorageError, StoredRecord, StreamConfig,
};
use tracing::{error, info, warn};

pub use compaction::CompactionStats;
pub use partition::PartitionStatus;
pub use writer::{AppenderConfig, RetentionStats, DEFAULT_SEGMENT_MAX_BYTES};

use crate::file::fsutil::fsync_dir;
use crate::file::partition::{Durability, PartitionShared};
use crate::file::stream_config::StreamConfigFile;
use crate::file::writer::{Cmd, Reply, WriterOptions};

/// Storage durability mode.
///
/// * `Sync` (default) — group commit with one `fdatasync` per batch before
///   the batch is acknowledged or visible to readers.
/// * `Async` — batches are acknowledged and visible once written; the
///   writer fsyncs every `interval`, or earlier once `threshold_bytes` are
///   unsynced (`0` disables the byte trigger). A crash may lose up to that
///   much acknowledged data. A failed background fsync fences the partition.
#[derive(Debug, Clone, Copy, Default)]
pub enum StorageSyncMode {
    #[default]
    Sync,
    Async {
        interval: Duration,
        threshold_bytes: usize,
    },
}

/// Everything [`FileStorage::open_with_options`] can tune.
#[derive(Debug, Clone, Copy)]
pub struct StorageOptions {
    pub sync_mode: StorageSyncMode,
    pub appender: AppenderConfig,
    /// Roll the active segment once it reaches this size.
    pub segment_max_bytes: u64,
    /// How often the background compactor visits streams with
    /// `compaction = true`. `Duration::ZERO` disables it.
    pub compaction_interval: Duration,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            sync_mode: StorageSyncMode::Sync,
            appender: AppenderConfig::default(),
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            compaction_interval: Duration::from_secs(60),
        }
    }
}

impl StorageOptions {
    fn writer(&self) -> WriterOptions {
        let (durability, async_interval, async_threshold_bytes) = match self.sync_mode {
            StorageSyncMode::Sync => (Durability::Sync, Duration::from_secs(1), usize::MAX),
            StorageSyncMode::Async {
                interval,
                threshold_bytes,
            } => (
                Durability::Async,
                interval,
                if threshold_bytes == 0 {
                    usize::MAX
                } else {
                    threshold_bytes
                },
            ),
        };
        WriterOptions {
            durability,
            async_interval,
            async_threshold_bytes,
            appender: self.appender,
            segment_max_bytes: self.segment_max_bytes,
        }
    }
}

/// A running partition: shared reader state plus the writer thread.
pub(crate) struct PartitionHandle {
    pub(crate) shared: Arc<PartitionShared>,
    tx: Sender<Cmd>,
    thread: Mutex<Option<JoinHandle<()>>>,
    compaction_lock: Mutex<()>,
}

impl PartitionHandle {
    async fn call<T: Send + 'static>(
        &self,
        make: impl FnOnce(Reply<T>) -> Cmd,
    ) -> Result<T, StorageError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(make(Reply::Async(tx)))
            .map_err(|_| StorageError::ChannelClosed)?;
        rx.await.map_err(|_| StorageError::ChannelClosed)
    }

    fn call_blocking<T>(&self, make: impl FnOnce(Reply<T>) -> Cmd) -> Result<T, StorageError> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.tx
            .send(make(Reply::Sync(tx)))
            .map_err(|_| StorageError::ChannelClosed)?;
        rx.recv().map_err(|_| StorageError::ChannelClosed)
    }

    /// Stop the writer after it finished everything queued before.
    fn shutdown_blocking(&self) {
        let _ = self.call_blocking(|r| Cmd::Shutdown { reply: Some(r) });
        if let Some(t) = self.thread.lock().unwrap().take() {
            let _ = t.join();
        }
    }

    fn compact(&self) -> Result<CompactionStats, StorageError> {
        let cfg = self.shared.config().ok_or_else(|| {
            StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("stream.json of {} is unreadable", self.shared.stream),
            ))
        })?;
        if !cfg.compaction {
            return Ok(CompactionStats::default());
        }
        if let Some(e) = self.shared.failed_error() {
            return Err(e);
        }
        let _g = self.compaction_lock.lock().unwrap();
        let jobs = compaction::prepare(
            &self.shared,
            cfg.tombstone_retention_secs,
            writer::now_nanos(),
        )?;
        let mut total = CompactionStats::default();
        for (job, stats) in jobs {
            if self.call_blocking(|reply| Cmd::InstallCompacted { job, reply })?? {
                total += stats;
            }
        }
        Ok(total)
    }
}

impl Drop for PartitionHandle {
    fn drop(&mut self) {
        let _ = self.tx.send(Cmd::Shutdown { reply: None });
        if let Some(t) = self.thread.get_mut().unwrap().take() {
            if t.thread().id() != std::thread::current().id() {
                let _ = t.join();
            }
        }
    }
}

struct Inner {
    data_dir: PathBuf,
    opts: StorageOptions,
    partitions: DashMap<String, Arc<PartitionHandle>>,
    /// Serializes stream create / delete.
    lifecycle: Mutex<()>,
    compactor_stop: Mutex<Option<Sender<()>>>,
    compactor: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.compactor_stop.get_mut().unwrap().take();
        if let Some(t) = self.compactor.get_mut().unwrap().take() {
            if t.thread().id() != std::thread::current().id() {
                let _ = t.join();
            }
        }
    }
}

/// File-backed storage engine: one writer thread per partition, lock-free
/// readers. Cheap to clone.
#[derive(Clone)]
pub struct FileStorage {
    inner: Arc<Inner>,
}

fn load_config(stream: &str, stream_dir: &Path) -> Option<StreamConfig> {
    match StreamConfig::load(stream_dir) {
        Ok(c) => Some(c),
        Err(e) => {
            error!(
                stream,
                error = %e,
                "stream.json is unreadable; retention and compaction are skipped for this stream until it is fixed"
            );
            None
        }
    }
}

impl FileStorage {
    /// Open (or create) a storage directory with default options.
    pub fn new(data_dir: &Path) -> io::Result<Self> {
        Self::open(data_dir)
    }

    /// Open (or create) a storage directory with default options, recovering
    /// every partition found on disk.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        Self::open_with_options(data_dir, StorageOptions::default())
    }

    /// Open with an explicit durability mode and group-commit settings.
    pub fn open_with_mode(
        data_dir: &Path,
        mode: StorageSyncMode,
        appender_config: AppenderConfig,
    ) -> io::Result<Self> {
        Self::open_with_options(
            data_dir,
            StorageOptions {
                sync_mode: mode,
                appender: appender_config,
                ..StorageOptions::default()
            },
        )
    }

    pub fn open_with_options(data_dir: &Path, opts: StorageOptions) -> io::Result<Self> {
        let streams_dir = data_dir.join("streams");
        fs::create_dir_all(&streams_dir)?;
        let trash = data_dir.join(".trash");
        if trash.exists() {
            fs::remove_dir_all(&trash)?;
        }

        let partitions = DashMap::new();
        for entry in fs::read_dir(&streams_dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !path.is_dir() || name.starts_with('.') {
                continue;
            }
            let part_dir = path.join("partitions").join("0");
            if !part_dir.is_dir() {
                continue;
            }
            let config = load_config(&name, &path);
            let handle = start_partition(&opts, &name, &part_dir, config)?;
            partitions.insert(name, handle);
        }

        let inner = Arc::new(Inner {
            data_dir: data_dir.to_path_buf(),
            opts,
            partitions,
            lifecycle: Mutex::new(()),
            compactor_stop: Mutex::new(None),
            compactor: Mutex::new(None),
        });
        if !opts.compaction_interval.is_zero() {
            let (tx, handle) = spawn_compactor(Arc::downgrade(&inner), opts.compaction_interval)?;
            *inner.compactor_stop.lock().unwrap() = Some(tx);
            *inner.compactor.lock().unwrap() = Some(handle);
        }
        Ok(Self { inner })
    }

    /// The root data directory.
    pub fn data_dir(&self) -> &Path {
        &self.inner.data_dir
    }

    fn stream_dir(&self, stream: &str) -> PathBuf {
        self.inner.data_dir.join("streams").join(stream)
    }

    fn handle(&self, stream: &StreamName) -> Result<Arc<PartitionHandle>, StorageError> {
        self.inner
            .partitions
            .get(stream.as_str())
            .map(|e| e.value().clone())
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))
    }

    fn handle_by_name(&self, stream: &str) -> Option<Arc<PartitionHandle>> {
        self.inner.partitions.get(stream).map(|e| e.value().clone())
    }

    /// All stream names, sorted.
    pub fn list_streams(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .inner
            .partitions
            .iter()
            .map(|e| e.key().clone())
            .collect();
        v.sort();
        v
    }

    /// Bytes stored for a stream (all segments). `None` if unknown.
    pub fn stream_storage_bytes(&self, stream: &str) -> Option<u64> {
        self.handle_by_name(stream).map(|h| h.shared.total_bytes())
    }

    /// The high watermark (offset the next visible record gets). `None` if
    /// the stream is unknown.
    pub fn stream_head_offset(&self, stream: &str) -> Option<u64> {
        self.handle_by_name(stream)
            .map(|h| h.shared.high_watermark())
    }

    /// Health of a stream's partition. `None` if the stream is unknown.
    pub fn partition_status(&self, stream: &str) -> Option<PartitionStatus> {
        self.handle_by_name(stream).map(|h| h.shared.status())
    }

    /// Every fenced (read-only) stream with the reason.
    pub fn failed_streams(&self) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = self
            .inner
            .partitions
            .iter()
            .filter_map(|e| e.value().shared.failure().map(|r| (e.key().clone(), r)))
            .collect();
        v.sort();
        v
    }

    /// Hold a stream's high watermark at or below `floor` (replication
    /// hook: visible = min(committed, floor)). `None` removes the floor.
    /// Returns `false` if the stream is unknown.
    pub fn set_replication_floor(&self, stream: &str, floor: Option<u64>) -> bool {
        match self.handle_by_name(stream) {
            Some(h) => {
                h.shared.set_floor(floor);
                true
            }
            None => false,
        }
    }

    /// Override the segment size at which a stream rolls (tests use tiny
    /// values to get many segments). Takes effect for appends queued after
    /// this call. Returns `false` if the stream is unknown.
    pub fn set_stream_segment_max_bytes(&self, stream: &str, max: u64) -> bool {
        match self.handle_by_name(stream) {
            Some(h) => h.tx.send(Cmd::SetSegmentMaxBytes(max)).is_ok(),
            None => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_io_hooks(
        &self,
        stream: &str,
        hooks: Option<Arc<dyn writer::IoHooks>>,
    ) -> bool {
        match self.handle_by_name(stream) {
            Some(h) => h.tx.send(Cmd::SetHooks(hooks)).is_ok(),
            None => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn shared(&self, stream: &str) -> Option<Arc<PartitionShared>> {
        self.handle_by_name(stream).map(|h| h.shared.clone())
    }

    /// Enforce retention (age and size) on every stream. Blocking; call it
    /// from `spawn_blocking` or a plain thread. Errors are logged per stream
    /// so one bad stream never stops the others.
    /// Flush and fsync every partition and stop the writer threads. Call
    /// once at shutdown after all writers are done; appends afterwards fail
    /// with `ChannelClosed`. Blocking: run it on a blocking thread.
    pub fn close(&self) {
        self.inner.compactor_stop.lock().unwrap().take();
        let handles: Vec<Arc<PartitionHandle>> = self
            .inner
            .partitions
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for h in handles {
            h.shutdown_blocking();
        }
    }

    pub fn enforce_all_retention(&self) -> io::Result<()> {
        let handles: Vec<Arc<PartitionHandle>> = self
            .inner
            .partitions
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for h in handles {
            let Some(cfg) = h.shared.config() else {
                warn!(
                    stream = h.shared.stream.as_str(),
                    "skipping retention: stream.json is unreadable"
                );
                continue;
            };
            match h.call_blocking(|reply| Cmd::Retention {
                max_age_secs: cfg.max_age_secs,
                max_bytes: cfg.max_bytes,
                reply,
            }) {
                Ok(Ok(stats)) if stats.segments_deleted > 0 => info!(
                    stream = h.shared.stream.as_str(),
                    segments_deleted = stats.segments_deleted,
                    bytes_reclaimed = stats.bytes_reclaimed,
                    "retention enforced"
                ),
                Ok(Ok(_)) => {}
                Ok(Err(e)) | Err(e) => error!(
                    stream = h.shared.stream.as_str(),
                    error = %e,
                    "retention failed"
                ),
            }
        }
        Ok(())
    }

    /// Compact one stream now (only if its config has `compaction = true`).
    /// Blocking.
    pub fn compact_stream(&self, stream: &str) -> Result<CompactionStats, StorageError> {
        let h = self.handle_by_name(stream).ok_or_else(|| {
            StorageError::StreamNotFound(
                StreamName::try_from(stream).unwrap_or_else(|_| StreamName::try_from("_").unwrap()),
            )
        })?;
        h.compact()
    }

    /// Compact every stream with `compaction = true`. Blocking.
    pub fn compact_all(&self) -> CompactionStats {
        compact_all(&self.inner)
    }

    fn create_stream_sync(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), StorageError> {
        let _g = self.inner.lifecycle.lock().unwrap();
        if self.inner.partitions.contains_key(stream.as_str()) {
            return Err(StorageError::StreamAlreadyExists(stream.clone()));
        }
        let stream_dir = self.stream_dir(stream.as_str());
        if stream_dir.exists() {
            // Leftover of an interrupted create; never resurrect its data.
            self.move_to_trash(stream.as_str())?;
        }
        let part_dir = stream_dir.join("partitions").join("0");
        fs::create_dir_all(&part_dir)?;
        fsync_dir(&self.inner.data_dir.join("streams"))?;
        fsync_dir(&stream_dir)?;
        fsync_dir(&stream_dir.join("partitions"))?;
        config.save(&stream_dir)?;
        let handle = start_partition(
            &self.inner.opts,
            stream.as_str(),
            &part_dir,
            Some(config.clone()),
        )?;
        self.inner
            .partitions
            .insert(stream.as_str().to_string(), handle);
        Ok(())
    }

    /// Atomically move a stream directory out of `streams/` (so a crash
    /// mid-delete never leaves a half-deleted stream), then delete it.
    fn move_to_trash(&self, stream: &str) -> io::Result<()> {
        let src = self.stream_dir(stream);
        let trash = self.inner.data_dir.join(".trash");
        fs::create_dir_all(&trash)?;
        let dst = trash.join(format!("{stream}-{}", writer::now_nanos()));
        fs::rename(&src, &dst)?;
        fsync_dir(&self.inner.data_dir.join("streams"))?;
        fs::remove_dir_all(&dst)
    }

    fn delete_stream_sync(&self, stream: &StreamName) -> Result<(), StorageError> {
        let _g = self.inner.lifecycle.lock().unwrap();
        if let Some((_, h)) = self.inner.partitions.remove(stream.as_str()) {
            h.shutdown_blocking();
        }
        if self.stream_dir(stream.as_str()).exists() {
            self.move_to_trash(stream.as_str())?;
        }
        Ok(())
    }
}

fn start_partition(
    opts: &StorageOptions,
    stream: &str,
    dir: &Path,
    config: Option<StreamConfig>,
) -> io::Result<Arc<PartitionHandle>> {
    let wopts = opts.writer();
    let rec = partition::recover(dir, wopts.durability)?;
    let shared = Arc::new(PartitionShared::new(
        stream,
        dir,
        rec.segments.clone(),
        rec.next_offset,
        config,
    ));
    let (tx, thread) = writer::spawn(shared.clone(), rec, wopts)?;
    Ok(Arc::new(PartitionHandle {
        shared,
        tx,
        thread: Mutex::new(Some(thread)),
        compaction_lock: Mutex::new(()),
    }))
}

fn compact_all(inner: &Inner) -> CompactionStats {
    let handles: Vec<Arc<PartitionHandle>> =
        inner.partitions.iter().map(|e| e.value().clone()).collect();
    let mut total = CompactionStats::default();
    for h in handles {
        if !h.shared.config().is_some_and(|c| c.compaction) {
            continue;
        }
        match h.compact() {
            Ok(s) => {
                if s.segments_rewritten > 0 {
                    info!(
                        stream = h.shared.stream.as_str(),
                        segments = s.segments_rewritten,
                        records_removed = s.records_removed,
                        bytes_reclaimed = s.bytes_reclaimed,
                        "compacted"
                    );
                }
                total += s;
            }
            Err(e) => warn!(stream = h.shared.stream.as_str(), error = %e, "compaction failed"),
        }
    }
    total
}

fn spawn_compactor(
    inner: Weak<Inner>,
    interval: Duration,
) -> io::Result<(Sender<()>, JoinHandle<()>)> {
    let (tx, rx) = crossbeam_channel::bounded::<()>(1);
    let handle = std::thread::Builder::new()
        .name("exspeed-compactor".into())
        .spawn(move || loop {
            match rx.recv_timeout(interval) {
                Err(RecvTimeoutError::Timeout) => {}
                _ => return,
            }
            let Some(inner) = inner.upgrade() else { return };
            compact_all(&inner);
        })?;
    Ok((tx, handle))
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, StorageError> + Send + 'static,
) -> Result<T, StorageError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| StorageError::Io(io::Error::other(e)))?
}

fn invalid_config(m: String) -> StorageError {
    StorageError::Io(io::Error::new(io::ErrorKind::InvalidInput, m))
}

#[async_trait]
impl StorageEngine for FileStorage {
    async fn create_stream(
        &self,
        stream: &StreamName,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<(), StorageError> {
        let cfg = StreamConfig::from_request(max_age_secs, max_bytes, 0, 0);
        self.create_stream_with(stream, &cfg).await
    }

    async fn create_stream_with(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), StorageError> {
        config.check().map_err(invalid_config)?;
        let this = self.clone();
        let stream = stream.clone();
        let config = config.clone();
        blocking(move || this.create_stream_sync(&stream, &config)).await
    }

    async fn stream_config(&self, stream: &StreamName) -> Result<StreamConfig, StorageError> {
        let h = self.handle(stream)?;
        h.shared.config().ok_or_else(|| {
            StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("stream.json of {stream} is unreadable"),
            ))
        })
    }

    async fn update_stream_config(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), StorageError> {
        let h = self.handle(stream)?;
        config.check().map_err(invalid_config)?;
        let dir = self.stream_dir(stream.as_str());
        let cfg = config.clone();
        blocking(move || cfg.save(&dir).map_err(StorageError::Io)).await?;
        h.shared.set_config(config.clone());
        Ok(())
    }

    async fn append(
        &self,
        stream: &StreamName,
        record: &Record,
    ) -> Result<(Offset, u64), StorageError> {
        let records = vec![record.clone()];
        let mut out = self.append_batch(stream, records).await?;
        out.pop().ok_or(StorageError::ChannelClosed)
    }

    async fn append_batch(
        &self,
        stream: &StreamName,
        records: Vec<Record>,
    ) -> Result<Vec<(Offset, u64)>, StorageError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let h = self.handle(stream)?;
        h.call(|reply| Cmd::Append(writer::AppendReq::Records { records, reply }))
            .await?
    }

    async fn append_at(
        &self,
        stream: &StreamName,
        records: Vec<StoredRecord>,
    ) -> Result<(), StorageError> {
        let h = self.handle(stream)?;
        h.call(|reply| Cmd::Append(writer::AppendReq::At { records, reply }))
            .await?
    }

    async fn read(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let h = self.handle(stream)?;
        blocking(move || {
            h.shared
                .read(from.0, max_records, usize::MAX, true)
                .map(|b| b.records)
        })
        .await
    }

    async fn read_batch(
        &self,
        stream: &StreamName,
        from: Offset,
        limits: ReadLimits,
    ) -> Result<ReadBatch, StorageError> {
        let h = self.handle(stream)?;
        blocking(move || {
            h.shared
                .read(from.0, limits.max_records.max(1), limits.max_bytes, false)
        })
        .await
    }

    async fn seek_by_time(
        &self,
        stream: &StreamName,
        timestamp: u64,
    ) -> Result<Offset, StorageError> {
        let h = self.handle(stream)?;
        blocking(move || h.shared.seek_by_time(timestamp).map(Offset)).await
    }

    async fn list_streams(&self) -> Result<Vec<StreamName>, StorageError> {
        Ok(FileStorage::list_streams(self)
            .into_iter()
            .filter_map(|n| StreamName::try_from(n.as_str()).ok())
            .collect())
    }

    async fn trim_up_to(&self, stream: &StreamName, keep_from: Offset) -> Result<(), StorageError> {
        let h = self.handle(stream)?;
        h.call(|reply| Cmd::Trim {
            keep_from: keep_from.0,
            reply,
        })
        .await??;
        Ok(())
    }

    async fn delete_stream(&self, stream: &StreamName) -> Result<(), StorageError> {
        let this = self.clone();
        let stream = stream.clone();
        blocking(move || this.delete_stream_sync(&stream)).await
    }

    async fn stream_bounds(&self, stream: &StreamName) -> Result<(Offset, Offset), StorageError> {
        let h = self.handle(stream)?;
        Ok((
            Offset(h.shared.earliest()),
            Offset(h.shared.high_watermark()),
        ))
    }

    async fn truncate_from(
        &self,
        stream: &StreamName,
        drop_from: Offset,
    ) -> Result<(), StorageError> {
        let h = self.handle(stream)?;
        h.call(|reply| Cmd::Truncate {
            drop_from: drop_from.0,
            reply,
        })
        .await?
    }

    fn watch_appends(&self, stream: &StreamName) -> Option<tokio::sync::watch::Receiver<u64>> {
        self.handle(stream).ok().map(|h| h.shared.subscribe())
    }
}
