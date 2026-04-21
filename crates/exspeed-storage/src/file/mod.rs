pub mod io_errors;
pub mod offset_index;
pub mod partition;
pub mod segment_reader;
pub mod segment_writer;
pub mod stream_config;
pub mod time_index;
pub mod wal;
pub mod wal_appender;
pub mod wal_syncer;

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{mpsc, Mutex};

use async_trait::async_trait;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError, StoredRecord};

use crate::file::partition::Partition;
use crate::file::stream_config::StreamConfig;
use crate::file::wal_appender::{AppenderConfig, AppenderHandle, AppenderMode};
use crate::file::wal_syncer::WalSyncerHandle;

/// Storage durability mode. `Sync` = group commit + fsync per batch (default,
/// strongest durability). `Async` = batch writes immediately, fsync on a timer
/// (faster, may lose up to `interval` of acked data on crash).
///
/// # Note on `threshold_bytes`
/// The `threshold_bytes` field in `Async` is part of the public API for
/// future use (trigger a mid-interval fsync when unflushed bytes exceed this
/// value). In the current implementation only the timer fires — byte-threshold
/// triggering is a planned follow-up.
#[derive(Debug, Clone, Copy)]
pub enum StorageSyncMode {
    Sync,
    Async {
        interval: std::time::Duration,
        /// Future: trigger an early fsync when unflushed bytes exceed this threshold.
        /// Currently unused — timer-only. TODO: wire up byte-threshold trigger.
        threshold_bytes: usize,
    },
}

impl Default for StorageSyncMode {
    fn default() -> Self {
        StorageSyncMode::Sync
    }
}

struct FileStorageInner {
    data_dir: PathBuf,
    /// Drop order matters here — Rust drops struct fields in declaration order
    /// (first-declared = first-dropped). We want:
    ///   1. `appenders` dropped first  → writer tasks drain their in-flight
    ///      batch and exit, releasing their Arc<Mutex<Partition>>.
    ///   2. `syncers` dropped second   → shutdown signal fires; each syncer
    ///      task performs a final fsync and exits, releasing its Arc.
    ///   3. `partitions` dropped last  → by this point all background tasks
    ///      have released their Arc<Mutex<Partition>> references.
    appenders: DashMap<(String, u32), AppenderHandle>,
    syncers: DashMap<(String, u32), WalSyncerHandle>,
    partitions: DashMap<(String, u32), Arc<Mutex<Partition>>>,
    appender_config: AppenderConfig,
    storage_sync_mode: StorageSyncMode,
    seal_tx: std::sync::Mutex<Option<mpsc::UnboundedSender<partition::SealedSegmentInfo>>>,
}

/// File-backed storage engine.
///
/// Directory layout:
///   `{data_dir}/streams/{stream}/partitions/0/`
///
/// Each partition directory contains `.seg` segment files and a `wal.log`.
#[derive(Clone)]
pub struct FileStorage {
    inner: Arc<FileStorageInner>,
}

impl FileStorage {
    /// Create a new, empty `FileStorage` rooted at `data_dir`.
    ///
    /// Creates the `data_dir` directory if it does not exist.
    pub fn new(data_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;
        Ok(Self {
            inner: Arc::new(FileStorageInner {
                data_dir: data_dir.to_path_buf(),
                appenders: DashMap::new(),
                syncers: DashMap::new(),
                partitions: DashMap::new(),
                appender_config: AppenderConfig::default(),
                storage_sync_mode: StorageSyncMode::default(),
                seal_tx: std::sync::Mutex::new(None),
            }),
        })
    }

    /// Open an existing `FileStorage`, scanning for streams and partitions
    /// on disk and recovering each partition (including WAL replay).
    ///
    /// Uses the default `StorageSyncMode::Sync` durability mode. To opt into
    /// async-sync mode use [`FileStorage::open_with_mode`].
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        Self::open_with_mode(data_dir, StorageSyncMode::default(), AppenderConfig::default())
    }

    /// Open an existing `FileStorage` with an explicit durability mode and appender config.
    ///
    /// - `StorageSyncMode::Sync` — group commit + fsync per batch (default).
    /// - `StorageSyncMode::Async { interval, .. }` — writes are acked without
    ///   fsync; a `WalSyncer` task fires every `interval` to call `sync_data`.
    ///   On crash, up to `interval` of acked data may be lost.
    /// - `appender_config` — batching tunables (flush window, record count threshold,
    ///   byte threshold). Use [`AppenderConfig::default()`] for standard settings.
    ///
    /// Creates `data_dir` if it does not exist.
    pub fn open_with_mode(
        data_dir: &Path,
        mode: StorageSyncMode,
        appender_config: AppenderConfig,
    ) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;

        let mut partitions: HashMap<(String, u32), Arc<Mutex<Partition>>> = HashMap::new();
        let mut appenders: HashMap<(String, u32), AppenderHandle> = HashMap::new();
        let mut syncers: HashMap<(String, u32), WalSyncerHandle> = HashMap::new();
        let appender_mode = match mode {
            StorageSyncMode::Sync => AppenderMode::Sync,
            StorageSyncMode::Async { .. } => AppenderMode::Async,
        };
        let streams_dir = data_dir.join("streams");

        if streams_dir.is_dir() {
            for stream_entry in fs::read_dir(&streams_dir)? {
                let stream_entry = stream_entry?;
                let stream_path = stream_entry.path();
                if !stream_path.is_dir() {
                    continue;
                }
                let stream_name = stream_entry.file_name().to_string_lossy().into_owned();

                let partitions_dir = stream_path.join("partitions");
                if !partitions_dir.is_dir() {
                    continue;
                }

                for part_entry in fs::read_dir(&partitions_dir)? {
                    let part_entry = part_entry?;
                    let part_path = part_entry.path();
                    if !part_path.is_dir() {
                        continue;
                    }
                    let part_id: u32 = match part_entry.file_name().to_string_lossy().parse() {
                        Ok(id) => id,
                        Err(_) => continue, // skip non-numeric directories
                    };

                    let partition = Partition::open(&part_path, &stream_name, part_id)?;
                    let key = (stream_name.clone(), part_id);
                    let partition_arc = Arc::new(Mutex::new(partition));
                    let appender =
                        wal_appender::spawn(partition_arc.clone(), appender_config, appender_mode);
                    if let StorageSyncMode::Async { interval, .. } = mode {
                        let syncer = wal_syncer::spawn(partition_arc.clone(), interval);
                        syncers.insert(key.clone(), syncer);
                    }
                    appenders.insert(key.clone(), appender);
                    partitions.insert(key, partition_arc);
                }
            }
        }

        Ok(Self {
            inner: Arc::new(FileStorageInner {
                data_dir: data_dir.to_path_buf(),
                appenders: appenders.into_iter().collect(),
                syncers: syncers.into_iter().collect(),
                partitions: partitions.into_iter().collect(),
                appender_config,
                storage_sync_mode: mode,
                seal_tx: std::sync::Mutex::new(None),
            }),
        })
    }

    /// Return the root data directory for this storage engine.
    pub fn data_dir(&self) -> &Path {
        &self.inner.data_dir
    }

    /// List all stream names.
    pub fn list_streams(&self) -> Vec<String> {
        let mut streams: Vec<String> = self
            .inner
            .partitions
            .iter()
            .map(|entry| entry.key().0.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        streams.sort();
        streams
    }

    /// Get total storage bytes for a stream.
    ///
    /// Uses `try_lock` to avoid blocking the caller (async or sync). The
    /// lock is held for microseconds per batch flush, so contention is rare.
    /// Returns `None` if the stream is unknown; also `None` on the very
    /// unlikely event that the appender is mid-flush (the API handler
    /// already falls back to 0 in that case via `unwrap_or`).
    pub fn stream_storage_bytes(&self, stream: &str) -> Option<u64> {
        let key = (stream.to_string(), 0u32);
        let part_arc = self.inner.partitions.get(&key).map(|r| r.value().clone())?;
        part_arc.try_lock().ok().map(|g| g.total_bytes())
    }

    /// Get the head offset (next offset to be assigned) for a stream.
    ///
    /// Uses `try_lock` — see [`stream_storage_bytes`] for the rationale.
    pub fn stream_head_offset(&self, stream: &str) -> Option<u64> {
        let key = (stream.to_string(), 0u32);
        let part_arc = self.inner.partitions.get(&key).map(|r| r.value().clone())?;
        part_arc.try_lock().ok().map(|g| g.next_offset())
    }

    /// Return the directory path for a given stream + partition.
    pub fn partition_dir(&self, stream: &str, partition: u32) -> PathBuf {
        self.inner
            .data_dir
            .join("streams")
            .join(stream)
            .join("partitions")
            .join(partition.to_string())
    }

    /// Register a notification sender for sealed (rolled) segments.
    ///
    /// Sets the sender on all existing partitions and stores it so that
    /// partitions created later will also receive the sender.
    ///
    /// Called once at startup before any appenders begin producing, so
    /// `try_lock` is expected to succeed immediately for all partitions.
    pub(crate) fn set_seal_notifier(&self, tx: mpsc::UnboundedSender<partition::SealedSegmentInfo>) {
        // Set on all existing partitions.
        for entry in self.inner.partitions.iter() {
            entry
                .value()
                .try_lock()
                .expect("partition lock held during set_seal_notifier — unexpected contention at startup")
                .set_seal_notifier(tx.clone());
        }
        // Store for new partitions created later.
        *self.inner.seal_tx.lock().unwrap() = Some(tx);
    }
}

impl FileStorage {
    /// Re-open a partition from disk, picking up newly downloaded segment files.
    ///
    /// This is used by the S3 tiered storage layer after downloading a segment
    /// from S3 — the partition is replaced with a fresh `Partition::open` so the
    /// new segment files become visible.  The seal notifier is preserved from
    /// the previous partition instance.
    pub fn reload_partition(&self, stream: &str, partition_id: u32) -> io::Result<()> {
        let dir = self.partition_dir(stream, partition_id);
        if !dir.exists() {
            return Ok(());
        }

        // Preserve seal notifier from existing partition.
        // Drop the DashMap shard lock (via .map that clones the Arc out of the Ref)
        // before entering blocking_lock on the partition's inner Mutex.
        let seal_tx = {
            let key = (stream.to_string(), partition_id);
            self.inner
                .partitions
                .get(&key)
                .map(|r| r.value().clone())
                .and_then(|p| tokio::task::block_in_place(|| p.blocking_lock().seal_tx_clone()))
        };

        let mut new_partition = Partition::open(&dir, stream, partition_id)?;
        if let Some(tx) = seal_tx {
            new_partition.set_seal_notifier(tx);
        }

        let key = (stream.to_string(), partition_id);
        let partition_arc = Arc::new(Mutex::new(new_partition));
        let appender_mode = match self.inner.storage_sync_mode {
            StorageSyncMode::Sync => AppenderMode::Sync,
            StorageSyncMode::Async { .. } => AppenderMode::Async,
        };
        let appender =
            wal_appender::spawn(partition_arc.clone(), self.inner.appender_config, appender_mode);
        let syncer = if let StorageSyncMode::Async { interval, .. } = self.inner.storage_sync_mode {
            Some(wal_syncer::spawn(partition_arc.clone(), interval))
        } else {
            None
        };

        self.inner.partitions.insert(key.clone(), partition_arc);
        self.inner.appenders.insert(key.clone(), appender);
        if let Some(s) = syncer {
            self.inner.syncers.insert(key, s);
        } else {
            self.inner.syncers.remove(&key);
        }

        Ok(())
    }
}

impl FileStorage {
    /// Override the segment max bytes threshold for a specific stream.
    /// Returns `false` if the stream is unknown. Intended for tests that
    /// need to force segment rolling without producing 256 MiB of data.
    /// # Panics
    ///
    /// Panics if the partition lock is already held (e.g. a flush is in
    /// progress). Safe to call from tests where flush timing is controlled.
    /// Not intended for production call sites; called by `exspeed-broker`
    /// integration tests to force segment rolling without producing real data.
    pub fn set_stream_segment_max_bytes(&self, stream: &str, max: u64) -> bool {
        let key = (stream.to_string(), 0u32);
        match self.inner.partitions.get(&key).map(|r| r.value().clone()) {
            Some(arc) => {
                // Called from tests only (possibly async context). try_lock is
                // safe here — test code controls the timing and there is no
                // concurrent flush in progress when this is called.
                arc.try_lock()
                    .expect("partition lock held during set_segment_max_bytes — unexpected")
                    .set_segment_max_bytes(max);
                true
            }
            None => false,
        }
    }

    /// Enforce retention for all streams.
    pub fn enforce_all_retention(&self) -> io::Result<()> {
        // Collect (stream_name, Arc<Mutex<Partition>>) pairs without holding the
        // DashMap shard lock across blocking_lock() calls.
        let entries: Vec<(String, Arc<Mutex<Partition>>)> = self
            .inner
            .partitions
            .iter()
            .map(|entry| (entry.key().0.clone(), entry.value().clone()))
            .collect();

        for (stream_name, partition_arc) in entries {
            let stream_dir = self.inner.data_dir.join("streams").join(&stream_name);
            let config = StreamConfig::load(&stream_dir)?;

            let stats = tokio::task::block_in_place(|| {
                partition_arc
                    .blocking_lock()
                    .enforce_retention(config.max_age_secs, config.max_bytes)
            })?;
            if stats.segments_deleted > 0 {
                tracing::info!(
                    stream = stream_name.as_str(),
                    segments_deleted = stats.segments_deleted,
                    bytes_reclaimed = stats.bytes_reclaimed,
                    "retention enforced"
                );
            }
        }

        Ok(())
    }
}

// -- Sync implementations (called from spawn_blocking) -------------------------

impl FileStorage {
    fn create_stream_sync(
        &self,
        stream: &StreamName,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<(), StorageError> {
        let key = (stream.as_str().to_string(), 0u32);

        // Fast path: check existence without the entry lock.
        if self.inner.partitions.contains_key(&key) {
            return Err(StorageError::StreamAlreadyExists(stream.clone()));
        }

        let dir = self.partition_dir(stream.as_str(), 0);
        let mut partition = Partition::create(&dir, stream.as_str(), 0)?;
        if let Some(ref tx) = *self.inner.seal_tx.lock().unwrap() {
            partition.set_seal_notifier(tx.clone());
        }

        let partition_arc = Arc::new(Mutex::new(partition));
        let appender_mode = match self.inner.storage_sync_mode {
            StorageSyncMode::Sync => AppenderMode::Sync,
            StorageSyncMode::Async { .. } => AppenderMode::Async,
        };
        let appender =
            wal_appender::spawn(partition_arc.clone(), self.inner.appender_config, appender_mode);
        let syncer = if let StorageSyncMode::Async { interval, .. } = self.inner.storage_sync_mode {
            Some(wal_syncer::spawn(partition_arc.clone(), interval))
        } else {
            None
        };

        // Atomic check-and-insert: use entry() API to guard against a race where
        // two callers pass the fast-path check simultaneously.
        let entry = self.inner.partitions.entry(key.clone());
        use dashmap::mapref::entry::Entry;
        match entry {
            Entry::Occupied(_) => {
                return Err(StorageError::StreamAlreadyExists(stream.clone()));
            }
            Entry::Vacant(v) => {
                v.insert(partition_arc);
            }
        }

        self.inner.appenders.insert(key.clone(), appender);
        if let Some(s) = syncer {
            self.inner.syncers.insert(key.clone(), s);
        }

        // Save stream config
        let config = StreamConfig::from_request(max_age_secs, max_bytes, 0, 0);
        let stream_dir = self.inner.data_dir.join("streams").join(stream.as_str());
        config.save(&stream_dir).map_err(StorageError::Io)?;

        Ok(())
    }

    fn read_sync(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        // Clone the Arc out of the DashMap guard before acquiring the partition Mutex.
        let part_arc = {
            let key = (stream.as_str().to_string(), 0u32);
            self.inner
                .partitions
                .get(&key)
                .map(|r| r.value().clone())
                .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?
        };

        // We're inside spawn_blocking — use blocking_lock to acquire the tokio Mutex.
        let part = tokio::task::block_in_place(|| part_arc.blocking_lock());

        // Refuse to silently skip over trimmed-away history. `from < earliest`
        // indicates the consumer is behind the retention window — they must
        // explicitly re-seek. Tailing past `next` is still legal (empty Ok).
        let earliest = part.earliest_offset();
        let next = part.next_offset();
        if from.0 < earliest && from.0 < next {
            return Err(StorageError::OffsetOutOfRange {
                requested: from.0,
                earliest,
            });
        }

        let records = part.read(from, max_records)?;
        Ok(records)
    }

    fn seek_by_time_sync(
        &self,
        stream: &StreamName,
        timestamp: u64,
    ) -> Result<Offset, StorageError> {
        let part_arc = {
            let key = (stream.as_str().to_string(), 0u32);
            self.inner
                .partitions
                .get(&key)
                .map(|r| r.value().clone())
                .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?
        };

        let part = tokio::task::block_in_place(|| part_arc.blocking_lock());
        let offset = part.seek_by_time(timestamp)?;
        Ok(offset)
    }

    fn list_streams_sync(&self) -> Result<Vec<StreamName>, StorageError> {
        let mut streams: Vec<StreamName> = self
            .inner
            .partitions
            .iter()
            .map(|entry| entry.key().0.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .filter_map(|n| StreamName::try_from(n.as_str()).ok())
            .collect();
        streams.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(streams)
    }

    fn trim_up_to_sync(
        &self,
        stream: &StreamName,
        keep_from: Offset,
    ) -> Result<(), StorageError> {
        let part_arc = {
            let key = (stream.as_str().to_string(), 0u32);
            self.inner
                .partitions
                .get(&key)
                .map(|r| r.value().clone())
                .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?
        };

        let mut part = tokio::task::block_in_place(|| part_arc.blocking_lock());
        part.trim_up_to(keep_from.0).map_err(StorageError::Io)?;
        Ok(())
    }

    fn delete_stream_sync(&self, stream: &StreamName) -> Result<(), StorageError> {
        // Idempotent: removing a non-existent stream is Ok.
        // Drop appenders first (writer tasks drain and exit), then syncers
        // (shutdown signal fires, final fsync, tasks exit), then partitions.
        let key_prefix = stream.as_str().to_string();
        self.inner.appenders.retain(|(name, _), _| name != &key_prefix);
        self.inner.syncers.retain(|(name, _), _| name != &key_prefix);
        self.inner.partitions.retain(|(name, _), _| name != &key_prefix);

        let stream_dir = self.inner.data_dir.join("streams").join(stream.as_str());
        if stream_dir.exists() {
            fs::remove_dir_all(&stream_dir).map_err(StorageError::Io)?;
        }
        Ok(())
    }

    fn stream_bounds_sync(
        &self,
        stream: &StreamName,
    ) -> Result<(Offset, Offset), StorageError> {
        let part_arc = {
            let key = (stream.as_str().to_string(), 0u32);
            self.inner
                .partitions
                .get(&key)
                .map(|r| r.value().clone())
                .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?
        };

        let part = tokio::task::block_in_place(|| part_arc.blocking_lock());
        Ok((Offset(part.earliest_offset()), Offset(part.next_offset())))
    }

    fn truncate_from_sync(
        &self,
        stream: &StreamName,
        drop_from: Offset,
    ) -> Result<(), StorageError> {
        let part_arc = {
            let key = (stream.as_str().to_string(), 0u32);
            self.inner
                .partitions
                .get(&key)
                .map(|r| r.value().clone())
                .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?
        };

        let mut part = tokio::task::block_in_place(|| part_arc.blocking_lock());
        part.truncate_from(drop_from.0).map_err(StorageError::Io)?;
        Ok(())
    }
}

#[async_trait]
impl StorageEngine for FileStorage {
    async fn create_stream(
        &self,
        stream: &StreamName,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<(), StorageError> {
        let this = self.clone();
        let stream = stream.clone();
        tokio::task::spawn_blocking(move || this.create_stream_sync(&stream, max_age_secs, max_bytes))
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    async fn append(
        &self,
        stream: &StreamName,
        record: &Record,
    ) -> Result<(Offset, u64), StorageError> {
        let key = (stream.as_str().to_string(), 0u32);
        // Clone the AppenderHandle out of the DashMap guard before awaiting.
        let appender = self
            .inner
            .appenders
            .get(&key)
            .map(|r| r.value().clone())
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        appender.append(record.clone()).await
    }

    async fn read(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let this = self.clone();
        let stream = stream.clone();
        tokio::task::spawn_blocking(move || this.read_sync(&stream, from, max_records))
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    async fn seek_by_time(
        &self,
        stream: &StreamName,
        timestamp: u64,
    ) -> Result<Offset, StorageError> {
        let this = self.clone();
        let stream = stream.clone();
        tokio::task::spawn_blocking(move || this.seek_by_time_sync(&stream, timestamp))
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    async fn list_streams(&self) -> Result<Vec<StreamName>, StorageError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.list_streams_sync())
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    async fn trim_up_to(
        &self,
        stream: &StreamName,
        keep_from: Offset,
    ) -> Result<(), StorageError> {
        let this = self.clone();
        let stream = stream.clone();
        tokio::task::spawn_blocking(move || this.trim_up_to_sync(&stream, keep_from))
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    async fn delete_stream(&self, stream: &StreamName) -> Result<(), StorageError> {
        let this = self.clone();
        let stream = stream.clone();
        tokio::task::spawn_blocking(move || this.delete_stream_sync(&stream))
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    async fn stream_bounds(
        &self,
        stream: &StreamName,
    ) -> Result<(Offset, Offset), StorageError> {
        let this = self.clone();
        let stream = stream.clone();
        tokio::task::spawn_blocking(move || this.stream_bounds_sync(&stream))
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    async fn truncate_from(
        &self,
        stream: &StreamName,
        drop_from: Offset,
    ) -> Result<(), StorageError> {
        let this = self.clone();
        let stream = stream.clone();
        tokio::task::spawn_blocking(move || this.truncate_from_sync(&stream, drop_from))
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    async fn append_batch(
        &self,
        stream: &StreamName,
        records: Vec<exspeed_streams::Record>,
    ) -> Result<Vec<(exspeed_common::Offset, u64)>, StorageError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let key = (stream.as_str().to_string(), 0u32);
        let appender = self
            .inner
            .appenders
            .get(&key)
            .map(|r| r.value().clone())
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        appender.append_batch(records).await
    }
}
