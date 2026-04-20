pub mod io_errors;
pub mod offset_index;
pub mod partition;
pub mod segment_reader;
pub mod segment_writer;
pub mod stream_config;
pub mod time_index;
pub mod wal;

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use tokio::sync::mpsc;

use async_trait::async_trait;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError, StoredRecord};

use crate::file::partition::Partition;
use crate::file::stream_config::StreamConfig;

struct FileStorageInner {
    data_dir: PathBuf,
    partitions: RwLock<HashMap<(String, u32), Partition>>,
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
                partitions: RwLock::new(HashMap::new()),
                seal_tx: std::sync::Mutex::new(None),
            }),
        })
    }

    /// Open an existing `FileStorage`, scanning for streams and partitions
    /// on disk and recovering each partition (including WAL replay).
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;

        let mut partitions = HashMap::new();
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
                    partitions.insert((stream_name.clone(), part_id), partition);
                }
            }
        }

        Ok(Self {
            inner: Arc::new(FileStorageInner {
                data_dir: data_dir.to_path_buf(),
                partitions: RwLock::new(partitions),
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
        let map = self.inner.partitions.read().unwrap();
        let mut streams: Vec<String> = map
            .keys()
            .map(|(name, _)| name.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        streams.sort();
        streams
    }

    /// Get total storage bytes for a stream.
    pub fn stream_storage_bytes(&self, stream: &str) -> Option<u64> {
        let map = self.inner.partitions.read().unwrap();
        let key = (stream.to_string(), 0u32);
        map.get(&key).map(|p| p.total_bytes())
    }

    /// Get the head offset (next offset to be assigned) for a stream.
    pub fn stream_head_offset(&self, stream: &str) -> Option<u64> {
        let map = self.inner.partitions.read().unwrap();
        let key = (stream.to_string(), 0u32);
        map.get(&key).map(|p| p.next_offset())
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
    pub fn set_seal_notifier(&self, tx: mpsc::UnboundedSender<partition::SealedSegmentInfo>) {
        // Set on all existing partitions.
        let mut map = self.inner.partitions.write().unwrap();
        for partition in map.values_mut() {
            partition.set_seal_notifier(tx.clone());
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
        let seal_tx = {
            let map = self.inner.partitions.read().unwrap();
            let key = (stream.to_string(), partition_id);
            map.get(&key).and_then(|p| p.seal_tx_clone())
        };

        let mut new_partition = Partition::open(&dir, stream, partition_id)?;
        if let Some(tx) = seal_tx {
            new_partition.set_seal_notifier(tx);
        }

        let mut map = self.inner.partitions.write().unwrap();
        map.insert((stream.to_string(), partition_id), new_partition);
        Ok(())
    }
}

impl FileStorage {
    /// Override the segment max bytes threshold for a specific stream.
    /// Returns `false` if the stream is unknown. Intended for tests that
    /// need to force segment rolling without producing 256 MiB of data.
    pub fn set_stream_segment_max_bytes(&self, stream: &str, max: u64) -> bool {
        let mut map = self.inner.partitions.write().unwrap();
        let key = (stream.to_string(), 0u32);
        match map.get_mut(&key) {
            Some(p) => {
                p.set_segment_max_bytes(max);
                true
            }
            None => false,
        }
    }

    /// Enforce retention for all streams.
    pub fn enforce_all_retention(&self) -> io::Result<()> {
        let mut map = self.inner.partitions.write().unwrap();

        for ((stream_name, _), partition) in map.iter_mut() {
            let stream_dir = self.inner.data_dir.join("streams").join(stream_name);
            let config = StreamConfig::load(&stream_dir)?;

            let stats = partition.enforce_retention(config.max_age_secs, config.max_bytes)?;
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
        let mut map = self.inner.partitions.write().unwrap();
        let key = (stream.as_str().to_string(), 0u32);
        if map.contains_key(&key) {
            return Err(StorageError::StreamAlreadyExists(stream.clone()));
        }

        let dir = self.partition_dir(stream.as_str(), 0);
        let mut partition = Partition::create(&dir, stream.as_str(), 0)?;
        if let Some(ref tx) = *self.inner.seal_tx.lock().unwrap() {
            partition.set_seal_notifier(tx.clone());
        }
        map.insert(key, partition);

        // Save stream config
        let config = StreamConfig::from_request(max_age_secs, max_bytes, 0, 0);
        let stream_dir = self.inner.data_dir.join("streams").join(stream.as_str());
        config.save(&stream_dir).map_err(StorageError::Io)?;

        Ok(())
    }

    fn append_sync(
        &self,
        stream: &StreamName,
        record: &Record,
    ) -> Result<(Offset, u64), StorageError> {
        let mut map = self.inner.partitions.write().unwrap();
        let key = (stream.as_str().to_string(), 0u32);

        let part = map
            .get_mut(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;

        let (offset, timestamp) = part.append(record)?;
        Ok((offset, timestamp))
    }

    fn read_sync(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let map = self.inner.partitions.read().unwrap();
        let key = (stream.as_str().to_string(), 0u32);

        let part = map
            .get(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;

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
        let map = self.inner.partitions.read().unwrap();
        let key = (stream.as_str().to_string(), 0u32);
        let part = map
            .get(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        let offset = part.seek_by_time(timestamp)?;
        Ok(offset)
    }

    fn list_streams_sync(&self) -> Result<Vec<StreamName>, StorageError> {
        let map = self.inner.partitions.read().unwrap();
        let mut streams: Vec<StreamName> = map
            .keys()
            .map(|(name, _)| name.clone())
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
        let mut map = self.inner.partitions.write().unwrap();
        let key = (stream.as_str().to_string(), 0u32);
        let part = map
            .get_mut(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        part.trim_up_to(keep_from.0).map_err(StorageError::Io)?;
        Ok(())
    }

    fn delete_stream_sync(&self, stream: &StreamName) -> Result<(), StorageError> {
        // Idempotent: removing a non-existent stream is Ok.
        let mut map = self.inner.partitions.write().unwrap();
        let key_prefix = stream.as_str().to_string();
        map.retain(|(name, _), _| name != &key_prefix);
        drop(map);

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
        let map = self.inner.partitions.read().unwrap();
        let key = (stream.as_str().to_string(), 0u32);
        let part = map
            .get(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        Ok((Offset(part.earliest_offset()), Offset(part.next_offset())))
    }

    fn truncate_from_sync(
        &self,
        stream: &StreamName,
        drop_from: Offset,
    ) -> Result<(), StorageError> {
        let mut map = self.inner.partitions.write().unwrap();
        let key = (stream.as_str().to_string(), 0u32);
        let part = map
            .get_mut(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
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
        let this = self.clone();
        let stream = stream.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || this.append_sync(&stream, &record))
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
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
}
