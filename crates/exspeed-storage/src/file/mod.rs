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
use std::sync::RwLock;

use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError, StoredRecord};

use crate::file::partition::Partition;
use crate::file::stream_config::StreamConfig;

/// File-backed storage engine.
///
/// Directory layout:
///   `{data_dir}/streams/{stream}/partitions/0/`
///
/// Each partition directory contains `.seg` segment files and a `wal.log`.
pub struct FileStorage {
    data_dir: PathBuf,
    partitions: RwLock<HashMap<(String, u32), Partition>>,
}

impl FileStorage {
    /// Create a new, empty `FileStorage` rooted at `data_dir`.
    ///
    /// Creates the `data_dir` directory if it does not exist.
    pub fn new(data_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            partitions: RwLock::new(HashMap::new()),
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
            data_dir: data_dir.to_path_buf(),
            partitions: RwLock::new(partitions),
        })
    }

    /// Return the root data directory for this storage engine.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// List all stream names.
    pub fn list_streams(&self) -> Vec<String> {
        let map = self.partitions.read().unwrap();
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
        let map = self.partitions.read().unwrap();
        let key = (stream.to_string(), 0u32);
        map.get(&key).map(|p| p.total_bytes())
    }

    /// Get the head offset (next offset to be assigned) for a stream.
    pub fn stream_head_offset(&self, stream: &str) -> Option<u64> {
        let map = self.partitions.read().unwrap();
        let key = (stream.to_string(), 0u32);
        map.get(&key).map(|p| p.next_offset())
    }

    /// Return the directory path for a given stream + partition.
    pub fn partition_dir(&self, stream: &str, partition: u32) -> PathBuf {
        self.data_dir
            .join("streams")
            .join(stream)
            .join("partitions")
            .join(partition.to_string())
    }
}

impl FileStorage {
    /// Enforce retention for all streams.
    pub fn enforce_all_retention(&self) -> io::Result<()> {
        let mut map = self.partitions.write().unwrap();

        for ((stream_name, _), partition) in map.iter_mut() {
            let stream_dir = self.data_dir.join("streams").join(stream_name);
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

impl StorageEngine for FileStorage {
    fn create_stream(
        &self,
        stream: &StreamName,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<(), StorageError> {
        let mut map = self.partitions.write().unwrap();
        let key = (stream.as_str().to_string(), 0u32);
        if map.contains_key(&key) {
            return Err(StorageError::StreamAlreadyExists(stream.clone()));
        }

        let dir = self.partition_dir(stream.as_str(), 0);
        let partition = Partition::create(&dir, stream.as_str(), 0)?;
        map.insert(key, partition);

        // Save stream config
        let config = StreamConfig::from_request(max_age_secs, max_bytes);
        let stream_dir = self.data_dir.join("streams").join(stream.as_str());
        config.save(&stream_dir).map_err(StorageError::Io)?;

        Ok(())
    }

    fn append(&self, stream: &StreamName, record: &Record) -> Result<Offset, StorageError> {
        let mut map = self.partitions.write().unwrap();
        let key = (stream.as_str().to_string(), 0u32);

        let part = map
            .get_mut(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;

        let offset = part.append(record)?;
        Ok(offset)
    }

    fn read(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let map = self.partitions.read().unwrap();
        let key = (stream.as_str().to_string(), 0u32);

        let part = map
            .get(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;

        let records = part.read(from, max_records)?;
        Ok(records)
    }

    fn seek_by_time(&self, stream: &StreamName, timestamp: u64) -> Result<Offset, StorageError> {
        let map = self.partitions.read().unwrap();
        let key = (stream.as_str().to_string(), 0u32);
        let part = map
            .get(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        let offset = part.seek_by_time(timestamp)?;
        Ok(offset)
    }
}
