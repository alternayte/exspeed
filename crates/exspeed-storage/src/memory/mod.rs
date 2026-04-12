use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use exspeed_common::{Offset, PartitionId, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError, StoredRecord};

pub struct MemoryStorage {
    partitions: RwLock<HashMap<(String, u32), Vec<StoredRecord>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            partitions: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

impl StorageEngine for MemoryStorage {
    fn create_stream(
        &self,
        stream: &StreamName,
        partition_count: u32,
    ) -> Result<(), StorageError> {
        let mut map = self.partitions.write().unwrap();
        let key = (stream.as_str().to_string(), 0u32);
        if map.contains_key(&key) {
            return Err(StorageError::StreamAlreadyExists(stream.clone()));
        }
        for p in 0..partition_count {
            map.insert((stream.as_str().to_string(), p), Vec::new());
        }
        Ok(())
    }

    fn append(
        &self,
        stream: &StreamName,
        partition: PartitionId,
        record: &Record,
    ) -> Result<Offset, StorageError> {
        let mut map = self.partitions.write().unwrap();
        let stream_str = stream.as_str().to_string();
        let key = (stream_str.clone(), partition.0);
        if !map.contains_key(&key) {
            let err = if map.contains_key(&(stream_str, 0)) {
                StorageError::PartitionNotFound { stream: stream.clone(), partition }
            } else {
                StorageError::StreamNotFound(stream.clone())
            };
            return Err(err);
        }
        let records = map.get_mut(&key).unwrap();
        let offset = Offset(records.len() as u64);
        let stored = StoredRecord {
            offset,
            timestamp: now_nanos(),
            subject: record.subject.clone(),
            key: record.key.clone(),
            value: record.value.clone(),
            headers: record.headers.clone(),
        };
        records.push(stored);
        Ok(offset)
    }

    fn read(
        &self,
        stream: &StreamName,
        partition: PartitionId,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let map = self.partitions.read().unwrap();
        let key = (stream.as_str().to_string(), partition.0);
        let records = map.get(&key).ok_or_else(|| {
            if map.contains_key(&(stream.as_str().to_string(), 0)) {
                StorageError::PartitionNotFound { stream: stream.clone(), partition }
            } else {
                StorageError::StreamNotFound(stream.clone())
            }
        })?;
        let start = from.0 as usize;
        if start >= records.len() {
            return Ok(Vec::new());
        }
        let end = (start + max_records).min(records.len());
        Ok(records[start..end].to_vec())
    }
}
