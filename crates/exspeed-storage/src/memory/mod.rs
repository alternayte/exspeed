use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError, StoredRecord};

pub struct MemoryStorage {
    streams: RwLock<HashMap<String, Vec<StoredRecord>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
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
        _max_age_secs: u64,
        _max_bytes: u64,
    ) -> Result<(), StorageError> {
        let mut map = self.streams.write().unwrap();
        let key = stream.as_str().to_string();
        if map.contains_key(&key) {
            return Err(StorageError::StreamAlreadyExists(stream.clone()));
        }
        map.insert(key, Vec::new());
        Ok(())
    }

    fn append(&self, stream: &StreamName, record: &Record) -> Result<Offset, StorageError> {
        let mut map = self.streams.write().unwrap();
        let key = stream.as_str().to_string();
        let records = map
            .get_mut(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
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
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let map = self.streams.read().unwrap();
        let key = stream.as_str().to_string();
        let records = map
            .get(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        let start = from.0 as usize;
        if start >= records.len() {
            return Ok(Vec::new());
        }
        let end = (start + max_records).min(records.len());
        Ok(records[start..end].to_vec())
    }

    fn seek_by_time(&self, stream: &StreamName, timestamp: u64) -> Result<Offset, StorageError> {
        let map = self.streams.read().unwrap();
        let name = stream.as_str().to_string();
        let records = map
            .get(&name)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        for record in records {
            if record.timestamp >= timestamp {
                return Ok(record.offset);
            }
        }
        Ok(Offset(records.len() as u64))
    }

    fn list_streams(&self) -> Result<Vec<StreamName>, StorageError> {
        let map = self.streams.read().unwrap();
        let mut streams: Vec<StreamName> = map
            .keys()
            .filter_map(|k| StreamName::try_from(k.as_str()).ok())
            .collect();
        streams.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(streams)
    }
}
