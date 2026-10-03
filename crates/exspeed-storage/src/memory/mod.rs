use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError, StoredRecord, StreamConfig};

/// Per-stream state: the retained records and the next offset to assign.
///
/// `next_offset` is tracked explicitly rather than derived from `records.len()`
/// so that `trim_up_to` (which drops leading records) does not regress the
/// offset counter. `truncate_from` is the only caller permitted to *lower*
/// `next_offset`.
struct StreamState {
    records: Vec<StoredRecord>,
    next_offset: u64,
    config: StreamConfig,
    watch: tokio::sync::watch::Sender<u64>,
}

impl StreamState {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            next_offset: 0,
            config: StreamConfig::default(),
            watch: tokio::sync::watch::channel(0).0,
        }
    }

    fn set_next(&mut self, next: u64) {
        self.next_offset = next;
        self.watch.send_replace(next);
    }
}

pub struct MemoryStorage {
    streams: RwLock<HashMap<String, StreamState>>,
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

#[async_trait]
impl StorageEngine for MemoryStorage {
    async fn create_stream_with(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), StorageError> {
        self.create_stream(stream, config.max_age_secs, config.max_bytes)
            .await?;
        self.update_stream_config(stream, config).await
    }

    async fn stream_config(&self, stream: &StreamName) -> Result<StreamConfig, StorageError> {
        let streams = self.streams.read().unwrap();
        streams
            .get(stream.as_str())
            .map(|s| s.config.clone())
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))
    }

    async fn update_stream_config(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), StorageError> {
        let mut streams = self.streams.write().unwrap();
        let state = streams
            .get_mut(stream.as_str())
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        state.config = config.clone();
        Ok(())
    }

    async fn create_stream(
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
        map.insert(key, StreamState::new());
        Ok(())
    }

    async fn append(
        &self,
        stream: &StreamName,
        record: &Record,
    ) -> Result<(Offset, u64), StorageError> {
        let mut map = self.streams.write().unwrap();
        let key = stream.as_str().to_string();
        let state = map
            .get_mut(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        let offset = Offset(state.next_offset);
        // Honor the caller-supplied timestamp when present (replication
        // follower path preserves the leader's persisted timestamp); else
        // mint a fresh one from the wall clock.
        let timestamp = record.timestamp_ns.unwrap_or_else(now_nanos);
        let stored = StoredRecord {
            offset,
            timestamp,
            subject: record.subject.clone(),
            key: record.key.clone(),
            value: record.value.clone(),
            headers: record.headers.clone(),
        };
        state.records.push(stored);
        let next = state.next_offset + 1;
        state.set_next(next);
        Ok((offset, timestamp))
    }

    async fn append_at(
        &self,
        stream: &StreamName,
        records: Vec<StoredRecord>,
    ) -> Result<(), StorageError> {
        let mut map = self.streams.write().unwrap();
        let state = map
            .get_mut(stream.as_str())
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        let mut min = state.next_offset;
        for r in &records {
            if r.offset.0 < min {
                return Err(StorageError::OffsetConflict {
                    offset: r.offset.0,
                    min_allowed: min,
                });
            }
            min = r.offset.0 + 1;
        }
        if let Some(last) = records.last() {
            let next = last.offset.0 + 1;
            state.records.extend(records);
            state.set_next(next);
        }
        Ok(())
    }

    fn watch_appends(&self, stream: &StreamName) -> Option<tokio::sync::watch::Receiver<u64>> {
        let map = self.streams.read().unwrap();
        map.get(stream.as_str()).map(|s| s.watch.subscribe())
    }

    async fn read(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let map = self.streams.read().unwrap();
        let key = stream.as_str().to_string();
        let state = map
            .get(&key)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;

        // Refuse to silently skip over trimmed-away history. `earliest` is
        // the first retained offset, or `next_offset` when the stream has no
        // records (either never written or fully trimmed) — the second case
        // still returns an empty Ok below because `from >= next_offset` is
        // legal tailing behavior.
        let earliest = state
            .records
            .first()
            .map(|r| r.offset.0)
            .unwrap_or(state.next_offset);
        if from.0 < earliest && from.0 < state.next_offset {
            return Err(StorageError::OffsetOutOfRange {
                requested: from.0,
                earliest,
            });
        }

        // Records are stored in offset order; find the first record whose
        // offset >= `from` and take up to `max_records` from there.
        let first_idx = state.records.partition_point(|r| r.offset.0 < from.0);
        if first_idx >= state.records.len() {
            return Ok(Vec::new());
        }
        let end = (first_idx + max_records).min(state.records.len());
        Ok(state.records[first_idx..end].to_vec())
    }

    async fn seek_by_time(
        &self,
        stream: &StreamName,
        timestamp: u64,
    ) -> Result<Offset, StorageError> {
        let map = self.streams.read().unwrap();
        let name = stream.as_str().to_string();
        let state = map
            .get(&name)
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        for record in &state.records {
            if record.timestamp >= timestamp {
                return Ok(record.offset);
            }
        }
        Ok(Offset(state.next_offset))
    }

    async fn list_streams(&self) -> Result<Vec<StreamName>, StorageError> {
        let map = self.streams.read().unwrap();
        let mut streams: Vec<StreamName> = map
            .keys()
            .filter_map(|k| StreamName::try_from(k.as_str()).ok())
            .collect();
        streams.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(streams)
    }

    async fn trim_up_to(&self, stream: &StreamName, keep_from: Offset) -> Result<(), StorageError> {
        let mut map = self.streams.write().unwrap();
        let state = map
            .get_mut(stream.as_str())
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        state.records.retain(|r| r.offset.0 >= keep_from.0);
        // `trim_up_to` never touches `next_offset` — a fully trimmed stream
        // still assigns the next offset from where it left off.
        Ok(())
    }

    async fn delete_stream(&self, stream: &StreamName) -> Result<(), StorageError> {
        let mut map = self.streams.write().unwrap();
        map.remove(stream.as_str());
        Ok(())
    }

    async fn stream_bounds(&self, stream: &StreamName) -> Result<(Offset, Offset), StorageError> {
        let map = self.streams.read().unwrap();
        let state = map
            .get(stream.as_str())
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        // `earliest` is the offset of the first retained record, or
        // `next_offset` when no records remain (stream empty / fully trimmed).
        let earliest = state
            .records
            .first()
            .map(|r| r.offset)
            .unwrap_or(Offset(state.next_offset));
        Ok((earliest, Offset(state.next_offset)))
    }

    async fn truncate_from(
        &self,
        stream: &StreamName,
        drop_from: Offset,
    ) -> Result<(), StorageError> {
        let mut map = self.streams.write().unwrap();
        let state = map
            .get_mut(stream.as_str())
            .ok_or_else(|| StorageError::StreamNotFound(stream.clone()))?;
        if drop_from.0 >= state.next_offset {
            // No-op: caller asked to drop records at offsets that don't exist.
            return Ok(());
        }
        state.records.retain(|r| r.offset.0 < drop_from.0);
        state.set_next(drop_from.0);
        Ok(())
    }
}
