use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use exspeed_common::{Offset, StreamName};
use exspeed_streams::record::Record;
use exspeed_streams::traits::StorageEngine;
use exspeed_streams::StorageError;

// ---------------------------------------------------------------------------
// AppendResult
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendResult {
    Written(Offset),
    Duplicate(Offset),
}

impl AppendResult {
    pub fn offset(&self) -> Offset {
        match self {
            AppendResult::Written(o) | AppendResult::Duplicate(o) => *o,
        }
    }
}

// ---------------------------------------------------------------------------
// DedupMap
// ---------------------------------------------------------------------------

struct DedupEntry {
    offset: u64,
    inserted_at: Instant,
}

pub(crate) struct DedupMap {
    seen: HashMap<String, DedupEntry>,
    window: Duration,
}

impl DedupMap {
    fn new(window: Duration) -> Self {
        Self {
            seen: HashMap::new(),
            window,
        }
    }

    /// Check if key is a duplicate. Returns Some(offset) if duplicate, None if new.
    fn check(&self, key: &str) -> Option<u64> {
        if let Some(entry) = self.seen.get(key) {
            if entry.inserted_at.elapsed() < self.window {
                return Some(entry.offset);
            }
        }
        None
    }

    /// Insert a key with its stored offset.
    fn insert(&mut self, key: String, offset: u64) {
        self.seen.insert(
            key,
            DedupEntry {
                offset,
                inserted_at: Instant::now(),
            },
        );
    }

    /// Remove entries older than the window.
    fn evict_expired(&mut self) {
        self.seen
            .retain(|_, entry| entry.inserted_at.elapsed() < self.window);
    }

    fn len(&self) -> usize {
        self.seen.len()
    }

    fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

// ---------------------------------------------------------------------------
// BrokerAppend
// ---------------------------------------------------------------------------

const IDEMPOTENCY_HEADER: &str = "x-idempotency-key";

pub struct BrokerAppend {
    storage: Arc<dyn StorageEngine>,
    dedup_maps: RwLock<HashMap<StreamName, DedupMap>>,
    default_window: Duration,
}

impl BrokerAppend {
    pub fn new(storage: Arc<dyn StorageEngine>, default_window_secs: u64) -> Self {
        Self {
            storage,
            dedup_maps: RwLock::new(HashMap::new()),
            default_window: Duration::from_secs(default_window_secs),
        }
    }

    /// Get a reference to the underlying storage engine (for read operations).
    pub fn storage(&self) -> &Arc<dyn StorageEngine> {
        &self.storage
    }

    /// Append a record with idempotency dedup.
    pub async fn append(
        &self,
        stream: &StreamName,
        record: &Record,
    ) -> Result<AppendResult, StorageError> {
        // Extract idempotency key from headers
        let idemp_key = record
            .headers
            .iter()
            .find(|(k, _)| k == IDEMPOTENCY_HEADER)
            .map(|(_, v)| v.clone());

        match idemp_key {
            Some(key) => {
                // Check dedup map
                {
                    let maps = self.dedup_maps.read().await;
                    if let Some(map) = maps.get(stream) {
                        if let Some(existing_offset) = map.check(&key) {
                            return Ok(AppendResult::Duplicate(Offset(existing_offset)));
                        }
                    }
                }

                // Not a duplicate — write to storage
                let offset = self.storage.append(stream, record).await?;

                // Insert into dedup map
                {
                    let mut maps = self.dedup_maps.write().await;
                    let map = maps
                        .entry(stream.clone())
                        .or_insert_with(|| DedupMap::new(self.default_window));
                    map.insert(key, offset.0);
                }

                Ok(AppendResult::Written(offset))
            }
            None => {
                // No idempotency key — pass through directly
                let offset = self.storage.append(stream, record).await?;
                Ok(AppendResult::Written(offset))
            }
        }
    }

    /// Run periodic eviction of expired dedup entries. Call from a background task.
    pub async fn evict_expired(&self) {
        let mut maps = self.dedup_maps.write().await;
        for map in maps.values_mut() {
            map.evict_expired();
        }
    }

    /// Rebuild dedup maps by scanning recent records from the log.
    /// Call at startup before accepting writes.
    pub async fn rebuild_from_log(&self) -> Result<(), StorageError> {
        let streams = self.storage.list_streams().await?;

        let cutoff_ns = {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            now_ns.saturating_sub(self.default_window.as_nanos() as u64)
        };

        let mut maps = self.dedup_maps.write().await;

        for stream_name in &streams {
            let mut map = DedupMap::new(self.default_window);

            // Use seek_by_time to jump to the dedup window boundary
            let mut offset = self.storage.seek_by_time(stream_name, cutoff_ns)
                .await
                .unwrap_or(Offset(0)); // fallback to full scan if seek fails

            let batch_size = 1000;

            // Scan forward through the stream
            loop {
                let records = self.storage.read(stream_name, offset, batch_size).await?;

                if records.is_empty() {
                    break;
                }

                for record in &records {
                    // Only index records within the dedup window
                    if record.timestamp >= cutoff_ns {
                        if let Some((_, v)) = record
                            .headers
                            .iter()
                            .find(|(k, _)| k == IDEMPOTENCY_HEADER)
                        {
                            map.insert(v.clone(), record.offset.0);
                        }
                    }
                }

                offset = Offset(records.last().unwrap().offset.0 + 1);
            }

            if !map.is_empty() {
                tracing::info!(
                    stream = %stream_name,
                    keys = map.len(),
                    "rebuilt dedup map from log"
                );
                maps.insert(stream_name.clone(), map);
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use exspeed_storage::memory::MemoryStorage;

    async fn make_broker_append() -> BrokerAppend {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create_stream(&StreamName::try_from("events").unwrap(), 0, 0)
            .await
            .unwrap();
        BrokerAppend::new(storage, 300) // 5 min window
    }

    fn make_record(subject: &str, value: &[u8], idemp_key: Option<&str>) -> Record {
        let mut headers = vec![];
        if let Some(key) = idemp_key {
            headers.push(("x-idempotency-key".to_string(), key.to_string()));
        }
        Record {
            key: None,
            value: Bytes::copy_from_slice(value),
            subject: subject.to_string(),
            headers,
        }
    }

    #[tokio::test]
    async fn append_without_idemp_key_always_writes() {
        let ba = make_broker_append().await;
        let stream = StreamName::try_from("events").unwrap();
        let r1 = make_record("events.created", b"data1", None);
        let r2 = make_record("events.created", b"data1", None); // same data, no key

        let res1 = ba.append(&stream, &r1).await.unwrap();
        let res2 = ba.append(&stream, &r2).await.unwrap();

        assert!(matches!(res1, AppendResult::Written(Offset(0))));
        assert!(matches!(res2, AppendResult::Written(Offset(1)))); // both written
    }

    #[tokio::test]
    async fn append_with_idemp_key_deduplicates() {
        let ba = make_broker_append().await;
        let stream = StreamName::try_from("events").unwrap();
        let r1 = make_record("events.created", b"data1", Some("order-123"));
        let r2 = make_record("events.created", b"data1", Some("order-123")); // same key

        let res1 = ba.append(&stream, &r1).await.unwrap();
        let res2 = ba.append(&stream, &r2).await.unwrap();

        assert!(matches!(res1, AppendResult::Written(Offset(0))));
        assert!(matches!(res2, AppendResult::Duplicate(Offset(0)))); // deduped
    }

    #[tokio::test]
    async fn different_idemp_keys_both_written() {
        let ba = make_broker_append().await;
        let stream = StreamName::try_from("events").unwrap();
        let r1 = make_record("events.created", b"data1", Some("order-123"));
        let r2 = make_record("events.created", b"data2", Some("order-456"));

        let res1 = ba.append(&stream, &r1).await.unwrap();
        let res2 = ba.append(&stream, &r2).await.unwrap();

        assert!(matches!(res1, AppendResult::Written(Offset(0))));
        assert!(matches!(res2, AppendResult::Written(Offset(1))));
    }

    #[tokio::test]
    async fn same_key_different_streams_both_written() {
        let ba = make_broker_append().await;
        let s1 = StreamName::try_from("stream-a").unwrap();
        let s2 = StreamName::try_from("stream-b").unwrap();
        ba.storage().create_stream(&s1, 0, 0).await.unwrap();
        ba.storage().create_stream(&s2, 0, 0).await.unwrap();

        let r = make_record("evt", b"data", Some("key-1"));
        let res1 = ba.append(&s1, &r).await.unwrap();
        let res2 = ba.append(&s2, &r).await.unwrap();

        assert!(matches!(res1, AppendResult::Written(Offset(0))));
        assert!(matches!(res2, AppendResult::Written(Offset(0))));
    }

    #[tokio::test]
    async fn rebuild_from_log_restores_dedup_state() {
        let storage = Arc::new(MemoryStorage::new());
        let stream = StreamName::try_from("events").unwrap();
        storage.create_stream(&stream, 0, 0).await.unwrap();

        // Write some records with idempotency keys directly to storage
        let r1 = Record {
            key: None,
            value: Bytes::from_static(b"data1"),
            subject: "evt".to_string(),
            headers: vec![("x-idempotency-key".to_string(), "key-A".to_string())],
        };
        let r2 = Record {
            key: None,
            value: Bytes::from_static(b"data2"),
            subject: "evt".to_string(),
            headers: vec![("x-idempotency-key".to_string(), "key-B".to_string())],
        };
        storage.append(&stream, &r1).await.unwrap();
        storage.append(&stream, &r2).await.unwrap();

        // Create BrokerAppend and rebuild
        let ba = BrokerAppend::new(storage, 300);
        ba.rebuild_from_log().await.unwrap();

        // Now duplicates should be caught
        let dup_a = make_record("evt", b"data1", Some("key-A"));
        let res = ba.append(&stream, &dup_a).await.unwrap();
        assert!(matches!(res, AppendResult::Duplicate(Offset(0))));

        let dup_b = make_record("evt", b"data2", Some("key-B"));
        let res = ba.append(&stream, &dup_b).await.unwrap();
        assert!(matches!(res, AppendResult::Duplicate(Offset(1))));

        // New key should still work
        let new = make_record("evt", b"data3", Some("key-C"));
        let res = ba.append(&stream, &new).await.unwrap();
        assert!(matches!(res, AppendResult::Written(Offset(2))));
    }

    #[tokio::test]
    async fn append_result_offset_helper() {
        assert_eq!(AppendResult::Written(Offset(5)).offset(), Offset(5));
        assert_eq!(AppendResult::Duplicate(Offset(3)).offset(), Offset(3));
    }
}
