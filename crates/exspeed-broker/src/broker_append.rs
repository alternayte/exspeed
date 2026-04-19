use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use twox_hash::XxHash64;

use exspeed_common::{Metrics, Offset, StreamName};
use exspeed_storage::file::io_errors::is_storage_full;
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
// body_hash
// ---------------------------------------------------------------------------

pub fn hash_body(body: &[u8]) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(body);
    hasher.finish()
}

// ---------------------------------------------------------------------------
// DedupMap
// ---------------------------------------------------------------------------

struct DedupEntry {
    offset: u64,
    inserted_at: Instant,
    body_hash: u64,
}

pub(crate) struct DedupMap {
    seen: HashMap<String, DedupEntry>,
    window: Duration,
    max_entries: u64,
}

enum DedupCheckResult {
    Miss,
    HitSameBody { offset: u64 },
    HitDifferentBody { stored_offset: u64 },
}

impl DedupMap {
    fn new(window: Duration, max_entries: u64) -> Self {
        Self {
            seen: HashMap::new(),
            window,
            max_entries,
        }
    }

    fn check(&self, key: &str, body_hash: u64) -> DedupCheckResult {
        match self.seen.get(key) {
            Some(entry) if entry.inserted_at.elapsed() < self.window => {
                if entry.body_hash == body_hash {
                    DedupCheckResult::HitSameBody { offset: entry.offset }
                } else {
                    DedupCheckResult::HitDifferentBody { stored_offset: entry.offset }
                }
            }
            _ => DedupCheckResult::Miss,
        }
    }

    fn insert(&mut self, key: String, offset: u64, body_hash: u64) {
        self.seen.insert(
            key,
            DedupEntry {
                offset,
                inserted_at: Instant::now(),
                body_hash,
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

    fn time_until_oldest_expires(&self) -> Duration {
        self.seen
            .values()
            .map(|e| self.window.saturating_sub(e.inserted_at.elapsed()))
            .min()
            .unwrap_or(Duration::ZERO)
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
    default_max_entries: u64,
    metrics: Option<Arc<Metrics>>,
}

impl BrokerAppend {
    pub fn new(storage: Arc<dyn StorageEngine>, default_window_secs: u64) -> Self {
        Self::new_with_cap(storage, default_window_secs, 500_000)
    }

    pub fn new_with_cap(
        storage: Arc<dyn StorageEngine>,
        default_window_secs: u64,
        default_max_entries: u64,
    ) -> Self {
        Self {
            storage,
            dedup_maps: RwLock::new(HashMap::new()),
            default_window: Duration::from_secs(default_window_secs),
            default_max_entries,
            metrics: None,
        }
    }

    /// Attach a Metrics handle so storage write failures bump
    /// `storage_write_errors_total{stream, kind}`.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get a reference to the underlying storage engine (for read operations).
    pub fn storage(&self) -> &Arc<dyn StorageEngine> {
        &self.storage
    }

    /// Configure per-stream dedup parameters. Creates the map if not present,
    /// updates window and cap if it already exists.
    pub async fn configure_stream(
        &self,
        stream: &StreamName,
        window_secs: u64,
        max_entries: u64,
    ) {
        let mut maps = self.dedup_maps.write().await;
        maps.entry(stream.clone())
            .and_modify(|m| {
                m.window = Duration::from_secs(window_secs);
                m.max_entries = max_entries;
            })
            .or_insert_with(|| DedupMap::new(Duration::from_secs(window_secs), max_entries));
    }

    /// Return the number of live dedup entries for a stream (tests / metrics).
    pub async fn entry_count(&self, stream: &StreamName) -> usize {
        let maps = self.dedup_maps.read().await;
        maps.get(stream).map(|m| m.len()).unwrap_or(0)
    }

    fn record_write_error(&self, stream: &StreamName, err: &StorageError) {
        let Some(m) = &self.metrics else { return };
        let kind = match err {
            StorageError::Io(io_err) if is_storage_full(io_err) => "storage_full",
            _ => "other",
        };
        m.record_storage_write_error(stream.as_str(), kind);
    }

    /// Append a record with idempotency dedup.
    ///
    /// Three-phase protocol to avoid holding any lock across `.await`:
    ///   Phase 1 — optimistic read-lock check (fast path for duplicates).
    ///   Phase 2 — write-lock re-check + cap enforcement (no await inside).
    ///   Phase 3 — storage write (no lock held).
    ///   Phase 4 — write-lock insert into dedup map.
    pub async fn append(
        &self,
        stream: &StreamName,
        record: &Record,
    ) -> Result<AppendResult, StorageError> {
        // Extract idempotency key from headers.
        let idemp_key = record
            .headers
            .iter()
            .find(|(k, _)| k == IDEMPOTENCY_HEADER)
            .map(|(_, v)| v.clone());

        let Some(key) = idemp_key else {
            // No msg_id — pass-through directly.
            let offset = self.storage.append(stream, record).await.map_err(|e| {
                self.record_write_error(stream, &e);
                e
            })?;
            return Ok(AppendResult::Written(offset));
        };

        let body_hash = hash_body(&record.value);

        // Phase 1: optimistic read-only check.
        {
            let maps = self.dedup_maps.read().await;
            if let Some(map) = maps.get(stream) {
                match map.check(&key, body_hash) {
                    DedupCheckResult::HitSameBody { offset } => {
                        return Ok(AppendResult::Duplicate(Offset(offset)));
                    }
                    DedupCheckResult::HitDifferentBody { stored_offset } => {
                        return Err(StorageError::KeyCollision { stored_offset });
                    }
                    DedupCheckResult::Miss => {}
                }
            }
        }

        // Phase 2: re-check + cap enforcement (under write lock, no await inside).
        // The re-check is load-bearing: two concurrent publishes with the same key
        // that both pass Phase 1 would both try to write without it.
        {
            let mut maps = self.dedup_maps.write().await;
            let map = maps
                .entry(stream.clone())
                .or_insert_with(|| DedupMap::new(self.default_window, self.default_max_entries));

            match map.check(&key, body_hash) {
                DedupCheckResult::HitSameBody { offset } => {
                    return Ok(AppendResult::Duplicate(Offset(offset)));
                }
                DedupCheckResult::HitDifferentBody { stored_offset } => {
                    return Err(StorageError::KeyCollision { stored_offset });
                }
                DedupCheckResult::Miss => {}
            }

            if map.seen.len() as u64 >= map.max_entries {
                map.evict_expired();
                if map.seen.len() as u64 >= map.max_entries {
                    let retry_after_secs = map.time_until_oldest_expires().as_secs() as u32;
                    return Err(StorageError::DedupMapFull { retry_after_secs });
                }
            }
        }

        // Phase 3: storage write (no lock held).
        let offset = self.storage.append(stream, record).await.map_err(|e| {
            self.record_write_error(stream, &e);
            e
        })?;

        // Phase 4: insert into dedup map.
        {
            let mut maps = self.dedup_maps.write().await;
            let map = maps
                .entry(stream.clone())
                .or_insert_with(|| DedupMap::new(self.default_window, self.default_max_entries));
            map.insert(key, offset.0, body_hash);
        }

        Ok(AppendResult::Written(offset))
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
            let map = maps
                .entry(stream_name.clone())
                .or_insert_with(|| DedupMap::new(self.default_window, self.default_max_entries));

            // Use seek_by_time to jump to the dedup window boundary.
            let mut offset = self
                .storage
                .seek_by_time(stream_name, cutoff_ns)
                .await
                .unwrap_or(Offset(0)); // fallback to full scan if seek fails

            let batch_size = 1000;

            // Scan forward through the stream.
            loop {
                let records = self.storage.read(stream_name, offset, batch_size).await?;

                if records.is_empty() {
                    break;
                }

                for record in &records {
                    // Only index records within the dedup window.
                    if record.timestamp >= cutoff_ns {
                        if let Some((_, v)) = record
                            .headers
                            .iter()
                            .find(|(k, _)| k == IDEMPOTENCY_HEADER)
                        {
                            let body_hash = hash_body(&record.value);
                            map.insert(v.clone(), record.offset.0, body_hash);
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

    async fn make_storage() -> Arc<dyn StorageEngine> {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create_stream(&StreamName::try_from("test").unwrap(), 0, 0)
            .await
            .unwrap();
        storage
    }

    async fn make_broker_append() -> BrokerAppend {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create_stream(&StreamName::try_from("events").unwrap(), 0, 0)
            .await
            .unwrap();
        BrokerAppend::new(storage, 300) // 5 min window
    }

    fn make_record(idemp_key: Option<&str>, value: Bytes) -> Record {
        let mut headers = vec![];
        if let Some(key) = idemp_key {
            headers.push(("x-idempotency-key".to_string(), key.to_string()));
        }
        Record {
            key: None,
            value,
            subject: "test.event".to_string(),
            headers,
        }
    }

    fn make_record_with_subject(subject: &str, value: &[u8], idemp_key: Option<&str>) -> Record {
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

    // --- Sub-unit 2.1: hash_body ---

    #[test]
    fn hash_body_deterministic() {
        let h1 = hash_body(b"hello");
        let h2 = hash_body(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_body_differs_for_different_input() {
        let h1 = hash_body(b"hello");
        let h2 = hash_body(b"world");
        assert_ne!(h1, h2);
    }

    // --- Sub-unit 2.2: three-way check ---

    #[tokio::test]
    async fn duplicate_same_body_returns_duplicate() {
        let ba = make_broker_append().await;
        let stream = StreamName::try_from("events").unwrap();
        let body = Bytes::from_static(b"hello");
        let r1 = ba.append(&stream, &make_record(Some("key-A"), body.clone())).await.unwrap();
        let r2 = ba.append(&stream, &make_record(Some("key-A"), body.clone())).await.unwrap();
        assert!(matches!(r1, AppendResult::Written(_)));
        assert!(matches!(r2, AppendResult::Duplicate(_)));
        assert_eq!(r1.offset(), r2.offset());
    }

    #[tokio::test]
    async fn duplicate_different_body_returns_key_collision() {
        let ba = make_broker_append().await;
        let stream = StreamName::try_from("events").unwrap();
        let r1 = ba.append(&stream, &make_record(Some("key-A"), Bytes::from_static(b"hello"))).await.unwrap();
        let err = ba.append(&stream, &make_record(Some("key-A"), Bytes::from_static(b"world"))).await.unwrap_err();
        match err {
            StorageError::KeyCollision { stored_offset } => assert_eq!(stored_offset, r1.offset().0),
            other => panic!("expected KeyCollision, got {other:?}"),
        }
    }

    // --- Sub-unit 2.3: cap + DedupMapFull + per-stream config ---

    #[tokio::test]
    async fn cap_full_rejects_with_dedup_map_full() {
        let ba = BrokerAppend::new_with_cap(make_storage().await, 300, 2);
        let stream = StreamName::try_from("test").unwrap();
        ba.configure_stream(&stream, 300, 2).await;
        ba.append(&stream, &make_record(Some("k1"), Bytes::from_static(b"a"))).await.unwrap();
        ba.append(&stream, &make_record(Some("k2"), Bytes::from_static(b"b"))).await.unwrap();
        let err = ba.append(&stream, &make_record(Some("k3"), Bytes::from_static(b"c"))).await.unwrap_err();
        assert!(matches!(err, StorageError::DedupMapFull { .. }));
    }

    #[tokio::test]
    async fn cap_full_does_not_block_non_msg_id_publishes() {
        let ba = BrokerAppend::new_with_cap(make_storage().await, 300, 1);
        let stream = StreamName::try_from("test").unwrap();
        ba.configure_stream(&stream, 300, 1).await;
        ba.append(&stream, &make_record(Some("k1"), Bytes::from_static(b"a"))).await.unwrap();
        let res = ba.append(&stream, &make_record(None, Bytes::from_static(b"b"))).await.unwrap();
        assert!(matches!(res, AppendResult::Written(_)));
    }

    #[tokio::test]
    async fn per_stream_config_overrides_defaults() {
        let storage = Arc::new(MemoryStorage::new());
        storage.create_stream(&StreamName::try_from("short").unwrap(), 0, 0).await.unwrap();
        let ba = BrokerAppend::new_with_cap(storage, 300, 500_000);
        let stream = StreamName::try_from("short").unwrap();
        ba.configure_stream(&stream, 300, 2).await;
        ba.append(&stream, &make_record(Some("k1"), Bytes::from_static(b"a"))).await.unwrap();
        ba.append(&stream, &make_record(Some("k2"), Bytes::from_static(b"b"))).await.unwrap();
        let err = ba.append(&stream, &make_record(Some("k3"), Bytes::from_static(b"c"))).await.unwrap_err();
        assert!(matches!(err, StorageError::DedupMapFull { .. }));
    }

    // --- Existing tests (preserved) ---

    #[tokio::test]
    async fn append_without_idemp_key_always_writes() {
        let ba = make_broker_append().await;
        let stream = StreamName::try_from("events").unwrap();
        let r1 = make_record_with_subject("events.created", b"data1", None);
        let r2 = make_record_with_subject("events.created", b"data1", None); // same data, no key

        let res1 = ba.append(&stream, &r1).await.unwrap();
        let res2 = ba.append(&stream, &r2).await.unwrap();

        assert!(matches!(res1, AppendResult::Written(Offset(0))));
        assert!(matches!(res2, AppendResult::Written(Offset(1)))); // both written
    }

    #[tokio::test]
    async fn append_with_idemp_key_deduplicates() {
        let ba = make_broker_append().await;
        let stream = StreamName::try_from("events").unwrap();
        let r1 = make_record_with_subject("events.created", b"data1", Some("order-123"));
        let r2 = make_record_with_subject("events.created", b"data1", Some("order-123")); // same key

        let res1 = ba.append(&stream, &r1).await.unwrap();
        let res2 = ba.append(&stream, &r2).await.unwrap();

        assert!(matches!(res1, AppendResult::Written(Offset(0))));
        assert!(matches!(res2, AppendResult::Duplicate(Offset(0)))); // deduped
    }

    #[tokio::test]
    async fn different_idemp_keys_both_written() {
        let ba = make_broker_append().await;
        let stream = StreamName::try_from("events").unwrap();
        let r1 = make_record_with_subject("events.created", b"data1", Some("order-123"));
        let r2 = make_record_with_subject("events.created", b"data2", Some("order-456"));

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

        let r = make_record_with_subject("evt", b"data", Some("key-1"));
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

        // Write some records with idempotency keys directly to storage.
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

        // Create BrokerAppend and rebuild.
        let ba = BrokerAppend::new(storage, 300);
        ba.rebuild_from_log().await.unwrap();

        // Now duplicates should be caught.
        let dup_a = make_record(Some("key-A"), Bytes::from_static(b"data1"));
        let res = ba.append(&stream, &dup_a).await.unwrap();
        assert!(matches!(res, AppendResult::Duplicate(Offset(0))));

        let dup_b = make_record(Some("key-B"), Bytes::from_static(b"data2"));
        let res = ba.append(&stream, &dup_b).await.unwrap();
        assert!(matches!(res, AppendResult::Duplicate(Offset(1))));

        // New key should still work.
        let new = make_record(Some("key-C"), Bytes::from_static(b"data3"));
        let res = ba.append(&stream, &new).await.unwrap();
        assert!(matches!(res, AppendResult::Written(Offset(2))));
    }

    #[tokio::test]
    async fn append_result_offset_helper() {
        assert_eq!(AppendResult::Written(Offset(5)).offset(), Offset(5));
        assert_eq!(AppendResult::Duplicate(Offset(3)).offset(), Offset(3));
    }
}
