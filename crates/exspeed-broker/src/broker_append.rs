use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::{Mutex, RwLock};
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
    /// A new record was persisted at the returned offset and timestamp
    /// (nanoseconds since the UNIX epoch, as assigned by the storage engine).
    Written(Offset, u64),
    /// Idempotency hit: the record was already persisted at the returned
    /// offset on an earlier publish. No new timestamp is meaningful here.
    Duplicate(Offset),
}

impl AppendResult {
    pub fn offset(&self) -> Offset {
        match self {
            AppendResult::Written(o, _) | AppendResult::Duplicate(o) => *o,
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
    /// One `tokio::sync::Mutex` per stream, held across Phases 2–4 for msg_id
    /// publishes so that two concurrent first-time publishes with the same key
    /// are serialized: the second racer blocks on `mutex.lock()` until the
    /// first has completed Phase 4's insert, guaranteeing the second call sees
    /// the entry at Phase 1 and returns `Duplicate`.
    per_stream_locks: RwLock<HashMap<StreamName, Arc<Mutex<()>>>>,
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
            per_stream_locks: RwLock::new(HashMap::new()),
            default_window: Duration::from_secs(default_window_secs),
            default_max_entries,
            metrics: None,
        }
    }

    /// Return (creating if absent) the per-stream `Mutex` used to serialize
    /// concurrent msg_id publishes to the same stream.
    async fn per_stream_lock(&self, stream: &StreamName) -> Arc<Mutex<()>> {
        {
            let map = self.per_stream_locks.read().await;
            if let Some(m) = map.get(stream) {
                return m.clone();
            }
        }
        let mut map = self.per_stream_locks.write().await;
        map.entry(stream.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
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
        if let Some(m) = &self.metrics {
            m.set_dedup_window_secs(stream.as_str(), window_secs as i64);
        }
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
    /// **No-msg_id path** (no `x-idempotency-key` header): pass-through to
    /// storage with no dedup overhead.
    ///
    /// **msg_id path**: four-phase protocol that serializes concurrent publishes
    /// for the same stream via a per-stream `tokio::Mutex` held across Phases 2–4.
    /// This closes the double-write window that the Phase 2 write-lock re-check
    /// alone cannot close: two concurrent first-time publishes with the same key
    /// would both see Miss at Phase 2 (neither has reached Phase 4's insert yet)
    /// without the per-stream mutex, causing two durable records with the same
    /// msg_id. With the mutex, the second racer blocks on `mutex.lock()` until
    /// the first completes Phase 4's insert, so the second call sees the entry
    /// at Phase 1 and returns `Duplicate` with the same offset.
    ///
    ///   Phase 1 — optimistic read-lock check (fast path for already-known keys).
    ///   Phase 2 — write-lock re-check + cap enforcement under the per-stream mutex.
    ///   Phase 3 — storage write (per-stream mutex held, no dedup_maps lock).
    ///   Phase 4 — write-lock insert into dedup map; mutex released on return.
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
            // No msg_id — pass-through directly; no dedup or locking overhead.
            let (offset, timestamp_ns) = self
                .storage
                .append(stream, record)
                .await
                .inspect_err(|e| self.record_write_error(stream, e))?;
            return Ok(AppendResult::Written(offset, timestamp_ns));
        };

        let body_hash = hash_body(&record.value);

        // === msg_id path: acquire per-stream append mutex ===
        // Held across Phases 2–4 so concurrent first-time publishes with the
        // same key are serialized and the second sees the Phase 4 insert.
        let mutex = self.per_stream_lock(stream).await;
        let _guard = mutex.lock().await;

        // Phase 1: optimistic read-only check (fast path under the mutex).
        // Under the per-stream mutex this is now strictly redundant with Phase 2,
        // but it avoids acquiring the write lock on the common duplicate path.
        {
            let maps = self.dedup_maps.read().await;
            if let Some(map) = maps.get(stream) {
                match map.check(&key, body_hash) {
                    DedupCheckResult::HitSameBody { offset } => {
                        if let Some(m) = &self.metrics {
                            m.record_dedup_write(stream.as_str(), "duplicate");
                        }
                        return Ok(AppendResult::Duplicate(Offset(offset)));
                    }
                    DedupCheckResult::HitDifferentBody { stored_offset } => {
                        if let Some(m) = &self.metrics {
                            m.record_dedup_collision(stream.as_str());
                        }
                        tracing::warn!(
                            stream = %stream,
                            msg_id = %key,
                            stored_offset = stored_offset,
                            body_hash_prefix = format!("{:08x}", body_hash >> 32),
                            "dedup key collision"
                        );
                        return Err(StorageError::KeyCollision { stored_offset });
                    }
                    DedupCheckResult::Miss => {}
                }
            }
        }

        // Phase 2: re-check + cap enforcement under write lock (no await inside).
        {
            let mut maps = self.dedup_maps.write().await;
            let map = maps
                .entry(stream.clone())
                .or_insert_with(|| DedupMap::new(self.default_window, self.default_max_entries));

            match map.check(&key, body_hash) {
                DedupCheckResult::HitSameBody { offset } => {
                    if let Some(m) = &self.metrics {
                        m.record_dedup_write(stream.as_str(), "duplicate");
                    }
                    return Ok(AppendResult::Duplicate(Offset(offset)));
                }
                DedupCheckResult::HitDifferentBody { stored_offset } => {
                    if let Some(m) = &self.metrics {
                        m.record_dedup_collision(stream.as_str());
                    }
                    tracing::warn!(
                        stream = %stream,
                        msg_id = %key,
                        stored_offset = stored_offset,
                        body_hash_prefix = format!("{:08x}", body_hash >> 32),
                        "dedup key collision"
                    );
                    return Err(StorageError::KeyCollision { stored_offset });
                }
                DedupCheckResult::Miss => {}
            }

            if map.seen.len() as u64 >= map.max_entries {
                map.evict_expired();
                if map.seen.len() as u64 >= map.max_entries {
                    let retry_after_secs = map.time_until_oldest_expires().as_secs() as u32;
                    if let Some(m) = &self.metrics {
                        m.record_dedup_map_full(stream.as_str());
                    }
                    tracing::warn!(
                        stream = %stream,
                        entries = map.seen.len(),
                        window_secs = map.window.as_secs(),
                        retry_after_secs = retry_after_secs,
                        "dedup map full"
                    );
                    return Err(StorageError::DedupMapFull { retry_after_secs });
                }
            }
        }

        // Phase 3: storage write — per-stream mutex still held, no dedup lock.
        let (offset, timestamp_ns) = self
            .storage
            .append(stream, record)
            .await
            .inspect_err(|e| self.record_write_error(stream, e))?;

        // Phase 4: insert into dedup map; _guard drops at end of this scope.
        {
            let mut maps = self.dedup_maps.write().await;
            let map = maps
                .entry(stream.clone())
                .or_insert_with(|| DedupMap::new(self.default_window, self.default_max_entries));
            map.insert(key, offset.0, body_hash);
        }

        if let Some(m) = &self.metrics {
            m.record_dedup_write(stream.as_str(), "written");
        }

        Ok(AppendResult::Written(offset, timestamp_ns))
    }

    /// Run periodic eviction of expired dedup entries. Call from a background task.
    pub async fn evict_expired(&self) {
        let mut maps = self.dedup_maps.write().await;
        for map in maps.values_mut() {
            map.evict_expired();
        }
    }

    /// Snapshot the dedup map for a single stream to disk.
    pub async fn snapshot_stream(
        &self,
        stream: &StreamName,
        stream_dir: &std::path::Path,
    ) -> std::io::Result<()> {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let maps = self.dedup_maps.read().await;
        let Some(map) = maps.get(stream) else {
            return Ok(());
        };

        let entries: Vec<crate::broker_append_snapshot::SnapshotEntry> = map
            .seen
            .iter()
            .map(|(k, e)| {
                let elapsed_ms = e.inserted_at.elapsed().as_millis() as u64;
                let inserted_at_unix_ms = now_ms.saturating_sub(elapsed_ms);
                crate::broker_append_snapshot::SnapshotEntry {
                    msg_id: k.clone(),
                    offset: e.offset,
                    inserted_at_unix_ms,
                    body_hash: e.body_hash,
                }
            })
            .collect();

        let snap = crate::broker_append_snapshot::Snapshot {
            covers_through_unix_ms: now_ms,
            entries,
        };
        let path = crate::broker_append_snapshot::snapshot_path(stream_dir);
        let write_start = Instant::now();
        let result = crate::broker_append_snapshot::write_snapshot(&path, &snap);
        if result.is_ok() {
            if let Some(m) = &self.metrics {
                m.observe_dedup_snapshot_write_duration(write_start.elapsed().as_secs_f64());
            }
        }
        result
    }

    /// Snapshot all streams' dedup maps to disk.
    pub async fn snapshot_all(&self, data_dir: &std::path::Path) -> std::io::Result<()> {
        let streams: Vec<StreamName> = {
            let maps = self.dedup_maps.read().await;
            maps.keys().cloned().collect()
        };
        for s in streams {
            let stream_dir = data_dir.join("streams").join(s.as_str());
            if let Err(e) = self.snapshot_stream(&s, &stream_dir).await {
                tracing::warn!(stream = %s, error = %e, "dedup snapshot write failed");
            } else if let Some(m) = &self.metrics {
                let count = self.entry_count(&s).await;
                m.set_dedup_map_entries(s.as_str(), count as i64);
            }
        }
        Ok(())
    }

    /// Rebuild the dedup map for a single stream using the snapshot + tail scan.
    ///
    /// Algorithm:
    ///   1. Try to load the snapshot — if valid, populate the map from it and
    ///      note `covers_through_unix_ms` so we only need to scan records written
    ///      after the snapshot was taken.
    ///   2. If the snapshot is missing or corrupt, log a warning and fall back to
    ///      a full scan from the dedup window boundary.
    ///   3. In either case, scan forward from the determined start offset,
    ///      inserting any idempotency-keyed records found.
    pub async fn rebuild_stream(
        &self,
        stream: &StreamName,
        stream_dir: &std::path::Path,
    ) -> Result<(), StorageError> {
        let rebuild_start = Instant::now();

        let (window_secs, max_entries) = {
            let maps = self.dedup_maps.read().await;
            maps.get(stream)
                .map(|m| (m.window.as_secs(), m.max_entries))
                .unwrap_or((self.default_window.as_secs(), self.default_max_entries))
        };

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let snapshot_covers_through_unix_ms: Option<u64> = match crate::broker_append_snapshot::read_snapshot(
            &crate::broker_append_snapshot::snapshot_path(stream_dir),
        ) {
            Ok(snap) => {
                let covers = snap.covers_through_unix_ms;
                let mut maps = self.dedup_maps.write().await;
                let map = maps.entry(stream.clone()).or_insert_with(|| {
                    DedupMap::new(Duration::from_secs(window_secs), max_entries)
                });
                for e in snap.entries {
                    let age_ms = now_ms.saturating_sub(e.inserted_at_unix_ms);
                    if age_ms >= window_secs * 1000 {
                        continue; // already expired
                    }
                    let inserted_at =
                        Instant::now() - Duration::from_millis(age_ms);
                    map.seen.insert(
                        e.msg_id,
                        DedupEntry {
                            offset: e.offset,
                            inserted_at,
                            body_hash: e.body_hash,
                        },
                    );
                }
                Some(covers)
            }
            Err(e) => {
                tracing::warn!(
                    stream = %stream,
                    error = %e,
                    "dedup snapshot invalid, falling back to full log scan"
                );
                None
            }
        };

        // Determine where to start scanning the log.
        let cutoff_unix_ms = snapshot_covers_through_unix_ms
            .unwrap_or_else(|| now_ms.saturating_sub(window_secs * 1000));
        let cutoff_ns = cutoff_unix_ms * 1_000_000;

        let start_offset = match self.storage.seek_by_time(stream, cutoff_ns).await {
            Ok(o) => o,
            Err(StorageError::StreamNotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
        };

        let mut cursor = start_offset;
        loop {
            let batch = match self.storage.read(stream, cursor, 1000).await {
                Ok(b) => b,
                Err(StorageError::StreamNotFound(_)) => break,
                Err(e) => return Err(e),
            };
            if batch.is_empty() {
                break;
            }
            for rec in &batch {
                if let Some((_, idemp)) = rec
                    .headers
                    .iter()
                    .find(|(k, _)| k == IDEMPOTENCY_HEADER)
                {
                    let body_hash = hash_body(&rec.value);
                    let rec_ms = rec.timestamp / 1_000_000;
                    let age_ms = now_ms.saturating_sub(rec_ms);
                    let clamped_age = age_ms.min(window_secs * 1000);
                    let inserted_at =
                        Instant::now() - Duration::from_millis(clamped_age);
                    let mut maps = self.dedup_maps.write().await;
                    let map = maps.entry(stream.clone()).or_insert_with(|| {
                        DedupMap::new(Duration::from_secs(window_secs), max_entries)
                    });
                    map.seen.insert(
                        idemp.clone(),
                        DedupEntry {
                            offset: rec.offset.0,
                            inserted_at,
                            body_hash,
                        },
                    );
                }
            }
            cursor = Offset(batch.last().unwrap().offset.0 + 1);
        }

        let elapsed = rebuild_start.elapsed();
        let source = if snapshot_covers_through_unix_ms.is_some() {
            "snapshot"
        } else {
            "full_scan"
        };
        tracing::info!(
            stream = %stream,
            window_secs = window_secs,
            max_entries = max_entries,
            rebuild_source = source,
            rebuild_ms = elapsed.as_millis(),
            "dedup rebuilt"
        );
        if let Some(m) = &self.metrics {
            m.observe_dedup_rebuild_duration(stream.as_str(), source, elapsed.as_secs_f64());
        }

        Ok(())
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
        assert!(matches!(r1, AppendResult::Written(_, _)));
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
        assert!(matches!(res, AppendResult::Written(..)));
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

        assert!(matches!(res1, AppendResult::Written(Offset(0), _)));
        assert!(matches!(res2, AppendResult::Written(Offset(1), _))); // both written
    }

    #[tokio::test]
    async fn append_with_idemp_key_deduplicates() {
        let ba = make_broker_append().await;
        let stream = StreamName::try_from("events").unwrap();
        let r1 = make_record_with_subject("events.created", b"data1", Some("order-123"));
        let r2 = make_record_with_subject("events.created", b"data1", Some("order-123")); // same key

        let res1 = ba.append(&stream, &r1).await.unwrap();
        let res2 = ba.append(&stream, &r2).await.unwrap();

        assert!(matches!(res1, AppendResult::Written(Offset(0), _)));
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

        assert!(matches!(res1, AppendResult::Written(Offset(0), _)));
        assert!(matches!(res2, AppendResult::Written(Offset(1), _)));
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

        assert!(matches!(res1, AppendResult::Written(Offset(0), _)));
        assert!(matches!(res2, AppendResult::Written(Offset(0), _)));
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
        assert!(matches!(res, AppendResult::Written(Offset(2), _)));
    }

    #[tokio::test]
    async fn append_result_offset_helper() {
        assert_eq!(AppendResult::Written(Offset(5), 0).offset(), Offset(5));
        assert_eq!(AppendResult::Duplicate(Offset(3)).offset(), Offset(3));
    }

    #[tokio::test]
    async fn concurrent_same_key_serializes() {
        // Two concurrent publishes with the same msg_id must produce
        // exactly one Written + one Duplicate, never two Writtens.
        use std::sync::Arc;
        let ba = Arc::new(make_broker_append().await);
        let stream = StreamName::try_from("events").unwrap();
        ba.configure_stream(&stream, 300, 500_000).await;

        let ba1 = ba.clone();
        let ba2 = ba.clone();
        let s1 = stream.clone();
        let s2 = stream.clone();

        let (r1, r2) = tokio::join!(
            tokio::spawn(async move {
                ba1.append(&s1, &make_record(Some("race-key"), Bytes::from_static(b"body"))).await
            }),
            tokio::spawn(async move {
                ba2.append(&s2, &make_record(Some("race-key"), Bytes::from_static(b"body"))).await
            }),
        );

        let r1 = r1.unwrap().unwrap();
        let r2 = r2.unwrap().unwrap();

        // Exactly one Written, one Duplicate — offsets must match.
        let (written_count, duplicate_count) = match (&r1, &r2) {
            (AppendResult::Written(..), AppendResult::Duplicate(_))
            | (AppendResult::Duplicate(_), AppendResult::Written(..)) => (1, 1),
            _ => (
                matches!(r1, AppendResult::Written(..)) as u32
                    + matches!(r2, AppendResult::Written(..)) as u32,
                matches!(r1, AppendResult::Duplicate(_)) as u32
                    + matches!(r2, AppendResult::Duplicate(_)) as u32,
            ),
        };
        assert_eq!(written_count, 1, "expected exactly 1 Written; got r1={r1:?}, r2={r2:?}");
        assert_eq!(duplicate_count, 1, "expected exactly 1 Duplicate; got r1={r1:?}, r2={r2:?}");
        assert_eq!(r1.offset(), r2.offset(), "offsets must agree");
    }

    // --- Sub-unit 3.2: snapshot_writes_file ---

    #[tokio::test]
    async fn snapshot_writes_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let stream_dir = data_dir.join("streams").join("test");
        std::fs::create_dir_all(&stream_dir).unwrap();

        let ba = Arc::new(BrokerAppend::new(make_storage().await, 300));
        let stream = StreamName::try_from("test").unwrap();
        ba.configure_stream(&stream, 300, 500_000).await;
        ba.append(&stream, &make_record(Some("k1"), Bytes::from_static(b"a")))
            .await
            .unwrap();

        ba.snapshot_all(&data_dir).await.unwrap();

        let path = crate::broker_append_snapshot::snapshot_path(&stream_dir);
        assert!(path.exists());
        let snap = crate::broker_append_snapshot::read_snapshot(&path).unwrap();
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(snap.entries[0].msg_id, "k1");
    }

    // --- Sub-unit 3.3: rebuild_stream ---

    #[tokio::test]
    async fn startup_loads_from_snapshot() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let stream_dir = data_dir.join("streams").join("test");
        std::fs::create_dir_all(&stream_dir).unwrap();

        let storage = make_storage().await;
        let stream = StreamName::try_from("test").unwrap();

        {
            let ba = BrokerAppend::new(storage.clone(), 300);
            ba.configure_stream(&stream, 300, 500_000).await;
            ba.append(
                &stream,
                &make_record(Some("k-retained"), Bytes::from_static(b"body")),
            )
            .await
            .unwrap();
            ba.snapshot_all(&data_dir).await.unwrap();
        }

        let ba2 = BrokerAppend::new(storage.clone(), 300);
        ba2.configure_stream(&stream, 300, 500_000).await;
        ba2.rebuild_stream(&stream, &stream_dir).await.unwrap();

        let res = ba2
            .append(
                &stream,
                &make_record(Some("k-retained"), Bytes::from_static(b"body")),
            )
            .await
            .unwrap();
        assert!(matches!(res, AppendResult::Duplicate(_)));
    }

    #[tokio::test]
    async fn startup_falls_back_to_full_scan_on_corrupt_snapshot() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let stream_dir = data_dir.join("streams").join("test");
        std::fs::create_dir_all(&stream_dir).unwrap();

        let storage = make_storage().await;
        let stream = StreamName::try_from("test").unwrap();

        {
            let ba = BrokerAppend::new(storage.clone(), 300);
            ba.configure_stream(&stream, 300, 500_000).await;
            ba.append(
                &stream,
                &make_record(Some("k-tail"), Bytes::from_static(b"body")),
            )
            .await
            .unwrap();
            // Write garbage to the snapshot file to simulate corruption.
            std::fs::write(stream_dir.join("dedup_snapshot.bin"), b"garbage").unwrap();
        }

        let ba2 = BrokerAppend::new(storage.clone(), 300);
        ba2.configure_stream(&stream, 300, 500_000).await;
        ba2.rebuild_stream(&stream, &stream_dir).await.unwrap();

        let res = ba2
            .append(
                &stream,
                &make_record(Some("k-tail"), Bytes::from_static(b"body")),
            )
            .await
            .unwrap();
        assert!(matches!(res, AppendResult::Duplicate(_)));
    }
}
