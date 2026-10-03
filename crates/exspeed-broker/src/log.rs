//! The single write path.
//!
//! Every record and every stream-metadata change goes through [`Log`]:
//! TCP publish and batch publish, HTTP publish, webhooks, connector sources,
//! ExQL outputs, dead-letter writes. Each write runs the same steps, in order:
//!
//! 1. **Leader check** — followers never accept writes ([`WriteGate`]).
//! 2. **Validation** — subject, key, value and header sizes ([`RecordLimits`]),
//!    so nothing the storage encoder can't represent ever reaches disk.
//! 3. **Dedup** — records carrying an idempotency key
//!    (`x-idempotency-key`) are deduplicated by [`BrokerAppend`].
//! 4. **Storage append**.
//! 5. **Replication** — in a cluster, wait until the in-sync followers have
//!    the records (`acks = all`; see `crate::cluster`).
//! 6. **Metrics**.
//!
//! Reads go straight to [`Log::storage`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use exspeed_common::{Metrics, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError, StreamConfig};

use crate::broker_append::{AppendResult, BrokerAppend, IDEMPOTENCY_HEADER};

/// A stream-metadata change, reported to [`ReplicaSync`].
#[derive(Debug, Clone, Copy)]
pub enum MetadataChange<'a> {
    Created(&'a StreamName),
    Updated(&'a StreamName),
    Deleted(&'a StreamName),
}

/// Hooks the cluster layer installs on the write path.
#[async_trait::async_trait]
pub trait ReplicaSync: Send + Sync {
    /// Fail before writing when the write can't be acknowledged (too few
    /// in-sync replicas).
    fn precheck(&self) -> Result<(), LogError>;
    /// Called after records up to `last_offset` (inclusive) of `stream` were
    /// written locally; returns once they are replicated as configured.
    async fn wait_replicated(&self, stream: &StreamName, last_offset: u64) -> Result<(), LogError>;
    /// A stream was created, reconfigured or deleted.
    fn metadata_changed(&self, change: MetadataChange<'_>);
}

/// Size limits enforced on every record before it is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordLimits {
    pub max_subject_bytes: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_headers: usize,
    pub max_header_key_bytes: usize,
    pub max_header_value_bytes: usize,
}

impl Default for RecordLimits {
    fn default() -> Self {
        Self {
            max_subject_bytes: 1024,
            max_key_bytes: 64 * 1024,
            // Frames are capped at 16 MiB; leave room for framing/headers.
            max_value_bytes: 8 * 1024 * 1024,
            max_headers: 256,
            max_header_key_bytes: 1024,
            max_header_value_bytes: 32 * 1024,
        }
    }
}

impl RecordLimits {
    /// Validate a record. Subjects are dot-delimited tokens; a published
    /// subject may be empty (no subject) but may not contain empty tokens,
    /// wildcards (`*`, `>`) or whitespace.
    pub fn check(&self, record: &Record) -> Result<(), String> {
        let subject = &record.subject;
        if subject.len() > self.max_subject_bytes {
            return Err(format!(
                "subject is {} bytes; the limit is {}",
                subject.len(),
                self.max_subject_bytes
            ));
        }
        if !subject.is_empty() {
            for token in subject.split('.') {
                if token.is_empty() {
                    return Err(format!("subject '{subject}' has an empty token"));
                }
                if token == "*" || token == ">" || token.contains(char::is_whitespace) {
                    return Err(format!(
                        "subject '{subject}' contains a wildcard or whitespace; \
                         wildcards are only valid in filters"
                    ));
                }
            }
        }
        if let Some(key) = &record.key {
            if key.len() > self.max_key_bytes {
                return Err(format!(
                    "key is {} bytes; the limit is {}",
                    key.len(),
                    self.max_key_bytes
                ));
            }
        }
        if record.value.len() > self.max_value_bytes {
            return Err(format!(
                "value is {} bytes; the limit is {}",
                record.value.len(),
                self.max_value_bytes
            ));
        }
        if record.headers.len() > self.max_headers {
            return Err(format!(
                "{} headers; the limit is {}",
                record.headers.len(),
                self.max_headers
            ));
        }
        for (k, v) in &record.headers {
            if k.len() > self.max_header_key_bytes {
                return Err(format!(
                    "header key is {} bytes; the limit is {}",
                    k.len(),
                    self.max_header_key_bytes
                ));
            }
            if v.len() > self.max_header_value_bytes {
                return Err(format!(
                    "header '{k}' value is {} bytes; the limit is {}",
                    v.len(),
                    self.max_header_value_bytes
                ));
            }
        }
        Ok(())
    }
}

/// Decides whether this process may accept writes (i.e. is the leader).
pub trait WriteGate: Send + Sync {
    fn can_write(&self) -> bool;
}

impl WriteGate for crate::leadership::ClusterLeadership {
    fn can_write(&self) -> bool {
        self.is_currently_leader()
    }
}

/// Errors from the write path.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("not the leader; writes must go to the leader")]
    NotLeader,
    #[error("invalid record: {0}")]
    InvalidRecord(String),
    #[error("invalid stream config: {0}")]
    InvalidConfig(String),
    #[error("dedup state is still loading after startup; retry shortly")]
    DedupNotReady,
    #[error("not enough in-sync replicas ({in_sync} of the required {required})")]
    NotEnoughReplicas { in_sync: usize, required: usize },
    #[error("the write was not replicated to the in-sync replicas in time; it may or may not be durable")]
    ReplicationTimeout,
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl LogError {
    /// Whether the caller can expect the same write to succeed later.
    pub fn is_retryable(&self) -> bool {
        match self {
            LogError::NotLeader
            | LogError::DedupNotReady
            | LogError::NotEnoughReplicas { .. }
            | LogError::ReplicationTimeout => true,
            LogError::InvalidRecord(_) | LogError::InvalidConfig(_) => false,
            LogError::Storage(e) => matches!(
                e,
                StorageError::DedupMapFull { .. }
                    | StorageError::Io(_)
                    | StorageError::ChannelClosed
            ),
        }
    }
}

/// The single write path. Cheap to share behind an `Arc`.
pub struct Log {
    storage: Arc<dyn StorageEngine>,
    dedup: Arc<BrokerAppend>,
    metrics: Arc<Metrics>,
    limits: RecordLimits,
    dedup_ready: Arc<AtomicBool>,
    gate: OnceLock<Arc<dyn WriteGate>>,
    sync: OnceLock<Arc<dyn ReplicaSync>>,
    /// Bumped after every write that stored at least one record.
    appended: tokio::sync::watch::Sender<u64>,
    /// Bumped on every stream create / update / delete.
    metadata: tokio::sync::watch::Sender<u64>,
}

impl Log {
    /// `dedup_ready` flips to `true` once startup has rebuilt the dedup maps;
    /// until then writes that carry an idempotency key are rejected with the
    /// retryable [`LogError::DedupNotReady`].
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        dedup: Arc<BrokerAppend>,
        metrics: Arc<Metrics>,
        dedup_ready: Arc<AtomicBool>,
    ) -> Self {
        Self {
            storage,
            dedup,
            metrics,
            limits: RecordLimits::default(),
            dedup_ready,
            gate: OnceLock::new(),
            sync: OnceLock::new(),
            appended: tokio::sync::watch::channel(0).0,
            metadata: tokio::sync::watch::channel(0).0,
        }
    }

    pub fn with_limits(mut self, limits: RecordLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Only accept writes while `gate` says so. Without a gate every write
    /// is accepted (single-node tests). Can be set once.
    pub fn set_write_gate(&self, gate: Arc<dyn WriteGate>) {
        let _ = self.gate.set(gate);
    }

    /// Install the cluster hooks. Can be set once.
    pub fn set_replica_sync(&self, sync: Arc<dyn ReplicaSync>) {
        let _ = self.sync.set(sync);
    }

    /// Changes after every write that stored records.
    pub fn watch_appends(&self) -> tokio::sync::watch::Receiver<u64> {
        self.appended.subscribe()
    }

    /// Changes after every stream create / update / delete.
    pub fn watch_metadata(&self) -> tokio::sync::watch::Receiver<u64> {
        self.metadata.subscribe()
    }

    /// Number of metadata changes so far (process-local).
    pub fn metadata_counter(&self) -> u64 {
        *self.metadata.borrow()
    }

    pub fn storage(&self) -> &Arc<dyn StorageEngine> {
        &self.storage
    }

    pub fn dedup(&self) -> &Arc<BrokerAppend> {
        &self.dedup
    }

    pub fn limits(&self) -> &RecordLimits {
        &self.limits
    }

    /// Whether this process currently accepts writes.
    pub fn can_write(&self) -> bool {
        self.gate.get().is_none_or(|g| g.can_write())
    }

    fn check_writable(&self) -> Result<(), LogError> {
        if self.can_write() {
            Ok(())
        } else {
            Err(LogError::NotLeader)
        }
    }

    fn check_appendable(&self) -> Result<(), LogError> {
        self.check_writable()?;
        match self.sync.get() {
            Some(s) => s.precheck(),
            None => Ok(()),
        }
    }

    fn check_record(&self, record: &Record) -> Result<(), LogError> {
        self.limits.check(record).map_err(LogError::InvalidRecord)?;
        if !self.dedup_ready.load(Ordering::Acquire)
            && record.headers.iter().any(|(k, _)| k == IDEMPOTENCY_HEADER)
        {
            return Err(LogError::DedupNotReady);
        }
        Ok(())
    }

    /// Append one record.
    pub async fn append(
        &self,
        stream: &StreamName,
        record: Record,
    ) -> Result<AppendResult, LogError> {
        self.check_appendable()?;
        self.check_record(&record)?;
        let result = self.dedup.append(stream, &record).await?;
        if let AppendResult::Written(..) = result {
            self.metrics.record_publish(stream.as_str());
        }
        self.replicated(stream, std::slice::from_ref(&result))
            .await?;
        Ok(result)
    }

    /// Append several records. The whole batch is validated first; nothing
    /// is written if any record is invalid. Results preserve input order.
    pub async fn append_batch(
        &self,
        stream: &StreamName,
        records: Vec<Record>,
    ) -> Result<Vec<AppendResult>, LogError> {
        self.check_appendable()?;
        for r in &records {
            self.check_record(r)?;
        }
        let results = self.dedup.append_batch(stream, records).await?;
        for r in &results {
            if let AppendResult::Written(..) = r {
                self.metrics.record_publish(stream.as_str());
            }
        }
        self.replicated(stream, &results).await?;
        Ok(results)
    }

    /// Create a stream.
    pub async fn create_stream(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), LogError> {
        self.check_writable()?;
        config.check().map_err(LogError::InvalidConfig)?;
        self.storage.create_stream_with(stream, config).await?;
        self.dedup
            .configure_stream(stream, config.dedup_window_secs, config.dedup_max_entries)
            .await;
        self.metadata_changed(MetadataChange::Created(stream));
        Ok(())
    }

    /// Create `stream` with default settings if it doesn't exist yet.
    pub async fn ensure_stream(&self, stream: &StreamName) -> Result<(), LogError> {
        match self.storage.stream_bounds(stream).await {
            Ok(_) => Ok(()),
            Err(StorageError::StreamNotFound(_)) => {
                match self.create_stream(stream, &StreamConfig::default()).await {
                    Ok(()) | Err(LogError::Storage(StorageError::StreamAlreadyExists(_))) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Replace a stream's retention/dedup settings.
    pub async fn update_stream_config(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), LogError> {
        self.check_writable()?;
        config.check().map_err(LogError::InvalidConfig)?;
        self.storage.update_stream_config(stream, config).await?;
        self.dedup
            .configure_stream(stream, config.dedup_window_secs, config.dedup_max_entries)
            .await;
        self.metadata_changed(MetadataChange::Updated(stream));
        Ok(())
    }

    /// Delete a stream and forget its dedup state.
    pub async fn delete_stream(&self, stream: &StreamName) -> Result<(), LogError> {
        self.check_writable()?;
        self.storage.delete_stream(stream).await?;
        self.dedup.forget_stream(stream).await;
        self.metadata_changed(MetadataChange::Deleted(stream));
        Ok(())
    }

    async fn replicated(
        &self,
        stream: &StreamName,
        results: &[AppendResult],
    ) -> Result<(), LogError> {
        if results
            .iter()
            .any(|r| matches!(r, AppendResult::Written(..)))
        {
            self.appended.send_modify(|v| *v += 1);
        }
        let Some(sync) = self.sync.get() else {
            return Ok(());
        };
        let Some(last) = results.iter().map(|r| r.offset().0).max() else {
            return Ok(());
        };
        sync.wait_replicated(stream, last).await
    }

    fn metadata_changed(&self, change: MetadataChange<'_>) {
        if let Some(sync) = self.sync.get() {
            sync.metadata_changed(change);
        }
        self.metadata.send_modify(|v| *v += 1);
    }
}

/// A [`StorageEngine`] whose writes go through a [`Log`] (reads go straight
/// to the underlying storage). Lets components written against the storage
/// trait — e.g. the ExQL engine writing query output — use the single write
/// path without knowing about it.
pub struct LogBackedStorage {
    log: Arc<Log>,
}

impl LogBackedStorage {
    pub fn new(log: Arc<Log>) -> Self {
        Self { log }
    }
}

fn into_storage_error(e: LogError) -> StorageError {
    match e {
        LogError::Storage(e) => e,
        other => StorageError::Io(std::io::Error::other(other.to_string())),
    }
}

#[async_trait::async_trait]
impl StorageEngine for LogBackedStorage {
    async fn create_stream(
        &self,
        stream: &StreamName,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<(), StorageError> {
        let config = StreamConfig::from_request(max_age_secs, max_bytes, 0, 0);
        self.log
            .create_stream(stream, &config)
            .await
            .map_err(into_storage_error)
    }

    async fn create_stream_with(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), StorageError> {
        self.log
            .create_stream(stream, config)
            .await
            .map_err(into_storage_error)
    }

    async fn append(
        &self,
        stream: &StreamName,
        record: &Record,
    ) -> Result<(exspeed_common::Offset, u64), StorageError> {
        match self
            .log
            .append(stream, record.clone())
            .await
            .map_err(into_storage_error)?
        {
            AppendResult::Written(o, ts) => Ok((o, ts)),
            AppendResult::Duplicate(o) => Ok((o, 0)),
        }
    }

    async fn append_batch(
        &self,
        stream: &StreamName,
        records: Vec<Record>,
    ) -> Result<Vec<(exspeed_common::Offset, u64)>, StorageError> {
        Ok(self
            .log
            .append_batch(stream, records)
            .await
            .map_err(into_storage_error)?
            .into_iter()
            .map(|r| match r {
                AppendResult::Written(o, ts) => (o, ts),
                AppendResult::Duplicate(o) => (o, 0),
            })
            .collect())
    }

    async fn read(
        &self,
        stream: &StreamName,
        from: exspeed_common::Offset,
        max_records: usize,
    ) -> Result<Vec<exspeed_streams::StoredRecord>, StorageError> {
        self.log.storage.read(stream, from, max_records).await
    }

    async fn read_batch(
        &self,
        stream: &StreamName,
        from: exspeed_common::Offset,
        limits: exspeed_streams::ReadLimits,
    ) -> Result<exspeed_streams::ReadBatch, StorageError> {
        self.log.storage.read_batch(stream, from, limits).await
    }

    async fn read_raw(
        &self,
        stream: &StreamName,
        from: exspeed_common::Offset,
        limits: exspeed_streams::ReadLimits,
    ) -> Result<exspeed_streams::RawBatch, StorageError> {
        self.log.storage.read_raw(stream, from, limits).await
    }

    async fn read_with_hints(
        &self,
        stream: &StreamName,
        from: exspeed_common::Offset,
        max_records: usize,
        key_filter: Option<&str>,
    ) -> Result<Vec<exspeed_streams::StoredRecord>, StorageError> {
        self.log
            .storage
            .read_with_hints(stream, from, max_records, key_filter)
            .await
    }

    async fn seek_by_time(
        &self,
        stream: &StreamName,
        timestamp: u64,
    ) -> Result<exspeed_common::Offset, StorageError> {
        self.log.storage.seek_by_time(stream, timestamp).await
    }

    async fn list_streams(&self) -> Result<Vec<StreamName>, StorageError> {
        self.log.storage.list_streams().await
    }

    async fn trim_up_to(
        &self,
        stream: &StreamName,
        keep_from: exspeed_common::Offset,
    ) -> Result<(), StorageError> {
        self.log.storage.trim_up_to(stream, keep_from).await
    }

    async fn delete_stream(&self, stream: &StreamName) -> Result<(), StorageError> {
        self.log
            .delete_stream(stream)
            .await
            .map_err(into_storage_error)
    }

    async fn stream_bounds(
        &self,
        stream: &StreamName,
    ) -> Result<(exspeed_common::Offset, exspeed_common::Offset), StorageError> {
        self.log.storage.stream_bounds(stream).await
    }

    async fn truncate_from(
        &self,
        stream: &StreamName,
        drop_from: exspeed_common::Offset,
    ) -> Result<(), StorageError> {
        self.log.storage.truncate_from(stream, drop_from).await
    }

    async fn register_secondary_index(
        &self,
        stream: &StreamName,
        name: String,
        field_path: String,
    ) -> Result<(), StorageError> {
        self.log
            .storage
            .register_secondary_index(stream, name, field_path)
            .await
    }

    fn partition_dir_path(&self, stream: &str, partition: u32) -> Option<std::path::PathBuf> {
        self.log.storage.partition_dir_path(stream, partition)
    }

    async fn stream_config(&self, stream: &StreamName) -> Result<StreamConfig, StorageError> {
        self.log.storage.stream_config(stream).await
    }

    async fn update_stream_config(
        &self,
        stream: &StreamName,
        config: &StreamConfig,
    ) -> Result<(), StorageError> {
        self.log
            .update_stream_config(stream, config)
            .await
            .map_err(into_storage_error)
    }

    fn watch_appends(&self, stream: &StreamName) -> Option<tokio::sync::watch::Receiver<u64>> {
        self.log.storage.watch_appends(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use exspeed_storage::memory::MemoryStorage;

    fn rec(subject: &str, value: &str) -> Record {
        Record {
            key: None,
            value: Bytes::copy_from_slice(value.as_bytes()),
            subject: subject.into(),
            headers: vec![],
            timestamp_ns: None,
        }
    }

    fn with_id(mut r: Record, id: &str) -> Record {
        r.headers.push((IDEMPOTENCY_HEADER.into(), id.into()));
        r
    }

    fn log() -> Log {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let dedup = Arc::new(BrokerAppend::new(storage.clone(), 300));
        let (metrics, _) = Metrics::new();
        Log::new(
            storage,
            dedup,
            Arc::new(metrics),
            Arc::new(AtomicBool::new(true)),
        )
    }

    fn name(s: &str) -> StreamName {
        StreamName::try_from(s).unwrap()
    }

    struct Closed;
    impl WriteGate for Closed {
        fn can_write(&self) -> bool {
            false
        }
    }

    #[test]
    fn subject_validation() {
        let l = RecordLimits::default();
        assert!(l.check(&rec("", "x")).is_ok());
        assert!(l.check(&rec("orders.eu.created", "x")).is_ok());
        assert!(l.check(&rec("orders..created", "x")).is_err());
        assert!(l.check(&rec("orders.*", "x")).is_err());
        assert!(l.check(&rec("orders.>", "x")).is_err());
        assert!(l.check(&rec("orders eu", "x")).is_err());
        assert!(l.check(&rec(&"a".repeat(2000), "x")).is_err());
    }

    #[tokio::test]
    async fn rejects_oversized_records_before_writing() {
        let log = log();
        let s = name("s");
        log.create_stream(&s, &StreamConfig::default())
            .await
            .unwrap();
        let mut r = rec("a", "x");
        r.headers = (0..300).map(|i| (format!("h{i}"), "v".into())).collect();
        assert!(matches!(
            log.append(&s, r).await,
            Err(LogError::InvalidRecord(_))
        ));
        let (_, next) = log.storage().stream_bounds(&s).await.unwrap();
        assert_eq!(next.0, 0, "nothing may be written");
    }

    #[tokio::test]
    async fn closed_gate_rejects_writes() {
        let log = log();
        log.set_write_gate(Arc::new(Closed));
        let s = name("s");
        assert!(matches!(
            log.create_stream(&s, &StreamConfig::default()).await,
            Err(LogError::NotLeader)
        ));
    }

    #[tokio::test]
    async fn dedup_not_ready_rejects_only_keyed_records() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let dedup = Arc::new(BrokerAppend::new(storage.clone(), 300));
        let (metrics, _) = Metrics::new();
        let ready = Arc::new(AtomicBool::new(false));
        let log = Log::new(storage, dedup, Arc::new(metrics), ready.clone());
        let s = name("s");
        log.create_stream(&s, &StreamConfig::default())
            .await
            .unwrap();
        assert!(log.append(&s, rec("a", "1")).await.is_ok());
        assert!(matches!(
            log.append(&s, with_id(rec("a", "2"), "m1")).await,
            Err(LogError::DedupNotReady)
        ));
        ready.store(true, Ordering::Release);
        assert!(log.append(&s, with_id(rec("a", "2"), "m1")).await.is_ok());
    }

    #[tokio::test]
    async fn batch_dedups_within_the_batch() {
        let log = log();
        let s = name("s");
        log.create_stream(&s, &StreamConfig::default())
            .await
            .unwrap();
        let results = log
            .append_batch(
                &s,
                vec![
                    with_id(rec("a", "1"), "m1"),
                    with_id(rec("a", "1"), "m1"),
                    rec("a", "2"),
                ],
            )
            .await
            .unwrap();
        assert!(matches!(results[0], AppendResult::Written(o, _) if o.0 == 0));
        assert!(matches!(results[1], AppendResult::Duplicate(o) if o.0 == 0));
        assert!(matches!(results[2], AppendResult::Written(o, _) if o.0 == 1));
        let (_, next) = log.storage().stream_bounds(&s).await.unwrap();
        assert_eq!(next.0, 2);
    }

    #[tokio::test]
    async fn batch_rejects_collision_within_the_batch() {
        let log = log();
        let s = name("s");
        log.create_stream(&s, &StreamConfig::default())
            .await
            .unwrap();
        let err = log
            .append_batch(
                &s,
                vec![with_id(rec("a", "1"), "m1"), with_id(rec("a", "2"), "m1")],
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            LogError::Storage(StorageError::KeyCollision { .. })
        ));
        let (_, next) = log.storage().stream_bounds(&s).await.unwrap();
        assert_eq!(next.0, 0, "a rejected batch writes nothing");
    }

    #[tokio::test]
    async fn delete_forgets_dedup_state() {
        let log = log();
        let s = name("s");
        log.create_stream(&s, &StreamConfig::default())
            .await
            .unwrap();
        log.append(&s, with_id(rec("a", "1"), "m1")).await.unwrap();
        log.delete_stream(&s).await.unwrap();
        log.create_stream(&s, &StreamConfig::default())
            .await
            .unwrap();
        let r = log.append(&s, with_id(rec("a", "1"), "m1")).await.unwrap();
        assert!(
            matches!(r, AppendResult::Written(..)),
            "a recreated stream must not report a phantom duplicate"
        );
    }

    #[tokio::test]
    async fn batch_respects_dedup_cap() {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let dedup = Arc::new(BrokerAppend::new(storage.clone(), 300));
        let (metrics, _) = Metrics::new();
        let log = Log::new(
            storage,
            dedup,
            Arc::new(metrics),
            Arc::new(AtomicBool::new(true)),
        );
        let s = name("s");
        let cfg = StreamConfig {
            dedup_max_entries: 2,
            ..StreamConfig::default()
        };
        log.create_stream(&s, &cfg).await.unwrap();
        let err = log
            .append_batch(
                &s,
                (0..3)
                    .map(|i| with_id(rec("a", &i.to_string()), &format!("m{i}")))
                    .collect(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            LogError::Storage(StorageError::DedupMapFull { .. })
        ));
    }
}
