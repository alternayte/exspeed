//! Shared helpers: an in-memory log, a runner, and fake plugins.

#![allow(dead_code)]

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use exspeed_broker::broker_append::BrokerAppend;
use exspeed_broker::log::Log;
use exspeed_common::{Metrics, Offset, StreamName};
use exspeed_connectors::config::ConnectorConfig;
use exspeed_connectors::offset_store::{OffsetStore, OffsetStoreError, StoredOffset};
use exspeed_connectors::retry::{RestartPolicy, RetryPolicy};
use exspeed_connectors::runtime::{self, RunContext, RunHandle};
use exspeed_connectors::status::ConnectorState;
use exspeed_connectors::{
    ConnectorError, Registry, SinkConnector, SinkRecord, SourceBatch, SourceConnector,
    SourceRecord, WriteResult,
};
use exspeed_storage::memory::MemoryStorage;
use exspeed_streams::{ReadLimits, Record, StorageEngine, StoredRecord};
use tokio_util::sync::CancellationToken;

pub struct Env {
    pub log: Arc<Log>,
    pub storage: Arc<dyn StorageEngine>,
    pub metrics: Arc<Metrics>,
    pub prometheus: prometheus::Registry,
}

impl Env {
    pub fn new() -> Self {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let dedup = Arc::new(BrokerAppend::new(storage.clone(), 3600));
        let (metrics, prometheus) = Metrics::new();
        let metrics = Arc::new(metrics);
        let log = Arc::new(Log::new(
            storage.clone(),
            dedup,
            metrics.clone(),
            Arc::new(AtomicBool::new(true)),
        ));
        Self {
            log,
            storage,
            metrics,
            prometheus,
        }
    }

    pub async fn read_all(&self, stream: &str) -> Vec<StoredRecord> {
        read_all(&self.storage, stream).await
    }

    pub async fn publish(&self, stream: &str, values: &[&str]) {
        let with: Vec<(&str, &str)> = values.iter().map(|v| ("t", *v)).collect();
        self.publish_with_subjects(stream, &with).await;
    }

    /// Append `(subject, value)` records.
    pub async fn publish_with_subjects(&self, stream: &str, records: &[(&str, &str)]) {
        let s = StreamName::try_from(stream).unwrap();
        self.log.ensure_stream(&s).await.unwrap();
        for (subject, v) in records {
            self.log
                .append(
                    &s,
                    Record {
                        key: None,
                        value: Bytes::from(v.to_string()),
                        subject: subject.to_string(),
                        headers: vec![],
                        timestamp_ns: None,
                    },
                )
                .await
                .unwrap();
        }
    }

    /// Start a supervisor for `config`.
    pub fn run(
        &self,
        registry: &Registry,
        config: ConnectorConfig,
        offsets: Arc<dyn OffsetStore>,
    ) -> (RunHandle, Arc<ConnectorState>) {
        let state = ConnectorState::new(&config.name, self.metrics.clone());
        let settings = config.settings.clone();
        let ctx = Arc::new(RunContext {
            config,
            settings,
            log: self.log.clone(),
            storage: self.storage.clone(),
            offsets,
            metrics: self.metrics.clone(),
            state: state.clone(),
            registry: registry.clone(),
        });
        (runtime::spawn(ctx, &CancellationToken::new()), state)
    }

    pub fn prometheus_text(&self) -> String {
        use prometheus::Encoder;
        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&self.prometheus.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }
}

pub async fn read_all(storage: &Arc<dyn StorageEngine>, stream: &str) -> Vec<StoredRecord> {
    let s = StreamName::try_from(stream).unwrap();
    let mut out = Vec::new();
    let mut from = 0;
    loop {
        let b = match storage
            .read_batch(
                &s,
                Offset(from),
                ReadLimits {
                    max_records: 1000,
                    max_bytes: 16 << 20,
                },
            )
            .await
        {
            Ok(b) => b,
            Err(_) => return out,
        };
        if b.records.is_empty() {
            return out;
        }
        from = b.next_offset.0;
        out.extend(b.records);
    }
}

pub fn values(records: &[StoredRecord]) -> Vec<String> {
    records
        .iter()
        .map(|r| String::from_utf8_lossy(&r.value).into_owned())
        .collect()
}

/// Poll `cond` every 20 ms until it holds or `secs` elapse.
pub async fn eventually<F, Fut>(secs: u64, what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !cond().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A config with fast, deterministic retry and restart policies.
pub fn fast_config(
    name: &str,
    ty: exspeed_connectors::ConnectorType,
    plugin: &str,
    stream: &str,
) -> ConnectorConfig {
    let mut c = ConnectorConfig::new(name, ty, plugin, stream);
    c.poll_interval_ms = 5;
    c.retry = RetryPolicy {
        max_retries: 3,
        initial_backoff_ms: 5,
        max_backoff_ms: 20,
        multiplier: 2.0,
        jitter: false,
    };
    c.restart = RestartPolicy {
        initial_backoff_ms: 10,
        max_backoff_ms: 50,
        multiplier: 2.0,
        jitter: false,
        max_restarts: 0,
    };
    c
}

// ---------------------------------------------------------------------------
// Offset stores
// ---------------------------------------------------------------------------

/// In-memory offset store.
#[derive(Default)]
pub struct MemOffsets(pub Mutex<std::collections::HashMap<String, StoredOffset>>);

#[async_trait]
impl OffsetStore for MemOffsets {
    async fn load(&self, c: &str) -> Result<Option<StoredOffset>, OffsetStoreError> {
        Ok(self.0.lock().unwrap().get(c).cloned())
    }
    async fn save(&self, c: &str, o: &StoredOffset) -> Result<(), OffsetStoreError> {
        self.0.lock().unwrap().insert(c.to_string(), o.clone());
        Ok(())
    }
    async fn delete(&self, c: &str) -> Result<(), OffsetStoreError> {
        self.0.lock().unwrap().remove(c);
        Ok(())
    }
}

/// Wraps a store; the first `crash_saves` saves panic (a process crash
/// after the append, before the checkpoint is persisted). Loads can be made
/// to fail.
pub struct CrashingOffsets {
    pub inner: Arc<dyn OffsetStore>,
    pub crash_saves: AtomicU32,
    pub fail_loads: AtomicBool,
}

impl CrashingOffsets {
    pub fn new(inner: Arc<dyn OffsetStore>, crash_saves: u32) -> Self {
        Self {
            inner,
            crash_saves: AtomicU32::new(crash_saves),
            fail_loads: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl OffsetStore for CrashingOffsets {
    async fn load(&self, c: &str) -> Result<Option<StoredOffset>, OffsetStoreError> {
        if self.fail_loads.load(Ordering::SeqCst) {
            return Err(OffsetStoreError::Read("disk on fire".into()));
        }
        self.inner.load(c).await
    }
    async fn save(&self, c: &str, o: &StoredOffset) -> Result<(), OffsetStoreError> {
        if take_atomic(&self.crash_saves) {
            panic!("simulated crash before the checkpoint is persisted");
        }
        self.inner.save(c, o).await
    }
    async fn delete(&self, c: &str) -> Result<(), OffsetStoreError> {
        self.inner.delete(c).await
    }
}

fn take(n: &mut u32) -> bool {
    if *n > 0 {
        *n -= 1;
        true
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// Fake source: an external "table" read with a numeric cursor
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct SourceExt {
    pub rows: Vec<String>,
    /// Highest cursor acknowledged externally via `ack()`.
    pub acked: Option<usize>,
    /// Checkpoints `start()` was called with.
    pub starts: Vec<Option<String>>,
    pub stops: u32,
    pub fail_start: u32,
    pub fatal_start: bool,
    pub transient_poll: u32,
    pub connection_poll: u32,
    pub panic_poll: u32,
    pub panic_ack: u32,
    /// Attach `x-idempotency-key = row:<index>`.
    pub idempotent: bool,
    /// Rows whose subject is invalid (poison for the log).
    pub bad_subject_rows: Vec<usize>,
    /// Number of `start()` calls currently without a matching `stop()`.
    pub live: i32,
    pub max_live: i32,
}

pub type Shared<T> = Arc<Mutex<T>>;

pub struct FakeSource {
    ext: Shared<SourceExt>,
    pos: usize,
}

#[async_trait]
impl SourceConnector for FakeSource {
    async fn start(&mut self, checkpoint: Option<String>) -> Result<(), ConnectorError> {
        let mut e = self.ext.lock().unwrap();
        e.starts.push(checkpoint.clone());
        e.live += 1;
        e.max_live = e.max_live.max(e.live);
        if e.fatal_start {
            return Err(ConnectorError::fatal("bad password"));
        }
        if take(&mut e.fail_start) {
            return Err(ConnectorError::connection("connection refused"));
        }
        self.pos = checkpoint.map(|c| c.parse().unwrap()).unwrap_or(0);
        Ok(())
    }

    async fn poll(&mut self, max: usize) -> Result<SourceBatch, ConnectorError> {
        let mut e = self.ext.lock().unwrap();
        if take(&mut e.panic_poll) {
            drop(e);
            panic!("plugin bug in poll");
        }
        if take(&mut e.transient_poll) {
            return Err(ConnectorError::transient("timeout"));
        }
        if take(&mut e.connection_poll) {
            return Err(ConnectorError::connection("server closed the connection"));
        }
        let end = (self.pos + max).min(e.rows.len());
        let records: Vec<SourceRecord> = (self.pos..end)
            .map(|i| SourceRecord {
                key: None,
                value: Bytes::from(e.rows[i].clone()),
                subject: if e.bad_subject_rows.contains(&i) {
                    "has space".into()
                } else {
                    "rows".into()
                },
                headers: if e.idempotent {
                    vec![("x-idempotency-key".into(), format!("row:{i}"))]
                } else {
                    vec![]
                },
            })
            .collect();
        if records.is_empty() {
            return Ok(SourceBatch::empty());
        }
        self.pos = end;
        Ok(SourceBatch {
            records,
            checkpoint: Some(end.to_string()),
        })
    }

    async fn ack(&mut self, checkpoint: Option<&str>) -> Result<(), ConnectorError> {
        let mut e = self.ext.lock().unwrap();
        if take(&mut e.panic_ack) {
            drop(e);
            panic!("crash before the external ack");
        }
        if let Some(c) = checkpoint {
            e.acked = Some(c.parse().unwrap());
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        let mut e = self.ext.lock().unwrap();
        e.stops += 1;
        e.live -= 1;
        Ok(())
    }
}

pub fn source_registry(ext: &Shared<SourceExt>) -> Registry {
    let mut r = Registry::empty();
    let ext = ext.clone();
    r.register_source("fake", move |_| {
        Ok(Box::new(FakeSource {
            ext: ext.clone(),
            pos: 0,
        }))
    });
    r
}

// ---------------------------------------------------------------------------
// Fake sink: buffers in write(), makes durable in flush()
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct SinkExt {
    /// Offsets durable in the "target" (after flush).
    pub durable: Vec<u64>,
    pub flushes: u32,
    pub fatal_flush: u32,
    pub transient_flush: u32,
    /// Offsets the sink rejects as poison.
    pub poison_offsets: Vec<u64>,
    pub flush_interval: Option<Duration>,
}

pub struct FakeSink {
    ext: Shared<SinkExt>,
    buffer: Vec<u64>,
}

#[async_trait]
impl SinkConnector for FakeSink {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        self.buffer.clear();
        Ok(())
    }

    async fn write(&mut self, records: &[SinkRecord]) -> Result<WriteResult, ConnectorError> {
        let poison = self.ext.lock().unwrap().poison_offsets.clone();
        for (i, r) in records.iter().enumerate() {
            if poison.contains(&r.offset) {
                return Ok(WriteResult::Poison {
                    index: i,
                    reason: exspeed_connectors::PoisonReason::SinkRejected {
                        detail: "constraint violation".into(),
                    },
                });
            }
            self.buffer.push(r.offset);
        }
        Ok(WriteResult::Accepted)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        let mut e = self.ext.lock().unwrap();
        if take(&mut e.fatal_flush) {
            return Err(ConnectorError::fatal("bucket deleted"));
        }
        if take(&mut e.transient_flush) {
            return Err(ConnectorError::transient("503"));
        }
        e.flushes += 1;
        e.durable.append(&mut self.buffer);
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        // Whatever is still buffered is lost (and re-read after a restart).
        self.buffer.clear();
        Ok(())
    }

    fn default_flush_interval(&self) -> Duration {
        self.ext
            .lock()
            .unwrap()
            .flush_interval
            .unwrap_or(Duration::from_millis(50))
    }
}

pub fn sink_registry(ext: &Shared<SinkExt>) -> Registry {
    let mut r = Registry::empty();
    let ext = ext.clone();
    r.register_sink("fake", move |_| {
        Ok(Box::new(FakeSink {
            ext: ext.clone(),
            buffer: vec![],
        }))
    });
    r
}

pub fn shared<T: Default>() -> Shared<T> {
    Arc::new(Mutex::new(T::default()))
}

// ---------------------------------------------------------------------------
// Service-backed tests
// ---------------------------------------------------------------------------

/// The value of a service env var (`EXSPEED_RABBITMQ_URL`, …). Unset or
/// empty: `None` (the test skips), except under `CI=true`, where it panics
/// so a misconfigured service job can't pass silently.
pub fn service_env(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            let ci = std::env::var("CI").unwrap_or_default();
            if ci == "true" || ci == "1" {
                panic!("{var} must be set when CI=true (the service tests would silently pass)");
            }
            eprintln!("SKIP: {var} not set");
            None
        }
    }
}

static UNIQUE: AtomicU32 = AtomicU32::new(0);

/// A unique lowercase identifier usable as a table, queue, bucket prefix and
/// connector name.
pub fn unique(tag: &str) -> String {
    format!(
        "it_{tag}_{}_{}",
        std::process::id(),
        UNIQUE.fetch_add(1, Ordering::Relaxed)
    )
}

/// Wait for the connector to reach `running` (panics if it fails).
pub async fn wait_running(state: &ConnectorState) {
    eventually(60, "connector running", || async {
        let s = state.snapshot();
        assert_ne!(
            s.status,
            exspeed_connectors::status::Status::Failed,
            "connector failed: {:?}",
            s.last_error
        );
        s.status == exspeed_connectors::status::Status::Running
    })
    .await;
}

/// Wait until `stream` holds at least `n` records and return them all.
pub async fn wait_records(env: &Env, stream: &str, n: usize) -> Vec<StoredRecord> {
    eventually(60, &format!("{n} records in {stream}"), || async {
        env.read_all(stream).await.len() >= n
    })
    .await;
    env.read_all(stream).await
}

/// Wait until the sink has committed position `pos`.
pub async fn wait_committed(offsets: &Arc<dyn OffsetStore>, name: &str, pos: u64) {
    eventually(60, &format!("{name} committed {pos}"), || async {
        offsets.load_sink(name).await.unwrap() == Some(pos)
    })
    .await;
}

/// Publish JSON values with `x-idempotency-key`-free records.
pub async fn publish_json(env: &Env, stream: &str, values: &[serde_json::Value]) {
    let v: Vec<String> = values.iter().map(|v| v.to_string()).collect();
    let refs: Vec<&str> = v.iter().map(String::as_str).collect();
    env.publish(stream, &refs).await;
}

/// The settings map of a connector config.
pub fn settings(v: serde_json::Value) -> exspeed_connectors::config::Settings {
    v.as_object().unwrap().clone()
}

/// Wraps a built-in source: the first `crashes` calls to `ack()` panic
/// before reaching the plugin. That is a crash after the batch is durable in
/// the log (and its checkpoint saved) but before the external
/// acknowledgement (AMQP ack, WAL confirm, outbox delete).
pub struct CrashBeforeAck {
    inner: Box<dyn SourceConnector>,
    crashes: Arc<AtomicU32>,
}

/// Decrement-if-positive (a CAS loop; `fetch_update` is deprecated on
/// newer toolchains and its replacement is not on older ones).
pub fn take_atomic(n: &AtomicU32) -> bool {
    loop {
        let v = n.load(Ordering::SeqCst);
        if v == 0 {
            return false;
        }
        if n.compare_exchange(v, v - 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return true;
        }
    }
}

#[async_trait]
impl SourceConnector for CrashBeforeAck {
    async fn start(&mut self, checkpoint: Option<String>) -> Result<(), ConnectorError> {
        self.inner.start(checkpoint).await
    }
    async fn poll(&mut self, max: usize) -> Result<SourceBatch, ConnectorError> {
        self.inner.poll(max).await
    }
    async fn ack(&mut self, checkpoint: Option<&str>) -> Result<(), ConnectorError> {
        if take_atomic(&self.crashes) {
            panic!("simulated crash before the external ack");
        }
        self.inner.ack(checkpoint).await
    }
    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.inner.stop().await
    }
    fn lag(&self) -> Option<exspeed_connectors::Lag> {
        self.inner.lag()
    }
}

/// The built-in registry with source `plugin` wrapped in [`CrashBeforeAck`].
pub fn crash_before_ack_registry(plugin: &str, crashes: Arc<AtomicU32>) -> Registry {
    let builtin = Registry::builtin();
    let mut r = Registry::builtin();
    r.register_source(plugin, move |init| {
        Ok(Box::new(CrashBeforeAck {
            inner: builtin.create_source(init)?,
            crashes: crashes.clone(),
        }))
    });
    r
}
