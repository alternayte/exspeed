//! Test harness: an engine over in-memory storage with a real `Log`.

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use exspeed_broker::broker_append::BrokerAppend;
use exspeed_broker::leadership::ClusterLeadership;
use exspeed_broker::lease::NoopLeaderLease;
use exspeed_broker::log::Log;
use exspeed_common::metrics::Metrics;
use exspeed_common::{Offset, StreamName};
use exspeed_storage::memory::MemoryStorage;
use exspeed_streams::{ReadLimits, Record, StorageEngine, StoredRecord, StreamConfig};
use serde_json::Value as Json;
use tokio_util::sync::CancellationToken;

use crate::engine::ExqlEngine;
use crate::session::ExqlConfig;

pub struct World {
    pub storage: Arc<dyn StorageEngine>,
    pub log: Arc<Log>,
    pub metrics: Arc<Metrics>,
    pub leadership: Arc<ClusterLeadership>,
}

impl World {
    pub async fn new() -> Self {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let (metrics, _reg) = Metrics::new();
        let metrics = Arc::new(metrics);
        let dedup = Arc::new(BrokerAppend::new(storage.clone(), 300));
        let log = Arc::new(Log::new(
            storage.clone(),
            dedup,
            metrics.clone(),
            Arc::new(AtomicBool::new(true)),
        ));
        let lease: Arc<dyn exspeed_broker::LeaderLease> = Arc::new(NoopLeaderLease::new());
        let leadership = Arc::new(ClusterLeadership::spawn(lease, metrics.clone(), None).await);
        for _ in 0..100 {
            if leadership.is_currently_leader() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(leadership.is_currently_leader());
        Self {
            storage,
            log,
            metrics,
            leadership,
        }
    }

    pub async fn stream(&self, name: &str) {
        self.log
            .create_stream(
                &StreamName::try_from(name).unwrap(),
                &StreamConfig::default(),
            )
            .await
            .unwrap();
    }

    pub async fn publish(&self, stream: &str, key: Option<&str>, subject: &str, payload: Json) {
        self.log
            .append(
                &StreamName::try_from(stream).unwrap(),
                Record {
                    key: key.map(|k| Bytes::from(k.to_string())),
                    value: Bytes::from(payload.to_string()),
                    subject: subject.to_string(),
                    headers: vec![],
                    timestamp_ns: None,
                },
            )
            .await
            .unwrap();
    }

    pub async fn read_all(&self, stream: &str) -> Vec<StoredRecord> {
        let s = StreamName::try_from(stream).unwrap();
        let mut out = vec![];
        let mut p = 0;
        loop {
            let b = match self
                .storage
                .read_batch(
                    &s,
                    Offset(p),
                    ReadLimits {
                        max_records: 1000,
                        max_bytes: 1 << 24,
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
            p = b.next_offset.0;
            out.extend(b.records);
        }
    }

    /// Payloads of a stream, parsed.
    pub async fn payloads(&self, stream: &str) -> Vec<Json> {
        self.read_all(stream)
            .await
            .iter()
            .map(|r| serde_json::from_slice(&r.value).unwrap_or(Json::Null))
            .collect()
    }
}

pub fn test_config() -> ExqlConfig {
    ExqlConfig {
        checkpoint_interval: Duration::from_secs(3600),
        checkpoint_every_batches: 0,
        micro_batch_records: 3,
        poll_interval: Duration::from_millis(10),
        ..ExqlConfig::default()
    }
}

pub struct Node {
    pub engine: Arc<ExqlEngine>,
    pub tenure: CancellationToken,
}

impl Node {
    pub async fn start(world: &World, dir: &Path, cfg: ExqlConfig) -> Node {
        let engine = Arc::new(
            ExqlEngine::new(
                world.log.clone(),
                dir.to_path_buf(),
                world.leadership.clone(),
                world.metrics.clone(),
                cfg,
            )
            .unwrap(),
        );
        engine.load().await.unwrap();
        let tenure = CancellationToken::new();
        tokio::spawn(engine.clone().resume_all_and_run(tenure.clone()));
        // Let resume_all_and_run record the tenure.
        tokio::time::sleep(Duration::from_millis(20)).await;
        Node { engine, tenure }
    }

    /// Graceful stop (final checkpoints).
    pub async fn stop(self) {
        self.engine.shutdown().await;
        self.tenure.cancel();
    }

    /// Crash: no final checkpoint.
    pub async fn crash(self) {
        self.engine.abort_all().await;
        self.tenure.cancel();
    }

    pub async fn sql(&self, sql: &str) -> Json {
        match self.engine.execute(sql).await {
            Ok(r) => r.to_json(),
            Err(e) => panic!("{sql}: {e}"),
        }
    }

    /// Wait until query `id` has consumed `n` input records.
    pub async fn wait_input(&self, id: &str, n: u64) {
        for _ in 0..500 {
            let q = self.engine.get_query(id).unwrap();
            if q.stats["records_in"].as_u64().unwrap_or(0) >= n {
                // let the write of that micro-batch finish
                tokio::time::sleep(Duration::from_millis(30)).await;
                return;
            }
            if q.status == "failed" {
                panic!("query {id} failed: {:?}", q.error);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "query {id} did not consume {n} records: {:?}",
            self.engine.get_query(id)
        );
    }
}

/// Poll `f` until it returns true (5 s max).
pub async fn eventually<F, Fut>(what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..500 {
        if f().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}
