//! The checkpoint protocol and the supervisor, with fake plugins.

use std::sync::Arc;
use std::time::Duration;

use exspeed_connectors::config::{ConnectorConfig, OnTransientExhausted};
use exspeed_connectors::offset_store::OffsetStore;
use exspeed_connectors::status::Status;
use exspeed_connectors::ConnectorType::{Sink, Source};
use exspeed_connectors::{ConnectorManager, ManagerError, Registry};

use crate::common::*;

fn rows(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("r{i}")).collect()
}

fn source_ext(n: usize) -> Shared<SourceExt> {
    let ext = shared::<SourceExt>();
    ext.lock().unwrap().rows = rows(n);
    ext
}

fn source_config(name: &str) -> ConnectorConfig {
    let mut c = fast_config(name, Source, "fake", "out");
    c.batch_size = 5;
    c
}

// ---------------------------------------------------------------------------
// Checkpoint protocol: sources
// ---------------------------------------------------------------------------

/// A crash after the append but before the checkpoint is persisted replays
/// the batch: nothing is lost, and the replayed batch is the only duplicate
/// (the documented at-least-once window).
#[tokio::test]
async fn crash_before_checkpoint_replays_only_that_batch() {
    let env = Env::new();
    let ext = source_ext(12);
    let offsets = Arc::new(CrashingOffsets::new(Arc::new(MemOffsets::default()), 1));
    let (h, state) = env.run(&source_registry(&ext), source_config("c1"), offsets.clone());

    eventually(10, "all rows acked", || async {
        ext.lock().unwrap().acked == Some(12)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;

    let got = values(&env.read_all("out").await);
    let mut want: Vec<String> = rows(5); // first attempt, never checkpointed
    want.extend(rows(12)); // replay from scratch, then the rest
    assert_eq!(got, want);
    assert_eq!(ext.lock().unwrap().starts, vec![None, None]);
    assert_eq!(
        offsets.load_source("c1").await.unwrap().as_deref(),
        Some("12")
    );
    assert_eq!(state.snapshot().restart_count, 1);
}

/// Closed (not the leader) for the next `n` write checks, then open.
struct FlakyGate(std::sync::atomic::AtomicU32);

impl exspeed_broker::log::WriteGate for FlakyGate {
    fn can_write(&self) -> bool {
        !take_atomic(&self.0)
    }
}

/// Offsets that record whether the gate was already open at every save.
struct GateCheckedOffsets {
    inner: MemOffsets,
    gate: Arc<FlakyGate>,
    saves_while_closed: std::sync::atomic::AtomicU32,
}

#[async_trait::async_trait]
impl OffsetStore for GateCheckedOffsets {
    async fn load(
        &self,
        c: &str,
    ) -> Result<
        Option<exspeed_connectors::offset_store::StoredOffset>,
        exspeed_connectors::offset_store::OffsetStoreError,
    > {
        self.inner.load(c).await
    }
    async fn save(
        &self,
        c: &str,
        o: &exspeed_connectors::offset_store::StoredOffset,
    ) -> Result<(), exspeed_connectors::offset_store::OffsetStoreError> {
        use std::sync::atomic::Ordering::SeqCst;
        if self.gate.0.load(SeqCst) > 0 {
            self.saves_while_closed.fetch_add(1, SeqCst);
        }
        self.inner.save(c, o).await
    }
    async fn delete(
        &self,
        c: &str,
    ) -> Result<(), exspeed_connectors::offset_store::OffsetStoreError> {
        self.inner.delete(c).await
    }
}

/// 2026-10 review §3.6 #6: a retryable `LogError` (here `NotLeader` from the write
/// gate, twice) during a source append is retried in place: nothing goes to
/// the DLQ, the checkpoint isn't saved while the append is failing, no
/// restart, and the batch then lands exactly once.
#[tokio::test]
async fn retryable_append_error_is_retried_without_dlq_or_checkpoint() {
    let env = Env::new();
    let out = exspeed_common::StreamName::try_from("out").unwrap();
    let dlq = exspeed_common::StreamName::try_from("out_dlq").unwrap();
    env.log.ensure_stream(&out).await.unwrap();
    env.log.ensure_stream(&dlq).await.unwrap();
    let gate = Arc::new(FlakyGate(std::sync::atomic::AtomicU32::new(2)));
    env.log.set_write_gate(gate.clone());

    let ext = source_ext(5);
    let offsets = Arc::new(GateCheckedOffsets {
        inner: MemOffsets::default(),
        gate: gate.clone(),
        saves_while_closed: Default::default(),
    });
    let mut cfg = source_config("retry-append");
    cfg.dlq_stream = Some("out_dlq".into());
    let (h, state) = env.run(&source_registry(&ext), cfg, offsets.clone());
    eventually(10, "all rows acked", || async {
        ext.lock().unwrap().acked == Some(5)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;

    assert_eq!(
        gate.0.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "gate was hit"
    );
    assert_eq!(values(&env.read_all("out").await), rows(5), "exactly once");
    assert!(
        env.read_all("out_dlq").await.is_empty(),
        "nothing dead-lettered"
    );
    assert_eq!(
        offsets
            .saves_while_closed
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "checkpoint saved only after the append succeeded"
    );
    assert_eq!(
        offsets
            .load_source("retry-append")
            .await
            .unwrap()
            .as_deref(),
        Some("5")
    );
    let snap = state.snapshot();
    assert_eq!(snap.restart_count, 0, "retried in place, no restart");
    assert_eq!(ext.lock().unwrap().starts, vec![None]);
}

/// With idempotency keys the broker drops the replayed batch: exactly once.
#[tokio::test]
async fn crash_before_checkpoint_with_idempotency_keys_is_exactly_once() {
    let env = Env::new();
    let ext = source_ext(12);
    ext.lock().unwrap().idempotent = true;
    let offsets = Arc::new(CrashingOffsets::new(Arc::new(MemOffsets::default()), 1));
    let (h, _) = env.run(&source_registry(&ext), source_config("c2"), offsets);

    eventually(10, "all rows acked", || async {
        ext.lock().unwrap().acked == Some(12)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;
    assert_eq!(values(&env.read_all("out").await), rows(12));
}

/// A crash after the checkpoint is persisted but before `ack()` resumes
/// from the checkpoint: no duplicates in the stream, and the external ack
/// happens after the restart.
#[tokio::test]
async fn crash_before_ack_resumes_from_checkpoint() {
    let env = Env::new();
    let ext = source_ext(12);
    ext.lock().unwrap().panic_ack = 1;
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let (h, state) = env.run(&source_registry(&ext), source_config("c3"), offsets);

    eventually(10, "all rows acked", || async {
        ext.lock().unwrap().acked == Some(12)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;
    assert_eq!(values(&env.read_all("out").await), rows(12));
    assert_eq!(
        ext.lock().unwrap().starts,
        vec![None, Some("5".to_string())]
    );
    assert_eq!(state.snapshot().restart_count, 1);
}

/// `ack()` is called only after the batch is in the log and the checkpoint
/// is saved.
#[tokio::test]
async fn ack_follows_append_and_checkpoint() {
    let env = Env::new();
    let ext = source_ext(3);
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let (h, state) = env.run(&source_registry(&ext), source_config("c4"), offsets.clone());
    eventually(10, "acked", || async {
        ext.lock().unwrap().acked == Some(3)
    })
    .await;
    assert_eq!(env.read_all("out").await.len(), 3);
    assert_eq!(
        offsets.load_source("c4").await.unwrap().as_deref(),
        Some("3")
    );
    let snap = state.snapshot();
    assert_eq!(snap.status, Status::Running);
    assert_eq!(snap.checkpoint.as_deref(), Some("3"));
    assert_eq!(snap.records, 3);
    assert!(snap.last_success_ms.is_some());
    h.stop(Duration::from_secs(5)).await;
    assert_eq!(state.status(), Status::Stopped);
}

/// A record the log rejects goes to the DLQ with metadata headers; the
/// rest of the batch is appended.
#[tokio::test]
async fn poison_source_record_goes_to_dlq() {
    let env = Env::new();
    let ext = source_ext(6);
    ext.lock().unwrap().bad_subject_rows = vec![2];
    let mut cfg = source_config("c5");
    cfg.dlq_stream = Some("out-dlq".into());
    let (h, _) = env.run(&source_registry(&ext), cfg, Arc::new(MemOffsets::default()));
    eventually(10, "acked", || async {
        ext.lock().unwrap().acked == Some(6)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;

    assert_eq!(
        values(&env.read_all("out").await),
        vec!["r0", "r1", "r3", "r4", "r5"]
    );
    let dlq = env.read_all("out-dlq").await;
    assert_eq!(values(&dlq), vec!["r2"]);
    let h = &dlq[0].headers;
    let get = |k: &str| h.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    assert_eq!(get("exspeed-dlq-origin"), Some("c5"));
    assert_eq!(get("exspeed-dlq-reason"), Some("invalid_record"));
    assert!(get("exspeed-dlq-detail").unwrap().contains("whitespace"));
}

/// Without a DLQ, poison is dropped and counted.
#[tokio::test]
async fn poison_without_dlq_is_dropped_with_metric() {
    let env = Env::new();
    let ext = source_ext(3);
    ext.lock().unwrap().bad_subject_rows = vec![0];
    let (h, _) = env.run(
        &source_registry(&ext),
        source_config("c6"),
        Arc::new(MemOffsets::default()),
    );
    eventually(10, "acked", || async {
        ext.lock().unwrap().acked == Some(3)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;
    assert_eq!(values(&env.read_all("out").await), vec!["r1", "r2"]);
    let text = env.prometheus_text();
    assert!(
        text.lines()
            .any(|l| l.starts_with("exspeed_connector_records_skipped_total")
                && l.contains("connector=\"c6\"")
                && l.ends_with(" 1")),
        "{text}"
    );
}

// ---------------------------------------------------------------------------
// Supervisor
// ---------------------------------------------------------------------------

#[tokio::test]
async fn connection_failures_back_off_then_recover() {
    let env = Env::new();
    let ext = source_ext(4);
    ext.lock().unwrap().fail_start = 3;
    let mut cfg = source_config("sup1");
    cfg.restart.initial_backoff_ms = 150;
    cfg.restart.max_backoff_ms = 150;
    let (h, state) = env.run(&source_registry(&ext), cfg, Arc::new(MemOffsets::default()));

    // Observable while backing off.
    eventually(5, "backoff with last_error", || async {
        let s = state.snapshot();
        s.status == Status::Backoff
            && s.last_error
                .as_deref()
                .unwrap_or("")
                .contains("connection refused")
    })
    .await;
    eventually(10, "running and caught up", || async {
        ext.lock().unwrap().acked == Some(4)
    })
    .await;
    let snap = state.snapshot();
    assert_eq!(snap.status, Status::Running);
    assert_eq!(snap.restart_count, 3);
    assert_eq!(snap.last_error, None, "cleared once running again");

    // Every failed start was followed by a stop before the next start.
    {
        let e = ext.lock().unwrap();
        assert_eq!(e.starts.len(), 4);
        assert_eq!(
            e.max_live, 1,
            "a restart never overlaps the previous instance"
        );
    }

    h.stop(Duration::from_secs(5)).await;
    assert_eq!(
        ext.lock().unwrap().live,
        0,
        "stop() awaited the plugin's stop"
    );
    let text = env.prometheus_text();
    assert!(
        text.lines()
            .any(|l| l.starts_with("exspeed_connector_restarts_total")
                && l.contains("connector=\"sup1\"")
                && l.ends_with(" 3")),
        "{text}"
    );
    assert!(text.contains("exspeed_connector_state"), "{text}");
}

#[tokio::test]
async fn gives_up_after_max_restarts() {
    let env = Env::new();
    let ext = source_ext(1);
    ext.lock().unwrap().fail_start = 1000;
    let mut cfg = source_config("sup2");
    cfg.restart.max_restarts = 2;
    let (h, state) = env.run(&source_registry(&ext), cfg, Arc::new(MemOffsets::default()));
    eventually(10, "failed", || async { state.status() == Status::Failed }).await;
    let snap = state.snapshot();
    assert_eq!(snap.restart_count, 2);
    let err = snap.last_error.unwrap();
    assert!(
        err.contains("gave up after 2") && err.contains("connection refused"),
        "{err}"
    );
    eventually(5, "supervisor exited", || async { h.is_finished() }).await;
}

#[tokio::test]
async fn fatal_error_fails_without_restart() {
    let env = Env::new();
    let ext = source_ext(1);
    ext.lock().unwrap().fatal_start = true;
    let (h, state) = env.run(
        &source_registry(&ext),
        source_config("sup3"),
        Arc::new(MemOffsets::default()),
    );
    eventually(10, "failed", || async { state.status() == Status::Failed }).await;
    let snap = state.snapshot();
    assert_eq!(snap.restart_count, 0);
    assert!(snap.last_error.unwrap().contains("bad password"));
    eventually(5, "supervisor exited", || async { h.is_finished() }).await;
    assert_eq!(
        ext.lock().unwrap().live,
        0,
        "stop() ran after the failed start"
    );
}

#[tokio::test]
async fn transient_errors_retry_in_place_then_restart_by_default() {
    let env = Env::new();
    let ext = source_ext(4);
    // retry.max_retries = 3: 4 failures exhaust one round, the restart
    // succeeds after 2 more.
    ext.lock().unwrap().transient_poll = 6;
    let (h, state) = env.run(
        &source_registry(&ext),
        source_config("sup4"),
        Arc::new(MemOffsets::default()),
    );
    eventually(10, "caught up", || async {
        ext.lock().unwrap().acked == Some(4)
    })
    .await;
    assert_eq!(state.snapshot().restart_count, 1);
    assert_eq!(values(&env.read_all("out").await), rows(4));
    h.stop(Duration::from_secs(5)).await;
    let text = env.prometheus_text();
    assert!(
        text.lines().any(
            |l| l.starts_with("exspeed_connector_transient_exhausted_total")
                && l.contains("connector=\"sup4\"")
                && l.contains("action=\"restart\"")
        ),
        "{text}"
    );
}

#[tokio::test]
async fn transient_exhausted_with_fail_policy_fails() {
    let env = Env::new();
    let ext = source_ext(4);
    ext.lock().unwrap().transient_poll = 1000;
    let mut cfg = source_config("sup5");
    cfg.on_transient_exhausted = OnTransientExhausted::Fail;
    let (_h, state) = env.run(&source_registry(&ext), cfg, Arc::new(MemOffsets::default()));
    eventually(10, "failed", || async { state.status() == Status::Failed }).await;
    assert!(state
        .snapshot()
        .last_error
        .unwrap()
        .contains("retries exhausted"));
}

#[tokio::test]
async fn lost_connection_reconnects_via_restart() {
    let env = Env::new();
    let ext = source_ext(8);
    ext.lock().unwrap().connection_poll = 1;
    let (h, state) = env.run(
        &source_registry(&ext),
        source_config("sup6"),
        Arc::new(MemOffsets::default()),
    );
    eventually(10, "caught up", || async {
        ext.lock().unwrap().acked == Some(8)
    })
    .await;
    assert_eq!(state.snapshot().restart_count, 1);
    assert_eq!(values(&env.read_all("out").await), rows(8));
    assert_eq!(ext.lock().unwrap().starts.len(), 2);
    h.stop(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn panic_in_plugin_is_caught_and_restarted() {
    let env = Env::new();
    let ext = source_ext(7);
    ext.lock().unwrap().panic_poll = 1;
    let (h, state) = env.run(
        &source_registry(&ext),
        source_config("sup7"),
        Arc::new(MemOffsets::default()),
    );
    eventually(10, "caught up", || async {
        ext.lock().unwrap().acked == Some(7)
    })
    .await;
    assert_eq!(state.snapshot().restart_count, 1);
    assert_eq!(values(&env.read_all("out").await), rows(7));
    h.stop(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn offset_load_error_fails_instead_of_starting_over() {
    let env = Env::new();
    let ext = source_ext(3);
    let offsets = Arc::new(CrashingOffsets::new(Arc::new(MemOffsets::default()), 0));
    offsets
        .fail_loads
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (_h, state) = env.run(&source_registry(&ext), source_config("sup8"), offsets);
    eventually(10, "failed", || async { state.status() == Status::Failed }).await;
    assert!(state
        .snapshot()
        .last_error
        .unwrap()
        .contains("failed to load offset"));
    assert!(
        ext.lock().unwrap().starts.is_empty(),
        "never started from scratch"
    );
    assert!(env.read_all("out").await.is_empty());
}

// ---------------------------------------------------------------------------
// Checkpoint protocol: sinks
// ---------------------------------------------------------------------------

fn sink_config(name: &str) -> ConnectorConfig {
    let mut c = fast_config(name, Sink, "fake", "in");
    c.batch_size = 4;
    c
}

const TEN: [&str; 10] = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];

#[tokio::test]
async fn sink_flush_failure_leaves_offset_uncommitted() {
    let env = Env::new();
    env.publish("in", &TEN).await;
    let ext = shared::<SinkExt>();
    ext.lock().unwrap().fatal_flush = 1;
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let reg = sink_registry(&ext);

    let (_h, state) = env.run(&reg, sink_config("s1"), offsets.clone());
    eventually(10, "failed", || async { state.status() == Status::Failed }).await;
    assert!(state
        .snapshot()
        .last_error
        .unwrap()
        .contains("bucket deleted"));
    assert_eq!(
        offsets.load_sink("s1").await.unwrap(),
        None,
        "nothing committed"
    );
    assert!(ext.lock().unwrap().durable.is_empty());

    // After a restart every record is delivered again and committed.
    let (h, _) = env.run(&reg, sink_config("s1"), offsets.clone());
    eventually(10, "committed", || async {
        offsets.load_sink("s1").await.unwrap() == Some(10)
    })
    .await;
    assert_eq!(ext.lock().unwrap().durable, (0..10).collect::<Vec<u64>>());
    h.stop(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn sink_transient_flush_failure_is_retried_before_commit() {
    let env = Env::new();
    env.publish("in", &TEN).await;
    let ext = shared::<SinkExt>();
    ext.lock().unwrap().transient_flush = 2;
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let (h, state) = env.run(&sink_registry(&ext), sink_config("s2"), offsets.clone());
    eventually(10, "committed", || async {
        offsets.load_sink("s2").await.unwrap() == Some(10)
    })
    .await;
    assert_eq!(ext.lock().unwrap().durable, (0..10).collect::<Vec<u64>>());
    let snap = state.snapshot();
    assert_eq!(snap.status, Status::Running);
    assert_eq!(snap.restart_count, 0);
    assert_eq!(snap.lag, Some(0));
    h.stop(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn sink_flushes_on_timer_and_on_stop() {
    let env = Env::new();
    env.publish("in", &TEN).await;

    // Plugin default: flush hourly. Nothing is committed while running…
    let ext = shared::<SinkExt>();
    ext.lock().unwrap().flush_interval = Some(Duration::from_secs(3600));
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let (h, _) = env.run(&sink_registry(&ext), sink_config("s3"), offsets.clone());
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(offsets.load_sink("s3").await.unwrap(), None);
    assert!(ext.lock().unwrap().durable.is_empty());
    // …and a graceful stop flushes and commits.
    assert!(h.stop(Duration::from_secs(5)).await);
    assert_eq!(ext.lock().unwrap().durable, (0..10).collect::<Vec<u64>>());
    assert_eq!(offsets.load_sink("s3").await.unwrap(), Some(10));

    // `flush_interval_ms` overrides the plugin default.
    let ext = shared::<SinkExt>();
    ext.lock().unwrap().flush_interval = Some(Duration::from_secs(3600));
    let mut cfg = sink_config("s3b");
    cfg.flush_interval_ms = Some(50);
    let (h, _) = env.run(&sink_registry(&ext), cfg, offsets.clone());
    eventually(10, "timer flush", || async {
        offsets.load_sink("s3b").await.unwrap() == Some(10)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn sink_poison_goes_to_dlq_and_the_rest_commits() {
    let env = Env::new();
    env.publish("in", &TEN).await;
    let ext = shared::<SinkExt>();
    ext.lock().unwrap().poison_offsets = vec![3];
    let mut cfg = sink_config("s4");
    cfg.dlq_stream = Some("in-dlq".into());
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let (h, _) = env.run(&sink_registry(&ext), cfg, offsets.clone());
    eventually(10, "committed", || async {
        offsets.load_sink("s4").await.unwrap() == Some(10)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;
    assert_eq!(ext.lock().unwrap().durable, vec![0, 1, 2, 4, 5, 6, 7, 8, 9]);
    let dlq = env.read_all("in-dlq").await;
    assert_eq!(values(&dlq), vec!["d"]);
    let get = |k: &str| {
        dlq[0]
            .headers
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(get("exspeed-dlq-original-offset").as_deref(), Some("3"));
    assert_eq!(get("exspeed-dlq-reason").as_deref(), Some("sink_rejected"));
}

#[tokio::test]
async fn sink_resumes_from_committed_offset() {
    let env = Env::new();
    env.publish("in", &TEN[..4]).await;
    let ext = shared::<SinkExt>();
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let reg = sink_registry(&ext);
    let (h, _) = env.run(&reg, sink_config("s5"), offsets.clone());
    eventually(10, "first commit", || async {
        offsets.load_sink("s5").await.unwrap() == Some(4)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;
    env.publish("in", &TEN[4..]).await;
    let (h, _) = env.run(&reg, sink_config("s5"), offsets.clone());
    eventually(10, "second commit", || async {
        offsets.load_sink("s5").await.unwrap() == Some(10)
    })
    .await;
    h.stop(Duration::from_secs(5)).await;
    assert_eq!(
        ext.lock().unwrap().durable,
        (0..10).collect::<Vec<u64>>(),
        "no re-delivery"
    );
}

// ---------------------------------------------------------------------------
// Typed settings and validation (through the manager)
// ---------------------------------------------------------------------------

async fn manager(dir: &std::path::Path, env: &Env) -> ConnectorManager {
    let lease: Arc<dyn exspeed_broker::LeaderLease> =
        Arc::new(exspeed_broker::lease::NoopLeaderLease::new());
    let leadership = Arc::new(
        exspeed_broker::leadership::ClusterLeadership::spawn(lease, env.metrics.clone(), None)
            .await,
    );
    ConnectorManager::new(
        env.storage.clone(),
        env.log.clone(),
        dir.to_path_buf(),
        env.metrics.clone(),
        Arc::new(MemOffsets::default()),
        leadership,
    )
}

#[tokio::test]
async fn misspelled_setting_is_rejected_by_name() {
    let env = Env::new();
    let dir = tempfile::tempdir().unwrap();
    let mgr = manager(dir.path(), &env).await;
    let cfg = ConnectorConfig::new("poller", Source, "http_poll", "s")
        .with_setting("url", "http://localhost:1/x")
        .with_setting("interval_sec", 5);
    match mgr.create(cfg).await {
        Err(ManagerError::Invalid(msg)) => {
            assert!(
                msg.contains("interval_sec") && msg.contains("http_poll"),
                "{msg}"
            )
        }
        other => panic!("expected Invalid, got {other:?}"),
    }
    // Native types and strings are both accepted.
    for v in [serde_json::json!(5), serde_json::json!("5")] {
        let cfg = ConnectorConfig::new("poller", Source, "http_poll", "s")
            .with_setting("url", "http://localhost:1/x")
            .with_setting("interval_secs", v)
            .with_setting("headers", serde_json::json!({"Authorization": "Bearer x"}));
        Registry::builtin()
            .validate(&cfg, false, env.metrics.clone())
            .unwrap();
    }
    // Wrong type.
    let cfg = ConnectorConfig::new("poller", Source, "http_poll", "s")
        .with_setting("url", "http://localhost:1/x")
        .with_setting("interval_secs", "soon");
    assert!(Registry::builtin()
        .validate(&cfg, false, env.metrics.clone())
        .is_err());
    // Unknown plugin.
    let cfg = ConnectorConfig::new("x", Sink, "kafka", "s");
    let err = Registry::builtin()
        .validate(&cfg, false, env.metrics.clone())
        .unwrap_err();
    assert!(
        err.to_string().contains("unknown sink plugin 'kafka'"),
        "{err}"
    );
}

#[tokio::test]
async fn invalid_toml_connector_is_registered_failed_and_secrets_stay_unresolved() {
    let env = Env::new();
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().join("connectors.d");
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(
        d.join("bad.toml"),
        "[connector]\nname = \"bad\"\ntype = \"sink\"\nplugin = \"http_sink\"\nstream = \"s\"\n\n\
         [settings]\nurl = \"http://x\"\ntimeout = 5\n",
    )
    .unwrap();
    std::env::set_var("EXSPEED_IT_SECRET", "hunter2");
    std::fs::write(
        d.join("good.toml"),
        "[connector]\nname = \"good\"\ntype = \"sink\"\nplugin = \"http_sink\"\nstream = \"s\"\n\n\
         [settings]\nurl = \"http://x\"\nheaders = { Authorization = \"Bearer ${EXSPEED_IT_SECRET}\" }\n",
    )
    .unwrap();
    let mgr = manager(dir.path(), &env).await;
    mgr.load_all().await.unwrap();

    let bad = mgr.get_status("bad").await.unwrap();
    assert_eq!(bad.state.status, Status::Failed);
    assert!(bad.state.last_error.unwrap().contains("timeout"));
    let good = mgr.get_status("good").await.unwrap();
    assert_ne!(good.state.status, Status::Failed);
    assert_eq!(good.origin, "file");

    // The config keeps the reference; nothing is persisted.
    let cfg = mgr.get_config("good").await.unwrap();
    assert_eq!(
        cfg.settings["headers"]["Authorization"],
        "Bearer ${EXSPEED_IT_SECRET}"
    );
    assert!(!dir.path().join("connectors").exists());
    // The file is read-only through the API.
    assert!(matches!(
        mgr.update(cfg).await,
        Err(ManagerError::FileManaged { .. })
    ));
}

/// 2026-10 review §3.6 #3 / blocker 15: editing the `connectors.d` TOML of a running
/// connector restarts it with the new config but keeps its committed
/// offsets — nothing already delivered is replayed.
#[tokio::test]
async fn editing_a_toml_connector_keeps_its_offsets() {
    let env = Env::new();
    let ext = shared::<SinkExt>();
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().join("connectors.d");
    std::fs::create_dir_all(&d).unwrap();
    let toml = |batch: u32| {
        format!(
            "[connector]\nname = \"tomlsink\"\ntype = \"sink\"\nplugin = \"fake\"\n\
             stream = \"s\"\nbatch_size = {batch}\npoll_interval_ms = 10\n"
        )
    };
    std::fs::write(d.join("sink.toml"), toml(10)).unwrap();
    env.publish("s", &["0"; 20]).await;

    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let lease: Arc<dyn exspeed_broker::LeaderLease> =
        Arc::new(exspeed_broker::lease::NoopLeaderLease::new());
    let leadership = Arc::new(
        exspeed_broker::leadership::ClusterLeadership::spawn(lease, env.metrics.clone(), None)
            .await,
    );
    let mgr = Arc::new(
        ConnectorManager::new(
            env.storage.clone(),
            env.log.clone(),
            dir.path().to_path_buf(),
            env.metrics.clone(),
            offsets.clone(),
            leadership,
        )
        .with_registry(sink_registry(&ext)),
    );
    mgr.load_all().await.unwrap();
    let token = tokio_util::sync::CancellationToken::new();
    let runner = {
        let (mgr, token) = (mgr.clone(), token.clone());
        tokio::spawn(async move { mgr.run_all(token).await })
    };
    wait_committed(&offsets, "tomlsink", 20).await;

    // Edit the file and reconcile, as the watcher does on a change event.
    std::fs::write(d.join("sink.toml"), toml(5)).unwrap();
    exspeed_connectors::file_watcher::sync_connectors(&mgr, &d).await;
    assert_eq!(mgr.get_config("tomlsink").await.unwrap().batch_size, 5);
    assert_eq!(
        offsets.load_sink("tomlsink").await.unwrap(),
        Some(20),
        "the edit must not reset the committed offset"
    );

    env.publish("s", &["1"; 5]).await;
    wait_committed(&offsets, "tomlsink", 25).await;
    token.cancel();
    runner.await.unwrap();

    let durable = ext.lock().unwrap().durable.clone();
    assert_eq!(
        durable,
        (0..25).collect::<Vec<u64>>(),
        "every record delivered exactly once: no replay after the edit"
    );
}

#[tokio::test]
async fn connector_names_are_validated_and_collisions_rejected() {
    let env = Env::new();
    let dir = tempfile::tempdir().unwrap();
    let mgr = manager(dir.path(), &env).await;
    let hook = |name: &str| {
        ConnectorConfig::new(name, Source, "http_webhook", "s")
            .with_setting("path", format!("p-{}", name.len()))
            .with_setting("auth_type", "none")
    };
    for bad in ["../evil", "a/b", "", "-x", "a.b"] {
        assert!(
            matches!(mgr.create(hook(bad)).await, Err(ManagerError::Invalid(_))),
            "{bad:?}"
        );
    }
    mgr.create(hook("orders-cdc")).await.unwrap();
    match mgr.create(hook("Orders_CDC")).await {
        Err(ManagerError::Invalid(msg)) => assert!(msg.contains("collides"), "{msg}"),
        other => panic!("expected collision, got {other:?}"),
    }
    assert!(matches!(
        mgr.create(hook("orders-cdc")).await,
        Err(ManagerError::AlreadyExists(_))
    ));
    assert!(catalog(&env).await.contains_key("orders-cdc"));
    assert!(!dir.path().join("connectors").exists(), "no config files");
}

// ---------------------------------------------------------------------------
// The API connector catalog lives in `__connectors`
// ---------------------------------------------------------------------------

async fn catalog(env: &Env) -> std::collections::BTreeMap<String, bytes::Bytes> {
    exspeed_broker::catalog::CatalogStore::new(
        env.log.clone(),
        exspeed_connectors::manager::CONNECTORS_STREAM,
        "connector.config",
    )
    .load()
    .await
    .unwrap()
}

fn webhook(name: &str, path: &str) -> ConnectorConfig {
    ConnectorConfig::new(name, Source, "http_webhook", "s")
        .with_setting("path", path)
        .with_setting("auth_type", "none")
}

#[tokio::test]
async fn api_connectors_are_stored_in_the_log_and_reloaded() {
    let env = Env::new();
    let dir = tempfile::tempdir().unwrap();
    let mgr = manager(dir.path(), &env).await;
    mgr.load_all().await.unwrap();
    mgr.create(webhook("a", "pa")).await.unwrap();
    mgr.create(webhook("b", "pb")).await.unwrap();

    // Another node (or a restart) reading the same log sees both.
    let other = manager(dir.path(), &env).await;
    other.load_all().await.unwrap();
    let names: Vec<String> = other.list().await.into_iter().map(|c| c.name).collect();
    assert_eq!(names, vec!["a", "b"]);
    assert!(other.list().await.iter().all(|c| c.origin == "api"));

    // Delete one, change the other; a reload (next leader tenure) follows.
    mgr.delete("a").await.unwrap();
    mgr.update(webhook("b", "pb2")).await.unwrap();
    other.reload_api_configs().await.unwrap();
    let names: Vec<String> = other.list().await.into_iter().map(|c| c.name).collect();
    assert_eq!(names, vec!["b"]);
    assert_eq!(other.get_config("b").await.unwrap().settings["path"], "pb2");
    assert!(other.find_webhook("pa").await.is_none());
    // Reloading again is a no-op.
    other.reload_api_configs().await.unwrap();
    assert_eq!(other.list().await.len(), 1);
    assert_eq!(catalog(&env).await.len(), 1);
}

#[tokio::test]
async fn legacy_json_configs_are_migrated_into_the_log() {
    let env = Env::new();
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("connectors");
    std::fs::create_dir_all(&legacy).unwrap();
    webhook("old-hook", "old")
        .save_json(&legacy.join("old-hook.json"))
        .unwrap();
    std::fs::write(legacy.join("garbage.json"), "{").unwrap();

    let mgr = manager(dir.path(), &env).await;
    mgr.load_all().await.unwrap();
    let info = mgr.get_status("old-hook").await.expect("migrated");
    assert_eq!(info.origin, "api");
    assert!(!legacy.exists());
    assert!(dir
        .path()
        .join("connectors.migrated/old-hook.json")
        .exists());
    assert!(catalog(&env).await.contains_key("old-hook"));

    // A fresh manager finds it in the log, without the files.
    let other = manager(dir.path(), &env).await;
    other.load_all().await.unwrap();
    assert!(other.get_status("old-hook").await.is_some());
}

#[tokio::test]
async fn file_definition_replaces_the_api_copy() {
    let env = Env::new();
    let dir = tempfile::tempdir().unwrap();
    let mgr = manager(dir.path(), &env).await;
    mgr.create(webhook("x", "px")).await.unwrap();
    let d = dir.path().join("connectors.d");
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(
        d.join("x.toml"),
        "[connector]\nname = \"x\"\ntype = \"source\"\nplugin = \"http_webhook\"\nstream = \"s\"\n\n\
         [settings]\npath = \"from-file\"\nauth_type = \"none\"\n",
    )
    .unwrap();
    let other = manager(dir.path(), &env).await;
    other.load_all().await.unwrap();
    let info = other.get_status("x").await.unwrap();
    assert_eq!(info.origin, "file");
    assert!(
        !catalog(&env).await.contains_key("x"),
        "API copy tombstoned"
    );
    other.reload_api_configs().await.unwrap();
    assert_eq!(other.get_status("x").await.unwrap().origin, "file");
}
