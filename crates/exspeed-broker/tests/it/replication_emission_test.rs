//! Wave 2 integration tests for the Broker emission hooks.
//!
//! Each test builds a minimal Broker wired to a `ReplicationCoordinator`,
//! registers a follower mpsc, exercises one mutation path, and asserts the
//! follower receives exactly the event(s) the leader persisted. These
//! tests are the guard-rail for the regression the Wave 2 review caught:
//! that the emit sites stay in lock-step with the storage writes.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use exspeed_broker::broker_append::AppendResult;
use exspeed_broker::replication::{ReplicationCoordinator, ReplicationEvent};
use exspeed_broker::{broker::Broker, broker_append::BrokerAppend};
use exspeed_common::{Metrics, StreamName};
use exspeed_storage::memory::MemoryStorage;
use exspeed_streams::{Record, StreamConfig};
use tempfile::TempDir;
use tokio::sync::mpsc;

fn make_broker(coord: Arc<ReplicationCoordinator>) -> (Broker, TempDir) {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(MemoryStorage::new());
    let broker_append = Arc::new(BrokerAppend::new(storage.clone(), 300));
    let lease = Arc::new(exspeed_broker::lease::NoopLeaderLease::new());
    let metrics = Arc::new(Metrics::new().0);
    let broker = Broker::new(
        storage,
        broker_append,
        dir.path().to_path_buf(),
        lease,
        metrics,
    )
    .with_replication_coordinator(coord);
    broker
        .dedup_ready
        .store(true, std::sync::atomic::Ordering::Release);
    (broker, dir)
}

async fn create(broker: &Broker, name: &str, max_age_secs: u64, max_bytes: u64) {
    let cfg = StreamConfig::from_request(max_age_secs, max_bytes, 0, 0);
    broker
        .log
        .create_stream(&StreamName::try_from(name).unwrap(), &cfg)
        .await
        .unwrap();
}

async fn publish(broker: &Broker, stream: &str, msg_id: Option<&str>) -> AppendResult {
    let mut headers = vec![];
    if let Some(id) = msg_id {
        headers.push(("x-idempotency-key".to_string(), id.to_string()));
    }
    broker
        .log
        .append(
            &StreamName::try_from(stream).unwrap(),
            Record {
                key: None,
                value: Bytes::from_static(b"payload"),
                subject: "orders.created".into(),
                headers,
                timestamp_ns: None,
            },
        )
        .await
        .unwrap()
}

fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new().0)
}

/// Drain the follower mpsc with a short timeout. Returns whatever events
/// arrived within the budget.
async fn drain(rx: &mut mpsc::Receiver<ReplicationEvent>) -> Vec<ReplicationEvent> {
    let mut out = Vec::new();
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
        out.push(ev);
    }
    out
}

#[tokio::test]
async fn publish_emits_records_appended() {
    let coord = ReplicationCoordinator::new(metrics(), 100);
    let (_id, mut rx) = coord.register_follower();
    let (broker, _dir) = make_broker(coord.clone());

    create(&broker, "orders", 0, 0).await;

    // Drain the StreamCreated event emitted by create_stream; the test
    // below has a dedicated case for that.
    let _ = drain(&mut rx).await;

    match publish(&broker, "orders", None).await {
        AppendResult::Written(offset, _) => assert_eq!(offset.0, 0),
        other => panic!("unexpected publish response: {other:?}"),
    }

    let events = drain(&mut rx).await;
    assert_eq!(events.len(), 1, "expected exactly one RecordsAppended");
    match &events[0] {
        ReplicationEvent::RecordsAppended(r) => {
            assert_eq!(r.stream, "orders");
            assert_eq!(r.base_offset, 0);
            assert_eq!(r.records.len(), 1);
            assert_eq!(r.records[0].payload, b"payload");
        }
        other => panic!("expected RecordsAppended, got {other:?}"),
    }
}

#[tokio::test]
async fn create_stream_emits_stream_created() {
    let coord = ReplicationCoordinator::new(metrics(), 100);
    let (_id, mut rx) = coord.register_follower();
    let (broker, _dir) = make_broker(coord.clone());

    create(&broker, "orders", 3600, 1_000_000).await;

    let events = drain(&mut rx).await;
    assert_eq!(events.len(), 1);
    match &events[0] {
        ReplicationEvent::StreamCreated(s) => {
            assert_eq!(s.name, "orders");
            assert_eq!(s.max_age_secs, 3600);
            assert_eq!(s.max_bytes, 1_000_000);
        }
        other => panic!("expected StreamCreated, got {other:?}"),
    }
}

#[tokio::test]
async fn delete_stream_emits_stream_deleted() {
    let coord = ReplicationCoordinator::new(metrics(), 100);
    let (_id, mut rx) = coord.register_follower();
    let (broker, _dir) = make_broker(coord.clone());

    create(&broker, "orders", 0, 0).await;
    // drain the StreamCreated event.
    let _ = drain(&mut rx).await;

    let stream_name = StreamName::try_from("orders").unwrap();
    broker.delete_stream(&stream_name).await.unwrap();

    let events = drain(&mut rx).await;
    assert_eq!(events.len(), 1);
    match &events[0] {
        ReplicationEvent::StreamDeleted(s) => assert_eq!(s.name, "orders"),
        other => panic!("expected StreamDeleted, got {other:?}"),
    }
}

#[tokio::test]
async fn duplicate_publish_does_not_emit() {
    // AppendResult::Duplicate means the record was already persisted on
    // an earlier publish (which was itself replicated at that time).
    // A duplicate must NOT re-emit — otherwise the follower would apply
    // the same record twice.
    let coord = ReplicationCoordinator::new(metrics(), 100);
    let (_id, mut rx) = coord.register_follower();
    let (broker, _dir) = make_broker(coord.clone());

    create(&broker, "orders", 0, 0).await;
    let _ = drain(&mut rx).await;

    // First publish with msg_id → Written + emits.
    assert!(matches!(
        publish(&broker, "orders", Some("unique-id")).await,
        AppendResult::Written(..)
    ));

    let first = drain(&mut rx).await;
    assert_eq!(first.len(), 1, "first publish should emit one event");

    // Second publish with same msg_id + body → Duplicate, must NOT emit.
    assert!(matches!(
        publish(&broker, "orders", Some("unique-id")).await,
        AppendResult::Duplicate(..)
    ));

    let second = drain(&mut rx).await;
    assert!(
        second.is_empty(),
        "duplicate publish must not emit, got {second:?}"
    );
}
