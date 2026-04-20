use std::sync::Arc;
use std::time::Duration;

use exspeed_broker::replication::{ReplicationCoordinator, ReplicationEvent};
use exspeed_common::Metrics;
use exspeed_protocol::messages::replicate::StreamCreatedEvent;
use tokio::time::timeout;

fn metrics() -> Arc<Metrics> {
    let (m, _reg) = Metrics::new();
    Arc::new(m)
}

fn sample_event() -> ReplicationEvent {
    ReplicationEvent::StreamCreated(StreamCreatedEvent {
        name: "orders".into(),
        max_age_secs: 3600,
        max_bytes: 0,
    })
}

#[tokio::test]
async fn register_follower_receives_broadcast() {
    let coord = ReplicationCoordinator::new(metrics(), 100);
    let (id, mut rx) = coord.register_follower();
    coord.emit(sample_event());

    let ev = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("receive within 1s")
        .expect("not closed");
    matches!(ev, ReplicationEvent::StreamCreated(_));
    assert!(coord.is_registered(id));
}

#[tokio::test]
async fn deregister_follower_stops_receiving() {
    let coord = ReplicationCoordinator::new(metrics(), 100);
    let (id, mut rx) = coord.register_follower();
    coord.deregister_follower(id);
    coord.emit(sample_event());
    // Channel has been closed — recv returns None.
    assert!(rx.recv().await.is_none());
    assert!(!coord.is_registered(id));
}

#[tokio::test]
async fn slow_follower_queue_fills_and_is_dropped() {
    let coord = ReplicationCoordinator::new(metrics(), 2 /* tiny queue cap */);
    let (id, _rx) = coord.register_follower();
    assert_eq!(coord.connected_followers(), 1);

    // Emit more events than the queue can hold. The slow follower (never
    // draining) hits the cap; coordinator drops it.
    for _ in 0..10 {
        coord.emit(sample_event());
    }
    // Allow the drop task a moment to run (fan-out is async).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!coord.is_registered(id), "slow follower should be dropped");
    // The gauge is a `.record(len)` on every emit that drops. We assert
    // the map-backed count reflects the drop — the gauge itself is
    // painful to read from a unit test. TODO: once we have a
    // Metrics registry test helper, assert
    // `exspeed_replication_follower_queue_drops_total` also incremented.
    assert_eq!(coord.connected_followers(), 0);
}

#[tokio::test]
async fn multiple_followers_each_get_every_event() {
    let coord = ReplicationCoordinator::new(metrics(), 100);
    let (_, mut rx_a) = coord.register_follower();
    let (_, mut rx_b) = coord.register_follower();
    coord.emit(sample_event());
    coord.emit(sample_event());

    for _ in 0..2 {
        let _ = timeout(Duration::from_secs(1), rx_a.recv())
            .await
            .unwrap()
            .unwrap();
        let _ = timeout(Duration::from_secs(1), rx_b.recv())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn connected_followers_count_reflects_registrations() {
    let coord = ReplicationCoordinator::new(metrics(), 100);
    assert_eq!(coord.connected_followers(), 0);
    let (id1, _rx1) = coord.register_follower();
    let (_id2, _rx2) = coord.register_follower();
    assert_eq!(coord.connected_followers(), 2);
    coord.deregister_follower(id1);
    assert_eq!(coord.connected_followers(), 1);
}

#[tokio::test]
async fn snapshot_returns_registered_followers_with_timestamps() {
    let coord = ReplicationCoordinator::new(metrics(), 100);
    assert!(
        coord.snapshot().is_empty(),
        "snapshot of empty coordinator is empty"
    );

    let before = chrono::Utc::now();
    let (id1, _rx1) = coord.register_follower();
    let (id2, _rx2) = coord.register_follower();
    let after = chrono::Utc::now();

    let mut snap = coord.snapshot();
    assert_eq!(snap.len(), 2, "both followers should appear in snapshot");

    // Sort by id so assertions don't depend on HashMap iteration order.
    snap.sort_by_key(|s| s.follower_id);
    let mut expected = vec![id1, id2];
    expected.sort();

    let ids: Vec<_> = snap.iter().map(|s| s.follower_id).collect();
    assert_eq!(ids, expected, "snapshot ids should match registered ids");

    // Registration times fall inside the wall-clock window we framed
    // around the `register_follower` calls. Chrono is monotonic enough
    // on every platform we care about; allow ±1ms slack if we ever hit
    // a clock that isn't.
    for s in &snap {
        assert!(
            s.registered_at >= before - chrono::Duration::milliseconds(1),
            "registered_at {:?} < before {:?}",
            s.registered_at,
            before
        );
        assert!(
            s.registered_at <= after + chrono::Duration::milliseconds(1),
            "registered_at {:?} > after {:?}",
            s.registered_at,
            after
        );
    }

    // Deregister one → snapshot shrinks; remaining follower keeps its
    // original registered_at.
    let remaining_before = snap
        .iter()
        .find(|s| s.follower_id == id2)
        .expect("id2 in snapshot")
        .registered_at;
    coord.deregister_follower(id1);
    let snap2 = coord.snapshot();
    assert_eq!(snap2.len(), 1);
    assert_eq!(snap2[0].follower_id, id2);
    assert_eq!(
        snap2[0].registered_at, remaining_before,
        "registered_at is stable across other followers' deregistration"
    );
}
