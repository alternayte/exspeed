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
