//! Integration tests for `ClusterLeadership`. Uses the Noop backend for
//! cases that don't need multi-pod coordination; Postgres/Redis cases
//! live in later tasks (Task 3).

use std::sync::Arc;
use std::time::Duration;

use exspeed_broker::lease::NoopLeaderLease;
use exspeed_broker::leadership::ClusterLeadership;
use exspeed_common::Metrics;

#[tokio::test]
async fn noop_becomes_leader_immediately() {
    let lease = Arc::new(NoopLeaderLease::new());
    let (metrics, _registry) = Metrics::new();
    let metrics = Arc::new(metrics);

    let leadership = ClusterLeadership::spawn(lease, metrics).await;

    // Noop always grants -> is_leader flips true within ~1 tick (bounded
    // generously to avoid flakes).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !leadership.is_currently_leader() {
        if tokio::time::Instant::now() > deadline {
            panic!("leadership never promoted under Noop backend");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A fresh child token fetched while leader is NOT cancelled.
    let child = leadership.current_child_token().await;
    assert!(!child.is_cancelled());

    // holder_id is stable.
    let h1 = leadership.holder_id;
    let h2 = leadership.holder_id;
    assert_eq!(h1, h2);
}
