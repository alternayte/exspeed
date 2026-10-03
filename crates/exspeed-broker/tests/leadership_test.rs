//! `ClusterLeadership` against the in-memory lease backend: one leader,
//! failover on resignation and on partition, epochs, ISR-gated election,
//! leader hints and role hooks.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use exspeed_broker::leadership::{ClusterLeadership, LeadershipOptions, RoleHooks};
use exspeed_broker::lease::{LeaderLease, LeaseRecord, MemoryLeaseBackend, CLUSTER_LEASE};
use exspeed_common::Metrics;

fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new().0)
}

fn opts(id: &str) -> LeadershipOptions {
    let mut o = LeadershipOptions::new(id);
    o.ttl = Duration::from_millis(600);
    o.heartbeat = Duration::from_millis(100);
    o.client_endpoint = Some(format!("{id}:5933"));
    o
}

async fn wait_for(cond: impl Fn() -> bool, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Default)]
struct Recorder(Mutex<Vec<String>>);

#[async_trait]
impl RoleHooks for Recorder {
    async fn follow(&self) {
        self.0.lock().push("follow".into());
    }
    async fn promote(&self, lease: &LeaseRecord) -> Result<(), String> {
        self.0.lock().push(format!("promote:{}", lease.epoch));
        Ok(())
    }
    async fn demoted(&self) {
        self.0.lock().push("demoted".into());
    }
}

#[tokio::test]
async fn single_leader_and_failover_on_resign() {
    let lease = MemoryLeaseBackend::new();
    let a = ClusterLeadership::start(lease.clone(), metrics(), opts("a"), None);
    wait_for(|| a.is_currently_leader(), "a leads").await;
    let b = ClusterLeadership::start(lease.clone(), metrics(), opts("b"), None);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!b.is_currently_leader());
    assert_eq!(b.leader_hint().as_deref(), Some("a:5933"));
    assert_eq!(a.leader_hint(), None);
    let ea = a.epoch();
    a.resign().await;
    assert!(!a.is_currently_leader());
    wait_for(|| b.is_currently_leader(), "b takes over").await;
    assert!(b.epoch() > ea);
}

#[tokio::test]
async fn partitioned_leader_steps_down_and_peer_takes_over() {
    let lease = MemoryLeaseBackend::new();
    let a = ClusterLeadership::start(lease.clone(), metrics(), opts("a"), None);
    wait_for(|| a.is_currently_leader(), "a leads").await;
    let b = ClusterLeadership::start(lease.clone(), metrics(), opts("b"), None);
    let token = a.current_child_token().await;
    lease.set_partitioned("a", true);
    wait_for(|| !a.is_currently_leader(), "a steps down").await;
    assert!(token.is_cancelled(), "leader work is cancelled on loss");
    // a stepped down before b could take over: never two leaders.
    wait_for(|| b.is_currently_leader(), "b takes over").await;
    assert!(!a.is_currently_leader());
    lease.set_partitioned("a", false);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !a.is_currently_leader(),
        "a stays a follower while b holds the lease"
    );
}

#[tokio::test]
async fn isr_gates_election() {
    let lease = MemoryLeaseBackend::new();
    let a = ClusterLeadership::start(lease.clone(), metrics(), opts("a"), None);
    wait_for(|| a.is_currently_leader(), "a leads").await;
    assert!(lease
        .set_isr(CLUSTER_LEASE, "a", a.epoch(), &["a".into(), "c".into()])
        .await
        .unwrap());
    let b = ClusterLeadership::start(lease.clone(), metrics(), opts("b"), None);
    let c = ClusterLeadership::start(lease.clone(), metrics(), opts("c"), None);
    a.resign().await;
    wait_for(|| c.is_currently_leader(), "c (in the ISR) takes over").await;
    assert!(!b.is_currently_leader());
}

#[tokio::test]
async fn hooks_run_in_order() {
    let lease = MemoryLeaseBackend::new();
    let rec = Arc::new(Recorder::default());
    let a = ClusterLeadership::start(
        lease.clone(),
        metrics(),
        opts("a"),
        Some(rec.clone() as Arc<dyn RoleHooks>),
    );
    wait_for(|| a.is_currently_leader(), "a leads").await;
    lease.set_partitioned("a", true);
    wait_for(|| !a.is_currently_leader(), "a steps down").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let log = rec.0.lock().clone();
    assert_eq!(log[0], "follow");
    assert!(log[1].starts_with("promote:"), "{log:?}");
    assert_eq!(&log[2..4], &["demoted".to_string(), "follow".to_string()]);
}

#[tokio::test]
async fn noop_lease_leads_immediately() {
    let lease: Arc<dyn LeaderLease> = Arc::new(exspeed_broker::lease::NoopLeaderLease::new());
    let l = ClusterLeadership::spawn(lease, metrics(), None).await;
    wait_for(|| l.is_currently_leader(), "noop leads").await;
    assert_eq!(l.epoch(), 0);
    assert!(!l.current_child_token().await.is_cancelled());
}
