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

/// Resign releases the lease (expires it in the backend) rather than leave
/// peers waiting out the TTL.
#[tokio::test]
async fn resign_releases_lease_before_ttl() {
    let lease = MemoryLeaseBackend::new();
    let mut o = opts("a");
    o.ttl = Duration::from_secs(60);
    let a = ClusterLeadership::start(lease.clone(), metrics(), o, None);
    wait_for(|| a.is_currently_leader(), "a leads").await;
    a.resign().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let rec = lease.get(CLUSTER_LEASE).await.unwrap().unwrap();
        if !rec.is_live() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "lease still live after resign"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut ob = opts("b");
    ob.ttl = Duration::from_secs(60);
    let b = ClusterLeadership::start(lease.clone(), metrics(), ob, None);
    wait_for(|| b.is_currently_leader(), "b takes over before the TTL").await;
}

#[tokio::test]
async fn step_down_releases_the_lease_and_holds_off() {
    let lease = MemoryLeaseBackend::new();
    let rec = Arc::new(Recorder::default());
    // a's lease TTL is long: b can only take over within the test's wait if
    // a released the lease instead of letting it expire.
    let mut oa = opts("a");
    oa.ttl = Duration::from_secs(60);
    let a = ClusterLeadership::start(
        lease.clone(),
        metrics(),
        oa,
        Some(rec.clone() as Arc<dyn RoleHooks>),
    );
    wait_for(|| a.is_currently_leader(), "a leads").await;
    let b = ClusterLeadership::start(lease.clone(), metrics(), opts("b"), None);
    let token = a.current_child_token().await;
    let ea = a.epoch();

    a.step_down(Duration::from_secs(2)).await;
    assert!(!a.is_currently_leader(), "writes close at once");
    assert!(token.is_cancelled(), "leader work is cancelled");
    assert_eq!(a.epoch(), 0);
    wait_for(|| b.is_currently_leader(), "b takes over").await;
    assert!(b.epoch() > ea);
    let events = rec.0.lock().clone();
    assert!(
        events.ends_with(&["demoted".to_string(), "follow".to_string()]),
        "a goes back to following: {events:?}"
    );

    // Once b goes away, a competes again (after its hold-off).
    b.resign().await;
    wait_for(|| a.is_currently_leader(), "a leads again").await;
}

#[tokio::test]
async fn single_node_step_down_retries_after_the_hold_off() {
    let lease = MemoryLeaseBackend::new();
    let a = ClusterLeadership::start(lease.clone(), metrics(), opts("a"), None);
    wait_for(|| a.is_currently_leader(), "a leads").await;
    let start = tokio::time::Instant::now();
    a.step_down(Duration::from_millis(500)).await;
    assert!(!a.is_currently_leader());
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!a.is_currently_leader(), "holds off before competing again");
    wait_for(|| a.is_currently_leader(), "a re-acquires").await;
    assert!(start.elapsed() >= Duration::from_millis(500));
    // A no-op when not leading.
    a.resign().await;
    a.step_down(Duration::from_secs(1)).await;
    assert!(!a.is_currently_leader());
}
