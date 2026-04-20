//! Integration tests for `ClusterLeadership`. Uses the Noop backend for
//! cases that don't need multi-pod coordination; Postgres/Redis integration
//! tests require a running backend (skipped gracefully when unavailable).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use exspeed_broker::lease::postgres::PostgresLeaseBackend;
use exspeed_broker::lease::redis::RedisLeaseBackend;
use exspeed_broker::lease::LeaderLease;
use exspeed_broker::lease::NoopLeaderLease;
use exspeed_broker::lease::{LeaseError, LeaseGuard, LeaseInfo};
use exspeed_broker::leadership::ClusterLeadership;
use exspeed_common::Metrics;

/// In-memory fake backend for exercising the endpoint round-trip. Unlike
/// `NoopLeaderLease` (which is stateless and returns `[]` from `list_all`),
/// this records every successful acquire so `leader_replication_endpoint`
/// has something to read back. Single-holder semantics only — good enough
/// for the single-`spawn` tests that need it.
#[derive(Default)]
struct RecordingLease {
    rows: parking_lot::Mutex<Vec<LeaseInfo>>,
}

#[async_trait]
impl LeaderLease for RecordingLease {
    fn supports_coordination(&self) -> bool {
        true
    }

    async fn try_acquire(
        &self,
        name: &str,
        ttl: Duration,
        replication_endpoint: Option<&str>,
    ) -> Result<Option<LeaseGuard>, LeaseError> {
        let (cancel_tx, _cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let (lost_tx, lost_rx) = tokio::sync::watch::channel(false);
        let holder_id = uuid::Uuid::new_v4();
        {
            let mut rows = self.rows.lock();
            rows.retain(|r| r.name != name);
            rows.push(LeaseInfo {
                name: name.to_string(),
                holder: holder_id,
                expires_at: chrono::Utc::now() + chrono::Duration::from_std(ttl).unwrap(),
                replication_endpoint: replication_endpoint.map(|s| s.to_string()),
            });
        }
        Ok(Some(LeaseGuard {
            name: name.to_string(),
            holder_id,
            on_lost: lost_rx,
            _lost_tx: Some(lost_tx),
            _cancel_heartbeat: cancel_tx,
        }))
    }

    async fn list_all(&self) -> Result<Vec<LeaseInfo>, LeaseError> {
        Ok(self.rows.lock().clone())
    }
}

fn skip_if_no_postgres() -> bool {
    std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").is_err()
}

fn skip_if_no_redis() -> bool {
    std::env::var("EXSPEED_OFFSET_STORE_REDIS_URL").is_err()
}

/// Serialize backend construction across tests. `from_env()` reads env vars
/// that are per-process global mutable state. Without serialization, two
/// parallel tests can clobber each other's schema/prefix env-var.
static SETUP_LOCK: Mutex<()> = Mutex::new(());

/// Build a Postgres backend with an isolated schema. Returns `(backend, schema)`
/// so callers can construct additional backends pointing at the same schema.
async fn make_pg_backend_isolated() -> (Arc<dyn LeaderLease>, String) {
    let schema = format!("leadership_test_{}", uuid::Uuid::new_v4().simple());
    let b = {
        let _guard = SETUP_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        std::env::set_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA", &schema);
        let b = PostgresLeaseBackend::from_env().await.unwrap();
        std::env::remove_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA");
        b
    };
    (Arc::new(b), schema)
}

/// Build a second Postgres backend pointing at an existing schema.
async fn make_pg_backend_for_schema(schema: &str) -> Arc<dyn LeaderLease> {
    let _guard = SETUP_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA", schema);
    let b = PostgresLeaseBackend::from_env().await.unwrap();
    std::env::remove_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA");
    Arc::new(b)
}

/// Build a Redis backend with an isolated key prefix.
async fn make_redis_backend_with_prefix(prefix: &str) -> Arc<dyn LeaderLease> {
    let _guard = SETUP_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var("EXSPEED_LEASE_REDIS_KEY_PREFIX", prefix);
    let b = RedisLeaseBackend::from_env().await.unwrap();
    std::env::remove_var("EXSPEED_LEASE_REDIS_KEY_PREFIX");
    Arc::new(b)
}

#[tokio::test]
async fn noop_becomes_leader_immediately() {
    let lease = Arc::new(NoopLeaderLease::new());
    let (metrics, _registry) = Metrics::new();
    let metrics = Arc::new(metrics);

    let leadership = ClusterLeadership::spawn(lease, metrics, None).await;

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

// ---------------------------------------------------------------------------
// Postgres integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn postgres_two_leaderships_race_exactly_one_wins() {
    if skip_if_no_postgres() {
        eprintln!("EXSPEED_OFFSET_STORE_POSTGRES_URL unset; skipping");
        return;
    }

    let (a_lease, schema) = make_pg_backend_isolated().await;
    let b_lease = make_pg_backend_for_schema(&schema).await;

    let (metrics, _r) = Metrics::new();
    let metrics = Arc::new(metrics);

    let a = ClusterLeadership::spawn(a_lease, metrics.clone(), None).await;
    let b = ClusterLeadership::spawn(b_lease, metrics.clone(), None).await;

    // Give both retry loops a full tick. Default TTL=30s, tick=TTL/3=10s;
    // 12s is enough for at least one full acquire attempt from each.
    tokio::time::sleep(Duration::from_secs(12)).await;

    let a_leader = a.is_currently_leader();
    let b_leader = b.is_currently_leader();
    assert!(
        a_leader ^ b_leader,
        "exactly one should be leader; got a={a_leader} b={b_leader}"
    );
}

#[tokio::test]
async fn postgres_heartbeat_loss_cancels_leader_token() {
    if skip_if_no_postgres() {
        eprintln!("EXSPEED_OFFSET_STORE_POSTGRES_URL unset; skipping");
        return;
    }

    let (lease, schema) = make_pg_backend_isolated().await;

    let (metrics, _r) = Metrics::new();
    let metrics = Arc::new(metrics);

    let leadership = ClusterLeadership::spawn(lease, metrics, None).await;

    // Wait to become leader (the schema was pre-created by make_pg_backend_isolated
    // via from_env/ensure_schema, so the table already exists).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    while !leadership.is_currently_leader() {
        if tokio::time::Instant::now() > deadline {
            panic!("never became leader");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let initial_token = leadership.current_child_token().await;
    assert!(!initial_token.is_cancelled(), "token should be live while leader");

    // Steal the lease row: overwrite the holder UUID so the current holder's
    // next heartbeat UPDATE (which conditions on its own UUID) returns zero
    // rows, triggering on_lost → demote() → token cancellation.
    let url = std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").unwrap();
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("steal: connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .execute(&format!("SET search_path = {schema}"), &[])
        .await
        .expect("steal: set search_path");
    client
        .execute(
            "UPDATE exspeed_leases \
             SET holder = gen_random_uuid(), expires_at = now() + interval '60 seconds' \
             WHERE name = $1",
            &[&"cluster:leader"],
        )
        .await
        .expect("steal: update");

    // Within one heartbeat cycle (~10s default), the guard's heartbeat UPDATE
    // returns zero rows. After 2 consecutive misses, on_lost fires, demote
    // runs, and the initial child token is cancelled. Allow 30s (3× interval).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !initial_token.is_cancelled() {
        if tokio::time::Instant::now() > deadline {
            panic!("leader_token never cancelled after lease theft");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(!leadership.is_currently_leader());
}

// ---------------------------------------------------------------------------
// Redis integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn redis_two_leaderships_race_exactly_one_wins() {
    if skip_if_no_redis() {
        eprintln!("EXSPEED_OFFSET_STORE_REDIS_URL unset; skipping");
        return;
    }

    // Both backends must share the SAME key prefix to race on the same lease.
    let prefix = format!("leadership-test-{}-", uuid::Uuid::new_v4().simple());
    let a_lease = make_redis_backend_with_prefix(&prefix).await;
    let b_lease = make_redis_backend_with_prefix(&prefix).await;

    let (metrics, _r) = Metrics::new();
    let metrics = Arc::new(metrics);

    let a = ClusterLeadership::spawn(a_lease, metrics.clone(), None).await;
    let b = ClusterLeadership::spawn(b_lease, metrics.clone(), None).await;

    tokio::time::sleep(Duration::from_secs(12)).await;

    let a_leader = a.is_currently_leader();
    let b_leader = b.is_currently_leader();
    assert!(
        a_leader ^ b_leader,
        "exactly one should be leader; got a={a_leader} b={b_leader}"
    );
}

// ---------------------------------------------------------------------------
// replication_endpoint advertisement
// ---------------------------------------------------------------------------

/// `spawn` with `Some("…")` writes the endpoint into the lease row on acquire,
/// and `leader_replication_endpoint` reads it back. Uses `RecordingLease`
/// rather than Noop because Noop's `list_all` is intentionally empty.
#[tokio::test]
async fn spawn_with_endpoint_advertises_via_list_all() {
    let lease: Arc<dyn LeaderLease> = Arc::new(RecordingLease::default());
    let (metrics, _r) = Metrics::new();
    let metrics = Arc::new(metrics);
    let endpoint = "10.0.0.42:5934";

    let leadership =
        ClusterLeadership::spawn(lease, metrics, Some(endpoint.to_string())).await;

    // Wait for the retry loop to acquire. Default TTL=30s tick=TTL/3≈10s;
    // the first tick fires immediately, so ~50ms is plenty on RecordingLease.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !leadership.is_currently_leader() {
        if tokio::time::Instant::now() > deadline {
            panic!("leadership never acquired on RecordingLease");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let got = leadership
        .leader_replication_endpoint()
        .await
        .expect("endpoint should be advertised");
    assert_eq!(got, endpoint);
}

#[tokio::test]
async fn spawn_without_endpoint_reports_none() {
    let lease: Arc<dyn LeaderLease> = Arc::new(RecordingLease::default());
    let (metrics, _r) = Metrics::new();
    let metrics = Arc::new(metrics);

    let leadership = ClusterLeadership::spawn(lease, metrics, None).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !leadership.is_currently_leader() {
        if tokio::time::Instant::now() > deadline {
            panic!("leadership never acquired on RecordingLease");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        leadership.leader_replication_endpoint().await.is_none(),
        "no endpoint was advertised"
    );
}
