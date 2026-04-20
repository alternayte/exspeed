#![cfg(test)]
//! Integration tests for RedisLeaseBackend. Requires a running Redis;
//! point `EXSPEED_OFFSET_STORE_REDIS_URL` at it.
//!
//! Tests skip gracefully when the URL is unset.

use std::sync::Mutex;
use std::time::Duration;

use exspeed_broker::lease::redis::RedisLeaseBackend;
use exspeed_broker::lease::LeaderLease;

fn skip_if_no_redis() -> bool {
    std::env::var("EXSPEED_OFFSET_STORE_REDIS_URL").is_err()
}

/// Unique key prefix per test run to avoid cross-test pollution in a shared
/// Redis. We don't use Redis DB numbers to stay compatible with Redis
/// Cluster if operators switch to that later.
fn unique_prefix() -> String {
    format!("test-{}-", uuid::Uuid::new_v4().simple())
}

/// Serialize backend construction across tests. `from_env()` reads
/// `EXSPEED_LEASE_REDIS_KEY_PREFIX` from the process environment, which is
/// shared mutable state — without serialization, two parallel tests can
/// clobber each other's prefix env-var between set_var and from_env.
static SETUP_LOCK: Mutex<()> = Mutex::new(());

async fn make_backend() -> RedisLeaseBackend {
    let prefix = unique_prefix();
    let _guard = SETUP_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    std::env::set_var("EXSPEED_LEASE_REDIS_KEY_PREFIX", &prefix);
    let b = RedisLeaseBackend::from_env().await.unwrap();
    std::env::remove_var("EXSPEED_LEASE_REDIS_KEY_PREFIX");
    b
}

#[tokio::test]
async fn redis_first_acquire_wins_second_rejects() {
    if skip_if_no_redis() {
        return;
    }
    let b = make_backend().await;

    let g1 = b
        .try_acquire("race-1", Duration::from_secs(30), None)
        .await
        .unwrap();
    assert!(g1.is_some(), "first acquire should win");

    let g2 = b
        .try_acquire("race-1", Duration::from_secs(30), None)
        .await
        .unwrap();
    assert!(g2.is_none(), "second acquire should reject while g1 holds");
}

#[tokio::test]
async fn redis_release_on_drop_enables_reacquire() {
    if skip_if_no_redis() {
        return;
    }
    let b = make_backend().await;

    let g1 = b
        .try_acquire("drop-1", Duration::from_secs(30), None)
        .await
        .unwrap();
    drop(g1);

    // Give the spawned drop-handler a moment to run the async release.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let g2 = b
        .try_acquire("drop-1", Duration::from_secs(30), None)
        .await
        .unwrap();
    assert!(g2.is_some(), "reacquire should succeed after release");
}

#[tokio::test]
async fn redis_expired_lease_can_be_stolen() {
    if skip_if_no_redis() {
        return;
    }
    let b = make_backend().await;

    // Acquire with a tiny TTL so PEXPIRE elapses before the heartbeat
    // (default 10s) gets a chance to refresh.
    let _g1 = b
        .try_acquire("expire-1", Duration::from_secs(1), None)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let stolen = b
        .try_acquire("expire-1", Duration::from_secs(30), None)
        .await
        .unwrap();
    assert!(stolen.is_some(), "expired lease should be stealable");
}

#[tokio::test]
async fn redis_endpoint_is_stored_and_preserved_across_heartbeats() {
    if skip_if_no_redis() {
        return;
    }
    let b = make_backend().await;

    let _g = b
        .try_acquire(
            "endpoint-1",
            Duration::from_secs(5),
            Some("10.0.0.2:5934"),
        )
        .await
        .unwrap()
        .expect("first acquire wins");

    let listed = b.list_all().await.unwrap();
    let row = listed
        .iter()
        .find(|i| i.name == "endpoint-1")
        .expect("row present");
    assert_eq!(row.replication_endpoint.as_deref(), Some("10.0.0.2:5934"));

    // Brief sleep to let the heartbeat task exist alongside; value is
    // byte-stable (heartbeat refreshes TTL only via Lua, never rewrites
    // the JSON blob), so this is a stability check on reads, not a
    // timing-sensitive race against the heartbeat itself.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let listed = b.list_all().await.unwrap();
    let row = listed
        .iter()
        .find(|i| i.name == "endpoint-1")
        .expect("row still present");
    assert_eq!(
        row.replication_endpoint.as_deref(),
        Some("10.0.0.2:5934"),
        "heartbeat refresh must preserve replication_endpoint"
    );
}

#[tokio::test]
async fn redis_list_reports_active_holders() {
    if skip_if_no_redis() {
        return;
    }
    let b = make_backend().await;

    let _g = b
        .try_acquire("listed", Duration::from_secs(30), None)
        .await
        .unwrap();

    let all = b.list_all().await.unwrap();
    assert!(
        all.iter().any(|i| i.name == "listed"),
        "should list active lease, got {:?}",
        all
    );
}
