//! Lease conformance against a real Redis. Set `EXSPEED_LEASE_REDIS_URL`
//! (or `EXSPEED_OFFSET_STORE_REDIS_URL`); skipped when unset.

use std::sync::Arc;
use std::time::Duration;

use exspeed_broker::lease::redis::RedisLeaseBackend;
use exspeed_broker::lease::{conformance, AcquireRequest, LeaderLease};

fn url() -> Option<String> {
    std::env::var("EXSPEED_LEASE_REDIS_URL")
        .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE_REDIS_URL"))
        .ok()
}

async fn backend(prefix: &str) -> Arc<dyn LeaderLease> {
    Arc::new(
        RedisLeaseBackend::connect(&url().unwrap(), prefix, Duration::from_secs(5))
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn redis_lease_conformance() {
    if url().is_none() {
        eprintln!("skipping: EXSPEED_LEASE_REDIS_URL is not set");
        return;
    }
    let prefix = format!("lease_test:{}:", uuid::Uuid::new_v4().simple());
    conformance::run(backend(&prefix).await).await;
}

#[tokio::test]
async fn redis_two_nodes_share_leases() {
    if url().is_none() {
        return;
    }
    let prefix = format!("lease_test:{}:", uuid::Uuid::new_v4().simple());
    let a = backend(&prefix).await;
    let b = backend(&prefix).await;
    let rec = a
        .try_acquire(&AcquireRequest::new("x", "a", Duration::from_secs(30)))
        .await
        .unwrap()
        .unwrap();
    assert!(b
        .try_acquire(&AcquireRequest::new("x", "b", Duration::from_secs(30)))
        .await
        .unwrap()
        .is_none());
    let listed = b.list_all().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].epoch, rec.epoch);
}
