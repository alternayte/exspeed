//! Lease conformance against a real Postgres. Set `EXSPEED_LEASE_POSTGRES_URL`
//! (or `EXSPEED_OFFSET_STORE_POSTGRES_URL`); skipped when unset.

use std::sync::Arc;
use std::time::Duration;

use exspeed_broker::lease::postgres::PostgresLeaseBackend;
use exspeed_broker::lease::{conformance, LeaderLease};

fn url() -> Option<String> {
    std::env::var("EXSPEED_LEASE_POSTGRES_URL")
        .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL"))
        .ok()
}

async fn backend(schema: &str) -> Arc<dyn LeaderLease> {
    Arc::new(
        PostgresLeaseBackend::connect(&url().unwrap(), schema, Duration::from_secs(5))
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn postgres_lease_conformance() {
    if url().is_none() {
        eprintln!("skipping: EXSPEED_LEASE_POSTGRES_URL is not set");
        return;
    }
    let schema = format!("lease_test_{}", uuid::Uuid::new_v4().simple());
    conformance::run(backend(&schema).await).await;
}

#[tokio::test]
async fn postgres_lease_survives_reconnect_and_rejects_bad_schema() {
    let Some(u) = url() else { return };
    assert!(
        PostgresLeaseBackend::connect(&u, "bad;schema", Duration::from_secs(1))
            .await
            .is_err()
    );
    // Two backends (two nodes) on one schema see each other's leases.
    let schema = format!("lease_test_{}", uuid::Uuid::new_v4().simple());
    let a = backend(&schema).await;
    let b = backend(&schema).await;
    let req = exspeed_broker::lease::AcquireRequest::new("x", "a", Duration::from_secs(30));
    let rec = a.try_acquire(&req).await.unwrap().unwrap();
    let seen = b.get("x").await.unwrap().unwrap();
    assert_eq!(seen.holder, "a");
    assert_eq!(seen.epoch, rec.epoch);
}
