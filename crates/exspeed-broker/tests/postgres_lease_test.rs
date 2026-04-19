#![cfg(test)]
//! Integration tests for PostgresLeaseBackend. Requires a running Postgres;
//! point `EXSPEED_OFFSET_STORE_POSTGRES_URL` at it (docker-compose up -d postgres).
//! Tests skip gracefully when the URL is unset.

use std::sync::Mutex;
use std::time::Duration;

use exspeed_broker::lease::postgres::PostgresLeaseBackend;
use exspeed_broker::lease::LeaderLease;

fn skip_if_no_postgres() -> bool {
    std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").is_err()
}

/// Serialize backend construction across tests. `from_env()` reads
/// `EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA` from the process environment,
/// which is shared mutable state — without serialization, two parallel
/// tests can clobber each other's schema env-var, and concurrent
/// `CREATE SCHEMA IF NOT EXISTS` calls can race in Postgres itself
/// (a known PG quirk on `pg_namespace_nspname_index`).
static SETUP_LOCK: Mutex<()> = Mutex::new(());

/// Build a backend with an isolated schema. Returns `(backend, schema)` so
/// callers can construct additional backends pointing at the same schema
/// (used to simulate a second pod stealing an expired lease).
async fn make_backend() -> (PostgresLeaseBackend, String) {
    let schema = format!("lease_test_{}", uuid::Uuid::new_v4().simple());
    let b = {
        let _guard = SETUP_LOCK.lock().unwrap();
        std::env::set_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA", &schema);
        PostgresLeaseBackend::from_env().await.unwrap()
    };
    (b, schema)
}

/// Build a second backend pointing at the same schema. Used to simulate a
/// second pod sharing the same Postgres schema.
async fn make_backend_for_schema(schema: &str) -> PostgresLeaseBackend {
    let _guard = SETUP_LOCK.lock().unwrap();
    std::env::set_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA", schema);
    PostgresLeaseBackend::from_env().await.unwrap()
}

#[tokio::test]
async fn postgres_first_acquire_wins_second_rejects() {
    if skip_if_no_postgres() {
        return;
    }
    let (b, _schema) = make_backend().await;

    let g1 = b
        .try_acquire("race-1", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(g1.is_some(), "first acquire should win");

    let g2 = b
        .try_acquire("race-1", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(g2.is_none(), "second acquire should reject while g1 holds");
}

#[tokio::test]
async fn postgres_release_on_drop_enables_reacquire() {
    if skip_if_no_postgres() {
        return;
    }
    let (b, _schema) = make_backend().await;

    let g1 = b
        .try_acquire("drop-1", Duration::from_secs(30))
        .await
        .unwrap();
    drop(g1);

    // Give the drop a moment to run the async release.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let g2 = b
        .try_acquire("drop-1", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(g2.is_some(), "reacquire should succeed after release");
}

#[tokio::test]
async fn postgres_expired_lease_can_be_stolen() {
    if skip_if_no_postgres() {
        return;
    }
    let (b, schema) = make_backend().await;

    // Acquire with a tiny TTL so the heartbeat can't refresh it.
    let _g1 = b
        .try_acquire("expire-1", Duration::from_secs(1))
        .await
        .unwrap();

    // Wait past the TTL (plus slack for Postgres's `now()` + round-trip).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // A second actor (sharing the schema) should be able to steal it.
    let b2 = make_backend_for_schema(&schema).await;
    let stolen = b2
        .try_acquire("expire-1", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(stolen.is_some(), "expired lease should be stealable");
}

#[tokio::test]
async fn postgres_list_reports_active_holders() {
    if skip_if_no_postgres() {
        return;
    }
    let (b, _schema) = make_backend().await;

    let _g = b
        .try_acquire("listed", Duration::from_secs(30))
        .await
        .unwrap();

    let all = b.list_all().await.unwrap();
    assert!(
        all.iter().any(|i| i.name == "listed"),
        "should list active lease"
    );
}
