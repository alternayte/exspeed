#![cfg(test)]

use std::path::PathBuf;
use std::time::Duration;

use tempfile::TempDir;

fn skip_if_no_postgres() -> bool {
    std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").is_err()
}

/// Pre-create the Postgres schema both brokers will share. The lease
/// backend creates its own schema on startup, but the consumer store and
/// work coordinator issue `CREATE TABLE IF NOT EXISTS {schema}.{table}`
/// statements that assume the schema already exists. Without this, server
/// startup panics before the lease backend ever runs.
async fn ensure_schema(schema: &str) {
    let url = std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").unwrap();
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let sql = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
    client.execute(&sql, &[]).await.expect("create schema");
}

/// Spin up a broker instance in the background. Returns a handle whose
/// `.abort()` kills the tokio task (simulating pod crash).
struct BrokerHandle {
    #[allow(dead_code)]
    tcp_port: u16,
    api_port: u16,
    _tmp: TempDir,
    task: tokio::task::JoinHandle<()>,
}

async fn spawn_broker(schema: &str) -> BrokerHandle {
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let api_port = portpicker::pick_unused_port().unwrap();
    let tmp = tempfile::tempdir().unwrap();

    std::env::set_var("EXSPEED_CONSUMER_STORE", "postgres");
    std::env::set_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA", schema);
    // Use a short TTL so the test finishes quickly.
    std::env::set_var("EXSPEED_LEASE_TTL_SECS", "5");
    std::env::set_var("EXSPEED_LEASE_HEARTBEAT_SECS", "1");

    let data_dir: PathBuf = tmp.path().to_path_buf();
    let bind = format!("127.0.0.1:{tcp_port}");
    let api_bind = format!("127.0.0.1:{api_port}");

    let task = tokio::spawn(async move {
        let args = exspeed::cli::server::ServerArgs {
            bind,
            api_bind,
            data_dir,
            auth_token: None,
            tls_cert: None,
            tls_key: None,
        };
        let _ = exspeed::cli::server::run(args).await;
    });

    // Wait long enough for startup.
    tokio::time::sleep(Duration::from_millis(500)).await;

    BrokerHandle {
        tcp_port,
        api_port,
        _tmp: tmp,
        task,
    }
}

/// Query /api/v1/leases to see what's currently held.
async fn list_leases(api_port: u16) -> Vec<serde_json::Value> {
    reqwest::get(format!("http://127.0.0.1:{api_port}/api/v1/leases"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn multipod_lease_failover_between_brokers() {
    if skip_if_no_postgres() {
        return;
    }

    let schema = format!("e2e_{}", uuid::Uuid::new_v4().simple());
    ensure_schema(&schema).await;

    // Spin up two brokers sharing the same Postgres schema.
    let b1 = spawn_broker(&schema).await;
    let b2 = spawn_broker(&schema).await;

    // Wait for at least one to have acquired any lease. Because the test
    // brokers have no connectors/queries configured by default, the lease
    // list may be empty. This test therefore validates the primary property:
    // both brokers run, neither errors on startup, and /api/v1/leases is
    // queryable from both. A richer scenario (with a mock connector) needs
    // additional test infrastructure; we leave that for a future test.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let l1 = list_leases(b1.api_port).await;
    let l2 = list_leases(b2.api_port).await;

    // Both should agree (they're reading the same Postgres).
    assert_eq!(l1.len(), l2.len(), "both brokers should see the same leases");

    // Simulate crash of b1.
    b1.task.abort();

    // Wait past TTL (5s) + retry interval (TTL/3 ≈ 2s) + slack.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // b2 is still responsive.
    let resp = reqwest::get(format!("http://127.0.0.1:{}/healthz", b2.api_port))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "b2 should still be healthy");

    // Cleanup.
    b2.task.abort();

    std::env::remove_var("EXSPEED_CONSUMER_STORE");
    std::env::remove_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA");
    std::env::remove_var("EXSPEED_LEASE_TTL_SECS");
    std::env::remove_var("EXSPEED_LEASE_HEARTBEAT_SECS");
}
