#![cfg(test)]
//! End-to-end test: two brokers sharing a Postgres lease backend race for
//! the `cluster:leader` lease. Exactly one is the leader at any moment
//! (its /healthz returns 200, the other's returns 503). Killing the
//! leader causes the survivor's /healthz to flip to 200 within TTL +
//! retry + slack.

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

/// Forcibly delete all lease rows in the schema, simulating a pod crash
/// that leaves no running heartbeat. In-process `task.abort()` only
/// cancels the main task, not the independently-spawned heartbeat task
/// (which lives in a separate `tokio::spawn`). The heartbeat sub-task
/// keeps the Postgres row alive. Deleting the row directly mimics what
/// happens when an OS process dies and the Postgres connection closes:
/// the lease row expires / is removed and the survivor can re-acquire.
async fn force_expire_leases(schema: &str) {
    let url = std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").unwrap();
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to postgres for force_expire");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let sql = format!("DELETE FROM {schema}.exspeed_leases");
    // Ignore errors — table may not exist yet.
    let _ = client.execute(sql.as_str(), &[]).await;
}

/// Spin up a broker instance in the background. Returns a handle whose
/// `.task.abort()` kills the tokio task (simulating pod crash).
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
    // Short TTL for test speed. 5s TTL + 1s heartbeat means failover
    // completes well within the 30s test budget.
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

    // Wait long enough for HTTP to bind. Leadership acquisition happens
    // during startup inside run(), so a modest sleep here is sufficient.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    BrokerHandle {
        tcp_port,
        api_port,
        _tmp: tmp,
        task,
    }
}

/// Probe /healthz and return the HTTP status code, or None if the request
/// failed (e.g. server not yet ready).
async fn healthz_code(api_port: u16) -> Option<u16> {
    reqwest::get(format!("http://127.0.0.1:{api_port}/healthz"))
        .await
        .ok()
        .map(|r| r.status().as_u16())
}

#[tokio::test]
async fn failover_shifts_healthz_between_brokers() {
    if skip_if_no_postgres() {
        return;
    }

    let schema = format!("plan_e_e2e_{}", uuid::Uuid::new_v4().simple());
    ensure_schema(&schema).await;

    // Spin up two brokers sharing the same Postgres schema.
    let b1 = spawn_broker(&schema).await;
    let b2 = spawn_broker(&schema).await;

    // Within TTL (5s) + startup slack, exactly one pod should have
    // /healthz=200 and the other /healthz=503. Poll up to 20s total.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let leader_port = loop {
        if tokio::time::Instant::now() > deadline {
            panic!("neither broker became leader within 20s");
        }
        let h1 = healthz_code(b1.api_port).await;
        let h2 = healthz_code(b2.api_port).await;
        match (h1, h2) {
            (Some(200), Some(503)) => break b1.api_port,
            (Some(503), Some(200)) => break b2.api_port,
            (Some(200), Some(200)) => {
                panic!("both brokers reporting leader — split-brain bug!");
            }
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    };

    // Identify which handle is the leader vs the survivor.
    let (leader_handle, survivor_port) = if leader_port == b1.api_port {
        (b1, b2.api_port)
    } else {
        (b2, b1.api_port)
    };

    // Kill the leader (simulates pod crash).
    //
    // IMPORTANT: in-process `task.abort()` only cancels the main task; all
    // independently-`tokio::spawn`'d sub-tasks (heartbeat, retry loop,
    // Axum server) survive because they hold their own `Arc` references.
    // The heartbeat sub-task keeps refreshing the Postgres lease row, so
    // the survivor would never see the lease expire naturally. We therefore
    // also delete the lease row directly — exactly what happens in
    // production when the OS kills the process and Postgres closes the
    // connection (the row is never refreshed and expires / is deleted).
    leader_handle.task.abort();
    force_expire_leases(&schema).await;

    // Survivor's /healthz flips to 200 within TTL (5s) + retry (TTL/3 ≈ 2s)
    // + slack. Poll up to 15s.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("survivor never became leader after leader crashed");
        }
        if healthz_code(survivor_port).await == Some(200) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Cleanup env vars (best-effort; test isolation is per-process anyway).
    std::env::remove_var("EXSPEED_CONSUMER_STORE");
    std::env::remove_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA");
    std::env::remove_var("EXSPEED_LEASE_TTL_SECS");
    std::env::remove_var("EXSPEED_LEASE_HEARTBEAT_SECS");
}
