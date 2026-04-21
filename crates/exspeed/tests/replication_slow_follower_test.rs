#![cfg(test)]
//! Slow follower → leader's per-follower queue fills → drops (Plan G,
//! Wave 6 / Plan Task 25).
//!
//! Three pods: leader, fast follower, slow follower. The slow follower
//! uses the test-only `EXSPEED_TEST_REPLICATION_APPLY_SLEEP_MS=500` hook
//! in the replication client apply loop. The leader is configured with
//! `EXSPEED_REPLICATION_FOLLOWER_QUEUE_RECORDS=10` so any sustained
//! backpressure triggers drops. Publishing 100 records forces the slow
//! follower's queue to overflow; assertion is that
//! `exspeed_replication_follower_queue_drops_total_total` increments.
//!
//! ## Why this can't be a unit test
//!
//! The drop counter is incremented inside the leader's coordinator
//! broadcast fan-out, which only has a follower channel attached once
//! the replication handshake has bound a session. To exercise it end
//! to end we need a real follower TCP connection against a real
//! coordinator — which means a real pod.
//!
//! # Requirements
//!
//! ```text
//! docker compose up -d postgres
//! export EXSPEED_OFFSET_STORE_POSTGRES_URL=postgres://exspeed:exspeed@localhost:5432/exspeed
//! cargo test -p exspeed --test replication_slow_follower_test -- --ignored --nocapture
//! ```

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use tempfile::TempDir;

const REPLICATOR_TOKEN: &str = "repl-token";
const ADMIN_TOKEN: &str = "admin-token";

fn skip_if_no_postgres() -> bool {
    std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").is_err()
}

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

fn write_credentials_toml(tmp_dir: &std::path::Path) -> PathBuf {
    let admin_hash = exspeed_common::auth::sha256_hex(ADMIN_TOKEN.as_bytes());
    let repl_hash = exspeed_common::auth::sha256_hex(REPLICATOR_TOKEN.as_bytes());
    let toml = format!(
        r#"
[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [
  {{ streams = "*", actions = ["publish", "subscribe", "admin"] }},
]

[[credentials]]
name = "replicator"
token_sha256 = "{repl_hash}"
permissions = [
  {{ streams = "*", actions = ["replicate"] }},
]
"#
    );
    let path = tmp_dir.join("credentials.toml");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    f.flush().unwrap();
    path
}

struct PodHandle {
    api_port: u16,
    _tmp: TempDir,
    _task: tokio::task::JoinHandle<()>,
}

fn set_shared_env(schema: &str) {
    std::env::set_var("EXSPEED_CONSUMER_STORE", "postgres");
    std::env::set_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA", schema);
    std::env::set_var("EXSPEED_LEASE_TTL_SECS", "5");
    std::env::set_var("EXSPEED_LEASE_HEARTBEAT_SECS", "1");
    std::env::set_var("EXSPEED_REPLICATOR_CREDENTIAL", REPLICATOR_TOKEN);
    // Tiny leader queue: 10 records of backpressure headroom per
    // follower. Combined with the 500ms per-record apply sleep, 100
    // records hit the drop path hard.
    std::env::set_var("EXSPEED_REPLICATION_FOLLOWER_QUEUE_RECORDS", "10");
    std::env::set_var("EXSPEED_REPLICATION_BATCH_RECORDS", "1");
    // Apply-hook (slow follower apply loop reads this at record-grain).
    std::env::set_var("EXSPEED_TEST_REPLICATION_APPLY_SLEEP_MS", "500");
}

fn clear_shared_env() {
    for k in [
        "EXSPEED_CONSUMER_STORE",
        "EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA",
        "EXSPEED_LEASE_TTL_SECS",
        "EXSPEED_LEASE_HEARTBEAT_SECS",
        "EXSPEED_REPLICATOR_CREDENTIAL",
        "EXSPEED_REPLICATION_FOLLOWER_QUEUE_RECORDS",
        "EXSPEED_REPLICATION_BATCH_RECORDS",
        "EXSPEED_TEST_REPLICATION_APPLY_SLEEP_MS",
        "EXSPEED_CLUSTER_BIND",
    ] {
        std::env::remove_var(k);
    }
}

async fn spawn_pod() -> PodHandle {
    let api_port = portpicker::pick_unused_port().unwrap();
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let cluster_port = portpicker::pick_unused_port().unwrap();
    let tmp = tempfile::tempdir().unwrap();

    let cred_path = write_credentials_toml(tmp.path());
    std::env::set_var("EXSPEED_CLUSTER_BIND", format!("127.0.0.1:{cluster_port}"));

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{tcp_port}"),
        api_bind: format!("127.0.0.1:{api_port}"),
        data_dir: tmp.path().to_path_buf(),
        auth_token: None,
        credentials_file: Some(cred_path),
        tls_cert: None,
        tls_key: None,
            storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
            storage_flush_window_us: 500,
            storage_flush_threshold_records: 256,
            storage_flush_threshold_bytes: 1_048_576,
            storage_sync_interval_ms: 10,
            storage_sync_bytes: 4 * 1024 * 1024,
            delivery_buffer: 8192,
    };

    let task = tokio::spawn(async move {
        let _ = exspeed::cli::server::run(args).await;
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;

    PodHandle {
        api_port,
        _tmp: tmp,
        _task: task,
    }
}

async fn healthz_code(api_port: u16) -> Option<u16> {
    reqwest::get(format!("http://127.0.0.1:{api_port}/healthz"))
        .await
        .ok()
        .map(|r| r.status().as_u16())
}

async fn create_stream(api_port: u16, name: &str) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{api_port}/api/v1/streams"))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .json(&serde_json::json!({
            "name": name,
            "max_age_secs": 0,
            "max_bytes": 0,
        }))
        .send()
        .await
        .expect("create_stream HTTP");
    assert!(resp.status().is_success());
}

async fn publish(api_port: u16, stream: &str, i: u64) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://127.0.0.1:{api_port}/api/v1/streams/{stream}/publish"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .json(&serde_json::json!({
            "subject": stream,
            "data": { "i": i },
            "msg_id": format!("{stream}-{i}"),
        }))
        .send()
        .await
        .expect("publish HTTP");
    assert!(resp.status().is_success());
}

async fn queue_drops_total(api_port: u16) -> u64 {
    let body = match reqwest::get(format!("http://127.0.0.1:{api_port}/metrics")).await {
        Ok(resp) => resp.text().await.unwrap_or_default(),
        Err(_) => String::new(),
    };
    let mut total = 0u64;
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        // Again the OTel `_total_total` quirk.
        if !line.starts_with("exspeed_replication_follower_queue_drops_total_total") {
            continue;
        }
        if let Some(val) = line.rsplit(' ').next() {
            if let Ok(n) = val.parse::<f64>() {
                total = total.saturating_add(n as u64);
            }
        }
    }
    total
}

#[tokio::test]
#[ignore = "requires postgres; set EXSPEED_OFFSET_STORE_POSTGRES_URL and run with --ignored"]
async fn slow_follower_triggers_leader_queue_drop() {
    if skip_if_no_postgres() {
        return;
    }
    let schema = format!("repl_slow_{}", uuid::Uuid::new_v4().simple());
    ensure_schema(&schema).await;
    set_shared_env(&schema);

    // All three pods read the same env. Slow-sleep is global to the
    // process, so every follower applies slowly — good, since for this
    // test the drop metric on the leader is insensitive to whether one
    // or both followers are slow. The interesting observation is:
    // publish > queue_cap + apply_rate → drops.
    let p1 = spawn_pod().await;
    let p2 = spawn_pod().await;
    let p3 = spawn_pod().await;

    // Find the leader.
    let leader_api = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!("no leader emerged within 30s");
            }
            let h1 = healthz_code(p1.api_port).await;
            let h2 = healthz_code(p2.api_port).await;
            let h3 = healthz_code(p3.api_port).await;
            if h1 == Some(200) {
                break p1.api_port;
            }
            if h2 == Some(200) {
                break p2.api_port;
            }
            if h3 == Some(200) {
                break p3.api_port;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    };

    let stream = "hotstream";
    create_stream(leader_api, stream).await;

    // Burst-publish 100 records. With queue_cap=10 + 500ms apply sleep
    // per record, the followers' per-session mpsc channels fill quickly
    // and the coordinator's try_send returns Err, bumping the drop
    // counter.
    let before = queue_drops_total(leader_api).await;
    for i in 0..100u64 {
        publish(leader_api, stream, i).await;
    }

    // Poll drops. Allow 30s for the burst to propagate through the
    // coordinator fan-out. The counter is bumped synchronously on
    // try_send failure in the leader's emit path, so this is fast in
    // practice (<1s) — we over-budget to tolerate Postgres-induced
    // startup jitter.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "follower_queue_drops_total_total did not increment within 30s (was {before})"
            );
        }
        let now = queue_drops_total(leader_api).await;
        if now > before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    clear_shared_env();
}
