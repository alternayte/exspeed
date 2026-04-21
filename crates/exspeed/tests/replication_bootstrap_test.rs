#![cfg(test)]
//! Two-pod bootstrap integration test (Plan G, Wave 6 / Plan Task 22).
//!
//! Spins up leader + follower against a shared Postgres lease backend.
//! Leader publishes 100 records across 3 streams; asserts that the
//! follower's `head_offset` reaches parity and that the leader's
//! `/api/v1/cluster/followers` shows exactly one connected follower.
//!
//! # Requirements
//!
//! ```text
//! docker compose up -d postgres
//! export EXSPEED_OFFSET_STORE_POSTGRES_URL=postgres://exspeed:exspeed@localhost:5432/exspeed
//! cargo test -p exspeed --test replication_bootstrap_test -- --ignored --nocapture
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
    cluster_port: u16,
    _tmp: TempDir,
    _task: tokio::task::JoinHandle<()>,
}

fn set_shared_env(schema: &str) {
    std::env::set_var("EXSPEED_CONSUMER_STORE", "postgres");
    std::env::set_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA", schema);
    std::env::set_var("EXSPEED_LEASE_TTL_SECS", "5");
    std::env::set_var("EXSPEED_LEASE_HEARTBEAT_SECS", "1");
    std::env::set_var("EXSPEED_REPLICATOR_CREDENTIAL", REPLICATOR_TOKEN);
    // Small batch size keeps this test snappy — headers + wire sync on
    // every record, but the test only publishes 300 so latency dominates.
    std::env::set_var("EXSPEED_REPLICATION_BATCH_RECORDS", "32");
}

fn clear_shared_env() {
    for k in [
        "EXSPEED_CONSUMER_STORE",
        "EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA",
        "EXSPEED_LEASE_TTL_SECS",
        "EXSPEED_LEASE_HEARTBEAT_SECS",
        "EXSPEED_REPLICATOR_CREDENTIAL",
        "EXSPEED_REPLICATION_BATCH_RECORDS",
    ] {
        std::env::remove_var(k);
    }
}

/// Spawn a broker + cluster listener. `cluster_port` is pre-picked so we
/// can set `EXSPEED_CLUSTER_BIND` before the server reads it. Each pod
/// gets its own tempdir (data_dir).
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

    // Give it a moment to bind HTTP + race the lease. Startup sleeps are
    // cheap on modern hardware; the leadership poll below is the
    // authoritative signal.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    PodHandle {
        api_port,
        cluster_port,
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

async fn wait_for_leader(pods: &[&PodHandle], deadline_secs: u64) -> u16 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(deadline_secs);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("no pod became leader within {deadline_secs}s");
        }
        for p in pods {
            if healthz_code(p.api_port).await == Some(200) {
                return p.api_port;
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
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
    assert!(
        resp.status().is_success(),
        "create_stream({name}) returned {}",
        resp.status()
    );
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
    assert!(
        resp.status().is_success(),
        "publish({stream}, {i}) returned {}",
        resp.status()
    );
}

async fn head_offset(api_port: u16, stream: &str) -> Option<u64> {
    let resp = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{api_port}/api/v1/streams/{stream}"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: serde_json::Value = resp.json().await.ok()?;
    v["head_offset"].as_u64()
}

async fn followers_count(api_port: u16) -> Option<usize> {
    let resp = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{api_port}/api/v1/cluster/followers"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: serde_json::Value = resp.json().await.ok()?;
    v.as_array().map(|a| a.len())
}

#[tokio::test]
#[ignore = "requires postgres; set EXSPEED_OFFSET_STORE_POSTGRES_URL and run with --ignored"]
async fn two_pod_bootstrap_catches_up_on_follower() {
    if skip_if_no_postgres() {
        return;
    }
    let schema = format!("repl_boot_{}", uuid::Uuid::new_v4().simple());
    ensure_schema(&schema).await;
    set_shared_env(&schema);

    let p1 = spawn_pod().await;
    let p2 = spawn_pod().await;

    // Decide leader vs follower by /healthz.
    let leader_api = wait_for_leader(&[&p1, &p2], 30).await;
    let follower_api = if leader_api == p1.api_port {
        p2.api_port
    } else {
        p1.api_port
    };

    // Leader creates 3 streams + publishes 100 records each.
    let streams = ["alpha", "beta", "gamma"];
    for s in &streams {
        create_stream(leader_api, s).await;
    }
    for s in &streams {
        for i in 0..100u64 {
            publish(leader_api, s, i).await;
        }
    }

    // Poll follower until head_offset=100 on every stream. Allow 30s.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("follower did not catch up within 30s");
        }
        let mut all = true;
        for s in &streams {
            match head_offset(follower_api, s).await {
                Some(n) if n >= 100 => {}
                _ => {
                    all = false;
                    break;
                }
            }
        }
        if all {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Leader reports exactly one connected follower. Poll briefly: the
    // coordinator snapshot is updated on register/drop which happens on
    // the same task as the handshake, so this should be immediate, but
    // a short deadline is robust to scheduling.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "leader never reported a follower in /api/v1/cluster/followers — got {:?}",
                followers_count(leader_api).await
            );
        }
        if followers_count(leader_api).await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Sanity: follower's cluster_port was bound (demoted pod still binds
    // its cluster listener at startup, which then sits idle while it
    // runs the client side). Confirms the Wave 5 startup wiring is
    // symmetrical.
    let follower_cluster_port = if leader_api == p1.api_port {
        p2.cluster_port
    } else {
        p1.cluster_port
    };
    let _ = tokio::net::TcpStream::connect(format!("127.0.0.1:{follower_cluster_port}"))
        .await
        .expect("follower cluster_port should still be bound");

    clear_shared_env();
    std::env::remove_var("EXSPEED_CLUSTER_BIND");
}
