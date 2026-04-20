#![cfg(test)]
//! End-to-end replication scenario (Plan G, Wave 6 / Plan Task 27).
//!
//! Covers the full operator-visible lifecycle:
//!
//!   1. Bootstrap: two pods; leader creates 5 streams + patches
//!      retention + publishes 10k records with `x-idempotency-key`.
//!   2. Catch-up: poll until follower head_offset matches leader on
//!      every stream.
//!   3. Failover: abort the leader task + delete its lease row so the
//!      survivor promotes. /healthz flips to 200 on the survivor within
//!      60s.
//!   4. Durability: all 10k original records still readable on the new
//!      leader via `head_offset` equal to 10k on every stream.
//!   5. Steady-state on new leader: publish 1k more records.
//!   6. Rejoin: restart the old leader pointed at its original data_dir.
//!      It demotes (lease already held by the new leader), follows, and
//!      catches up to 11k on every stream.
//!
//! Budget: 120s. Actual runtime is dominated by 11k HTTP publishes +
//! the failover TTL (5s).
//!
//! # Requirements
//!
//! ```text
//! docker compose up -d postgres
//! export EXSPEED_OFFSET_STORE_POSTGRES_URL=postgres://exspeed:exspeed@localhost:5432/exspeed
//! cargo test -p exspeed --test multipod_replication_test -- --ignored --nocapture
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

async fn force_expire_leases(schema: &str) {
    let url = std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").unwrap();
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let sql = format!("DELETE FROM {schema}.exspeed_leases");
    let _ = client.execute(sql.as_str(), &[]).await;
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
    data_dir: PathBuf,
    cred_path: PathBuf,
    bind: String,
    api_bind: String,
    cluster_bind: String,
    _tmp: TempDir,
    task: tokio::task::JoinHandle<()>,
}

fn set_shared_env(schema: &str) {
    std::env::set_var("EXSPEED_CONSUMER_STORE", "postgres");
    std::env::set_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA", schema);
    std::env::set_var("EXSPEED_LEASE_TTL_SECS", "5");
    std::env::set_var("EXSPEED_LEASE_HEARTBEAT_SECS", "1");
    std::env::set_var("EXSPEED_REPLICATOR_CREDENTIAL", REPLICATOR_TOKEN);
    std::env::set_var("EXSPEED_REPLICATION_BATCH_RECORDS", "256");
}

fn clear_shared_env() {
    for k in [
        "EXSPEED_CONSUMER_STORE",
        "EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA",
        "EXSPEED_LEASE_TTL_SECS",
        "EXSPEED_LEASE_HEARTBEAT_SECS",
        "EXSPEED_REPLICATOR_CREDENTIAL",
        "EXSPEED_REPLICATION_BATCH_RECORDS",
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
    let data_dir = tmp.path().to_path_buf();
    let cred_path = write_credentials_toml(tmp.path());

    let bind = format!("127.0.0.1:{tcp_port}");
    let api_bind = format!("127.0.0.1:{api_port}");
    let cluster_bind = format!("127.0.0.1:{cluster_port}");
    std::env::set_var("EXSPEED_CLUSTER_BIND", &cluster_bind);

    let args = exspeed::cli::server::ServerArgs {
        bind: bind.clone(),
        api_bind: api_bind.clone(),
        data_dir: data_dir.clone(),
        auth_token: None,
        credentials_file: Some(cred_path.clone()),
        tls_cert: None,
        tls_key: None,
    };
    let task = tokio::spawn(async move {
        let _ = exspeed::cli::server::run(args).await;
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;
    PodHandle {
        api_port,
        data_dir,
        cred_path,
        bind,
        api_bind,
        cluster_bind,
        _tmp: tmp,
        task,
    }
}

async fn healthz_code(api_port: u16) -> Option<u16> {
    reqwest::get(format!("http://127.0.0.1:{api_port}/healthz"))
        .await
        .ok()
        .map(|r| r.status().as_u16())
}

async fn wait_for_leader_split(a: &PodHandle, b: &PodHandle, deadline_secs: u64) -> (u16, u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(deadline_secs);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("no leader emerged within {deadline_secs}s");
        }
        match (healthz_code(a.api_port).await, healthz_code(b.api_port).await) {
            (Some(200), Some(503)) => return (a.api_port, b.api_port),
            (Some(503), Some(200)) => return (b.api_port, a.api_port),
            _ => tokio::time::sleep(Duration::from_millis(300)).await,
        }
    }
}

async fn create_stream(api_port: u16, name: &str, max_bytes: u64) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{api_port}/api/v1/streams"))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .json(&serde_json::json!({
            "name": name,
            "max_age_secs": 0,
            "max_bytes": max_bytes,
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

async fn patch_retention(api_port: u16, name: &str, max_bytes: u64) {
    let resp = reqwest::Client::new()
        .patch(format!(
            "http://127.0.0.1:{api_port}/api/v1/streams/{name}"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .json(&serde_json::json!({ "max_bytes": max_bytes }))
        .send()
        .await
        .expect("patch_stream HTTP");
    assert!(
        resp.status().is_success(),
        "patch_stream({name}) returned {}",
        resp.status()
    );
}

async fn publish(client: &reqwest::Client, api_port: u16, stream: &str, i: u64) {
    let resp = client
        .post(format!(
            "http://127.0.0.1:{api_port}/api/v1/streams/{stream}/publish"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-idempotency-key", format!("{stream}-{i}"))
        .json(&serde_json::json!({
            "subject": stream,
            "data": { "i": i },
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

async fn wait_all_streams_reach(
    api_port: u16,
    streams: &[&str],
    target: u64,
    deadline_secs: u64,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(deadline_secs);
    loop {
        if tokio::time::Instant::now() > deadline {
            let mut observed = Vec::new();
            for s in streams {
                observed.push(format!("{s}={:?}", head_offset(api_port, s).await));
            }
            panic!(
                "not all streams reached {target} within {deadline_secs}s: {}",
                observed.join(", ")
            );
        }
        let mut all = true;
        for s in streams {
            match head_offset(api_port, s).await {
                Some(n) if n >= target => {}
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
}

#[tokio::test]
#[ignore = "requires postgres; set EXSPEED_OFFSET_STORE_POSTGRES_URL and run with --ignored"]
async fn multipod_e2e_bootstrap_failover_rejoin() {
    if skip_if_no_postgres() {
        return;
    }
    let schema = format!("repl_e2e_{}", uuid::Uuid::new_v4().simple());
    ensure_schema(&schema).await;
    set_shared_env(&schema);

    let a = spawn_pod().await;
    let b = spawn_pod().await;

    // ---- Phase 1: bootstrap leader/follower -------------------------------
    let (leader_api, follower_api) = wait_for_leader_split(&a, &b, 30).await;

    let streams = ["alpha", "beta", "gamma", "delta", "epsilon"];

    // Create streams; immediately patch one retention to prove the
    // RetentionUpdatedEvent path wires through (currently a no-op on
    // the apply side per Wave 4 TODO, but manifest seeds retention on
    // pre-create and this exercises the HTTP surface).
    for s in &streams {
        create_stream(leader_api, s, 0).await;
    }
    patch_retention(leader_api, "alpha", 1024 * 1024 * 1024 /* 1 GB */).await;

    // Publish 10k records total = 2k per stream.
    let client = reqwest::Client::new();
    let per_stream: u64 = 2_000;
    for s in &streams {
        for i in 0..per_stream {
            publish(&client, leader_api, s, i).await;
        }
    }

    // Verify on the leader.
    for s in &streams {
        assert_eq!(
            head_offset(leader_api, s).await,
            Some(per_stream),
            "leader head_offset wrong for {s}"
        );
    }

    // Phase 2: follower catches up.
    wait_all_streams_reach(follower_api, &streams, per_stream, 60).await;

    // ---- Phase 3: failover ------------------------------------------------
    // Identify handles.
    let (dying, survivor) = if leader_api == a.api_port {
        (a, b)
    } else {
        (b, a)
    };
    let dying_cluster_bind = dying.cluster_bind.clone();
    let dying_data_dir = dying.data_dir.clone();
    let dying_bind = dying.bind.clone();
    let dying_api_bind = dying.api_bind.clone();
    let dying_cred_path = dying.cred_path.clone();

    dying.task.abort();
    force_expire_leases(&schema).await;

    // Survivor /healthz flips to 200 within 60s.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("survivor never became leader after failover");
        }
        if healthz_code(survivor.api_port).await == Some(200) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // ---- Phase 4: durability after failover ------------------------------
    for s in &streams {
        assert_eq!(
            head_offset(survivor.api_port, s).await,
            Some(per_stream),
            "survivor lost records for {s} after failover"
        );
    }

    // ---- Phase 5: steady-state publish to new leader --------------------
    let extra: u64 = 200;
    for s in &streams {
        for i in per_stream..(per_stream + extra) {
            publish(&client, survivor.api_port, s, i).await;
        }
    }
    for s in &streams {
        assert_eq!(
            head_offset(survivor.api_port, s).await,
            Some(per_stream + extra),
            "new leader head_offset wrong for {s}"
        );
    }

    // ---- Phase 6: rejoin old leader as follower -------------------------
    // Use a hand-rolled respawn pointed at the same data_dir. We did
    // NOT go through respawn_pod() because `dying` was partially moved
    // above for cluster_bind extraction.
    std::env::set_var("EXSPEED_CLUSTER_BIND", &dying_cluster_bind);
    let args = exspeed::cli::server::ServerArgs {
        bind: dying_bind,
        api_bind: dying_api_bind.clone(),
        data_dir: dying_data_dir,
        auth_token: None,
        credentials_file: Some(dying_cred_path),
        tls_cert: None,
        tls_key: None,
    };
    let rejoined_api_port: u16 = dying_api_bind
        .rsplit(':')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let _rejoined_task = tokio::spawn(async move {
        let _ = exspeed::cli::server::run(args).await;
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // It should now be a follower (the survivor owns the lease).
    // Catches up to the extra 200 records per stream.
    wait_all_streams_reach(
        rejoined_api_port,
        &streams,
        per_stream + extra,
        60,
    )
    .await;

    // ---- Final assertion: leader holds total count on every stream -----
    for s in &streams {
        assert_eq!(
            head_offset(survivor.api_port, s).await,
            Some(per_stream + extra),
            "leader lost records for {s} on rejoin"
        );
        assert_eq!(
            head_offset(rejoined_api_port, s).await,
            Some(per_stream + extra),
            "rejoined follower behind for {s}"
        );
    }

    clear_shared_env();
}
