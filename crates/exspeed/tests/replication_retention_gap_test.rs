#![cfg(test)]
//! Retention-gap reseed (Plan G, Wave 6 / Plan Task 23).
//!
//! Sequence:
//!
//!   1. Start leader + follower against shared Postgres.
//!   2. Follower catches up.
//!   3. Kill follower task (simulates pod crash).
//!   4. Leader publishes enough to roll segments + retention trim drops
//!      the follower's last-known cursor below `earliest_offset`.
//!   5. Restart follower.
//!   6. Assert `exspeed_replication_reseed_total_total{stream=...}`
//!      increments.
//!
//! # Timing
//!
//! The retention task has a 10s initial delay + 60s tick interval
//! baked in (`crates/exspeed-broker/src/retention_task.rs`), with no
//! env-var tunable at time of writing. The overall budget below
//! allows ~180s for the trim + reseed to surface. This is intentional:
//! the test is `#[ignore]`d and only runs with `--ignored`, so the
//! slow cadence is acceptable.
//!
//! # Requirements
//!
//! ```text
//! docker compose up -d postgres
//! export EXSPEED_OFFSET_STORE_POSTGRES_URL=postgres://exspeed:exspeed@localhost:5432/exspeed
//! cargo test -p exspeed --test replication_retention_gap_test -- --ignored --nocapture
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
        data_dir,
        cred_path,
        bind,
        api_bind,
        cluster_bind,
        _tmp: tmp,
        task,
    }
}

/// Restart a pod with the *same* data_dir + ports. Lets a follower come
/// back up and observe a leader that has trimmed records out from under
/// its cursor. We do NOT move the tempdir — it lives in `_tmp` on the
/// returned handle and the caller keeps that alive.
async fn respawn_pod(prev: PodHandle) -> PodHandle {
    // Abort the old task. The lease/cursor/cluster socket were owned by
    // that task; the OS will close them when it exits.
    prev.task.abort();
    // Let the lease heartbeat sub-task wind down. In practice the lease
    // TTL (5s) + retry makes this unnecessary, but it's cheap.
    tokio::time::sleep(Duration::from_millis(500)).await;

    std::env::set_var("EXSPEED_CLUSTER_BIND", &prev.cluster_bind);

    let args = exspeed::cli::server::ServerArgs {
        bind: prev.bind.clone(),
        api_bind: prev.api_bind.clone(),
        data_dir: prev.data_dir.clone(),
        auth_token: None,
        credentials_file: Some(prev.cred_path.clone()),
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

    PodHandle { task, ..prev }
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

async fn publish(api_port: u16, stream: &str, i: u64, payload_kb: usize) {
    // Big-ish payload so we can exhaust `max_bytes` with a modest record
    // count. File storage rolls segments at 256MB by default, so we need
    // many KB per record for retention to actually trim within the test.
    let filler: String = "x".repeat(payload_kb * 1024);
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://127.0.0.1:{api_port}/api/v1/streams/{stream}/publish"
        ))
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .json(&serde_json::json!({
            "subject": stream,
            "data": { "i": i, "f": filler },
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

async fn reseed_total(api_port: u16, stream: &str) -> u64 {
    let body = match reqwest::get(format!("http://127.0.0.1:{api_port}/metrics")).await {
        Ok(resp) => resp.text().await.unwrap_or_default(),
        Err(_) => String::new(),
    };
    let mut total = 0u64;
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        // Note the double `_total_total`: OTel appends a `_total` suffix
        // to every `u64_counter` unconditionally, while the counter is
        // declared as `exspeed_replication_reseed_total`. Resulting
        // series name is `exspeed_replication_reseed_total_total`.
        if !line.starts_with("exspeed_replication_reseed_total_total") {
            continue;
        }
        if !line.contains(&format!("stream=\"{stream}\"")) {
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
#[ignore = "requires postgres + slow (≥90s for retention tick); run with --ignored"]
async fn follower_reseeds_when_leader_retention_trims_past_cursor() {
    if skip_if_no_postgres() {
        return;
    }
    let schema = format!("repl_reseed_{}", uuid::Uuid::new_v4().simple());
    ensure_schema(&schema).await;
    set_shared_env(&schema);

    let p1 = spawn_pod().await;
    let p2 = spawn_pod().await;

    let (leader_api, follower_api) = wait_for_leader_split(&p1, &p2, 30).await;

    // Stream with a small retention cap so the retention task will trim
    // most of the early records once we publish a burst.
    //
    // The leader holds its data_dir; segments roll at the default 256MB
    // threshold, so with ~200 records at 2MB each we get multiple sealed
    // segments, giving retention something to trim.
    let stream = "orders";
    create_stream(leader_api, stream, 4 * 1024 * 1024 /* 4 MB cap */).await;

    // Publish a handful first so the follower has something to replicate,
    // then wait for it to catch up.
    for i in 0..5u64 {
        publish(leader_api, stream, i, 64 /* KB */).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("follower did not catch up to initial 5 records within 30s");
        }
        if head_offset(follower_api, stream).await == Some(5) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Identify the follower handle so we can bounce it. leader_api is one
    // of p1.api_port / p2.api_port; the other is the follower.
    let (leader, follower) = if p1.api_port == leader_api {
        (p1, p2)
    } else {
        (p2, p1)
    };

    // Kill the follower — simulates pod crash / OOM. Note: the client task
    // stops on abort, but the data_dir lives on `_tmp` in the handle so we
    // can respawn into the same dir.
    follower.task.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Publish a flood big enough that a single retention pass trims past
    // offset 5 (the follower's last cursor). 64 KB × 120 = 7.5 MB — well
    // past the 4 MB cap. Retention will keep only the active segment
    // (~1 segment worth) after the next tick.
    for i in 5..125u64 {
        publish(leader.api_port, stream, i, 64 /* KB */).await;
    }

    // Wait for the leader's background retention task to tick. The task
    // has a 10s initial delay + 60s interval — total worst-case ~70s from
    // its own startup. We spawned the leader long ago, so we're on the
    // 60s cadence. Give 120s of slack.
    //
    // TODO: if a `EXSPEED_RETENTION_INTERVAL_SECS` tunable lands later,
    // set it low here and trim this budget.
    let trim_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        if tokio::time::Instant::now() > trim_deadline {
            panic!("leader retention never trimmed past offset 5 within 120s");
        }
        // stream_bounds is not surfaced via HTTP; instead we infer the
        // trim from a `head_offset` that's advanced AND a subsequent
        // bootstrap that reseeds the follower. Short-circuit: if
        // head_offset is past 100 we've certainly published; we just
        // need time for the retention ticker to fire.
        if let Some(h) = head_offset(leader.api_port, stream).await {
            if h >= 125 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Extra slack so the 60s retention tick definitely fires.
    tokio::time::sleep(Duration::from_secs(75)).await;

    // Bring the follower back up pointed at the same data_dir. On
    // handshake, the manifest check spots `earliest_offset > follower
    // cursor (=5)` and emits `StreamReseedEvent`, which bumps the reseed
    // counter on both sides.
    let follower_before = reseed_total(follower.api_port, stream).await;
    let leader_before = reseed_total(leader.api_port, stream).await;
    let follower = respawn_pod(follower).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "reseed counter did not increment within 30s (follower={}, leader={})",
                reseed_total(follower.api_port, stream).await,
                reseed_total(leader.api_port, stream).await
            );
        }
        let now_f = reseed_total(follower.api_port, stream).await;
        let now_l = reseed_total(leader.api_port, stream).await;
        if now_f > follower_before || now_l > leader_before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    clear_shared_env();
}
