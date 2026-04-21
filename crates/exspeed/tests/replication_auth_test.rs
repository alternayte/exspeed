#![cfg(test)]
//! Cluster-port handshake auth gating.
//!
//! Plan G, spec §7 + §9: the leader-side replication handshake enforces
//! `Action::Replicate` on the Connect identity. A credential with
//! publish/subscribe but no replicate is rejected; one with replicate is
//! accepted.
//!
//! `ReplicationServer` is unit-tested against `MemoryStorage` in
//! `exspeed-broker/tests/replication_server_test.rs`. The test here is a
//! higher-level integration test running against a full-fat server
//! startup: exercises the role-transition supervisor's bind path + the
//! credentials.toml that tests in production would look like.
//!
//! # Requirements
//!
//! Multi-pod mode = Postgres lease backend. Set
//! `EXSPEED_OFFSET_STORE_POSTGRES_URL=postgres://...` before invoking
//! `cargo test --ignored`; without it the test short-circuits. Typical
//! local invocation:
//!
//! ```
//! docker compose up -d postgres
//! export EXSPEED_OFFSET_STORE_POSTGRES_URL=postgres://exspeed:exspeed@localhost:5432/exspeed
//! cargo test -p exspeed --test replication_auth_test -- --ignored --nocapture
//! ```

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::replicate::{ReplicateResume, REPLICATION_WIRE_VERSION};
use exspeed_protocol::opcodes::OpCode;

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

/// Assemble a `credentials.toml` with two identities:
///
///   * `publisher` — actions = [publish, subscribe]
///   * `replicator` — actions = [replicate]
///
/// Returns `(path, publisher_token, replicator_token)`. The file is rooted
/// in the given tempdir so it lives exactly as long as the test does.
fn write_credentials_toml(tmp: &TempDir) -> (PathBuf, String, String) {
    let pub_token = "pub-token";
    let rep_token = "rep-token";
    let pub_hash = exspeed_common::auth::sha256_hex(pub_token.as_bytes());
    let rep_hash = exspeed_common::auth::sha256_hex(rep_token.as_bytes());
    let toml = format!(
        r#"
[[credentials]]
name = "publisher"
token_sha256 = "{pub_hash}"
permissions = [
  {{ streams = "*", actions = ["publish", "subscribe"] }},
]

[[credentials]]
name = "replicator"
token_sha256 = "{rep_hash}"
permissions = [
  {{ streams = "*", actions = ["replicate"] }},
]
"#
    );
    let path = tmp.path().join("credentials.toml");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    f.flush().unwrap();
    (path, pub_token.to_string(), rep_token.to_string())
}

struct LeaderHarness {
    api_port: u16,
    cluster_addr: String,
    pub_token: String,
    rep_token: String,
    _tmp: TempDir,
}

/// Start a leader in multi-pod mode (Postgres backend) with credentials +
/// a bound cluster listener on 127.0.0.1:<random>. Returns the cluster
/// port address, both bearer tokens, and the API port.
async fn start_leader(schema: &str) -> LeaderHarness {
    let api_port = portpicker::pick_unused_port().unwrap();
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let cluster_port = portpicker::pick_unused_port().unwrap();
    let cluster_addr = format!("127.0.0.1:{cluster_port}");

    let tmp = tempfile::tempdir().unwrap();
    let (cred_path, pub_token, rep_token) = write_credentials_toml(&tmp);

    // Postgres + leader wiring env. Short lease TTL keeps the test fast.
    std::env::set_var("EXSPEED_CONSUMER_STORE", "postgres");
    std::env::set_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA", schema);
    std::env::set_var("EXSPEED_LEASE_TTL_SECS", "5");
    std::env::set_var("EXSPEED_LEASE_HEARTBEAT_SECS", "1");
    std::env::set_var("EXSPEED_CLUSTER_BIND", &cluster_addr);
    // This is the replicator credential THIS pod would use if it
    // demoted to follower. Startup hard-fails if unset in multi-pod mode
    // (server.rs:800-804) — set it even though we only exercise the
    // leader side.
    std::env::set_var("EXSPEED_REPLICATOR_CREDENTIAL", &rep_token);

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

    tokio::spawn(async move {
        let _ = exspeed::cli::server::run(args).await;
    });

    // Wait for leadership + cluster listener bind. 2s is generous on a
    // 5s TTL / 1s heartbeat config.
    tokio::time::sleep(Duration::from_secs(2)).await;

    LeaderHarness {
        api_port,
        cluster_addr,
        pub_token,
        rep_token,
        _tmp: tmp,
    }
}

/// Dial the cluster port, send Connect (Token auth with the given bearer),
/// then send ReplicateResume. Returns the framed reader — the caller pulls
/// frames to verify acceptance or close.
async fn dial_and_handshake(
    addr: &str,
    bearer: &str,
) -> FramedRead<tokio::io::ReadHalf<TcpStream>, ExspeedCodec> {
    let sock = TcpStream::connect(addr).await.expect("tcp connect cluster");
    let (r, w) = tokio::io::split(sock);
    let framed_read = FramedRead::new(r, ExspeedCodec::new());
    let mut framed_write = FramedWrite::new(w, ExspeedCodec::new());

    let req = ConnectRequest {
        client_id: "replication-auth-test".into(),
        auth_type: AuthType::Token,
        auth_payload: Bytes::from(bearer.as_bytes().to_vec()),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    framed_write
        .send(Frame::new(OpCode::Connect, 1, buf.freeze()))
        .await
        .expect("send connect");

    // ReplicateResume is sent eagerly regardless of the Connect outcome —
    // we want the same code path for both success (server proceeds to
    // manifest) and failure (server closes after 403 frame).
    let resume = ReplicateResume {
        follower_id: Uuid::new_v4(),
        wire_version: REPLICATION_WIRE_VERSION,
        cursor: BTreeMap::new(),
    };
    let bytes = bincode::serialize(&resume).unwrap();
    let _ = framed_write
        .send(Frame::new(OpCode::ReplicateResume, 0, Bytes::from(bytes)))
        .await;
    // Drop writer — tests only read from here.
    drop(framed_write);
    framed_read
}

fn clear_env() {
    std::env::remove_var("EXSPEED_CONSUMER_STORE");
    std::env::remove_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA");
    std::env::remove_var("EXSPEED_LEASE_TTL_SECS");
    std::env::remove_var("EXSPEED_LEASE_HEARTBEAT_SECS");
    std::env::remove_var("EXSPEED_CLUSTER_BIND");
    std::env::remove_var("EXSPEED_REPLICATOR_CREDENTIAL");
}

/// Scrape the `/metrics` endpoint and pick out the total for
/// `exspeed_auth_denied_total{action="replicate"}`. Returns 0 when the
/// series hasn't been incremented yet.
async fn auth_denied_replicate_total(api_port: u16) -> u64 {
    let body = reqwest::get(format!("http://127.0.0.1:{api_port}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let mut total: u64 = 0;
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        if !line.starts_with("exspeed_auth_denied_total") {
            continue;
        }
        if !line.contains("action=\"replicate\"") {
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
async fn cluster_port_rejects_non_replicate_credential_and_accepts_replicate() {
    if skip_if_no_postgres() {
        return;
    }

    let schema = format!("repl_auth_{}", uuid::Uuid::new_v4().simple());
    ensure_schema(&schema).await;

    let leader = start_leader(&schema).await;

    // ---- Case 1: publisher (no Replicate verb) → handshake rejected ----
    let before = auth_denied_replicate_total(leader.api_port).await;
    let mut rx = dial_and_handshake(&leader.cluster_addr, &leader.pub_token).await;

    // Server closes promptly after sending 403 Error. Drain — we expect
    // either an Error frame or EOF; on some scheduling, the RST races the
    // Error, so we accept both.
    let first = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("leader failed to respond within 5s");
    match first {
        Some(Ok(f)) => {
            assert_eq!(
                f.opcode,
                OpCode::Error,
                "expected 403 Error frame, got {:?}",
                f.opcode
            );
        }
        Some(Err(_)) | None => {
            // Socket closed — acceptable; the 403 frame may have been
            // folded into the same TCP segment as the FIN. Metric check
            // below is the authoritative assertion.
        }
    }
    // Next frame must be EOF.
    let next = tokio::time::timeout(Duration::from_secs(3), rx.next())
        .await
        .expect("leader failed to close socket within 3s after 403");
    assert!(
        next.is_none() || matches!(next, Some(Err(_))),
        "leader should close socket after 403, got {next:?}"
    );

    // The `exspeed_auth_denied_total{action="replicate"}` counter must
    // have incremented. Poll briefly — OTel -> Prometheus exporter flushes
    // on the scrape cycle; a tight assertion can race it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "replicate-denied counter did not increment within 5s (was {before})"
            );
        }
        let now = auth_denied_replicate_total(leader.api_port).await;
        if now > before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // ---- Case 2: replicator (has Replicate verb) → handshake succeeds ----
    let mut rx = dial_and_handshake(&leader.cluster_addr, &leader.rep_token).await;

    // Drain frames until we either get ClusterManifest or time out. The
    // server sends ConnectOk then ClusterManifest.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_manifest = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), rx.next()).await {
            Ok(Some(Ok(f))) => {
                if f.opcode == OpCode::ClusterManifest {
                    saw_manifest = true;
                    break;
                }
                // ConnectOk / Heartbeat / other — keep draining.
            }
            Ok(Some(Err(_))) | Ok(None) => {
                panic!("replicator session closed before ClusterManifest arrived");
            }
            Err(_) => { /* read timeout — keep spinning until deadline */ }
        }
    }
    assert!(
        saw_manifest,
        "replicator with Action::Replicate should receive a ClusterManifest"
    );

    clear_env();
}
