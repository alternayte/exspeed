#![cfg(test)]

//! `GET /api/v1/cluster/followers` surface tests.
//!
//! Wave 5 ships the single-pod behavior: the handler returns 503 with a
//! machine-readable error body when the pod was started without a
//! replication coordinator (i.e. `EXSPEED_CONSUMER_STORE` is unset or
//! `file`). The happy-path — a live leader with connected followers —
//! belongs to Wave 6's end-to-end multi-pod tests.

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

async fn start_server(auth_token: Option<String>) -> (u16, TempDir) {
    let api_port = portpicker::pick_unused_port().unwrap();
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let tmp = tempfile::tempdir().unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{tcp_port}"),
        api_bind: format!("127.0.0.1:{api_port}"),
        data_dir: tmp.path().to_path_buf(),
        auth_token,
        credentials_file: None,
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
        exspeed::cli::server::run(args).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    (api_port, tmp)
}

/// Write a `credentials.toml` with two identities:
///
///   * `admin-id` — has `admin` on "*" (the cluster-followers bearer
///     we want the handler's 503 path to fall through to).
///   * `publisher-id` — only publish/subscribe; no admin. Sending
///     this one's bearer must produce 403 from `require_admin` BEFORE
///     the handler runs.
fn write_two_creds(tmp: &TempDir) -> (PathBuf, String, String) {
    let admin_token = "admin-tok";
    let pub_token = "publish-only-tok";
    let admin_hash = exspeed_common::auth::sha256_hex(admin_token.as_bytes());
    let pub_hash = exspeed_common::auth::sha256_hex(pub_token.as_bytes());
    let toml = format!(
        r#"
[[credentials]]
name = "admin-id"
token_sha256 = "{admin_hash}"
permissions = [
  {{ streams = "*", actions = ["publish", "subscribe", "admin"] }},
]

[[credentials]]
name = "publisher-id"
token_sha256 = "{pub_hash}"
permissions = [
  {{ streams = "*", actions = ["publish", "subscribe"] }},
]
"#
    );
    let path = tmp.path().join("credentials.toml");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    f.flush().unwrap();
    (path, admin_token.to_string(), pub_token.to_string())
}

async fn start_server_with_creds(creds_path: PathBuf) -> (u16, TempDir) {
    let api_port = portpicker::pick_unused_port().unwrap();
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let tmp = tempfile::tempdir().unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{tcp_port}"),
        api_bind: format!("127.0.0.1:{api_port}"),
        data_dir: tmp.path().to_path_buf(),
        auth_token: None,
        credentials_file: Some(creds_path),
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
        exspeed::cli::server::run(args).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    (api_port, tmp)
}

#[tokio::test]
async fn cluster_followers_endpoint_returns_503_in_single_pod_mode() {
    let (api_port, _tmp) = start_server(None).await;

    let resp = reqwest::get(format!(
        "http://127.0.0.1:{api_port}/api/v1/cluster/followers"
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 503, "single-pod deployments return 503");

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["error"], "not a multi-pod deployment",
        "error field pinned so operator tooling can match on it"
    );
    assert!(
        body["hint"]
            .as_str()
            .unwrap_or_default()
            .contains("EXSPEED_CONSUMER_STORE"),
        "hint should mention the env var the operator needs to set; got {body:?}"
    );
}

#[tokio::test]
async fn cluster_followers_endpoint_requires_auth_when_configured() {
    let (api_port, _tmp) = start_server(Some("cluster-followers-test-token".into())).await;

    // No bearer → 401 from the require_admin middleware, before the
    // handler even runs. Confirms the route sits behind the admin gate.
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{api_port}/api/v1/cluster/followers"
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 401);

    // With bearer → 503 (single-pod) rather than 401. Proves that auth
    // succeeded and the handler ran; 503 is the right "no coordinator"
    // signal.
    let resp = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{api_port}/api/v1/cluster/followers"
        ))
        .header("Authorization", "Bearer cluster-followers-test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
}

/// Non-admin credential → 403 from the `require_admin` middleware,
/// before the handler runs. This is the contract the endpoint docs
/// promise: `GET /api/v1/cluster/followers` is an operator surface,
/// not a tenant surface. A publish/subscribe-only credential MUST get
/// 403 (not 503 / 401 / a leaked follower list).
#[tokio::test]
async fn cluster_followers_endpoint_rejects_non_admin_credential_with_403() {
    let creds_tmp = tempfile::tempdir().unwrap();
    let (creds_path, _admin_tok, pub_tok) = write_two_creds(&creds_tmp);
    let (api_port, _tmp) = start_server_with_creds(creds_path).await;

    let resp = reqwest::Client::new()
        .get(format!(
            "http://127.0.0.1:{api_port}/api/v1/cluster/followers"
        ))
        .header("Authorization", format!("Bearer {pub_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        403,
        "publish-only credential should be rejected BEFORE the handler — \
         the endpoint sits under require_admin, not require_authenticated"
    );
}
