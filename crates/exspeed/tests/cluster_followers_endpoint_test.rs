#![cfg(test)]

//! `GET /api/v1/cluster/followers` surface tests.
//!
//! Wave 5 ships the single-pod behavior: the handler returns 503 with a
//! machine-readable error body when the pod was started without a
//! replication coordinator (i.e. `EXSPEED_CONSUMER_STORE` is unset or
//! `file`). The happy-path — a live leader with connected followers —
//! belongs to Wave 6's end-to-end multi-pod tests.

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
