#![cfg(test)]

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
async fn leases_endpoint_noop_backend_returns_empty_list() {
    let (api_port, _tmp) = start_server(None).await;

    let resp = reqwest::get(format!("http://127.0.0.1:{api_port}/api/v1/leases"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(body.is_empty(), "noop backend should return empty lease list");
}

#[tokio::test]
async fn leases_endpoint_requires_auth_when_configured() {
    let (api_port, _tmp) = start_server(Some("lease-test-token".into())).await;

    // Without bearer → 401.
    let resp = reqwest::get(format!("http://127.0.0.1:{api_port}/api/v1/leases"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // With bearer → 200.
    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{api_port}/api/v1/leases"))
        .header("Authorization", "Bearer lease-test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn metrics_expose_lease_counters() {
    let (api_port, _tmp) = start_server(None).await;

    let body = reqwest::get(format!("http://127.0.0.1:{api_port}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // Counters show up after at least one acquire attempt; with noop backend,
    // every connector always succeeds — but we start the server without any
    // connectors here, so only the metric *descriptor* needs to exist.
    // Presence of the name in the output is enough.
    assert!(
        body.contains("exspeed_lease_held") || body.contains("exspeed_lease_acquire_total"),
        "expected lease metrics in /metrics body; got:\n{body}"
    );
}
