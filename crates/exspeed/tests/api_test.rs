use serde_json::Value;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_server() -> (String, String) {
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let http_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{}", tcp_port);
    let http_addr = format!("127.0.0.1:{}", http_port);

    let dir = tempfile::TempDir::new().unwrap();
    let args = exspeed::cli::server::ServerArgs {
        bind: tcp_addr.clone(),
        data_dir: dir.path().to_path_buf(),
        api_bind: http_addr.clone(),
    };

    tokio::spawn(async move {
        let _keep = dir;
        exspeed::cli::server::run(args).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    (tcp_addr, format!("http://{}", http_addr))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn healthz_returns_ok() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/healthz", http)).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn create_and_list_streams() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    // Create stream "orders"
    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "orders"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // List streams
    let resp = client
        .get(format!("{}/api/v1/streams", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    let streams = body.as_array().expect("expected JSON array");
    assert!(
        streams.iter().any(|s| s["name"] == "orders"),
        "expected 'orders' in stream list, got: {:?}",
        streams
    );
}

#[tokio::test]
async fn get_stream_detail() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    // Create stream via HTTP
    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "detail-test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // GET the stream detail (newly created, no records yet)
    let resp = client
        .get(format!("{}/api/v1/streams/detail-test", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "detail-test");
    // storage_bytes and head_offset should be present (possibly 0 for empty stream)
    assert!(body.get("storage_bytes").is_some());
    assert!(body.get("head_offset").is_some());
}

#[tokio::test]
async fn get_stream_not_found() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/api/v1/streams/nonexistent", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn metrics_endpoint_returns_prometheus_text() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/metrics", http)).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("uptime_seconds"),
        "expected prometheus metrics containing 'uptime_seconds', got: {}",
        &body[..body.len().min(500)]
    );
}
