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
        auth_token: None,
        tls_cert: None,
        tls_key: None,
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

    // /healthz is leader-aware: returns 200 with {"leader": true} when this
    // pod holds the cluster-leader lease (always the case with Noop backend).
    let resp = client
        .get(format!("{}/healthz", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["leader"],
        true,
        "single-pod server should always be leader; body: {:?}",
        body
    );
    assert!(
        body.get("holder").is_some(),
        "response should include holder id; body: {:?}",
        body
    );
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

// ---------------------------------------------------------------------------
// Dedup field tests (Task 5)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_stream_rejects_dedup_window_longer_than_retention() {
    let (_tcp, http) = start_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({
            "name": "bad-dedup",
            "max_age_secs": 60,
            "dedup_window_secs": 120
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "expected 400 for invalid dedup window");
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("dedup window"),
        "expected 'dedup window' in error message, got: {}",
        body
    );
}

#[tokio::test]
async fn create_stream_persists_dedup_fields() {
    let (_tcp, http) = start_server().await;
    // Create stream with explicit dedup fields.
    reqwest::Client::new()
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({
            "name": "dedup-good",
            "dedup_window_secs": 600,
            "dedup_max_entries": 100000
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // GET should echo them back.
    let info: serde_json::Value = reqwest::get(format!("{}/api/v1/streams/dedup-good", http))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        info["dedup_window_secs"], 600,
        "dedup_window_secs mismatch; body: {:?}",
        info
    );
    assert_eq!(
        info["dedup_max_entries"], 100000,
        "dedup_max_entries mismatch; body: {:?}",
        info
    );
}

#[tokio::test]
async fn patch_updates_dedup_window_on_live_stream() {
    let (_tcp, http) = start_server().await;
    // Create stream.
    reqwest::Client::new()
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({ "name": "live-patch" }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // PATCH to update dedup_window_secs.
    let patch_resp = reqwest::Client::new()
        .patch(format!("{}/api/v1/streams/live-patch", http))
        .json(&serde_json::json!({ "dedup_window_secs": 900 }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        patch_resp.status(),
        200,
        "PATCH failed: {}",
        patch_resp.text().await.unwrap_or_default()
    );

    // GET should reflect the new value.
    let info: serde_json::Value = reqwest::get(format!("{}/api/v1/streams/live-patch", http))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        info["dedup_window_secs"], 900,
        "dedup_window_secs not updated; body: {:?}",
        info
    );
}

#[tokio::test]
async fn patch_stream_not_found_returns_404() {
    let (_tcp, http) = start_server().await;
    let resp = reqwest::Client::new()
        .patch(format!("{}/api/v1/streams/nonexistent-stream", http))
        .json(&serde_json::json!({ "dedup_window_secs": 300 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn patch_rejects_invalid_dedup_config() {
    let (_tcp, http) = start_server().await;
    // Create stream with a short retention and matching short dedup window so create succeeds.
    reqwest::Client::new()
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({
            "name": "short-ret",
            "max_age_secs": 3600,
            "dedup_window_secs": 60
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // PATCH to shrink retention so dedup window would exceed it — should fail.
    let resp = reqwest::Client::new()
        .patch(format!("{}/api/v1/streams/short-ret", http))
        .json(&serde_json::json!({ "max_age_secs": 30 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "expected 400 for retention < dedup window");
}

#[tokio::test]
async fn metrics_endpoint_returns_prometheus_text() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/metrics", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("uptime_seconds"),
        "expected prometheus metrics containing 'uptime_seconds', got: {}",
        &body[..body.len().min(500)]
    );
}
