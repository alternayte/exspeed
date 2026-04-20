use std::time::Duration;

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
    let data_dir = dir.path().to_path_buf();
    let tcp_addr_clone = tcp_addr.clone();
    let http_addr_clone = http_addr.clone();

    tokio::spawn(async move {
        let _keep = dir;
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind: tcp_addr_clone,
            api_bind: http_addr_clone,
            data_dir,
            auth_token: None,
            credentials_file: None,
            tls_cert: None,
            tls_key: None,
        })
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    (tcp_addr, format!("http://127.0.0.1:{}", http_port))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_webhook_and_receive() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream "webhook-test"
    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "webhook-test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create stream should return 201");

    // 2. Create HTTP webhook connector
    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "test-webhook",
            "type": "source",
            "plugin": "http_webhook",
            "stream": "webhook-test",
            "subject_template": "webhook.{$.type}",
            "settings": {
                "path": "/webhooks/test-hook",
                "auth_type": "none"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create connector should return 201, body: {:?}",
        resp.text().await.unwrap_or_default()
    );

    // 3. POST to the webhook endpoint
    let resp = client
        .post(format!("{}/webhooks/test-hook", http))
        .json(&serde_json::json!({"type": "test.event", "data": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "webhook POST should return 200");

    let body: Value = resp.json().await.unwrap();
    assert!(
        body.get("offset").is_some(),
        "webhook response should contain 'offset', got: {:?}",
        body
    );

    // The offset should be a non-negative number (first record = offset 0)
    let offset = body["offset"].as_u64().expect("offset should be a u64");
    assert_eq!(offset, 0, "first record should have offset 0");
}

#[tokio::test]
async fn list_and_delete_connectors() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream "conn-test"
    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "conn-test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // 2. Create first webhook connector
    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "webhook-alpha",
            "type": "source",
            "plugin": "http_webhook",
            "stream": "conn-test",
            "subject_template": "webhook.alpha",
            "settings": {
                "path": "/webhooks/alpha",
                "auth_type": "none"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create webhook-alpha should return 201");

    // 3. Create second webhook connector
    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "webhook-beta",
            "type": "source",
            "plugin": "http_webhook",
            "stream": "conn-test",
            "subject_template": "webhook.beta",
            "settings": {
                "path": "/webhooks/beta",
                "auth_type": "none"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create webhook-beta should return 201");

    // 4. List connectors — expect 2
    let resp = client
        .get(format!("{}/api/v1/connectors", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    let list = body.as_array().expect("expected JSON array");
    assert_eq!(list.len(), 2, "should have 2 connectors, got: {:?}", list);

    let names: Vec<&str> = list.iter().filter_map(|c| c["name"].as_str()).collect();
    assert!(
        names.contains(&"webhook-alpha"),
        "should contain webhook-alpha"
    );
    assert!(
        names.contains(&"webhook-beta"),
        "should contain webhook-beta"
    );

    // 5. Delete first connector
    let resp = client
        .delete(format!("{}/api/v1/connectors/webhook-alpha", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "delete should return 200");

    // 6. List connectors — expect 1 remaining
    let resp = client
        .get(format!("{}/api/v1/connectors", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    let list = body.as_array().expect("expected JSON array");
    assert_eq!(
        list.len(),
        1,
        "should have 1 connector after deletion, got: {:?}",
        list
    );
    assert_eq!(
        list[0]["name"].as_str().unwrap(),
        "webhook-beta",
        "remaining connector should be webhook-beta"
    );
}

#[tokio::test]
async fn connector_status() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream
    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "status-test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // 2. Create webhook connector
    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "status-webhook",
            "type": "source",
            "plugin": "http_webhook",
            "stream": "status-test",
            "subject_template": "webhook.status",
            "settings": {
                "path": "/webhooks/status-hook",
                "auth_type": "none"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // 3. GET connector status
    let resp = client
        .get(format!("{}/api/v1/connectors/status-webhook", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "status-webhook");
    assert_eq!(body["plugin"], "http_webhook");
    assert_eq!(body["stream"], "status-test");
    assert_eq!(body["connector_type"], "source");
    assert_eq!(
        body["status"], "running",
        "connector should be running, got: {:?}",
        body["status"]
    );
    assert!(
        body.get("uptime_secs").is_some(),
        "response should include uptime_secs"
    );
}
