use std::time::Duration;

use serde_json::Value;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_server() -> (String, String) {
    let tcp_port = exspeed_testkit::pick_unused_port().unwrap();
    let http_port = exspeed_testkit::pick_unused_port().unwrap();
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
            storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
            storage_flush_window_us: 500,
            storage_flush_threshold_records: 256,
            storage_flush_threshold_bytes: 1_048_576,
            storage_sync_interval_ms: 10,
            storage_sync_bytes: 4 * 1024 * 1024,
            delivery_buffer: 8192,
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

async fn start_server_in(data_dir: std::path::PathBuf) -> String {
    let tcp_port = exspeed_testkit::pick_unused_port().unwrap();
    let http_port = exspeed_testkit::pick_unused_port().unwrap();
    tokio::spawn(async move {
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind: format!("127.0.0.1:{tcp_port}"),
            api_bind: format!("127.0.0.1:{http_port}"),
            data_dir,
            auth_token: None,
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
        })
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    format!("http://127.0.0.1:{http_port}")
}

async fn connector_names(client: &reqwest::Client, http: &str) -> Vec<String> {
    let body: Value = client
        .get(format!("{http}/api/v1/connectors"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut names: Vec<String> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    names
}

/// Regression (REVIEW blocker 15): the connectors.d watcher used to delete
/// every connector without a matching `<name>.toml`, i.e. all API-created
/// connectors, on the first filesystem event.
#[tokio::test]
async fn file_watcher_leaves_api_created_connectors_alone() {
    let dir = tempfile::TempDir::new().unwrap();
    let http = start_server_in(dir.path().to_path_buf()).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{http}/api/v1/connectors"))
        .json(&serde_json::json!({
            "name": "api-hook",
            "type": "source",
            "plugin": "http_webhook",
            "stream": "api-events",
            "settings": {"path": "api-hook"}
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "create: {}", resp.status());

    // A file whose name differs from the connector it defines.
    std::fs::write(
        dir.path().join("connectors.d").join("hooks.toml"),
        "[connector]\nname = \"file-hook\"\ntype = \"source\"\nplugin = \"http_webhook\"\n\
         stream = \"file-events\"\n\n[settings]\npath = \"file-hook\"\n",
    )
    .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let names = connector_names(&client, &http).await;
        if names.contains(&"file-hook".to_string()) {
            assert!(
                names.contains(&"api-hook".to_string()),
                "API-created connector was deleted by the watcher: {names:?}"
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "file connector never appeared"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Give the watcher a few more cycles; nothing should flap.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        connector_names(&client, &http).await,
        vec!["api-hook".to_string(), "file-hook".to_string()]
    );

    // Removing the file removes only the connector it defined.
    std::fs::remove_file(dir.path().join("connectors.d").join("hooks.toml")).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let names = connector_names(&client, &http).await;
        if names == vec!["api-hook".to_string()] {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "unexpected: {names:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
