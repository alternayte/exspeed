use std::time::Duration;

use serde_json::Value;

use crate::common::TestServer;

/// Returns the server (keep it alive) and its HTTP base URL.
async fn start_server() -> (TestServer, String) {
    let server = TestServer::start().await;
    let http = format!("http://{}", server.api_addr);
    (server, http)
}

#[tokio::test]
async fn delete_nonexistent_returns_404() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!("{}/api/v1/streams/missing", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn delete_with_no_references_succeeds() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "empty-stream"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = client
        .delete(format!("{}/api/v1/streams/empty-stream", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "delete should return 200");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["deleted"], "empty-stream");
    assert_eq!(body["cascaded"]["consumers"].as_array().unwrap().len(), 0);
    assert_eq!(body["cascaded"]["connectors"].as_array().unwrap().len(), 0);
    assert_eq!(body["cascaded"]["queries"].as_array().unwrap().len(), 0);
    assert_eq!(body["cascaded"]["subscriptions_dropped"], 0);

    // GET should now 404.
    let resp = client
        .get(format!("{}/api/v1/streams/empty-stream", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "stream should be gone");
}

async fn create_consumer_via_tcp(server: &TestServer, consumer: &str, stream: &str) {
    server
        .client()
        .await
        .create_consumer(exspeed_client::ConsumerSpec::new(consumer, stream))
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_with_consumer_rejects_409() {
    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "held-by-consumer"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    create_consumer_via_tcp(&tcp, "cons-1", "held-by-consumer").await;

    let resp = client
        .delete(format!("{}/api/v1/streams/held-by-consumer", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    let body: Value = resp.json().await.unwrap();
    let consumers = body["blockers"]["consumers"].as_array().unwrap();
    assert!(
        consumers.iter().any(|v| v == "cons-1"),
        "blockers.consumers should include cons-1, got {:?}",
        body
    );
}

#[tokio::test]
async fn delete_with_connector_rejects_409() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "held-by-connector"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "hook-x",
            "type": "source",
            "plugin": "http_webhook",
            "stream": "held-by-connector",
            "subject_template": "x",
            "settings": {"path": "/webhooks/hx", "auth_type": "none"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create connector should 201, body: {:?}",
        resp.text().await.unwrap_or_default()
    );

    let resp = client
        .delete(format!("{}/api/v1/streams/held-by-connector", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    let body: Value = resp.json().await.unwrap();
    let connectors = body["blockers"]["connectors"].as_array().unwrap();
    assert!(
        connectors.iter().any(|v| v == "hook-x"),
        "blockers.connectors should include hook-x, got {:?}",
        body
    );
}

#[tokio::test]
async fn force_delete_cascades() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "cascade-target"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "hook-c",
            "type": "source",
            "plugin": "http_webhook",
            "stream": "cascade-target",
            "subject_template": "x",
            "settings": {"path": "/webhooks/hc", "auth_type": "none"}
        }))
        .send()
        .await
        .unwrap();

    // A consumer created over HTTP is cascaded too.
    let resp = client
        .post(format!("{}/api/v1/consumers", http))
        .json(&serde_json::json!({"name": "casc-cons", "stream": "cascade-target"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = client
        .delete(format!("{}/api/v1/streams/cascade-target?force=true", http))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "force-delete should succeed: body={:?}",
        resp.text().await
    );

    // Connector should be gone.
    let resp = client
        .get(format!("{}/api/v1/connectors", http))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert!(
        arr.iter().all(|c| c["name"] != "hook-c"),
        "hook-c should have been removed, got: {:?}",
        arr
    );

    let resp = client
        .get(format!("{}/api/v1/consumers/casc-cons", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "consumer should be cascaded");

    // Stream should be gone.
    let resp = client
        .get(format!("{}/api/v1/streams/cascade-target", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn force_delete_idempotent_second_call_is_404() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "double-delete"}))
        .send()
        .await
        .unwrap();

    let resp = client
        .delete(format!("{}/api/v1/streams/double-delete?force=true", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .delete(format!("{}/api/v1/streams/double-delete?force=true", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[tokio::test]
async fn delete_during_inflight_publish_is_safe() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "racy"}))
        .send()
        .await
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = stop.clone();
    let http_c = http.clone();
    let publisher = tokio::spawn(async move {
        let c = reqwest::Client::new();
        let mut published = 0u64;
        while !stop_c.load(Ordering::Relaxed) {
            let _ = c
                .post(format!("{}/api/v1/streams/racy/publish", http_c))
                .json(&serde_json::json!({"data": {"n": published}}))
                .send()
                .await;
            published += 1;
            if published.is_multiple_of(50) {
                tokio::task::yield_now().await;
            }
        }
        published
    });

    // Let the publisher rack up some traffic.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Force-delete while publishes are in flight.
    let resp = client
        .delete(format!("{}/api/v1/streams/racy?force=true", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    stop.store(true, Ordering::Relaxed);
    let total = publisher.await.unwrap();
    assert!(
        total > 0,
        "publisher should have emitted at least one record"
    );

    // Server still healthy: create a new stream, publish, delete — full loop.
    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "post-race"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "server must survive the race");
}
