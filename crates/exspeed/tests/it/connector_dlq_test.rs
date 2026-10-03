//! DLQ + RetryPolicy E2E tests — use an in-process axum mock HTTP server
//! as the sink target, so no external infra is needed.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;

use crate::common::TestServer;

// ---------------------------------------------------------------------------
// Mock HTTP server
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MockState {
    received: Arc<AtomicU64>,
    fail_503_first_n: Arc<AtomicU64>,
    always_404: Arc<AtomicBool>,
    always_503: Arc<AtomicBool>,
}

async fn mock_handler(State(s): State<MockState>, _body: Body) -> Response {
    s.received.fetch_add(1, Ordering::Relaxed);

    if s.always_404.load(Ordering::Relaxed) {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("nope"))
            .unwrap();
    }
    if s.always_503.load(Ordering::Relaxed) {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from("busy"))
            .unwrap();
    }
    let remaining = s.fail_503_first_n.load(Ordering::Relaxed);
    if remaining > 0 {
        s.fail_503_first_n.fetch_sub(1, Ordering::Relaxed);
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from("retry please"))
            .unwrap();
    }
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from("ok"))
        .unwrap()
}

async fn start_mock_http() -> (String, MockState) {
    let state = MockState {
        received: Arc::new(AtomicU64::new(0)),
        fail_503_first_n: Arc::new(AtomicU64::new(0)),
        always_404: Arc::new(AtomicBool::new(false)),
        always_503: Arc::new(AtomicBool::new(false)),
    };
    let app = Router::new()
        .route("/hook", post(mock_handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (format!("http://{addr}/hook"), state)
}

// ---------------------------------------------------------------------------
// Exspeed server startup
// ---------------------------------------------------------------------------

/// Returns the server (keep it alive) and its HTTP base URL.
async fn start_server() -> (TestServer, String) {
    let server = TestServer::start().await;
    let http = format!("http://{}", server.api_addr);
    (server, http)
}

// ---------------------------------------------------------------------------
// Record fetch helper
// ---------------------------------------------------------------------------

async fn fetch_records(
    server: &TestServer,
    stream: &str,
    max: u32,
) -> Vec<(u64, Vec<u8>, Vec<(String, String)>)> {
    match server
        .client()
        .await
        .read(stream, 0, max, Duration::ZERO, "")
        .await
    {
        Ok(r) => r
            .records
            .into_iter()
            .map(|rec| (rec.offset, rec.value.to_vec(), rec.headers))
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Poll for at least `want` records in a stream, up to `deadline_secs`.
async fn wait_for_records(
    tcp_addr: &TestServer,
    stream: &str,
    want: usize,
    deadline_secs: u64,
) -> Vec<(u64, Vec<u8>, Vec<(String, String)>)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(deadline_secs);
    loop {
        let recs = fetch_records(tcp_addr, stream, 100).await;
        if recs.len() >= want || std::time::Instant::now() > deadline {
            return recs;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn poison_record_routes_to_dlq_stream() {
    let (tcp, http) = start_server().await;
    let (mock_url, mock_state) = start_mock_http().await;
    mock_state.always_404.store(true, Ordering::Relaxed);

    let client = reqwest::Client::new();
    client
        .post(format!("{http}/api/v1/streams"))
        .json(&serde_json::json!({"name": "dlq-src-1"}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{http}/api/v1/connectors"))
        .json(&serde_json::json!({
            "name": "dlq-sink-1",
            "type": "sink",
            "plugin": "http_sink",
            "stream": "dlq-src-1",
            "settings": {
                "url": mock_url,
                "dlq_stream": "dlq-out-1"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create connector: {}",
        resp.text().await.unwrap()
    );

    client
        .post(format!("{http}/api/v1/streams/dlq-src-1/publish"))
        .json(&serde_json::json!({"data": {"n": 1, "name": "payload-1"}}))
        .send()
        .await
        .unwrap();

    let recs = wait_for_records(&tcp, "dlq-out-1", 1, 5).await;
    assert_eq!(recs.len(), 1, "DLQ stream should hold the poison record");

    // Inspect DLQ metadata headers.
    let headers: &[(String, String)] = &recs[0].2;
    assert!(
        headers
            .iter()
            .any(|(k, v)| k == "exspeed-dlq-origin" && v == "dlq-sink-1"),
        "header exspeed-dlq-origin missing/wrong: {headers:?}"
    );
    assert!(
        headers
            .iter()
            .any(|(k, v)| k == "exspeed-dlq-reason" && v == "http_client_error"),
        "header exspeed-dlq-reason missing/wrong: {headers:?}"
    );
    assert!(
        headers
            .iter()
            .any(|(k, _)| k == "exspeed-dlq-original-offset"),
        "header exspeed-dlq-original-offset missing"
    );
    assert!(mock_state.received.load(Ordering::Relaxed) >= 1);
}

#[tokio::test]
async fn poison_record_dropped_with_metric_when_dlq_unset() {
    let (_tcp, http) = start_server().await;
    let (mock_url, mock_state) = start_mock_http().await;
    mock_state.always_404.store(true, Ordering::Relaxed);

    let client = reqwest::Client::new();
    client
        .post(format!("{http}/api/v1/streams"))
        .json(&serde_json::json!({"name": "dlq-src-2"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{http}/api/v1/connectors"))
        .json(&serde_json::json!({
            "name": "dlq-sink-2",
            "type": "sink",
            "plugin": "http_sink",
            "stream": "dlq-src-2",
            "settings": { "url": mock_url }
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{http}/api/v1/streams/dlq-src-2/publish"))
        .json(&serde_json::json!({"data": {"n": 1}}))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let metrics = reqwest::get(format!("{http}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains("exspeed_connector_records_skipped_total")
            && metrics.contains("http_client_error"),
        "records_skipped_total{{reason=http_client_error}} should have incremented"
    );
}

#[tokio::test]
async fn transient_failure_retries_then_succeeds() {
    let (_tcp, http) = start_server().await;
    let (mock_url, mock_state) = start_mock_http().await;
    mock_state.fail_503_first_n.store(2, Ordering::Relaxed);

    let client = reqwest::Client::new();
    client
        .post(format!("{http}/api/v1/streams"))
        .json(&serde_json::json!({"name": "dlq-src-3"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{http}/api/v1/connectors"))
        .json(&serde_json::json!({
            "name": "dlq-sink-3",
            "type": "sink",
            "plugin": "http_sink",
            "stream": "dlq-src-3",
            "retry": {
                "max_retries": 5, "initial_backoff_ms": 50, "max_backoff_ms": 1000,
                "multiplier": 2.0, "jitter": false
            },
            "settings": { "url": mock_url }
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{http}/api/v1/streams/dlq-src-3/publish"))
        .json(&serde_json::json!({"data": {"n": 1}}))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Mock should have received 3 requests (2 503s + 1 OK).
    assert!(
        mock_state.received.load(Ordering::Relaxed) >= 3,
        "expected >=3 attempts, got {}",
        mock_state.received.load(Ordering::Relaxed)
    );
}

#[tokio::test]
async fn dlq_batch_on_transient_exhausted() {
    let (tcp, http) = start_server().await;
    let (mock_url, mock_state) = start_mock_http().await;
    mock_state.always_503.store(true, Ordering::Relaxed);

    let client = reqwest::Client::new();
    client
        .post(format!("{http}/api/v1/streams"))
        .json(&serde_json::json!({"name": "dlq-src-5"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{http}/api/v1/connectors"))
        .json(&serde_json::json!({
            "name": "dlq-sink-5",
            "type": "sink",
            "plugin": "http_sink",
            "stream": "dlq-src-5",
            "on_transient_exhausted": "dlq_batch",
            "retry": { "max_retries": 2, "initial_backoff_ms": 10, "max_backoff_ms": 100, "jitter": false },
            "settings": { "url": mock_url, "dlq_stream": "dlq-out-5" }
        }))
        .send()
        .await
        .unwrap();

    for n in 0..3 {
        client
            .post(format!("{http}/api/v1/streams/dlq-src-5/publish"))
            .json(&serde_json::json!({"data": {"n": n}}))
            .send()
            .await
            .unwrap();
    }

    let recs = wait_for_records(&tcp, "dlq-out-5", 3, 10).await;
    assert_eq!(
        recs.len(),
        3,
        "all 3 records should have been routed to dlq_batch"
    );

    // Each DLQ record carries reason=sink_rejected (transient exhaustion).
    for (_, _, headers) in &recs {
        let reason = headers
            .iter()
            .find(|(k, _)| k == "exspeed-dlq-reason")
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        assert_eq!(reason, "sink_rejected", "unexpected reason: {headers:?}");
    }
}
