use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use prometheus::{Encoder, TextEncoder};

use crate::state::AppState;

/// Prometheus metrics (text exposition format). Open unless the server
/// has a metrics token (`[server] metrics_token`), which then must be sent
/// as `Authorization: Bearer <token>`.
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "health",
    security((), ("bearer" = [])),
    responses(
        (status = 200, description = "Prometheus text format", content_type = "text/plain", body = String),
        (status = 401, description = "A metrics token is configured and the request lacks it"),
    )
)]
pub async fn prometheus_metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(expected) = state.metrics_token.as_deref() {
        let sent = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        if !constant_time_eq(sent.as_bytes(), expected.as_bytes()) {
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header("www-authenticate", "Bearer")
                .body(axum::body::Body::from("unauthorized\n"))
                .unwrap();
        }
    }
    // 1. Update uptime gauge.
    state
        .metrics
        .set_uptime(state.start_time.elapsed().as_secs_f64());

    // 2. Update storage bytes per stream.
    for stream in state.storage.list_streams() {
        let bytes = state.storage.stream_storage_bytes(&stream).unwrap_or(0) as i64;
        state.metrics.set_storage_bytes(&stream, bytes);
    }

    // 3. Update consumer lag per stream/consumer (leader only; followers
    // don't run consumers).
    if let Ok(list) = state.broker.consumers.list(None).await {
        for info in list {
            state
                .metrics
                .set_consumer_lag(&info.spec.stream, &info.spec.name, info.lag as i64);
        }
    }

    // 4. Render Prometheus text format.
    let encoder = TextEncoder::new();
    let metric_families = state.prometheus_registry.gather();
    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!("failed to encode prometheus metrics: {}", e);
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::from("failed to encode metrics\n"))
            .unwrap();
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )
        .body(axum::body::Body::from(buffer))
        .unwrap()
}

/// Compare without an early exit on the first differing byte.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use sha2::{Digest, Sha256};
    // Hash both sides so the comparison length doesn't depend on the input.
    let (x, y) = (Sha256::digest(a), Sha256::digest(b));
    x.iter()
        .zip(y.iter())
        .fold(0u8, |acc, (p, q)| acc | (p ^ q))
        == 0
}
