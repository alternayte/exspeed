use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use prometheus::{Encoder, TextEncoder};

use crate::state::AppState;

/// Prometheus metrics (text exposition format). No auth.
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "health",
    responses(
        (status = 200, description = "Prometheus text format", content_type = "text/plain", body = String),
    )
)]
pub async fn prometheus_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
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
