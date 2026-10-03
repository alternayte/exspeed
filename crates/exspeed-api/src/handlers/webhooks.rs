use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use exspeed_broker::log::LogError;
use exspeed_connectors::builtin::http_webhook::WebhookError;

use crate::state::AppState;

/// `POST /webhooks/{*path}`: append the body through the matching
/// `http_webhook` connector. 200 means the record is durable.
#[utoipa::path(
    post,
    path = "/webhooks/{path}",
    tag = "webhooks",
    params(("path" = String, Path, description = "Path the http_webhook connector listens on (may contain slashes)")),
    request_body(content = Vec<u8>, description = "Raw body, stored as the record value", content_type = "application/octet-stream"),
    responses(
        (status = 200, description = "Stored: `{offset}`", body = Object),
        (status = 400, description = "Rejected by the connector", body = crate::openapi::ErrorBody),
        (status = 401, description = "The connector's own auth failed", body = crate::openapi::ErrorBody),
        (status = 404, description = "No webhook connector for this path", body = crate::openapi::ErrorBody),
        (status = 409, description = "Idempotency key reused with another body", body = crate::openapi::ErrorBody),
        (status = 503, description = "Not the leader, or retryable", body = crate::openapi::ErrorBody),
    )
)]
pub async fn handle_webhook(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let Some(endpoint) = state.connector_manager.find_webhook(&path).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(
                json!({"error": format!("no webhook connector matches path '/{}'", path.trim_start_matches('/'))}),
            ),
        );
    };
    let header = |name: &str| -> Option<String> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    match endpoint.handle(&state.broker.log, body, &header).await {
        Ok(offset) => (StatusCode::OK, Json(json!({"offset": offset}))),
        Err(WebhookError::Unauthorized) => (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        ),
        Err(WebhookError::Log(e @ (LogError::NotLeader | LogError::DedupNotReady))) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": e.to_string()})),
        ),
        Err(WebhookError::Log(
            e @ LogError::Storage(exspeed_streams::StorageError::KeyCollision { .. }),
        )) => (StatusCode::CONFLICT, Json(json!({"error": e.to_string()}))),
        Err(WebhookError::Log(e)) if e.is_retryable() => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": e.to_string()})),
        ),
        Err(WebhookError::Log(e @ LogError::Storage(_))) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        ),
    }
}
