use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use exspeed_connectors::builtin::http_webhook::handle_webhook_post;

use crate::state::AppState;

pub async fn handle_webhook(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Normalize the incoming path: the route captures everything after /webhooks/
    // e.g. for POST /webhooks/stripe, path = "stripe"
    let normalized_incoming = path.trim_start_matches('/');

    // Scan connectors for a matching http_webhook source
    let connectors = state.connector_manager.connectors.read().await;
    let matched_config = connectors.values().find(|rc| {
        if rc.config.plugin != "http_webhook" {
            return false;
        }
        if let Some(cfg_path) = rc.config.settings.get("path") {
            // The settings path might be "/webhooks/stripe" — strip the prefix
            let cfg_normalized = cfg_path
                .trim_start_matches('/')
                .strip_prefix("webhooks/")
                .unwrap_or(cfg_path.trim_start_matches('/'));
            cfg_normalized == normalized_incoming
        } else {
            false
        }
    });

    let config = match matched_config {
        Some(rc) => rc.config.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("no webhook connector matches path '/{}'", path)})),
            );
        }
    };
    // Drop the read lock before doing work
    drop(connectors);

    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    let storage = state.connector_manager.storage.clone();

    match handle_webhook_post(&storage, &config, body, auth_header) {
        Ok(offset) => (StatusCode::OK, Json(json!({"offset": offset}))),
        Err(e) if e.contains("unauthorized") => {
            (StatusCode::UNAUTHORIZED, Json(json!({"error": e})))
        }
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))),
    }
}
