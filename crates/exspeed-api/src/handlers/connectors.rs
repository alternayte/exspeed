use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use exspeed_connectors::ConnectorConfig;

use crate::state::AppState;

pub async fn create_connector(
    State(state): State<Arc<AppState>>,
    Json(config): Json<ConnectorConfig>,
) -> impl IntoResponse {
    match state.connector_manager.create(config).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({"status": "created"})),
        ),
        Err(e) if e.contains("already exists") => (
            StatusCode::CONFLICT,
            Json(json!({"error": e})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        ),
    }
}

pub async fn list_connectors(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let list = state.connector_manager.list().await;
    (StatusCode::OK, Json(json!(list)))
}

pub async fn get_connector(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.connector_manager.get_status(&name).await {
        Some(info) => (StatusCode::OK, Json(json!(info))),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("connector '{}' not found", name)})),
        ),
    }
}

pub async fn delete_connector(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.connector_manager.delete(&name).await {
        Ok(()) => (StatusCode::OK, Json(json!({"deleted": name}))),
        Err(e) if e.contains("not found") => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": e})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        ),
    }
}

pub async fn restart_connector(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.connector_manager.restart(&name).await {
        Ok(()) => (StatusCode::OK, Json(json!({"restarted": name}))),
        Err(e) if e.contains("not found") => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": e})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        ),
    }
}
