use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use exspeed_common::auth::Identity;
use serde_json::json;

use exspeed_connectors::ConnectorConfig;

use crate::state::AppState;

pub async fn create_connector(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(config): Json<ConnectorConfig>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    match state.connector_manager.create(config).await {
        Ok(()) => (StatusCode::CREATED, Json(json!({"status": "created"}))).into_response(),
        Err(e) if e.contains("already exists") => {
            (StatusCode::CONFLICT, Json(json!({"error": e}))).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn list_connectors(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    let list = state.connector_manager.list().await;
    (StatusCode::OK, Json(json!(list))).into_response()
}

pub async fn get_connector(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    match state.connector_manager.get_status(&name).await {
        Some(info) => (StatusCode::OK, Json(json!(info))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("connector '{}' not found", name)})),
        )
            .into_response(),
    }
}

pub async fn delete_connector(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    match state.connector_manager.delete(&name).await {
        Ok(()) => (StatusCode::OK, Json(json!({"deleted": name}))).into_response(),
        Err(e) if e.contains("not found") => {
            (StatusCode::NOT_FOUND, Json(json!({"error": e}))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn restart_connector(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    match state.connector_manager.restart(&name).await {
        Ok(()) => (StatusCode::OK, Json(json!({"restarted": name}))).into_response(),
        Err(e) if e.contains("not found") => {
            (StatusCode::NOT_FOUND, Json(json!({"error": e}))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}
