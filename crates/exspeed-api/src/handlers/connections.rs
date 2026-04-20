use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use exspeed_common::auth::Identity;
use serde::Deserialize;
use serde_json::json;

use exspeed_processing::external::ConnectionConfig;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateConnectionRequest {
    pub name: String,
    pub driver: String,
    pub url: String,
}

/// POST /api/v1/connections
///
/// Add a new external database connection.
pub async fn create_connection(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(body): Json<CreateConnectionRequest>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    let config = ConnectionConfig {
        name: body.name.clone(),
        driver: body.driver.clone(),
        url: body.url,
    };

    match state.exql.connection_registry.add(config) {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({"name": body.name, "driver": body.driver, "status": "created"})),
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}

/// GET /api/v1/connections
///
/// List all registered connections (URLs are masked for security).
pub async fn list_connections(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    let connections: Vec<serde_json::Value> = state
        .exql
        .connection_registry
        .list()
        .into_iter()
        .map(|(name, driver)| json!({"name": name, "driver": driver}))
        .collect();

    (StatusCode::OK, Json(json!(connections))).into_response()
}

/// DELETE /api/v1/connections/{name}
///
/// Remove an external database connection.
pub async fn delete_connection(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    match state.exql.connection_registry.remove(&name) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"status": "removed", "name": name})),
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e}))).into_response(),
    }
}
