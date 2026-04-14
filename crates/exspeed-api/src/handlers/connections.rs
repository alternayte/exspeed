use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
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
    Json(body): Json<CreateConnectionRequest>,
) -> impl IntoResponse {
    let config = ConnectionConfig {
        name: body.name.clone(),
        driver: body.driver.clone(),
        url: body.url,
    };

    match state.exql.connection_registry.add(config) {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({"name": body.name, "driver": body.driver, "status": "created"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        ),
    }
}

/// GET /api/v1/connections
///
/// List all registered connections (URLs are masked for security).
pub async fn list_connections(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let connections: Vec<serde_json::Value> = state
        .exql
        .connection_registry
        .list()
        .into_iter()
        .map(|(name, driver)| json!({"name": name, "driver": driver}))
        .collect();

    (StatusCode::OK, Json(json!(connections)))
}

/// DELETE /api/v1/connections/{name}
///
/// Remove an external database connection.
pub async fn delete_connection(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.exql.connection_registry.remove(&name) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"status": "removed", "name": name})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e})),
        ),
    }
}
