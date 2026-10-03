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

#[derive(Deserialize, utoipa::ToSchema)]
pub struct CreateConnectionRequest {
    pub name: String,
    pub driver: String,
    pub url: String,
}

/// POST /api/v1/connections
///
/// Add a new external database connection.
#[utoipa::path(
    post,
    path = "/api/v1/connections",
    tag = "connections",
    security(("bearer" = [])),
    request_body = CreateConnectionRequest,
    responses(
        (status = 201, description = "`{name, driver, status: \"created\"}`", body = Object),
        (status = 400, description = "Invalid connection", body = crate::openapi::ExqlErrorBody),
        (status = 409, description = "Exists", body = crate::openapi::ExqlErrorBody),
    )
)]
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

    match state.exql.add_connection(config).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({"name": body.name, "driver": body.driver, "status": "created"})),
        )
            .into_response(),
        Err(e) => super::queries::exql_error(&e),
    }
}

/// GET /api/v1/connections
///
/// List all registered connections (URLs are masked for security).
#[utoipa::path(
    get,
    path = "/api/v1/connections",
    tag = "connections",
    security(("bearer" = [])),
    responses(
        (status = 200, description = "`[{name, driver}]`", body = Vec<Object>),
    )
)]
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
        .list_connections()
        .into_iter()
        .map(|(name, driver)| json!({"name": name, "driver": driver}))
        .collect();

    (StatusCode::OK, Json(json!(connections))).into_response()
}

/// DELETE /api/v1/connections/{name}
///
/// Remove an external database connection.
#[utoipa::path(
    delete,
    path = "/api/v1/connections/{name}",
    tag = "connections",
    security(("bearer" = [])),
    params(("name" = String, Path, description = "Connection name")),
    responses(
        (status = 200, description = "`{status: \"removed\", name}`", body = Object),
        (status = 404, description = "No such connection", body = crate::openapi::ExqlErrorBody),
    )
)]
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
    match state.exql.remove_connection(&name).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"status": "removed", "name": name})),
        )
            .into_response(),
        Err(e) => super::queries::exql_error(&e),
    }
}
