use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use exspeed_common::auth::Identity;
use serde_json::json;

use exspeed_connectors::{ConnectorConfig, ManagerError};

use crate::state::AppState;

fn error_response(e: ManagerError) -> Response {
    let status = match &e {
        ManagerError::NotFound(_) => StatusCode::NOT_FOUND,
        ManagerError::AlreadyExists(_) | ManagerError::FileManaged { .. } => StatusCode::CONFLICT,
        ManagerError::Invalid(_) => StatusCode::BAD_REQUEST,
        ManagerError::NotLeader => StatusCode::SERVICE_UNAVAILABLE,
        ManagerError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({"error": e.to_string()}))).into_response()
}

fn admin_only(identity: Option<Extension<Arc<Identity>>>) -> Option<Response> {
    identity.and_then(|Extension(id)| super::require_global_admin(&id))
}

/// `POST /api/v1/connectors`
#[utoipa::path(
    post,
    path = "/api/v1/connectors",
    tag = "connectors",
    security(("bearer" = [])),
    request_body(content = Object, description = "Connector config: the JSON form of a connectors.d TOML file (docs/connectors.md)"),
    responses(
        (status = 201, description = "Created: `{\"created\": name}`", body = Object),
        (status = 400, description = "Invalid config", body = crate::openapi::ErrorBody),
        (status = 409, description = "Exists, or collides with a file connector", body = crate::openapi::ErrorBody),
        (status = 503, description = "Not the leader", body = crate::openapi::ErrorBody),
    )
)]
pub async fn create_connector(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(config): Json<ConnectorConfig>,
) -> Response {
    if let Some(resp) = admin_only(identity) {
        return resp;
    }
    let name = config.name.clone();
    match state.connector_manager.create(config).await {
        Ok(()) => (StatusCode::CREATED, Json(json!({"created": name}))).into_response(),
        Err(e) => error_response(e),
    }
}

/// `PUT /api/v1/connectors/{name}`: replace an API-created connector's
/// config and restart it. Offsets are kept.
#[utoipa::path(
    put,
    path = "/api/v1/connectors/{name}",
    tag = "connectors",
    security(("bearer" = [])),
    params(("name" = String, Path, description = "Connector name")),
    request_body(content = Object, description = "Connector config: the JSON form of a connectors.d TOML file (docs/connectors.md)"),
    responses(
        (status = 200, description = "Updated: `{\"updated\": name}`", body = Object),
        (status = 400, description = "Invalid config, or body name differs from the path", body = crate::openapi::ErrorBody),
        (status = 404, description = "No such connector", body = crate::openapi::ErrorBody),
        (status = 409, description = "Defined by a connectors.d file", body = crate::openapi::ErrorBody),
    )
)]
pub async fn update_connector(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(config): Json<ConnectorConfig>,
) -> Response {
    if let Some(resp) = admin_only(identity) {
        return resp;
    }
    if config.name != name {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("body name '{}' does not match path '{name}'", config.name)})),
        )
            .into_response();
    }
    match state.connector_manager.update(config).await {
        Ok(()) => (StatusCode::OK, Json(json!({"updated": name}))).into_response(),
        Err(e) => error_response(e),
    }
}

/// `GET /api/v1/connectors`: every connector with its live status.
#[utoipa::path(
    get,
    path = "/api/v1/connectors",
    tag = "connectors",
    security(("bearer" = [])),
    responses(
        (status = 200, description = "Connectors", body = Vec<crate::openapi::ConnectorInfoDoc>),
    )
)]
pub async fn list_connectors(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(resp) = admin_only(identity) {
        return resp;
    }
    let list = state.connector_manager.list().await;
    (StatusCode::OK, Json(json!(list))).into_response()
}

/// `GET /api/v1/connectors/{name}`: status plus the (unresolved) config.
#[utoipa::path(
    get,
    path = "/api/v1/connectors/{name}",
    tag = "connectors",
    security(("bearer" = [])),
    params(("name" = String, Path, description = "Connector name")),
    responses(
        (status = 200, description = "The connector with its `config`", body = crate::openapi::ConnectorInfoDoc),
        (status = 404, description = "No such connector", body = crate::openapi::ErrorBody),
    )
)]
pub async fn get_connector(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(resp) = admin_only(identity) {
        return resp;
    }
    let mgr = &state.connector_manager;
    match (mgr.get_status(&name).await, mgr.get_config(&name).await) {
        (Some(info), Some(config)) => {
            let mut v = json!(info);
            v["config"] = json!(config);
            (StatusCode::OK, Json(v)).into_response()
        }
        _ => error_response(ManagerError::NotFound(name)),
    }
}

/// `DELETE /api/v1/connectors/{name}`: stop it and delete it with its offsets.
#[utoipa::path(
    delete,
    path = "/api/v1/connectors/{name}",
    tag = "connectors",
    security(("bearer" = [])),
    params(("name" = String, Path, description = "Connector name")),
    responses(
        (status = 200, description = "Deleted: `{\"deleted\": name}`", body = Object),
        (status = 404, description = "No such connector", body = crate::openapi::ErrorBody),
    )
)]
pub async fn delete_connector(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(resp) = admin_only(identity) {
        return resp;
    }
    match state.connector_manager.delete(&name).await {
        Ok(()) => (StatusCode::OK, Json(json!({"deleted": name}))).into_response(),
        Err(e) => error_response(e),
    }
}

/// `POST /api/v1/connectors/{name}/restart`: also revives a `failed`
/// connector.
#[utoipa::path(
    post,
    path = "/api/v1/connectors/{name}/restart",
    tag = "connectors",
    security(("bearer" = [])),
    params(("name" = String, Path, description = "Connector name")),
    responses(
        (status = 200, description = "Restarted: `{\"restarted\": name}`", body = Object),
        (status = 404, description = "No such connector", body = crate::openapi::ErrorBody),
        (status = 503, description = "Not the leader", body = crate::openapi::ErrorBody),
    )
)]
pub async fn restart_connector(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(resp) = admin_only(identity) {
        return resp;
    }
    match state.connector_manager.restart(&name).await {
        Ok(()) => (StatusCode::OK, Json(json!({"restarted": name}))).into_response(),
        Err(e) => error_response(e),
    }
}
