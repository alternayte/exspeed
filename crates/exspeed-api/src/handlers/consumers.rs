//! Consumer management over HTTP. Delivery itself is TCP-only (push
//! subscriptions and pulls); HTTP covers create / inspect / seek / delete.

use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use exspeed_broker::consumer::ConsumerError;
use exspeed_common::auth::{Action, Identity};
use exspeed_common::StreamName;
use exspeed_protocol::client::{ConsumerSpec, SeekTo};
use serde::Deserialize;
use serde_json::json;

use crate::state::AppState;

pub(crate) fn consumer_error(e: ConsumerError) -> Response {
    let status = StatusCode::from_u16(e.code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, Json(json!({"error": e.to_string()}))).into_response()
}

#[derive(Deserialize, Default, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListParams {
    /// Only consumers of this stream.
    pub stream: Option<String>,
}

/// List consumers with their state (only those on streams the caller
/// administers).
#[utoipa::path(
    get,
    path = "/api/v1/consumers",
    tag = "consumers",
    security(("bearer" = [])),
    params(ListParams),
    responses(
        (status = 200, description = "Consumers", body = Vec<crate::openapi::ConsumerInfoDoc>),
    )
)]

pub async fn list_consumers(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListParams>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    match state.broker.consumers.list(params.stream.as_deref()).await {
        Ok(list) => {
            let visible: Vec<_> = list
                .into_iter()
                .filter(|i| match identity.as_ref() {
                    None => true,
                    Some(Extension(id)) => StreamName::try_from(i.spec.stream.as_str())
                        .map(|s| id.authorize(Action::Admin, &s))
                        .unwrap_or(false),
                })
                .collect();
            (StatusCode::OK, Json(json!(visible))).into_response()
        }
        Err(e) => consumer_error(e),
    }
}

/// Resolve the consumer's stream and require admin on it. Returns the
/// response to send instead (404 for unknown consumers, 403 on deny), or
/// `None` when allowed.
async fn authorize(
    state: &AppState,
    name: &str,
    identity: &Option<Extension<Arc<Identity>>>,
) -> Option<Response> {
    let Some(stream) = state.broker.consumers.stream_of(name).await else {
        return Some(consumer_error(ConsumerError::NotFound(name.to_string())));
    };
    let Extension(id) = identity.as_ref()?;
    match StreamName::try_from(stream.as_str()) {
        Ok(s) => super::require_scoped_admin(id, &s),
        Err(_) => Some(super::forbid()),
    }
}

/// Create a durable consumer. Idempotent for an identical spec.
#[utoipa::path(
    post,
    path = "/api/v1/consumers",
    tag = "consumers",
    security(("bearer" = [])),
    request_body = crate::openapi::ConsumerSpecDoc,
    responses(
        (status = 201, description = "Created (or already existed with this spec)", body = crate::openapi::ConsumerInfoDoc),
        (status = 400, description = "Invalid spec, or ephemeral", body = crate::openapi::ErrorBody),
        (status = 403, description = "No admin permission on the stream or DLQ stream", body = crate::openapi::ErrorBody),
        (status = 404, description = "No such stream", body = crate::openapi::ErrorBody),
        (status = 409, description = "Exists with a different spec", body = crate::openapi::ErrorBody),
        (status = 503, description = "Not the leader", body = crate::openapi::ErrorBody),
    )
)]
pub async fn create_consumer(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(spec): Json<ConsumerSpec>,
) -> Response {
    if spec.ephemeral {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "ephemeral consumers are tied to a TCP connection; create them over the client protocol"})),
        )
            .into_response();
    }
    if let Some(Extension(id)) = identity.as_ref() {
        for s in std::iter::once(&spec.stream).chain(spec.dlq_stream.as_ref()) {
            match StreamName::try_from(s.as_str()) {
                Ok(n) => {
                    if let Some(resp) = super::require_scoped_admin(id, &n) {
                        return resp;
                    }
                }
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": format!("invalid stream name '{s}': {e}")})),
                    )
                        .into_response()
                }
            }
        }
    }
    match state.broker.consumers.create(spec).await {
        Ok(info) => (StatusCode::CREATED, Json(json!(info))).into_response(),
        Err(e) => consumer_error(e),
    }
}

/// One consumer's spec and live state.
#[utoipa::path(
    get,
    path = "/api/v1/consumers/{name}",
    tag = "consumers",
    security(("bearer" = [])),
    params(("name" = String, Path, description = "Consumer name")),
    responses(
        (status = 200, description = "The consumer", body = crate::openapi::ConsumerInfoDoc),
        (status = 403, description = "No admin permission on its stream", body = crate::openapi::ErrorBody),
        (status = 404, description = "No such consumer", body = crate::openapi::ErrorBody),
    )
)]
pub async fn get_consumer(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = authorize(&state, &name, &identity).await {
        return r;
    }
    match state.broker.consumers.info(&name).await {
        Ok(info) => (StatusCode::OK, Json(json!(info))).into_response(),
        Err(e) => consumer_error(e),
    }
}

/// Delete a consumer; its subscriptions end with code 404.
#[utoipa::path(
    delete,
    path = "/api/v1/consumers/{name}",
    tag = "consumers",
    security(("bearer" = [])),
    params(("name" = String, Path, description = "Consumer name")),
    responses(
        (status = 200, description = "Deleted: `{\"deleted\": name}`", body = Object),
        (status = 403, description = "No admin permission on its stream", body = crate::openapi::ErrorBody),
        (status = 404, description = "No such consumer", body = crate::openapi::ErrorBody),
    )
)]
pub async fn delete_consumer(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = authorize(&state, &name, &identity).await {
        return r;
    }
    match state.broker.consumers.delete(&name).await {
        Ok(()) => (StatusCode::OK, Json(json!({"deleted": name}))).into_response(),
        Err(e) => consumer_error(e),
    }
}

/// Body of `POST /api/v1/consumers/{name}/seek`: `"earliest"`,
/// `"latest"`, `{"offset": n}` or `{"timestamp_ms": t}`.
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SeekBody {
    Earliest,
    Latest,
    Offset(u64),
    /// Unix milliseconds.
    TimestampMs(u64),
}

/// Reposition a consumer; drops its unacked state.
#[utoipa::path(
    post,
    path = "/api/v1/consumers/{name}/seek",
    tag = "consumers",
    security(("bearer" = [])),
    params(("name" = String, Path, description = "Consumer name")),
    request_body = SeekBody,
    responses(
        (status = 200, description = "The consumer after the seek", body = crate::openapi::ConsumerInfoDoc),
        (status = 403, description = "No admin permission on its stream", body = crate::openapi::ErrorBody),
        (status = 404, description = "No such consumer", body = crate::openapi::ErrorBody),
    )
)]
pub async fn seek_consumer(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(body): Json<SeekBody>,
) -> Response {
    if let Some(r) = authorize(&state, &name, &identity).await {
        return r;
    }
    let to = match body {
        SeekBody::Earliest => SeekTo::Earliest,
        SeekBody::Latest => SeekTo::Latest,
        SeekBody::Offset(o) => SeekTo::Offset(o),
        SeekBody::TimestampMs(ms) => SeekTo::Time(ms),
    };
    match state.broker.consumers.seek(&name, to).await {
        Ok(()) => match state.broker.consumers.info(&name).await {
            Ok(info) => (StatusCode::OK, Json(json!(info))).into_response(),
            Err(e) => consumer_error(e),
        },
        Err(e) => consumer_error(e),
    }
}
