use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use exspeed_common::auth::Identity;
use exspeed_processing::ExqlError;
use serde::Deserialize;
use serde_json::json;

use crate::state::AppState;

#[derive(Deserialize, utoipa::ToSchema)]
pub struct ExecuteQueryRequest {
    /// One ExQL statement.
    pub sql: String,
}

/// JSON error response with the error's suggested status code.
pub(crate) fn exql_error(e: &ExqlError) -> Response {
    let status = StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::BAD_REQUEST);
    (status, Json(e.to_json())).into_response()
}

fn deny(identity: Option<Extension<Arc<Identity>>>) -> Option<Response> {
    identity.and_then(|Extension(id)| super::require_global_admin(&id))
}

/// POST /api/v1/queries
///
/// Run any ExQL statement. A bounded query returns
/// `{columns, rows, row_count, execution_time_ms, truncated}`; `CREATE
/// STREAM/TABLE` returns the new query (201); `DROP …`, `PAUSE QUERY` and
/// `RESUME QUERY` return their outcome. If the client disconnects, the
/// query is cancelled (the handler future is dropped).
#[utoipa::path(
    post,
    path = "/api/v1/queries",
    tag = "queries",
    security(("bearer" = [])),
    request_body = ExecuteQueryRequest,
    responses(
        (status = 200, description = "Rows of a bounded query (`{columns, rows, row_count, execution_time_ms, truncated}`), or the outcome of DROP/PAUSE/RESUME", body = Object),
        (status = 201, description = "CREATE STREAM/TABLE: the new query", body = crate::openapi::QueryInfoDoc),
        (status = 400, description = "Parse, plan or unsupported-statement error", body = crate::openapi::ExqlErrorBody),
        (status = 404, description = "Unknown stream, table or query", body = crate::openapi::ExqlErrorBody),
        (status = 408, description = "Timed out", body = crate::openapi::ExqlErrorBody),
        (status = 409, description = "Conflict", body = crate::openapi::ExqlErrorBody),
        (status = 422, description = "Memory limit exceeded", body = crate::openapi::ExqlErrorBody),
        (status = 503, description = "Not the leader", body = crate::openapi::ExqlErrorBody),
    )
)]
pub async fn execute_query(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(body): Json<ExecuteQueryRequest>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    match state.exql.execute(&body.sql).await {
        Ok(res) => {
            let status = StatusCode::from_u16(res.http_status()).unwrap_or(StatusCode::OK);
            (status, Json(res.to_json())).into_response()
        }
        Err(e) => exql_error(&e),
    }
}

/// POST /api/v1/queries/continuous
///
/// Create a continuous query (`CREATE STREAM|TABLE … AS SELECT …`).
#[utoipa::path(
    post,
    path = "/api/v1/queries/continuous",
    tag = "queries",
    security(("bearer" = [])),
    request_body = ExecuteQueryRequest,
    responses(
        (status = 201, description = "The new query", body = crate::openapi::QueryInfoDoc),
        (status = 400, description = "Not a CREATE statement, or invalid", body = crate::openapi::ExqlErrorBody),
        (status = 409, description = "Name taken", body = crate::openapi::ExqlErrorBody),
        (status = 503, description = "Not the leader", body = crate::openapi::ExqlErrorBody),
    )
)]
pub async fn create_continuous(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(body): Json<ExecuteQueryRequest>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    match state.exql.create_continuous(&body.sql).await {
        Ok(info) => {
            let mut v = json!(info);
            v["query_id"] = json!(info.id);
            (StatusCode::CREATED, Json(v)).into_response()
        }
        Err(e) => exql_error(&e),
    }
}

/// GET /api/v1/queries
#[utoipa::path(
    get,
    path = "/api/v1/queries",
    tag = "queries",
    security(("bearer" = [])),
    responses(
        (status = 200, description = "Continuous queries", body = Vec<crate::openapi::QueryInfoDoc>),
    )
)]
pub async fn list_queries(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    (StatusCode::OK, Json(json!(state.exql.list_queries()))).into_response()
}

/// GET /api/v1/queries/{id}
#[utoipa::path(
    get,
    path = "/api/v1/queries/{id}",
    tag = "queries",
    security(("bearer" = [])),
    params(("id" = String, Path, description = "Query id")),
    responses(
        (status = 200, description = "The query", body = crate::openapi::QueryInfoDoc),
        (status = 404, description = "No such query", body = crate::openapi::ExqlErrorBody),
    )
)]
pub async fn get_query(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    match state.exql.info(&id) {
        Ok(q) => (StatusCode::OK, Json(json!(q))).into_response(),
        Err(e) => exql_error(&e),
    }
}

/// DELETE /api/v1/queries/{id}
///
/// `DROP QUERY`: stop the query and forget it. Its output stream is kept.
#[utoipa::path(
    delete,
    path = "/api/v1/queries/{id}",
    tag = "queries",
    security(("bearer" = [])),
    params(("id" = String, Path, description = "Query id")),
    responses(
        (status = 200, description = "`{\"status\": \"removed\", \"query_id\": id}`", body = Object),
        (status = 404, description = "No such query", body = crate::openapi::ExqlErrorBody),
    )
)]
pub async fn delete_query(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    match state.exql.drop_query(&id).await {
        Ok(()) => {
            state.metrics.forget_query(&id);
            (
                StatusCode::OK,
                Json(json!({"status": "removed", "query_id": id})),
            )
                .into_response()
        }
        Err(e) => exql_error(&e),
    }
}

/// POST /api/v1/queries/{id}/pause
#[utoipa::path(
    post,
    path = "/api/v1/queries/{id}/pause",
    tag = "queries",
    security(("bearer" = [])),
    params(("id" = String, Path, description = "Query id")),
    responses(
        (status = 200, description = "The paused query", body = crate::openapi::QueryInfoDoc),
        (status = 404, description = "No such query", body = crate::openapi::ExqlErrorBody),
    )
)]
pub async fn pause_query(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    match state.exql.pause_query(&id).await {
        Ok(q) => (StatusCode::OK, Json(json!(q))).into_response(),
        Err(e) => exql_error(&e),
    }
}

/// POST /api/v1/queries/{id}/resume
#[utoipa::path(
    post,
    path = "/api/v1/queries/{id}/resume",
    tag = "queries",
    security(("bearer" = [])),
    params(("id" = String, Path, description = "Query id")),
    responses(
        (status = 200, description = "The resumed query", body = crate::openapi::QueryInfoDoc),
        (status = 404, description = "No such query", body = crate::openapi::ExqlErrorBody),
    )
)]
pub async fn resume_query(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    match state.exql.resume_query(&id).await {
        Ok(q) => (StatusCode::OK, Json(json!(q))).into_response(),
        Err(e) => exql_error(&e),
    }
}
