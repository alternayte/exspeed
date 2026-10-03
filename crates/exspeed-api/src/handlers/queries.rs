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

#[derive(Deserialize)]
pub struct ExecuteQueryRequest {
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
pub async fn delete_query(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(r) = deny(identity) {
        return r;
    }
    match state.exql.drop_query(&id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"status": "removed", "query_id": id})),
        )
            .into_response(),
        Err(e) => exql_error(&e),
    }
}

/// POST /api/v1/queries/{id}/pause
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
