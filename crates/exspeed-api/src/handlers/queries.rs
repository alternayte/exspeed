use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use exspeed_common::auth::Identity;
use serde::Deserialize;
use serde_json::json;

use exspeed_processing::types::value_to_json;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct ExecuteQueryRequest {
    pub sql: String,
}

#[derive(Deserialize)]
pub struct CreateContinuousRequest {
    pub sql: String,
}

/// POST /api/v1/queries
///
/// Execute a bounded (batch) SQL query and return the result set.
pub async fn execute_query(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(body): Json<ExecuteQueryRequest>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    let sql = body.sql;

    let result = state.exql.execute_bounded(&sql).await;

    match result {
        Ok(result_set) => {
            let rows: Vec<Vec<serde_json::Value>> = result_set
                .rows
                .iter()
                .map(|row| row.values.iter().map(value_to_json).collect())
                .collect();
            let row_count = rows.len();

            (
                StatusCode::OK,
                Json(json!({
                    "columns": result_set.columns,
                    "rows": rows,
                    "row_count": row_count,
                    "execution_time_ms": result_set.execution_time_ms,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(e.to_json()),
        )
            .into_response(),
    }
}

/// POST /api/v1/queries/continuous
///
/// Create and start a continuous query.
pub async fn create_continuous(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
    Json(body): Json<CreateContinuousRequest>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    match state.exql.create_continuous(&body.sql).await {
        Ok(query_id) => (
            StatusCode::CREATED,
            Json(json!({"query_id": query_id, "status": "running"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(e.to_json()),
        )
            .into_response(),
    }
}

/// GET /api/v1/queries
///
/// List all registered continuous queries.
pub async fn list_queries(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    let queries = state.exql.list_queries();
    (StatusCode::OK, Json(json!(queries))).into_response()
}

/// GET /api/v1/queries/{id}
///
/// Get details for a single query.
pub async fn get_query(
    State(state): State<Arc<AppState>>,
    Path(id_path): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    match state.exql.query_registry.get_snapshot(&id_path) {
        Some(snapshot) => (StatusCode::OK, Json(json!(snapshot))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("query '{id_path}' not found")})),
        )
            .into_response(),
    }
}

/// DELETE /api/v1/queries/{id}
///
/// Remove a continuous query (stops it if running, deletes from disk).
pub async fn delete_query(
    State(state): State<Arc<AppState>>,
    Path(id_path): Path<String>,
    identity: Option<Extension<Arc<Identity>>>,
) -> Response {
    if let Some(Extension(id)) = identity {
        if let Some(resp) = super::require_global_admin(&id) {
            return resp;
        }
    }
    match state.exql.remove_query(&id_path) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"status": "removed", "query_id": id_path})),
        )
            .into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(json!({"error": e.to_string()}))).into_response(),
    }
}
