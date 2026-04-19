use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use serde_json::json;

use exspeed_processing::types::Value;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct ExecuteQueryRequest {
    pub sql: String,
}

#[derive(Deserialize)]
pub struct CreateContinuousRequest {
    pub sql: String,
}

/// Convert an ExQL `Value` to a `serde_json::Value`.
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(n) => json!(n),
        Value::Float(f) => json!(f),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Json(j) => j.clone(),
        Value::Timestamp(ts) => json!(ts),
    }
}

/// POST /api/v1/queries
///
/// Execute a bounded (batch) SQL query and return the result set.
pub async fn execute_query(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ExecuteQueryRequest>,
) -> impl IntoResponse {
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
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        ),
    }
}

/// POST /api/v1/queries/continuous
///
/// Create and start a continuous query.
pub async fn create_continuous(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateContinuousRequest>,
) -> impl IntoResponse {
    match state.exql.create_continuous(&body.sql).await {
        Ok(query_id) => (
            StatusCode::CREATED,
            Json(json!({"query_id": query_id, "status": "running"})),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        ),
    }
}

/// GET /api/v1/queries
///
/// List all registered continuous queries.
pub async fn list_queries(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let queries = state.exql.list_queries();
    (StatusCode::OK, Json(json!(queries)))
}

/// GET /api/v1/queries/{id}
///
/// Get details for a single query.
pub async fn get_query(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.exql.query_registry.get_snapshot(&id) {
        Some(snapshot) => (StatusCode::OK, Json(json!(snapshot))),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("query '{id}' not found")})),
        ),
    }
}

/// DELETE /api/v1/queries/{id}
///
/// Remove a continuous query (stops it if running, deletes from disk).
pub async fn delete_query(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.exql.remove_query(&id) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"status": "removed", "query_id": id})),
        ),
        Err(e) => (StatusCode::NOT_FOUND, Json(json!({"error": e.to_string()}))),
    }
}
