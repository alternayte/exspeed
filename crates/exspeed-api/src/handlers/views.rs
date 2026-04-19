use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use serde_json::json;

use exspeed_processing::types::Value;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateViewRequest {
    pub sql: String,
}

#[derive(Deserialize)]
pub struct GetViewParams {
    pub key: Option<String>,
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

/// GET /api/v1/views
///
/// List all registered materialized views.
pub async fn list_views(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let infos = state.exql.mv_registry.list();
    (StatusCode::OK, Json(json!(infos)))
}

/// GET /api/v1/views/{name}
///
/// Return rows for a materialized view. With `?key=<k>` returns a single row;
/// without it returns all rows.
pub async fn get_view(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(params): Query<GetViewParams>,
) -> impl IntoResponse {
    if let Some(key) = params.key {
        // Single-row lookup
        match state.exql.mv_registry.get_row(&name, &key) {
            Some(row) => {
                let values: Vec<serde_json::Value> = row.values.iter().map(value_to_json).collect();
                (
                    StatusCode::OK,
                    Json(json!({
                        "columns": row.columns,
                        "row": values,
                    })),
                )
            }
            None => (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("key '{key}' not found in view '{name}'")})),
            ),
        }
    } else {
        // All-rows lookup
        match state.exql.mv_registry.get_rows(&name) {
            Some((columns, rows)) => {
                let json_rows: Vec<Vec<serde_json::Value>> = rows
                    .iter()
                    .map(|row| row.values.iter().map(value_to_json).collect())
                    .collect();
                let row_count = json_rows.len();
                (
                    StatusCode::OK,
                    Json(json!({
                        "columns": columns,
                        "rows": json_rows,
                        "row_count": row_count,
                    })),
                )
            }
            None => (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("view '{name}' not found")})),
            ),
        }
    }
}

/// POST /api/v1/views
///
/// Create and start a materialized view.
pub async fn create_view(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateViewRequest>,
) -> impl IntoResponse {
    match state.exql.create_materialized_view(&body.sql).await {
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
