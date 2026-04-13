use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::state::AppState;

pub async fn list_consumers(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    (StatusCode::OK, Json(json!([])))
}

pub async fn get_consumer(
    State(_state): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": "not implemented"})),
    )
}

pub async fn delete_consumer(
    State(_state): State<Arc<AppState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": "not implemented"})),
    )
}
