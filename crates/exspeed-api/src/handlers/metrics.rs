use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::state::AppState;

pub async fn prometheus_metrics(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    (StatusCode::OK, "# no metrics yet\n")
}
