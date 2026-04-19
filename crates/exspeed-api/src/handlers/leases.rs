use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::state::AppState;

/// `GET /api/v1/leases` — operator visibility into currently-held leases.
///
/// Returns a JSON array of `LeaseInfo` records (name, holder UUID, expiry).
/// Under the Noop backend this is always empty. Under postgres/redis it
/// lists every lease currently alive in the backend, regardless of which
/// pod holds it.
pub async fn list_leases(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.lease.list_all().await {
        Ok(leases) => (StatusCode::OK, Json(leases)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("lease backend: {e}")})),
        )
            .into_response(),
    }
}
