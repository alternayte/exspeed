use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::state::AppState;

/// `GET /api/v1/leases` — operator visibility into currently-held leases.
///
/// Returns a JSON array of live lease records (`name`, `holder`, `epoch`,
/// `expires_at`, `replication_endpoint`, `client_endpoint`, `isr`). Empty in
/// single-node mode.
#[utoipa::path(
    get,
    path = "/api/v1/leases",
    tag = "cluster",
    security(("bearer" = [])),
    responses(
        (status = 200, description = "Live leases (empty in single-node mode)", body = Vec<crate::openapi::LeaseRecord>),
        (status = 500, description = "Lease backend error", body = crate::openapi::ErrorBody),
    )
)]
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
