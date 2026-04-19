use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::state::AppState;

/// `GET /healthz` — leader-aware readiness probe.
///
/// Returns `200` with `{"leader": true, "holder": "<uuid>"}` when this pod
/// holds the cluster-leader lease, `503` with `{"leader": false}`
/// otherwise. Load balancers should probe this endpoint and route traffic
/// only to pods returning 200.
pub async fn healthz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let leader = state.leadership.is_currently_leader();
    let status = if leader {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "leader": leader,
            "holder": state.leadership.holder_id.to_string(),
        })),
    )
}

/// `GET /readyz` — alias for `/healthz`. Kept for backward compatibility
/// with any deployment that probes `/readyz`. Same semantics.
pub async fn readyz(state: State<Arc<AppState>>) -> impl IntoResponse {
    healthz(state).await
}
