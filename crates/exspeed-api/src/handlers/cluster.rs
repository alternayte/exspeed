use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::state::AppState;

/// `GET /api/v1/cluster/followers` — snapshot of currently-connected
/// replication followers on this leader.
///
/// Gating:
/// - Admin-bearer gated via the `require_admin` middleware.
/// - Leader-only via the `leader_gate` middleware applied to the same
///   authenticated router.
/// - Multi-pod mode gated here: if this pod has no attached
///   `ReplicationCoordinator` (the Wave 5 startup wiring only builds
///   one when `EXSPEED_CONSUMER_STORE=postgres|redis`), we return 503
///   with a hint pointing at the env var. A single-pod pod has nothing
///   meaningful to return; 503 makes the "you probably wanted to turn
///   replication on" signal explicit.
///
/// Success body: `Vec<FollowerSnapshot>` — `follower_id` + UTC
/// `registered_at` per connected follower. Richer data (lag seconds,
/// cursor summary, records applied) lands in a later wave.
pub async fn list_followers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let Some(coord) = state.replication_coordinator.as_ref() else {
        // Deliberately verbose: the 503 here almost always means "the
        // operator started the pod without setting EXSPEED_CONSUMER_STORE"
        // and pointing at the fix inline saves one round-trip to docs.
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "not a multi-pod deployment",
                "hint": "set EXSPEED_CONSUMER_STORE=postgres|redis to enable replication",
            })),
        )
            .into_response();
    };

    (StatusCode::OK, Json(coord.snapshot())).into_response()
}
