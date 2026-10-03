use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::state::AppState;

/// `GET /api/v1/cluster` — this node's view of the cluster: node id, role,
/// leader epoch, the leader's endpoints, and either the ISR plus follower
/// progress (on the leader) or the replication session (on a follower).
/// Answered by every node, leader or not.
pub async fn status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.cluster.as_ref() {
        Some(c) => (StatusCode::OK, Json(c.status().await)).into_response(),
        None => (
            StatusCode::OK,
            Json(json!({
                "node_id": state.leadership.node_id,
                "role": "standalone",
            })),
        )
            .into_response(),
    }
}
