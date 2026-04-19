use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::state::AppState;

/// Reject the request with 401 + the RFC 7235 `WWW-Authenticate: Bearer`
/// header. The body is always the same opaque `"unauthorized"` so the
/// failure reason (missing header / wrong scheme / wrong token) is not
/// exposed to an attacker.
fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [("www-authenticate", "Bearer")],
        Json(json!({"error": "unauthorized"})),
    )
        .into_response()
}

/// Axum middleware. Requires `Authorization: Bearer <token>` matching
/// `state.auth_token`. If `state.auth_token` is None, auth is off and every
/// request passes through.
pub async fn require_bearer(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let Some(expected) = state.auth_token.as_deref() else {
        return next.run(req).await;
    };

    let Some(header) = req.headers().get(AUTHORIZATION).and_then(|v| v.to_str().ok()) else {
        return unauthorized();
    };
    let Some(token) = header.strip_prefix("Bearer ") else {
        return unauthorized();
    };

    if !exspeed_common::auth::verify_token(token.as_bytes(), expected) {
        return unauthorized();
    }

    next.run(req).await
}

/// Axum middleware that returns 503 when this pod is not the cluster
/// leader. Apply AFTER `require_bearer` so standby pods don't leak
/// "who's the leader" discovery to unauthenticated callers. The
/// `/api/v1/leases` route is routed OUTSIDE this middleware so operators
/// can discover the current leader from any pod.
pub async fn leader_gate(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if state.leadership.is_currently_leader() {
        return next.run(req).await;
    }
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": "not leader",
            "hint": "this pod is a standby. GET /api/v1/leases on any pod to discover the current leader.",
        })),
    )
        .into_response()
}
