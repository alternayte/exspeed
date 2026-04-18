use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::state::AppState;

/// Reject the request with 401 and a JSON body.
fn unauthorized(msg: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": msg})),
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

    let header = match req.headers().get(AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        Some(h) => h,
        None => return unauthorized("missing Authorization header"),
    };
    let token = match header.strip_prefix("Bearer ") {
        Some(t) => t,
        None => return unauthorized("invalid Authorization header"),
    };

    if !constant_time_eq::constant_time_eq(token.as_bytes(), expected.as_bytes()) {
        return unauthorized("unauthorized");
    }

    next.run(req).await
}
