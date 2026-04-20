use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use sha2::{Digest, Sha256};

use serde_json::json;

use crate::state::AppState;

/// Reject the request with 401 + the RFC 7235 `WWW-Authenticate: Bearer`
/// header. The body is always the same opaque `"unauthorized"` so the
/// failure reason (missing header / wrong scheme / bad token) is not
/// exposed to an attacker.
fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [("www-authenticate", "Bearer")],
        Json(json!({"error": "unauthorized"})),
    )
        .into_response()
}

fn forbidden() -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": "forbidden"}))).into_response()
}

/// Extract a `Bearer <token>` value from the `Authorization` header.
fn extract_bearer<B>(req: &Request<B>) -> Option<String> {
    let v = req.headers().get(AUTHORIZATION)?.to_str().ok()?;
    v.strip_prefix("Bearer ").map(|t| t.to_string())
}

/// Axum middleware. Requires a valid `Authorization: Bearer <token>` that
/// hashes to an identity known to `state.credential_store`. The identity is
/// attached to request extensions for downstream handlers.
///
/// When `state.credential_store` is `None`, auth is globally disabled and
/// the request passes through WITHOUT an identity extension — handlers must
/// tolerate a missing identity in that case.
///
/// Does NOT check for admin privileges; use `require_admin` for the admin
/// HTTP surface. Used by `/api/v1/whoami` (Task 5), which is intended for
/// any authenticated caller.
pub async fn require_authenticated(
    State(state): State<Arc<AppState>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let Some(store) = state.credential_store.as_ref() else {
        return next.run(req).await; // auth off globally
    };

    let Some(raw) = extract_bearer(&req) else {
        return unauthorized();
    };
    let digest: [u8; 32] = Sha256::digest(raw.as_bytes()).into();
    let Some(identity) = store.lookup(&digest) else {
        state
            .metrics
            .auth_denied("unauthorized", "http", req.uri().path());
        return unauthorized();
    };

    // Record the authenticated name on the current tracing span (no-op if
    // the surrounding request has no span, e.g. in tests). Lets log
    // aggregators slice HTTP events by tenant when a request span is in
    // scope.
    tracing::Span::current().record("identity", identity.name.as_str());
    req.extensions_mut().insert(identity);
    next.run(req).await
}

/// Axum middleware. Same as `require_authenticated` but additionally gates
/// on `identity.has_any_admin_permission()`. Used on every admin HTTP
/// surface (streams, consumers, connectors, queries, views, connections).
/// Handlers then apply finer-grained scoped-admin or global-admin checks.
pub async fn require_admin(
    State(state): State<Arc<AppState>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let Some(store) = state.credential_store.as_ref() else {
        return next.run(req).await; // auth off globally
    };

    let Some(raw) = extract_bearer(&req) else {
        return unauthorized();
    };
    let digest: [u8; 32] = Sha256::digest(raw.as_bytes()).into();
    let Some(identity) = store.lookup(&digest) else {
        state
            .metrics
            .auth_denied("unauthorized", "http", req.uri().path());
        return unauthorized();
    };

    if !identity.has_any_admin_permission() {
        state
            .metrics
            .auth_denied("forbidden", "http", req.uri().path());
        return forbidden();
    }

    // Same rationale as `require_authenticated` above — record on current
    // span so downstream handler logs carry the identity field.
    tracing::Span::current().record("identity", identity.name.as_str());
    req.extensions_mut().insert(identity);
    next.run(req).await
}

/// Axum middleware that returns 503 when this pod is not the cluster
/// leader. Apply AFTER `require_admin` so standby pods don't leak
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
