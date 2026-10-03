pub mod backup;
pub mod cluster;
pub mod connections;
pub mod connectors;
pub mod consumers;
pub mod health;
pub mod leases;
pub mod metrics;
pub mod queries;
pub mod streams;
pub mod views;
pub mod webhooks;
pub mod whoami;

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Json;
use axum::Router;
use exspeed_common::auth::{Action, Identity};
use exspeed_common::types::StreamName;
use serde_json::json;

use crate::state::AppState;

/// Standard 403 response body used by every scoped-admin and global-admin
/// handler gate. Body is opaque to match the 401 convention: we don't
/// leak which stream / action was denied.
pub(crate) fn forbid() -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": "forbidden"}))).into_response()
}

/// Verify the authenticated identity holds `Admin` on the given stream.
/// Returns `Some(403 response)` on deny so callers can early-return via
/// `if let Some(r) = … { return r; }`. Returns `None` on allow.
pub(crate) fn require_scoped_admin(id: &Identity, stream: &StreamName) -> Option<Response> {
    if id.authorize(Action::Admin, stream) {
        None
    } else {
        Some(forbid())
    }
}

/// Internal `__` streams (consumer state, catalogs, offsets) are written
/// only by the server itself: every client write to one — create, update,
/// publish, delete — is refused with 403, as over TCP.
pub(crate) fn forbid_internal_write(stream: &StreamName) -> Option<Response> {
    if stream.is_internal() {
        Some(
            (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "internal streams are written only by the server"})),
            )
                .into_response(),
        )
    } else {
        None
    }
}

/// Verify the authenticated identity holds `Admin` with a wildcard-all
/// glob (`streams = "*"`). Used for endpoints that span multiple streams
/// (connectors, queries, views, connections).
pub(crate) fn require_global_admin(id: &Identity) -> Option<Response> {
    if id.has_global_admin() {
        None
    } else {
        Some(forbid())
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    use axum::middleware::from_fn_with_state;

    // Unauthenticated routes: probes, metrics, the OpenAPI document (it
    // describes the API, not its data), webhooks (webhooks carry their own
    // per-webhook auth — see handlers/webhooks.rs). Every route added to
    // this function must also be listed in `crate::openapi::ApiDoc`; the
    // `openapi_test` integration test enforces it.
    let unauth = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/metrics", get(metrics::prometheus_metrics))
        .route("/api/v1/openapi.json", get(crate::openapi::openapi_json))
        .route("/webhooks/{*path}", post(webhooks::handle_webhook))
        .with_state(state.clone());

    // Leases endpoint: bearer-gated but NOT leader-gated. Any pod can
    // answer "who's the leader?".
    let leases_router = Router::new()
        .route("/api/v1/leases", get(leases::list_leases))
        .route("/api/v1/cluster", get(cluster::status))
        .layer(from_fn_with_state(
            state.clone(),
            crate::middleware::require_admin,
        ))
        .with_state(state.clone());

    // Whoami endpoint: the one non-admin HTTP surface. Any authenticated
    // caller — including scoped publish/subscribe-only credentials — can
    // call it to inspect their own identity + permissions. NOT leader-gated
    // so any pod can answer "who am I?".
    let whoami_router = Router::new()
        .route("/api/v1/whoami", get(whoami::whoami))
        .layer(from_fn_with_state(
            state.clone(),
            crate::middleware::require_authenticated,
        ))
        .with_state(state.clone());

    // Record browsing: any authenticated caller (the handler requires
    // subscribe or admin on the stream), and NOT leader-gated: reads are
    // served by every node from its replica, like TCP reads.
    let readers_router = Router::new()
        .route("/api/v1/streams/{name}/records", get(streams::read_records))
        .layer(from_fn_with_state(
            state.clone(),
            crate::middleware::require_authenticated,
        ))
        .with_state(state.clone());

    // Main authenticated router: admin-gated AND leader-gated.
    // Tower wrapping: `.layer(A).layer(B)` produces `B(A(handler))`, so
    // B (the LAST .layer() call) is the outermost wrapper and runs first.
    // We want require_admin -> leader_gate -> handler, so require_admin
    // must be the LAST .layer() call.
    let authed = Router::new()
        .route(
            "/api/v1/streams",
            get(streams::list_streams).post(streams::create_stream),
        )
        .route(
            "/api/v1/streams/{name}",
            get(streams::get_stream)
                .patch(streams::patch_stream)
                .delete(streams::delete_stream),
        )
        .route(
            "/api/v1/streams/{name}/publish",
            post(streams::publish_to_stream),
        )
        .route(
            "/api/v1/consumers",
            get(consumers::list_consumers).post(consumers::create_consumer),
        )
        .route(
            "/api/v1/consumers/{name}",
            get(consumers::get_consumer).delete(consumers::delete_consumer),
        )
        .route(
            "/api/v1/consumers/{name}/seek",
            post(consumers::seek_consumer),
        )
        .route(
            "/api/v1/connectors",
            get(connectors::list_connectors).post(connectors::create_connector),
        )
        .route(
            "/api/v1/connectors/{name}",
            get(connectors::get_connector)
                .put(connectors::update_connector)
                .delete(connectors::delete_connector),
        )
        .route(
            "/api/v1/connectors/{name}/restart",
            post(connectors::restart_connector),
        )
        .route(
            "/api/v1/views",
            get(views::list_views).post(views::create_view),
        )
        .route("/api/v1/views/{name}", get(views::get_view))
        .route(
            "/api/v1/queries",
            get(queries::list_queries).post(queries::execute_query),
        )
        .route(
            "/api/v1/queries/continuous",
            post(queries::create_continuous),
        )
        .route(
            "/api/v1/queries/{id}",
            get(queries::get_query).delete(queries::delete_query),
        )
        .route("/api/v1/queries/{id}/pause", post(queries::pause_query))
        .route("/api/v1/queries/{id}/resume", post(queries::resume_query))
        .route(
            "/api/v1/connections",
            get(connections::list_connections).post(connections::create_connection),
        )
        .route(
            "/api/v1/connections/{name}",
            delete(connections::delete_connection),
        )
        .route("/api/v1/backup", get(backup::backup))
        .layer(from_fn_with_state(
            state.clone(),
            crate::middleware::leader_gate,
        ))
        .layer(from_fn_with_state(
            state.clone(),
            crate::middleware::require_admin,
        ))
        .with_state(state);

    unauth
        .merge(leases_router)
        .merge(whoami_router)
        .merge(readers_router)
        .merge(authed)
}
