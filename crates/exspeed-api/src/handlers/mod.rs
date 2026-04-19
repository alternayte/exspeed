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

use std::sync::Arc;

use axum::routing::{delete, get, post};
use axum::Router;

use crate::state::AppState;

pub fn build_router(state: Arc<AppState>) -> Router {
    use axum::middleware::from_fn_with_state;

    // Unauthenticated routes: probes, metrics, webhooks (webhooks carry their
    // own per-webhook auth — see handlers/webhooks.rs).
    let unauth = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/metrics", get(metrics::prometheus_metrics))
        .route("/webhooks/{*path}", post(webhooks::handle_webhook))
        .with_state(state.clone());

    // Leases endpoint: bearer-gated but NOT leader-gated. Any pod can
    // answer "who's the leader?".
    let leases_router = Router::new()
        .route("/api/v1/leases", get(leases::list_leases))
        .layer(from_fn_with_state(state.clone(), crate::middleware::require_bearer))
        .with_state(state.clone());

    // Main authenticated router: bearer-gated AND leader-gated.
    // Tower wrapping: `.layer(A).layer(B)` produces `B(A(handler))`, so
    // B (the LAST .layer() call) is the outermost wrapper and runs first.
    // We want require_bearer -> leader_gate -> handler, so require_bearer
    // must be the LAST .layer() call.
    let authed = Router::new()
        .route(
            "/api/v1/streams",
            get(streams::list_streams).post(streams::create_stream),
        )
        .route(
            "/api/v1/streams/{name}",
            get(streams::get_stream).patch(streams::patch_stream),
        )
        .route(
            "/api/v1/streams/{name}/publish",
            post(streams::publish_to_stream),
        )
        .route("/api/v1/consumers", get(consumers::list_consumers))
        .route(
            "/api/v1/consumers/{name}",
            get(consumers::get_consumer).delete(consumers::delete_consumer),
        )
        .route(
            "/api/v1/connectors",
            get(connectors::list_connectors).post(connectors::create_connector),
        )
        .route(
            "/api/v1/connectors/{name}",
            get(connectors::get_connector).delete(connectors::delete_connector),
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
        .route(
            "/api/v1/connections",
            get(connections::list_connections).post(connections::create_connection),
        )
        .route(
            "/api/v1/connections/{name}",
            delete(connections::delete_connection),
        )
        .layer(from_fn_with_state(state.clone(), crate::middleware::leader_gate))
        .layer(from_fn_with_state(state.clone(), crate::middleware::require_bearer))
        .with_state(state);

    unauth.merge(leases_router).merge(authed)
}
