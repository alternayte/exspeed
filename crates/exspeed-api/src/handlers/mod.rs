pub mod connections;
pub mod connectors;
pub mod consumers;
pub mod health;
pub mod metrics;
pub mod queries;
pub mod streams;
pub mod webhooks;

use std::sync::Arc;

use axum::routing::{delete, get, post};
use axum::Router;

use crate::state::AppState;

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route(
            "/api/v1/streams",
            get(streams::list_streams).post(streams::create_stream),
        )
        .route("/api/v1/streams/{name}", get(streams::get_stream))
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
        .route("/webhooks/{*path}", post(webhooks::handle_webhook))
        .route("/metrics", get(metrics::prometheus_metrics))
        .with_state(state)
}
