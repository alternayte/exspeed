pub mod consumers;
pub mod health;
pub mod metrics;
pub mod streams;

use std::sync::Arc;

use axum::routing::get;
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
        .route("/metrics", get(metrics::prometheus_metrics))
        .with_state(state)
}
