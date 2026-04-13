pub mod handlers;
pub mod state;

pub use state::AppState;

use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

pub async fn serve(state: Arc<AppState>, addr: SocketAddr) {
    let router = handlers::build_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    info!("HTTP API listening on {}", addr);
    axum::serve(listener, router).await.unwrap();
}
