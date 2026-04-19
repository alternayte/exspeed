pub mod handlers;
pub mod middleware;
pub mod state;

pub use state::AppState;

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Paths to a PEM-encoded cert chain and its private key, shared between
/// the TCP TLS listener (in the exspeed binary) and the HTTP TLS listener
/// (this crate).
#[derive(Debug, Clone)]
pub struct TlsPaths {
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl TlsPaths {
    /// Validate the "both-or-neither" constraint on optional cert/key paths.
    ///
    /// Returns:
    /// - `Ok(Some(TlsPaths))` when both are provided.
    /// - `Ok(None)` when neither is provided.
    /// - `Err(...)` when exactly one is provided.
    pub fn from_args(
        cert: Option<&std::path::Path>,
        key: Option<&std::path::Path>,
    ) -> anyhow::Result<Option<Self>> {
        match (cert, key) {
            (Some(c), Some(k)) => Ok(Some(Self {
                cert: c.to_path_buf(),
                key: k.to_path_buf(),
            })),
            (None, None) => Ok(None),
            _ => anyhow::bail!(
                "TLS configuration invalid: EXSPEED_TLS_CERT and EXSPEED_TLS_KEY must both be set or both unset"
            ),
        }
    }
}

/// Serve the HTTP API. When `tls` is Some, uses axum-server with rustls;
/// otherwise plain HTTP. Returns on error.
///
/// This variant runs until the listener fails fatally — it never receives a
/// shutdown signal. Use `serve_with_shutdown` from the binary so SIGTERM can
/// drain the API alongside the TCP listener.
pub async fn serve(
    state: Arc<AppState>,
    addr: SocketAddr,
    tls: Option<TlsPaths>,
) -> std::io::Result<()> {
    serve_with_shutdown(state, addr, tls, std::future::pending()).await
}

/// Serve the HTTP API until either the listener fails or `shutdown`
/// resolves. When `shutdown` resolves, axum-server stops accepting new
/// connections and gives in-flight requests up to 10s to complete before
/// closing them. Mirrors the 10s drain budget the TCP listener uses so the
/// two halves of the server tear down in the same window.
pub async fn serve_with_shutdown<F>(
    state: Arc<AppState>,
    addr: SocketAddr,
    tls: Option<TlsPaths>,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let router = handlers::build_router(state);

    // axum-server's Handle is the only documented hook for graceful
    // shutdown — bind/bind_rustls don't take a shutdown future directly.
    // Spawn a forwarder so the caller's shutdown future drives the handle.
    let handle = axum_server::Handle::new();
    {
        let handle_for_shutdown = handle.clone();
        tokio::spawn(async move {
            shutdown.await;
            handle_for_shutdown.graceful_shutdown(Some(Duration::from_secs(10)));
        });
    }

    match tls {
        Some(paths) => {
            let cfg = axum_server::tls_rustls::RustlsConfig::from_pem_file(
                &paths.cert, &paths.key,
            )
            .await?;
            info!("HTTP API listening on {} (TLS)", addr);
            axum_server::bind_rustls(addr, cfg)
                .handle(handle)
                .serve(router.into_make_service())
                .await
        }
        None => {
            info!("HTTP API listening on {}", addr);
            axum_server::bind(addr)
                .handle(handle)
                .serve(router.into_make_service())
                .await
        }
    }
}
