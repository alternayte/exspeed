pub mod handlers;
pub mod middleware;
pub mod state;

pub use state::AppState;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
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
pub async fn serve(
    state: Arc<AppState>,
    addr: SocketAddr,
    tls: Option<TlsPaths>,
) -> std::io::Result<()> {
    let router = handlers::build_router(state);

    match tls {
        Some(paths) => {
            let cfg = axum_server::tls_rustls::RustlsConfig::from_pem_file(
                &paths.cert, &paths.key,
            )
            .await?;
            info!("HTTP API listening on {} (TLS)", addr);
            axum_server::bind_rustls(addr, cfg)
                .serve(router.into_make_service())
                .await
        }
        None => {
            info!("HTTP API listening on {}", addr);
            axum_server::bind(addr)
                .serve(router.into_make_service())
                .await
        }
    }
}
