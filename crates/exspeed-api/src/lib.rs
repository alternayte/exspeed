pub mod handlers;
pub mod middleware;
pub mod openapi;
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

/// Serve the HTTP API on `addr` until the listener fails. Binds first, so
/// a bind error is returned at once.
pub async fn serve(
    state: Arc<AppState>,
    addr: SocketAddr,
    tls: Option<TlsPaths>,
) -> std::io::Result<()> {
    let listener = std::net::TcpListener::bind(addr)?;
    HttpServer::new(state, listener, tls)
        .await?
        .serve_with_shutdown(std::future::pending(), Duration::from_secs(10))
        .await
}

/// The HTTP API, bound and with its TLS config loaded, ready to serve.
/// Building it surfaces every startup error (bad TLS files, a listener that
/// can't be used) before the server reports itself ready.
pub struct HttpServer {
    router: axum::Router,
    listener: std::net::TcpListener,
    tls: Option<axum_server::tls_rustls::RustlsConfig>,
    addr: SocketAddr,
}

impl HttpServer {
    /// Wrap an already-bound listener (the caller binds it, so a port
    /// conflict is reported by the caller before anything is spawned).
    pub async fn new(
        state: Arc<AppState>,
        listener: std::net::TcpListener,
        tls: Option<TlsPaths>,
    ) -> std::io::Result<Self> {
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let tls = match tls {
            Some(paths) => Some(
                axum_server::tls_rustls::RustlsConfig::from_pem_file(&paths.cert, &paths.key)
                    .await?,
            ),
            None => None,
        };
        Ok(Self {
            router: handlers::build_router(state),
            listener,
            tls,
            addr,
        })
    }

    /// The bound address.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Serve until the listener fails or `shutdown` resolves. On shutdown
    /// the server stops accepting connections and gives in-flight requests
    /// up to `drain` to complete before closing them.
    pub async fn serve_with_shutdown<F>(self, shutdown: F, drain: Duration) -> std::io::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // axum-server's Handle is the only documented hook for graceful
        // shutdown; a forwarder lets the caller's future drive it.
        let handle = axum_server::Handle::new();
        {
            let handle_for_shutdown = handle.clone();
            tokio::spawn(async move {
                shutdown.await;
                handle_for_shutdown.graceful_shutdown(Some(drain));
            });
        }
        let service = self.router.into_make_service();
        match self.tls {
            Some(cfg) => {
                info!("HTTP API listening on {} (TLS)", self.addr);
                axum_server::from_tcp_rustls(self.listener, cfg)
                    .handle(handle)
                    .serve(service)
                    .await
            }
            None => {
                info!("HTTP API listening on {}", self.addr);
                axum_server::from_tcp(self.listener)
                    .handle(handle)
                    .serve(service)
                    .await
            }
        }
    }
}
