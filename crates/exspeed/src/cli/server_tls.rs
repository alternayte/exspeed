use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;

// TlsPaths now lives in exspeed-api so the HTTP TLS listener can share the
// same "both-or-neither" validation helper. Re-exported here so existing
// call sites in this binary keep working via `crate::cli::server_tls::TlsPaths`.
pub use exspeed_api::TlsPaths;

/// Load a PEM-encoded cert chain and private key from disk and build a
/// rustls ServerConfig. Returns an error if parsing or validation fails.
pub fn load_tls_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>> {
    // rustls 0.23+ requires a crypto provider to be registered before any
    // TLS operation. Install the ring-based default if one isn't already
    // active (the `.ok()` swallows the "already installed" error).
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();

    let certs = load_certs(cert_path)
        .with_context(|| format!("loading TLS cert {}", cert_path.display()))?;
    let key = load_private_key(key_path)
        .with_context(|| format!("loading TLS key {}", key_path.display()))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building rustls ServerConfig")?;

    Ok(Arc::new(config))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs: std::io::Result<Vec<_>> = rustls_pemfile::certs(&mut reader).collect();
    let certs = certs?;
    if certs.is_empty() {
        anyhow::bail!("no certificates found in {}", path.display());
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {}", path.display()))
}
