use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;

/// Paths to a PEM-encoded server certificate chain and its private key.
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
