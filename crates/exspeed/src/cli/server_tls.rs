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

/// Like [`load_tls_config`]; with `client_ca`, clients must present a
/// certificate signed by that CA (mutual TLS).
pub fn load_tls_config_with_clients(
    cert_path: &Path,
    key_path: &Path,
    client_ca: Option<&Path>,
) -> Result<Arc<ServerConfig>> {
    let Some(ca) = client_ca else {
        return load_tls_config(cert_path, key_path);
    };
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let certs = load_certs(cert_path)
        .with_context(|| format!("loading TLS cert {}", cert_path.display()))?;
    let key = load_private_key(key_path)
        .with_context(|| format!("loading TLS key {}", key_path.display()))?;
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for c in load_certs(ca).with_context(|| format!("loading client CA {}", ca.display()))? {
        roots
            .add(c)
            .with_context(|| format!("adding a client trust root from {}", ca.display()))?;
    }
    let verifier = tokio_rustls::rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .context("building the client certificate verifier")?;
    let config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .context("building rustls ServerConfig")?;
    Ok(Arc::new(config))
}

/// The name a verified client certificate stands for: its subject common
/// name, else its first DNS subject-alternative name.
pub fn client_cert_name(certs: Option<&[CertificateDer<'_>]>) -> Option<String> {
    let der = certs?.first()?;
    let (_, cert) = x509_parser::parse_x509_certificate(der.as_ref()).ok()?;
    if let Some(cn) = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
    {
        return Some(cn.to_string());
    }
    cert.subject_alternative_name()
        .ok()
        .flatten()
        .and_then(|san| {
            san.value.general_names.iter().find_map(|n| match n {
                x509_parser::extensions::GeneralName::DNSName(d) => Some(d.to_string()),
                _ => None,
            })
        })
}

/// A rustls ClientConfig trusting the certificates in `ca_path` (PEM).
pub fn load_client_config(ca_path: &Path) -> Result<Arc<tokio_rustls::rustls::ClientConfig>> {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for cert in load_certs(ca_path).with_context(|| format!("loading CA {}", ca_path.display()))? {
        roots
            .add(cert)
            .with_context(|| format!("adding a trust root from {}", ca_path.display()))?;
    }
    Ok(Arc::new(
        tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
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
