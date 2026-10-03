//! `http_webhook`: a passive source served at `POST /webhooks/<path>` by the
//! HTTP API. There is no task loop; each request is appended through the
//! broker `Log` before the response is sent, so a 200 means the record is
//! stored (at-least-once from the sender's point of view; effectively-once
//! when the sender sets `Idempotency-Key`).

use bytes::Bytes;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;

use crate::registry::PluginInit;
use crate::settings;
use exspeed_broker::broker_append::{AppendResult, IDEMPOTENCY_HEADER};
use exspeed_broker::log::{Log, LogError};
use exspeed_common::StreamName;
use exspeed_streams::record::Record;

use crate::traits::ConnectorError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookAuth {
    /// No authentication. Must be chosen explicitly.
    None,
    /// `Authorization: Bearer <secret>` (constant-time compare).
    Bearer,
    /// HMAC-SHA256 of the raw body, hex-encoded, in `signature_header`
    /// (optionally after `signature_prefix`, e.g. `sha256=`).
    HmacSha256,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookSettings {
    /// Served at `POST /webhooks/<path>`.
    pub path: String,
    /// Required: `none`, `bearer` or `hmac_sha256`.
    pub auth_type: WebhookAuth,
    #[serde(default)]
    pub auth_secret: Option<String>,
    #[serde(default = "default_signature_header")]
    pub signature_header: String,
    #[serde(default)]
    pub signature_prefix: String,
}

fn default_signature_header() -> String {
    "X-Signature-256".into()
}

/// A configured webhook, ready to serve requests.
#[derive(Debug, Clone)]
pub struct WebhookEndpoint {
    pub connector: String,
    pub stream: StreamName,
    pub subject_template: String,
    path: String,
    settings: WebhookSettings,
}

/// Why a webhook POST was rejected.
#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("unauthorized: invalid or missing credentials")]
    Unauthorized,
    #[error(transparent)]
    Log(#[from] LogError),
}

impl WebhookEndpoint {
    pub fn from_init(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: WebhookSettings = settings::parse("http_webhook", &init.settings)?;
        let path = normalize_path(&s.path);
        if path.is_empty() {
            return Err(ConnectorError::config(
                "http_webhook: 'path' must not be empty",
            ));
        }
        if path.split('/').any(|seg| seg == ".." || seg.is_empty()) {
            return Err(ConnectorError::config(format!(
                "http_webhook: invalid path '{}'",
                s.path
            )));
        }
        match s.auth_type {
            WebhookAuth::None => {}
            WebhookAuth::Bearer | WebhookAuth::HmacSha256 => {
                if s.auth_secret.as_deref().unwrap_or("").is_empty() {
                    return Err(ConnectorError::config(
                        "http_webhook: auth_secret is required for bearer and hmac_sha256",
                    ));
                }
            }
        }
        let stream = StreamName::try_from(init.config.stream.as_str())
            .map_err(|e| ConnectorError::config(format!("invalid stream name: {e}")))?;
        Ok(Self {
            connector: init.config.name.clone(),
            stream,
            subject_template: init.config.subject_template.clone(),
            path,
            settings: s,
        })
    }

    /// Normalised path (no leading `/` or `webhooks/` prefix).
    pub fn path(&self) -> &str {
        &self.path
    }

    fn authorize(&self, body: &[u8], header: &(dyn Fn(&str) -> Option<String> + Sync)) -> bool {
        let secret = self.settings.auth_secret.as_deref().unwrap_or("");
        match self.settings.auth_type {
            WebhookAuth::None => true,
            WebhookAuth::Bearer => {
                let expected = format!("Bearer {secret}");
                header("authorization").is_some_and(|v| {
                    constant_time_eq::constant_time_eq(v.as_bytes(), expected.as_bytes())
                })
            }
            WebhookAuth::HmacSha256 => {
                let Some(sig) = header(&self.settings.signature_header) else {
                    return false;
                };
                let sig = sig.trim();
                let Some(hex) = sig.strip_prefix(self.settings.signature_prefix.as_str()) else {
                    return false;
                };
                let Some(given) = decode_hex(hex.trim()) else {
                    return false;
                };
                let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
                    .expect("HMAC accepts any key length");
                mac.update(body);
                mac.verify_slice(&given).is_ok()
            }
        }
    }

    /// Authenticate and append one request body. `header` looks up a
    /// request header by (case-insensitive) name. Returns the record's
    /// offset (the original offset for a duplicate `Idempotency-Key`).
    pub async fn handle(
        &self,
        log: &Log,
        body: Bytes,
        header: &(dyn Fn(&str) -> Option<String> + Sync),
    ) -> Result<u64, WebhookError> {
        if !self.authorize(&body, header) {
            return Err(WebhookError::Unauthorized);
        }
        let json: Option<serde_json::Value> = serde_json::from_slice(&body).ok();
        let subject = crate::subject::render(&self.subject_template, &[], json.as_ref());
        let mut headers = vec![
            ("x-exspeed-source".to_string(), "http_webhook".to_string()),
            ("x-exspeed-connector".to_string(), self.connector.clone()),
        ];
        let idem = header("idempotency-key").or_else(|| header(IDEMPOTENCY_HEADER));
        if let Some(key) = idem.filter(|k| !k.is_empty()) {
            headers.push((IDEMPOTENCY_HEADER.to_string(), key));
        }
        let record = Record {
            key: None,
            value: body,
            subject,
            headers,
            timestamp_ns: None,
        };
        Ok(match log.append(&self.stream, record).await? {
            AppendResult::Written(offset, _) | AppendResult::Duplicate(offset) => offset.0,
        })
    }
}

fn normalize_path(p: &str) -> String {
    let p = p.trim().trim_matches('/');
    p.strip_prefix("webhooks/")
        .unwrap_or(p)
        .trim_matches('/')
        .to_string()
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectorConfig, ConnectorType};
    use serde_json::json;

    fn endpoint(settings: serde_json::Value) -> Result<WebhookEndpoint, ConnectorError> {
        let (m, _) = exspeed_common::Metrics::new();
        WebhookEndpoint::from_init(&PluginInit {
            config: ConnectorConfig::new("w", ConnectorType::Source, "http_webhook", "events"),
            settings: settings.as_object().unwrap().clone(),
            metrics: std::sync::Arc::new(m),
        })
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn settings_validation() {
        assert!(
            endpoint(json!({"path": "x"})).is_err(),
            "auth_type is required"
        );
        assert!(endpoint(json!({"path": "x", "auth_type": "bearer"})).is_err());
        assert!(endpoint(json!({"path": "../x", "auth_type": "none"})).is_err());
        assert!(endpoint(json!({"path": "x", "auth_type": "none", "auth": "x"})).is_err());
        let e = endpoint(json!({"path": "/webhooks/stripe", "auth_type": "none"})).unwrap();
        assert_eq!(e.path(), "stripe");
    }

    #[test]
    fn bearer_auth() {
        let e = endpoint(json!({"path": "x", "auth_type": "bearer", "auth_secret": "s3"})).unwrap();
        assert!(e.authorize(b"{}", &|h| (h == "authorization")
            .then(|| "Bearer s3".to_string())));
        assert!(!e.authorize(b"{}", &|h| (h == "authorization")
            .then(|| "Bearer no".to_string())));
        assert!(!e.authorize(b"{}", &|_| None));
    }

    #[test]
    fn hmac_auth() {
        let e = endpoint(json!({
            "path": "x", "auth_type": "hmac_sha256", "auth_secret": "key",
            "signature_header": "X-Hub-Signature-256", "signature_prefix": "sha256="
        }))
        .unwrap();
        let body = br#"{"a":1}"#;
        let mut mac = Hmac::<Sha256>::new_from_slice(b"key").unwrap();
        mac.update(body);
        let good = format!("sha256={}", hex(&mac.finalize().into_bytes()));
        let lookup = |v: String| move |h: &str| (h == "X-Hub-Signature-256").then(|| v.clone());
        assert!(e.authorize(body, &lookup(good.clone())));
        assert!(!e.authorize(b"tampered", &lookup(good.clone())));
        assert!(!e.authorize(
            body,
            &lookup(good.trim_start_matches("sha256=").to_string())
        ));
        assert!(!e.authorize(body, &lookup("sha256=zz".into())));
    }
}
