use anyhow::{anyhow, Context, Result};
use serde_json::Value;

/// A thin HTTP wrapper around reqwest with a fixed base URL.
///
/// Used by every management CLI subcommand to talk to the exspeed HTTP API.
pub struct CliClient {
    client: reqwest::Client,
    base_url: String,
}

impl CliClient {
    /// Create a new client targeting `server_url` (e.g. `http://localhost:8080`).
    ///
    /// Reads two environment variables at construction time:
    ///
    /// - `EXSPEED_AUTH_TOKEN` — if set and non-empty, every request is sent
    ///   with an `Authorization: Bearer <token>` header.
    /// - `EXSPEED_INSECURE_SKIP_VERIFY` — if set to `"1"`, TLS certificate
    ///   verification is disabled. Intended for dev/testing against a server
    ///   using a self-signed cert. A warning is logged when enabled.
    pub fn new(server_url: &str) -> Self {
        let mut builder = reqwest::Client::builder();

        // Allow self-signed certs for dev/testing.
        if std::env::var("EXSPEED_INSECURE_SKIP_VERIFY").ok().as_deref() == Some("1") {
            tracing::warn!(
                "EXSPEED_INSECURE_SKIP_VERIFY=1 set — TLS certs will not be verified"
            );
            builder = builder.danger_accept_invalid_certs(true);
        }

        // Bearer auth via env var.
        let mut default_headers = reqwest::header::HeaderMap::new();
        if let Ok(token) = std::env::var("EXSPEED_AUTH_TOKEN") {
            if !token.is_empty() {
                let value = reqwest::header::HeaderValue::from_str(
                    &format!("Bearer {token}"),
                )
                .expect("valid Authorization header");
                default_headers.insert(reqwest::header::AUTHORIZATION, value);
            }
        }
        if !default_headers.is_empty() {
            builder = builder.default_headers(default_headers);
        }

        let client = builder.build().expect("reqwest Client::build");
        Self {
            client,
            base_url: server_url.trim_end_matches('/').to_string(),
        }
    }

    /// GET `path`, parse the response as JSON.
    ///
    /// Returns an error on connection failure or non-2xx status.
    pub async fn get(&self, path: &str) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("failed to connect to {url}"))?;

        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .with_context(|| format!("failed to parse JSON from GET {url}"))?;

        if !status.is_success() {
            let msg = body["error"]
                .as_str()
                .unwrap_or("unknown error")
                .to_string();
            return Err(anyhow!("GET {url} returned {status}: {msg}"));
        }

        Ok(body)
    }

    /// POST `path` with a JSON body, returning `(status_code, response_body)`.
    ///
    /// Does **not** treat non-2xx as an automatic error — callers decide.
    pub async fn post(&self, path: &str, body: &Value) -> Result<(u16, Value)> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .post(&url)
            .json(body)
            .send()
            .await
            .with_context(|| format!("failed to connect to {url}"))?;

        let status = resp.status().as_u16();
        let resp_body: Value = resp
            .json()
            .await
            .with_context(|| format!("failed to parse JSON from POST {url}"))?;

        Ok((status, resp_body))
    }

    /// PATCH `path` with a JSON body, returning `(status_code, response_body)`.
    ///
    /// Does **not** treat non-2xx as an automatic error — callers decide.
    pub async fn patch(&self, path: &str, body: &Value) -> Result<(u16, Value)> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .patch(&url)
            .json(body)
            .send()
            .await
            .with_context(|| format!("failed to connect to {url}"))?;

        let status = resp.status().as_u16();
        let resp_body: Value = resp
            .json()
            .await
            .with_context(|| format!("failed to parse JSON from PATCH {url}"))?;

        Ok((status, resp_body))
    }

    /// DELETE `path`, parse the response as JSON.
    ///
    /// Returns an error on connection failure or non-2xx status.
    pub async fn delete(&self, path: &str) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .delete(&url)
            .send()
            .await
            .with_context(|| format!("failed to connect to {url}"))?;

        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .with_context(|| format!("failed to parse JSON from DELETE {url}"))?;

        if !status.is_success() {
            let msg = body["error"]
                .as_str()
                .unwrap_or("unknown error")
                .to_string();
            return Err(anyhow!("DELETE {url} returned {status}: {msg}"));
        }

        Ok(body)
    }
}
