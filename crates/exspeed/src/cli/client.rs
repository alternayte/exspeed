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
    pub fn new(server_url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
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
