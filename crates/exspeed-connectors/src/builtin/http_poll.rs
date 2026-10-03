//! `http_poll`: fetch a JSON endpoint every `interval_secs` and emit one
//! record per item. Every item of every response is emitted (no
//! truncation). With `next_page_path`, the connector follows pagination
//! links/tokens across polls without waiting for the interval.
//!
//! Guarantee: at-least-once per fetched response. There is no durable
//! cursor: after a restart the endpoint is polled from the first page. Set
//! `idempotent_items = true` (with `item_key`) to let the broker drop items
//! it already stored within the stream's dedup window.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde::Deserialize;
use tokio::time::Instant;

use crate::builtin::http::{self, classify_reqwest_error, classify_status};
use crate::registry::PluginInit;
use crate::settings::{self, de};
use crate::subject::lookup;
use crate::traits::{ConnectorError, SourceBatch, SourceConnector, SourceRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollAuth {
    None,
    Bearer,
    Basic,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpPollSettings {
    pub url: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default = "default_interval", deserialize_with = "de::u64")]
    pub interval_secs: u64,
    #[serde(default, deserialize_with = "de::string_map")]
    pub headers: Vec<(String, String)>,
    #[serde(default = "default_auth")]
    pub auth_type: PollAuth,
    #[serde(default)]
    pub auth_token: Option<String>,
    /// JSON path of the item array (`$`, `$.data.items`). Unset = the whole
    /// body is one item.
    #[serde(default)]
    pub items_path: Option<String>,
    /// JSON path of each item's key.
    #[serde(default)]
    pub item_key: Option<String>,
    /// Use `item_key` as the idempotency key (`x-idempotency-key`).
    #[serde(default, deserialize_with = "de::bool")]
    pub idempotent_items: bool,
    /// JSON path of the next page: a URL (absolute or relative), or a token
    /// put into `page_param`. Missing/null/empty = last page.
    #[serde(default)]
    pub next_page_path: Option<String>,
    #[serde(default)]
    pub page_param: Option<String>,
    #[serde(default = "default_timeout", deserialize_with = "de::u64")]
    pub timeout_secs: u64,
}

fn default_method() -> String {
    "GET".into()
}
fn default_interval() -> u64 {
    60
}
fn default_auth() -> PollAuth {
    PollAuth::None
}
fn default_timeout() -> u64 {
    http::DEFAULT_TIMEOUT_SECS
}

pub struct HttpPollSource {
    s: HttpPollSettings,
    name: String,
    method: reqwest::Method,
    base: reqwest::Url,
    subject_template: String,
    client: Option<reqwest::Client>,
    last_poll: Option<Instant>,
    next_url: Option<reqwest::Url>,
    last_etag: Option<String>,
    last_modified: Option<String>,
}

impl HttpPollSource {
    pub fn new(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: HttpPollSettings = settings::parse("http_poll", &init.settings)?;
        let method = reqwest::Method::from_bytes(s.method.to_ascii_uppercase().as_bytes())
            .map_err(|e| ConnectorError::config(format!("http_poll: invalid method: {e}")))?;
        let base = reqwest::Url::parse(&s.url).map_err(|e| {
            ConnectorError::config(format!("http_poll: invalid url '{}': {e}", s.url))
        })?;
        if s.auth_type != PollAuth::None && s.auth_token.as_deref().unwrap_or("").is_empty() {
            return Err(ConnectorError::config(
                "http_poll: auth_token is required for bearer/basic auth",
            ));
        }
        if s.idempotent_items && s.item_key.is_none() {
            return Err(ConnectorError::config(
                "http_poll: idempotent_items requires item_key",
            ));
        }
        if s.timeout_secs == 0 {
            return Err(ConnectorError::config(
                "http_poll: timeout_secs must be > 0",
            ));
        }
        Ok(Self {
            name: init.config.name.clone(),
            method,
            base,
            subject_template: init.config.subject_template.clone(),
            s,
            client: None,
            last_poll: None,
            next_url: None,
            last_etag: None,
            last_modified: None,
        })
    }

    fn next_page(&self, body: &serde_json::Value) -> Option<reqwest::Url> {
        let path = self.s.next_page_path.as_deref()?;
        let v = lookup(body, path)?;
        let token = match v {
            serde_json::Value::String(s) if !s.is_empty() => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            _ => return None,
        };
        match &self.s.page_param {
            Some(param) => {
                let mut u = self.base.clone();
                let pairs: Vec<(String, String)> = u
                    .query_pairs()
                    .filter(|(k, _)| k != param)
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect();
                u.query_pairs_mut()
                    .clear()
                    .extend_pairs(pairs)
                    .append_pair(param, &token);
                Some(u)
            }
            None => self.base.join(&token).ok(),
        }
    }
}

/// Items at `path` (`$` = root). A non-array is one item.
pub fn extract_items(body: &serde_json::Value, path: &str) -> Vec<serde_json::Value> {
    match lookup(body, path) {
        Some(serde_json::Value::Array(a)) => a.clone(),
        Some(serde_json::Value::Null) | None => Vec::new(),
        Some(other) => vec![other.clone()],
    }
}

fn key_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[async_trait]
impl SourceConnector for HttpPollSource {
    async fn start(&mut self, _checkpoint: Option<String>) -> Result<(), ConnectorError> {
        self.client = Some(http::client(
            Duration::from_secs(self.s.timeout_secs),
            "exspeed-http-poll",
        )?);
        self.next_url = None;
        self.last_poll = None;
        Ok(())
    }

    async fn poll(&mut self, _max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        if self.next_url.is_none() {
            if let Some(last) = self.last_poll {
                if last.elapsed() < Duration::from_secs(self.s.interval_secs) {
                    return Ok(SourceBatch::empty());
                }
            }
        }
        let client = self
            .client
            .clone()
            .ok_or_else(|| ConnectorError::connection("http_poll: not started"))?;
        let first_page = self.next_url.is_none();
        let url = self.next_url.clone().unwrap_or_else(|| self.base.clone());

        let mut req = client.request(self.method.clone(), url);
        for (k, v) in &self.s.headers {
            req = req.header(k, v);
        }
        if let Some(token) = &self.s.auth_token {
            match self.s.auth_type {
                PollAuth::Bearer => req = req.header("Authorization", format!("Bearer {token}")),
                PollAuth::Basic => req = req.header("Authorization", format!("Basic {token}")),
                PollAuth::None => {}
            }
        }
        if first_page {
            if let Some(etag) = &self.last_etag {
                req = req.header("If-None-Match", etag);
            }
            if let Some(lm) = &self.last_modified {
                req = req.header("If-Modified-Since", lm);
            }
        }

        let response = req.send().await.map_err(|e| classify_reqwest_error(&e))?;
        let status = response.status().as_u16();
        if status == 304 {
            self.last_poll = Some(Instant::now());
            return Ok(SourceBatch::empty());
        }
        if !(200..300).contains(&status) {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(String::from);
            let body = response.text().await.unwrap_or_default();
            return Err(
                match classify_status(status, retry_after.as_deref(), &body) {
                    // A request that can never succeed is a configuration
                    // problem for a poller.
                    Some(ConnectorError::Poison(_)) => {
                        ConnectorError::fatal(format!("http_poll: HTTP {status}: {body}"))
                    }
                    Some(e) => e,
                    None => {
                        ConnectorError::transient(format!("http_poll: unexpected status {status}"))
                    }
                },
            );
        }
        // Remember the validators only once the body has been turned into
        // records: if reading or parsing fails, the retried poll must not
        // send `If-None-Match` for a response it never delivered (a 304
        // would silently drop that data).
        let validators = first_page.then(|| {
            let h = response.headers();
            let get = |name: &str| h.get(name).and_then(|v| v.to_str().ok()).map(String::from);
            (get("etag"), get("last-modified"))
        });
        let text = response
            .text()
            .await
            .map_err(|e| ConnectorError::transient(format!("http_poll: reading body: {e}")))?;
        let body: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            ConnectorError::transient(format!("http_poll: response is not JSON: {e}"))
        })?;

        let items = match &self.s.items_path {
            None => vec![body.clone()],
            Some(path) => extract_items(&body, path),
        };
        let mut records = Vec::with_capacity(items.len());
        for item in items {
            let key = self
                .s
                .item_key
                .as_deref()
                .and_then(|p| lookup(&item, p))
                .map(key_string);
            let mut headers = vec![("x-exspeed-source".to_string(), "http_poll".to_string())];
            if self.s.idempotent_items {
                if let Some(k) = &key {
                    headers.push((
                        "x-idempotency-key".to_string(),
                        format!("http_poll:{}:{k}", self.name),
                    ));
                }
            }
            let value = serde_json::to_vec(&item)
                .map_err(|e| ConnectorError::transient(format!("http_poll: {e}")))?;
            records.push(SourceRecord {
                key: key.map(|k| Bytes::from(k.into_bytes())),
                subject: crate::subject::render(&self.subject_template, &[], Some(&item)),
                value: Bytes::from(value),
                headers,
            });
        }

        if let Some((etag, modified)) = validators {
            self.last_etag = etag;
            self.last_modified = modified;
        }
        self.next_url = self.next_page(&body);
        if self.next_url.is_none() {
            self.last_poll = Some(Instant::now());
        }
        Ok(SourceBatch {
            records,
            checkpoint: None,
        })
    }

    async fn ack(&mut self, _checkpoint: Option<&str>) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.client = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectorConfig, ConnectorType};
    use serde_json::json;

    fn source(settings: serde_json::Value) -> Result<HttpPollSource, ConnectorError> {
        let (m, _) = exspeed_common::Metrics::new();
        HttpPollSource::new(&PluginInit {
            config: ConnectorConfig::new("p", ConnectorType::Source, "http_poll", "s"),
            settings: settings.as_object().unwrap().clone(),
            metrics: std::sync::Arc::new(m),
        })
    }

    #[test]
    fn extract() {
        let body = json!({"data": {"items": [{"id": 1}, {"id": 2}, {"id": 3}]}});
        assert_eq!(extract_items(&body, "$.data.items").len(), 3);
        assert_eq!(extract_items(&json!([1, 2]), "$").len(), 2);
        assert_eq!(extract_items(&json!({"a": 1}), "$").len(), 1);
        assert!(extract_items(&body, "$.missing").is_empty());
    }

    #[test]
    fn settings() {
        let s = source(json!({
            "url": "https://api.example.com/v1/items?limit=10",
            "interval_secs": 30,
            "headers": {"X-Api-Key": "k"},
            "auth_type": "bearer", "auth_token": "t",
            "items_path": "$.results", "item_key": "id"
        }))
        .unwrap();
        assert_eq!(s.s.interval_secs, 30);
        assert!(source(json!({"url": "https://x", "auth_type": "bearer"})).is_err());
        assert!(source(json!({"url": "https://x", "intervl_secs": 3})).is_err());
        assert!(source(json!({"method": "GET"})).is_err());
    }

    #[test]
    fn pagination_links_and_tokens() {
        let s = source(
            json!({"url": "https://api.example.com/v1/items?limit=10", "next_page_path": "$.next"}),
        )
        .unwrap();
        let next = s.next_page(&json!({"next": "/v1/items?page=2"})).unwrap();
        assert_eq!(next.as_str(), "https://api.example.com/v1/items?page=2");
        assert!(s.next_page(&json!({"next": null})).is_none());
        assert!(s.next_page(&json!({})).is_none());

        let t = source(json!({
            "url": "https://api.example.com/v1/items?limit=10",
            "next_page_path": "$.meta.cursor", "page_param": "cursor"
        }))
        .unwrap();
        let next = t.next_page(&json!({"meta": {"cursor": "abc"}})).unwrap();
        assert_eq!(
            next.as_str(),
            "https://api.example.com/v1/items?limit=10&cursor=abc"
        );
    }
}
