use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::Instant;

use crate::config::ConnectorConfig;
use crate::traits::{ConnectorError, HealthStatus, SourceBatch, SourceConnector, SourceRecord};

#[derive(Debug)]
pub struct HttpPollSource {
    url: String,
    method: String,
    interval_secs: u64,
    headers: Vec<(String, String)>,
    auth_type: String,
    auth_token: Option<String>,
    items_path: Option<String>,
    item_key: Option<String>,
    subject_template: String,
    client: reqwest::Client,
    last_poll: Option<Instant>,
    last_etag: Option<String>,
    last_modified: Option<String>,
}

impl HttpPollSource {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config
            .setting("url")
            .map_err(ConnectorError::Config)?
            .to_string();

        let method = config.setting_or("method", "GET");

        let interval_secs: u64 = config
            .setting_or("interval_secs", "60")
            .parse()
            .map_err(|e| ConnectorError::Config(format!("invalid interval_secs: {e}")))?;

        let headers_str = config.setting_or("headers", "");
        let headers = if headers_str.is_empty() {
            Vec::new()
        } else {
            headers_str
                .split(',')
                .filter_map(|part| {
                    let part = part.trim();
                    if part.is_empty() {
                        return None;
                    }
                    let colon = part.find(':')?;
                    let key = part[..colon].trim().to_string();
                    let val = part[colon + 1..].trim().to_string();
                    Some((key, val))
                })
                .collect()
        };

        let auth_type = config.setting_or("auth_type", "none");
        let auth_token = config.settings.get("auth_token").cloned();

        let items_path = config.settings.get("items_path").cloned().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        });

        let item_key = config.settings.get("item_key").cloned().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        });

        let subject_template = config.subject_template.clone();

        Ok(Self {
            url,
            method,
            interval_secs,
            headers,
            auth_type,
            auth_token,
            items_path,
            item_key,
            subject_template,
            client: reqwest::Client::builder()
                .user_agent("exspeed-http-poll/0.1")
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            last_poll: None,
            last_etag: None,
            last_modified: None,
        })
    }
}

/// Navigate a simple JSON path like "$", "$.field", or "$.field.nested"
/// and return the items found at that location.
fn extract_items(body: &serde_json::Value, path: &str) -> Vec<serde_json::Value> {
    if path == "$" {
        // Root array
        match body.as_array() {
            Some(arr) => arr.clone(),
            None => vec![body.clone()],
        }
    } else if let Some(dotted) = path.strip_prefix("$.") {
        let segments: Vec<&str> = dotted.split('.').collect();
        let mut current = body;
        for seg in &segments {
            match current.get(*seg) {
                Some(val) => current = val,
                None => return Vec::new(),
            }
        }
        match current.as_array() {
            Some(arr) => arr.clone(),
            None => vec![current.clone()],
        }
    } else {
        // Unrecognized path format, treat body as single item
        vec![body.clone()]
    }
}

/// Extract a field value from a JSON object for use as a record key.
fn extract_key(item: &serde_json::Value, key_path: &str) -> Option<String> {
    let path = key_path.strip_prefix("$.").unwrap_or(key_path);
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = item;
    for seg in &segments {
        current = current.get(*seg)?;
    }
    match current {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => Some(current.to_string()),
    }
}

#[async_trait]
impl SourceConnector for HttpPollSource {
    async fn start(&mut self, _last_position: Option<String>) -> Result<(), ConnectorError> {
        self.client = reqwest::Client::builder()
            .user_agent("exspeed-http-poll/0.1")
            .build()
            .map_err(|e| ConnectorError::Connection(format!("failed to build HTTP client: {e}")))?;
        Ok(())
    }

    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError> {
        // Check if enough time has elapsed since last poll
        if let Some(last) = self.last_poll {
            let elapsed = last.elapsed();
            if elapsed.as_secs() < self.interval_secs {
                return Ok(SourceBatch {
                    records: Vec::new(),
                    position: None,
                });
            }
        }

        // Build request
        let method = reqwest::Method::from_bytes(self.method.as_bytes()).map_err(|e| {
            ConnectorError::Config(format!("invalid HTTP method '{}': {e}", self.method))
        })?;

        let mut req = self.client.request(method, &self.url);

        // Add custom headers
        for (k, v) in &self.headers {
            req = req.header(k, v);
        }

        // Add auth
        match self.auth_type.as_str() {
            "bearer" => {
                if let Some(ref token) = self.auth_token {
                    req = req.header("Authorization", format!("Bearer {token}"));
                }
            }
            "basic" => {
                if let Some(ref token) = self.auth_token {
                    req = req.header("Authorization", format!("Basic {token}"));
                }
            }
            _ => {} // "none" or unrecognized
        }

        // Add conditional headers for change detection
        if let Some(ref etag) = self.last_etag {
            req = req.header("If-None-Match", etag.as_str());
        }
        if let Some(ref lm) = self.last_modified {
            req = req.header("If-Modified-Since", lm.as_str());
        }

        // Send request
        let response = req
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(format!("HTTP poll request failed: {e}")))?;

        let status = response.status().as_u16();

        // 304 Not Modified — no changes
        if status == 304 {
            self.last_poll = Some(Instant::now());
            return Ok(SourceBatch {
                records: Vec::new(),
                position: None,
            });
        }

        if !(200..300).contains(&status) {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| String::from("<unreadable body>"));
            return Err(ConnectorError::Connection(format!(
                "HTTP poll returned status {status}: {body}"
            )));
        }

        // Store ETag / Last-Modified from response headers
        self.last_etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        self.last_modified = response
            .headers()
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Parse response body as JSON
        let body_text = response
            .text()
            .await
            .map_err(|e| ConnectorError::Data(format!("failed to read response body: {e}")))?;

        let body: serde_json::Value = serde_json::from_str(&body_text)
            .map_err(|e| ConnectorError::Data(format!("failed to parse JSON: {e}")))?;

        // Extract items
        let items = match &self.items_path {
            None => vec![body],
            Some(path) => extract_items(&body, path),
        };

        // Build records, respecting max_batch
        let mut records = Vec::new();
        for item in items.into_iter().take(max_batch) {
            let key = self
                .item_key
                .as_ref()
                .and_then(|kp| extract_key(&item, kp))
                .map(|k| Bytes::from(k.into_bytes()));

            let value = Bytes::from(
                serde_json::to_vec(&item)
                    .map_err(|e| ConnectorError::Data(format!("failed to serialize item: {e}")))?,
            );

            let subject = self.subject_template.clone();

            records.push(SourceRecord {
                key,
                value,
                subject,
                headers: Vec::new(),
            });
        }

        self.last_poll = Some(Instant::now());

        Ok(SourceBatch {
            records,
            position: None,
        })
    }

    async fn commit(&mut self, _position: String) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        HealthStatus::Healthy
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectorConfig;
    use std::collections::HashMap;

    fn make_config(settings: Vec<(&str, &str)>) -> ConnectorConfig {
        ConnectorConfig {
            name: "test-http-poll".to_string(),
            connector_type: "source".to_string(),
            plugin: "http_poll".to_string(),
            stream: "events".to_string(),
            subject_template: "poll.events".to_string(),
            subject_filter: String::new(),
            settings: settings
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
            batch_size: 100,
            poll_interval_ms: 50,
            dedup_enabled: true,
            dedup_key: String::new(),
            dedup_window_secs: 86400,
            transform_sql: String::new(),
            on_transient_exhausted: crate::config::OnTransientExhausted::default(),
            retry: crate::retry::RetryPolicy::default_transient(),
        }
    }

    #[test]
    fn extract_items_single_object_no_path() {
        let body = serde_json::json!({"id": 1, "name": "alice"});
        // When items_path is None, the poll method uses vec![body] directly.
        // The extract_items function with "$" on a non-array returns the item.
        let items = extract_items(&body, "$");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"], 1);
    }

    #[test]
    fn extract_items_root_array() {
        let body = serde_json::json!([
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"}
        ]);
        let items = extract_items(&body, "$");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["name"], "alice");
        assert_eq!(items[1]["name"], "bob");
    }

    #[test]
    fn extract_items_nested_path() {
        let body = serde_json::json!({
            "status": "ok",
            "results": [
                {"id": 10, "title": "post-a"},
                {"id": 20, "title": "post-b"},
                {"id": 30, "title": "post-c"}
            ]
        });
        let items = extract_items(&body, "$.results");
        assert_eq!(items.len(), 3);
        assert_eq!(items[0]["id"], 10);
        assert_eq!(items[2]["title"], "post-c");
    }

    #[test]
    fn extract_key_from_json_object() {
        let item = serde_json::json!({"id": 42, "name": "test"});
        assert_eq!(extract_key(&item, "id"), Some("42".to_string()));
        assert_eq!(extract_key(&item, "name"), Some("test".to_string()));
        assert_eq!(extract_key(&item, "$.name"), Some("test".to_string()));
        assert_eq!(extract_key(&item, "missing"), None);
    }

    #[test]
    fn config_parsing() {
        let config = make_config(vec![
            ("url", "https://api.example.com/data"),
            ("method", "POST"),
            ("interval_secs", "30"),
            ("headers", "Accept: application/json, X-Api-Key: secret123"),
            ("auth_type", "bearer"),
            ("auth_token", "my-token"),
            ("items_path", "$.results"),
            ("item_key", "id"),
        ]);
        let source = HttpPollSource::new(&config).expect("should parse");
        assert_eq!(source.url, "https://api.example.com/data");
        assert_eq!(source.method, "POST");
        assert_eq!(source.interval_secs, 30);
        assert_eq!(source.headers.len(), 2);
        assert_eq!(source.headers[0].0, "Accept");
        assert_eq!(source.headers[0].1, "application/json");
        assert_eq!(source.headers[1].0, "X-Api-Key");
        assert_eq!(source.headers[1].1, "secret123");
        assert_eq!(source.auth_type, "bearer");
        assert_eq!(source.auth_token, Some("my-token".to_string()));
        assert_eq!(source.items_path, Some("$.results".to_string()));
        assert_eq!(source.item_key, Some("id".to_string()));
        assert_eq!(source.subject_template, "poll.events");
    }

    #[test]
    fn config_defaults() {
        let config = make_config(vec![("url", "https://api.example.com/data")]);
        let source = HttpPollSource::new(&config).expect("should parse");
        assert_eq!(source.method, "GET");
        assert_eq!(source.interval_secs, 60);
        assert!(source.headers.is_empty());
        assert_eq!(source.auth_type, "none");
        assert_eq!(source.auth_token, None);
        assert_eq!(source.items_path, None);
        assert_eq!(source.item_key, None);
    }

    #[test]
    fn config_missing_url_returns_error() {
        let config = make_config(vec![("method", "GET")]);
        let err = HttpPollSource::new(&config).unwrap_err();
        assert!(err.to_string().contains("url"));
    }
}
