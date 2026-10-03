//! `http_sink`: one HTTP request per record.
//!
//! Guarantee: at-least-once. Each request carries an `Idempotency-Key`
//! header (the record's `x-idempotency-key`, or `<stream>:<offset>`) so a
//! receiver can drop the duplicates a retry or restart may produce.

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use crate::builtin::http::{self, classify_reqwest_error, classify_status};
use crate::registry::PluginInit;
use crate::settings::{self, de};
use crate::traits::{ConnectorError, SinkConnector, SinkRecord, WriteResult};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpSinkSettings {
    pub url: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default = "default_content_type")]
    pub content_type: String,
    #[serde(default, deserialize_with = "de::string_map")]
    pub headers: Vec<(String, String)>,
    #[serde(default = "default_timeout", deserialize_with = "de::u64")]
    pub timeout_secs: u64,
}

fn default_method() -> String {
    "POST".into()
}
fn default_content_type() -> String {
    "application/json".into()
}
fn default_timeout() -> u64 {
    http::DEFAULT_TIMEOUT_SECS
}

pub struct HttpSinkConnector {
    settings: HttpSinkSettings,
    method: reqwest::Method,
    stream: String,
    client: Option<reqwest::Client>,
}

impl HttpSinkConnector {
    pub fn new(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: HttpSinkSettings = settings::parse("http_sink", &init.settings)?;
        let method = reqwest::Method::from_bytes(s.method.to_ascii_uppercase().as_bytes())
            .map_err(|e| {
                ConnectorError::config(format!("http_sink: invalid method '{}': {e}", s.method))
            })?;
        reqwest::Url::parse(&s.url).map_err(|e| {
            ConnectorError::config(format!("http_sink: invalid url '{}': {e}", s.url))
        })?;
        if s.timeout_secs == 0 {
            return Err(ConnectorError::config(
                "http_sink: timeout_secs must be > 0",
            ));
        }
        Ok(Self {
            settings: s,
            method,
            stream: init.config.stream.clone(),
            client: None,
        })
    }

    async fn send_one(
        &self,
        client: &reqwest::Client,
        record: &SinkRecord,
    ) -> Result<(), ConnectorError> {
        let mut req = client
            .request(self.method.clone(), &self.settings.url)
            .header("Content-Type", &self.settings.content_type)
            .header("Idempotency-Key", record.idempotency_key(&self.stream))
            .header("X-Exspeed-Subject", &record.subject)
            .header("X-Exspeed-Offset", record.offset.to_string());
        for (k, v) in &self.settings.headers {
            req = req.header(k, v);
        }
        let response = req
            .body(record.value.to_vec())
            .send()
            .await
            .map_err(|e| classify_reqwest_error(&e))?;
        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let body = response.text().await.unwrap_or_default();
        Err(
            classify_status(status, retry_after.as_deref(), &body).unwrap_or_else(|| {
                ConnectorError::transient(format!("unexpected HTTP status {status}"))
            }),
        )
    }
}

#[async_trait]
impl SinkConnector for HttpSinkConnector {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        self.client = Some(http::client(
            Duration::from_secs(self.settings.timeout_secs),
            "exspeed-http-sink",
        )?);
        Ok(())
    }

    async fn write(&mut self, records: &[SinkRecord]) -> Result<WriteResult, ConnectorError> {
        let client = self
            .client
            .clone()
            .ok_or_else(|| ConnectorError::connection("http_sink: not started"))?;
        for (i, record) in records.iter().enumerate() {
            if let Err(error) = self.send_one(&client, record).await {
                return Ok(WriteResult::Failed { accepted: i, error });
            }
        }
        Ok(WriteResult::Accepted)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        self.client = None;
        Ok(())
    }

    fn default_flush_interval(&self) -> Duration {
        // Every request is already durable at the receiver: commit after
        // each batch to keep the replay window small.
        Duration::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectorConfig, ConnectorType};
    use serde_json::json;

    fn init(settings: serde_json::Value) -> PluginInit {
        let (m, _) = exspeed_common::Metrics::new();
        PluginInit {
            config: ConnectorConfig::new("h", ConnectorType::Sink, "http_sink", "s"),
            settings: settings.as_object().unwrap().clone(),
            metrics: std::sync::Arc::new(m),
        }
    }

    #[test]
    fn typed_settings() {
        let c = HttpSinkConnector::new(&init(json!({
            "url": "http://x/hook",
            "headers": {"Authorization": "Bearer t"},
            "timeout_secs": 5
        })))
        .unwrap();
        assert_eq!(
            c.settings.headers,
            vec![("Authorization".into(), "Bearer t".into())]
        );
        assert_eq!(c.settings.timeout_secs, 5);
        assert!(
            HttpSinkConnector::new(&init(json!({"url": "http://x", "retry_count": 3}))).is_err()
        );
        assert!(HttpSinkConnector::new(&init(json!({"url": "not a url"}))).is_err());
        assert!(HttpSinkConnector::new(&init(json!({}))).is_err());
    }
}
