use async_trait::async_trait;
use tracing::warn;

use crate::config::ConnectorConfig;
use crate::traits::{
    ConnectorError, HealthStatus, PoisonReason, SinkBatch, SinkConnector, SinkRecord, WriteResult,
};

pub struct HttpSinkConnector {
    url: String,
    method: String,
    content_type: String,
    extra_headers: Vec<(String, String)>,
    client: reqwest::Client,
}

impl HttpSinkConnector {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config
            .setting("url")
            .map_err(ConnectorError::Config)?
            .to_string();

        let method = config.setting_or("method", "POST");
        let content_type = config.setting_or("content_type", "application/json");

        if config.setting_or("retry_count", "").trim() != "" {
            warn!(
                connector = %config.name,
                "http_sink: 'retry_count' setting is deprecated — use the top-level [retry] config instead; this value is ignored"
            );
        }

        let headers_str = config.setting_or("headers", "");
        let extra_headers = if headers_str.is_empty() {
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

        let client = reqwest::Client::new();

        Ok(Self {
            url,
            method,
            content_type,
            extra_headers,
            client,
        })
    }

    async fn send_one(&self, record: &SinkRecord) -> Result<(), (u16, String)> {
        let method = reqwest::Method::from_bytes(self.method.as_bytes())
            .map_err(|e| (0u16, format!("invalid HTTP method '{}': {e}", self.method)))?;

        let mut req = self
            .client
            .request(method, &self.url)
            .header("Content-Type", &self.content_type)
            .header("X-Exspeed-Subject", &record.subject)
            .header("X-Exspeed-Offset", record.offset.to_string());

        for (k, v) in &self.extra_headers {
            req = req.header(k, v);
        }

        req = req.body(record.value.to_vec());

        let response = req.send().await.map_err(|e| (0u16, e.to_string()))?;

        let status = response.status().as_u16();

        if (200..300).contains(&status) {
            Ok(())
        } else {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| String::from("<unreadable body>"));
            Err((status, body))
        }
    }
}

#[async_trait]
impl SinkConnector for HttpSinkConnector {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write(&mut self, batch: SinkBatch) -> Result<WriteResult, ConnectorError> {
        let mut last_successful: Option<u64> = None;

        for record in &batch.records {
            match self.send_one(record).await {
                Ok(()) => {
                    last_successful = Some(record.offset);
                }
                Err((status, body)) if (400..500).contains(&status) => {
                    warn!(
                        offset = record.offset, subject = %record.subject, status,
                        "HTTP sink: 4xx, routing to poison: {body}"
                    );
                    return Ok(WriteResult::Poison {
                        last_successful_offset: last_successful,
                        poison_offset: record.offset,
                        reason: PoisonReason::HttpClientError { status },
                        record: record.clone(),
                    });
                }
                Err((status, body)) => {
                    let error = format!(
                        "HTTP sink transient error (status {status}): {body}"
                    );
                    warn!(offset = record.offset, "{}", error);
                    return Ok(WriteResult::TransientFailure {
                        last_successful_offset: last_successful,
                        error,
                    });
                }
            }
        }

        Ok(WriteResult::AllSuccess)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        HealthStatus::Healthy
    }
}
