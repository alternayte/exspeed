use async_trait::async_trait;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

use crate::config::ConnectorConfig;
use crate::traits::{
    ConnectorError, HealthStatus, SinkBatch, SinkConnector, SinkRecord, WriteResult,
};

pub struct HttpSinkConnector {
    url: String,
    method: String,
    content_type: String,
    extra_headers: Vec<(String, String)>,
    retry_count: u32,
    client: reqwest::Client,
}

impl HttpSinkConnector {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config
            .setting("url")
            .map_err(|e| ConnectorError::Config(e))?
            .to_string();

        let method = config.setting_or("method", "POST");
        let content_type = config.setting_or("content_type", "application/json");

        let retry_count_str = config.setting_or("retry_count", "3");
        let retry_count = retry_count_str
            .parse::<u32>()
            .map_err(|e| ConnectorError::Config(format!("invalid retry_count: {e}")))?;

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
            retry_count,
            client,
        })
    }

    async fn send_one(&self, record: &SinkRecord) -> Result<(), (u16, String)> {
        let method = reqwest::Method::from_bytes(self.method.as_bytes()).map_err(|e| {
            (0u16, format!("invalid HTTP method '{}': {e}", self.method))
        })?;

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

        if status >= 200 && status < 300 {
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
        let mut last_successful_offset: Option<u64> = None;
        let mut any_success = false;
        let mut first_hard_fail: Option<String> = None;

        'records: for record in &batch.records {
            // Attempt send with retries for 5xx errors.
            let mut attempt = 0u32;
            loop {
                match self.send_one(record).await {
                    Ok(()) => {
                        last_successful_offset = Some(record.offset);
                        any_success = true;
                        continue 'records;
                    }
                    Err((status, body)) if status >= 400 && status < 500 => {
                        // 4xx: bad data, skip and treat as "advanced past".
                        warn!(
                            offset = record.offset,
                            subject = %record.subject,
                            status,
                            "HTTP sink: 4xx response, skipping record: {body}"
                        );
                        last_successful_offset = Some(record.offset);
                        any_success = true;
                        continue 'records;
                    }
                    Err((status, body)) => {
                        // 5xx or network errors: retry with exponential backoff.
                        if attempt < self.retry_count {
                            let backoff_secs = 1u64 << attempt; // 1, 2, 4, ...
                            warn!(
                                offset = record.offset,
                                subject = %record.subject,
                                status,
                                attempt,
                                backoff_secs,
                                "HTTP sink: retryable error, will retry: {body}"
                            );
                            sleep(Duration::from_secs(backoff_secs)).await;
                            attempt += 1;
                        } else {
                            let msg = format!(
                                "HTTP sink: all {} retries failed for offset {} (status {status}): {body}",
                                self.retry_count, record.offset
                            );
                            warn!("{}", msg);
                            first_hard_fail = Some(msg);
                            break 'records;
                        }
                    }
                }
            }
        }

        if first_hard_fail.is_some() {
            if let Some(offset) = last_successful_offset {
                Ok(WriteResult::PartialSuccess {
                    last_successful_offset: offset,
                })
            } else {
                Ok(WriteResult::AllFailed(
                    first_hard_fail.unwrap_or_default(),
                ))
            }
        } else if any_success || batch.records.is_empty() {
            Ok(WriteResult::AllSuccess)
        } else {
            Ok(WriteResult::AllFailed("no records processed".into()))
        }
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
