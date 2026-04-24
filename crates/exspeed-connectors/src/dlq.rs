//! Dead-letter queue writer.
//!
//! Wraps `BrokerAppend`, pre-resolved to a single DLQ stream at construction.
//! Owned by each connector task when `dlq_stream` is configured; `None`
//! otherwise (drop-with-metric behavior preserved).

use std::sync::Arc;

use exspeed_broker::broker_append::BrokerAppend;
use exspeed_common::StreamName;
use exspeed_streams::record::Record;
use exspeed_streams::StorageError;
use tracing::{debug, error};

use crate::config::ConnectorConfig;
use crate::traits::{PoisonReason, SinkRecord};

pub struct DlqWriter {
    broker: Arc<BrokerAppend>,
    stream: StreamName,
    origin: String,
}

/// Pure parse: returns the configured DLQ stream name, if any.
/// Unit-testable without a broker.
pub(crate) fn parse_dlq_config(
    config: &ConnectorConfig,
) -> Result<Option<StreamName>, String> {
    let dlq_name = config.setting_or("dlq_stream", "");
    let dlq_name = dlq_name.trim();
    if dlq_name.is_empty() {
        return Ok(None);
    }
    let stream = StreamName::try_from(dlq_name)
        .map_err(|e| format!("invalid dlq_stream name '{dlq_name}': {e}"))?;
    Ok(Some(stream))
}

impl DlqWriter {
    /// Returns `Ok(None)` when the connector's `dlq_stream` setting is unset.
    /// Otherwise validates the stream name and returns a writer bound to it.
    pub fn from_config(
        broker: Arc<BrokerAppend>,
        config: &ConnectorConfig,
    ) -> Result<Option<Self>, String> {
        match parse_dlq_config(config)? {
            None => Ok(None),
            Some(stream) => Ok(Some(Self {
                broker,
                stream,
                origin: config.name.clone(),
            })),
        }
    }

    /// The target stream name (for auto-create + logging).
    pub fn stream(&self) -> &StreamName {
        &self.stream
    }

    /// Append a poison record to the DLQ stream. The record's original
    /// payload bytes are preserved verbatim; metadata is added as headers
    /// prefixed `exspeed-dlq-*`.
    pub async fn write(
        &self,
        record: &SinkRecord,
        reason: &PoisonReason,
    ) -> Result<(), StorageError> {
        let mut headers = record.headers.clone();
        headers.push(("exspeed-dlq-origin".into(), self.origin.clone()));
        headers.push(("exspeed-dlq-reason".into(), reason.label().to_string()));
        headers.push(("exspeed-dlq-detail".into(), reason.detail()));
        headers.push((
            "exspeed-dlq-original-offset".into(),
            record.offset.to_string(),
        ));
        headers.push((
            "exspeed-dlq-timestamp".into(),
            record.timestamp.to_string(),
        ));

        let r = Record {
            key: record.key.clone(),
            value: record.value.clone(),
            subject: record.subject.clone(),
            headers,
            timestamp_ns: None,
        };

        match self.broker.append(&self.stream, &r).await {
            Ok(_) => {
                debug!(
                    dlq_stream = %self.stream,
                    origin = %self.origin,
                    reason = reason.label(),
                    original_offset = record.offset,
                    "DLQ write"
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    dlq_stream = %self.stream,
                    origin = %self.origin,
                    error = %e,
                    "DLQ append failed; continuing (best-effort)"
                );
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_config(name: &str, settings: HashMap<String, String>) -> ConnectorConfig {
        ConnectorConfig {
            name: name.into(),
            connector_type: "sink".into(),
            plugin: "http_sink".into(),
            stream: "events".into(),
            subject_template: String::new(),
            subject_filter: String::new(),
            settings,
            batch_size: 100,
            poll_interval_ms: 50,
            dedup_enabled: true,
            dedup_key: String::new(),
            dedup_window_secs: 86400,
            transform_sql: String::new(),
            key_field: String::new(),
            on_transient_exhausted: crate::config::OnTransientExhausted::default(),
            retry: crate::retry::RetryPolicy::default_transient(),
        }
    }

    #[test]
    fn parse_dlq_config_returns_none_when_unset() {
        let cfg = make_config("c1", HashMap::new());
        assert!(parse_dlq_config(&cfg).unwrap().is_none());
    }

    #[test]
    fn parse_dlq_config_returns_none_when_empty_or_whitespace() {
        let cfg = make_config("c1", HashMap::from([("dlq_stream".into(), "  ".into())]));
        assert!(parse_dlq_config(&cfg).unwrap().is_none());
    }

    #[test]
    fn parse_dlq_config_returns_stream_name_when_set() {
        let cfg = make_config(
            "c1",
            HashMap::from([("dlq_stream".into(), "my-dlq".into())]),
        );
        let out = parse_dlq_config(&cfg).unwrap().expect("should be Some");
        assert_eq!(out.as_str(), "my-dlq");
    }
}
