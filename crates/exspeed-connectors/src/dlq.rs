//! Dead-letter queue writer.
//!
//! Poison records are appended to the connector's `dlq_stream` through the
//! broker [`Log`]. The original payload is kept byte for byte; metadata is
//! added as `exspeed-dlq-*` headers. Each DLQ record carries an idempotency
//! key derived from the connector and the record's origin, so replaying a
//! batch after a crash doesn't duplicate DLQ entries.
//!
//! Without a `dlq_stream` the record is dropped and counted in
//! `exspeed_connector_records_skipped_total`.

use std::sync::Arc;

use exspeed_broker::broker_append::IDEMPOTENCY_HEADER;
use exspeed_broker::log::{Log, LogError};
use exspeed_common::{Metrics, StreamName};
use exspeed_streams::record::Record;
use opentelemetry::KeyValue;
use tracing::{debug, error, warn};

use crate::traits::PoisonReason;

/// A record headed for the DLQ.
#[derive(Debug, Clone)]
pub struct DlqEntry {
    pub subject: String,
    pub key: Option<bytes::Bytes>,
    pub value: bytes::Bytes,
    pub headers: Vec<(String, String)>,
    /// Offset on the source stream (sinks) — `None` for source records.
    pub original_offset: Option<u64>,
    pub timestamp: Option<u64>,
    /// Stable identity used to dedup the DLQ write on replay.
    pub identity: String,
}

pub struct DlqWriter {
    log: Arc<Log>,
    stream: Option<StreamName>,
    origin: String,
    source_stream: String,
    metrics: Arc<Metrics>,
}

impl DlqWriter {
    pub fn new(
        log: Arc<Log>,
        stream: Option<StreamName>,
        origin: String,
        source_stream: String,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            log,
            stream,
            origin,
            source_stream,
            metrics,
        }
    }

    pub fn stream(&self) -> Option<&StreamName> {
        self.stream.as_ref()
    }

    pub async fn ensure_stream(&self) -> Result<(), LogError> {
        match &self.stream {
            Some(s) => self.log.ensure_stream(s).await,
            None => Ok(()),
        }
    }

    /// Route a poison record: DLQ if configured, otherwise drop + metric.
    /// Returns an error only for a retryable DLQ append failure, so the
    /// caller can retry instead of losing the record.
    pub async fn handle(&self, entry: DlqEntry, reason: &PoisonReason) -> Result<(), LogError> {
        let Some(stream) = &self.stream else {
            warn!(
                connector = %self.origin,
                reason = reason.label(),
                detail = %reason.detail(),
                "poison record dropped (no dlq_stream configured)"
            );
            self.metrics.connector_records_skipped_total.add(
                1,
                &[
                    KeyValue::new("connector", self.origin.clone()),
                    KeyValue::new("stream", self.source_stream.clone()),
                    KeyValue::new("reason", reason.label()),
                ],
            );
            return Ok(());
        };

        let record = self.build(entry, reason);
        match self.log.append(stream, record.clone()).await {
            Ok(_) => {}
            Err(LogError::InvalidRecord(detail)) => {
                // The record itself is unwritable (e.g. a subject with
                // whitespace): keep it, but under an empty subject.
                let mut r = record;
                r.headers
                    .push(("exspeed-dlq-original-subject".into(), r.subject.clone()));
                r.subject = String::new();
                if let Err(e) = self.log.append(stream, r).await {
                    return self.failed(e, &detail);
                }
            }
            Err(e) if e.is_retryable() => return Err(e),
            Err(e) => return self.failed(e, "append failed"),
        }
        debug!(dlq_stream = %stream, origin = %self.origin, reason = reason.label(), "DLQ write");
        self.metrics.connector_dlq_total.add(
            1,
            &[
                KeyValue::new("connector", self.origin.clone()),
                KeyValue::new("reason", reason.label()),
            ],
        );
        Ok(())
    }

    fn failed(&self, e: LogError, context: &str) -> Result<(), LogError> {
        error!(connector = %self.origin, error = %e, context, "DLQ append failed; record lost");
        self.metrics
            .connector_dlq_failures_total
            .add(1, &[KeyValue::new("connector", self.origin.clone())]);
        Ok(())
    }

    fn build(&self, entry: DlqEntry, reason: &PoisonReason) -> Record {
        let mut headers: Vec<(String, String)> = Vec::with_capacity(entry.headers.len() + 7);
        for (k, v) in entry.headers {
            if k.eq_ignore_ascii_case(IDEMPOTENCY_HEADER) {
                headers.push(("exspeed-dlq-original-idempotency-key".into(), v));
            } else {
                headers.push((k, v));
            }
        }
        headers.push(("exspeed-dlq-origin".into(), self.origin.clone()));
        headers.push(("exspeed-dlq-reason".into(), reason.label().to_string()));
        let mut detail = reason.detail();
        if detail.len() > 4096 {
            let mut cut = 4096;
            while !detail.is_char_boundary(cut) {
                cut -= 1;
            }
            detail.truncate(cut);
        }
        headers.push(("exspeed-dlq-detail".into(), detail));
        if let Some(o) = entry.original_offset {
            headers.push(("exspeed-dlq-original-offset".into(), o.to_string()));
        }
        if let Some(t) = entry.timestamp {
            headers.push(("exspeed-dlq-timestamp".into(), t.to_string()));
        }
        headers.push((
            IDEMPOTENCY_HEADER.into(),
            format!("dlq:{}:{}", self.origin, entry.identity),
        ));
        Record {
            key: entry.key,
            value: entry.value,
            subject: entry.subject,
            headers,
            timestamp_ns: None,
        }
    }
}
