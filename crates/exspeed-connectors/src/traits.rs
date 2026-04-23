use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("connection error: {0}")]
    Connection(String),

    #[error("data error: {0}")]
    Data(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

// ---------------------------------------------------------------------------
// Source types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SourceRecord {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub subject: String,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct SourceBatch {
    pub records: Vec<SourceRecord>,
    pub position: Option<String>,
}

// ---------------------------------------------------------------------------
// Sink types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SinkRecord {
    pub offset: u64,
    pub timestamp: u64,
    pub subject: String,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct SinkBatch {
    pub records: Vec<SinkRecord>,
}

#[derive(Debug, Clone)]
pub enum WriteResult {
    AllSuccess,
    PartialSuccess { last_successful_offset: u64 },
    /// A single record is unrecoverable. Manager routes to DLQ (if configured)
    /// and advances past `poison_offset`. Prior records in the same batch were
    /// already written by the sink (it returns `Poison` after completing
    /// partial work up to `last_successful_offset`).
    Poison {
        last_successful_offset: Option<u64>,
        poison_offset: u64,
        reason: PoisonReason,
        record: SinkRecord,
    },
    /// Whole-batch transient failure. Manager applies `RetryPolicy`; exhaustion
    /// dispatches on `on_transient_exhausted`.
    TransientFailure {
        last_successful_offset: Option<u64>,
        error: String,
    },
    /// DEPRECATED: kept for compatibility during sink migration. New sinks
    /// must return `TransientFailure` or `Poison` instead. Removed once all
    /// builtin sinks are migrated.
    #[deprecated(note = "use TransientFailure or Poison")]
    AllFailed(String),
}

// ---------------------------------------------------------------------------
// SourceConnector
// ---------------------------------------------------------------------------

#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Start the connector, optionally resuming from a previous position.
    async fn start(&mut self, last_position: Option<String>) -> Result<(), ConnectorError>;

    /// Poll for the next batch of records (up to `max_batch` items).
    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError>;

    /// Commit a position so that future restarts resume from here.
    async fn commit(&mut self, position: String) -> Result<(), ConnectorError>;

    /// Gracefully stop the connector.
    async fn stop(&mut self) -> Result<(), ConnectorError>;

    /// Check the health of the connector.
    async fn health(&self) -> HealthStatus;
}

// ---------------------------------------------------------------------------
// SinkConnector
// ---------------------------------------------------------------------------

#[async_trait]
pub trait SinkConnector: Send + Sync {
    /// Start the sink connector.
    async fn start(&mut self) -> Result<(), ConnectorError>;

    /// Write a batch of records to the sink.
    async fn write(&mut self, batch: SinkBatch) -> Result<WriteResult, ConnectorError>;

    /// Flush any buffered data.
    async fn flush(&mut self) -> Result<(), ConnectorError>;

    /// Gracefully stop the connector.
    async fn stop(&mut self) -> Result<(), ConnectorError>;

    /// Check the health of the connector.
    async fn health(&self) -> HealthStatus;
}

// ---------------------------------------------------------------------------
// Poison classification
// ---------------------------------------------------------------------------

/// Stable classification of why a single record was unrecoverable.
///
/// Used by `WriteResult::Poison`. The variant carries any structured context
/// needed by observability; `label()` returns a low-cardinality metric-safe
/// tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoisonReason {
    NonJsonRecord,
    TypeMismatch { field: String, expected: String, got: String },
    MissingRequiredField { field: String },
    TimestampParseFailed { field: String },
    HttpClientError { status: u16 },
    SinkRejected { detail: String },
}

impl PoisonReason {
    /// Low-cardinality stable label suitable for metric tagging.
    pub fn label(&self) -> &'static str {
        match self {
            Self::NonJsonRecord => "non_json_record",
            Self::TypeMismatch { .. } => "type_mismatch",
            Self::MissingRequiredField { .. } => "missing_required_field",
            Self::TimestampParseFailed { .. } => "timestamp_parse_failed",
            Self::HttpClientError { .. } => "http_client_error",
            Self::SinkRejected { .. } => "sink_rejected",
        }
    }

    /// Human-readable detail string for DLQ headers / log lines.
    pub fn detail(&self) -> String {
        match self {
            Self::NonJsonRecord => "record body is not valid JSON".into(),
            Self::TypeMismatch { field, expected, got } =>
                format!("field '{field}': expected {expected}, got {got}"),
            Self::MissingRequiredField { field } =>
                format!("field '{field}': missing required value"),
            Self::TimestampParseFailed { field } =>
                format!("field '{field}': could not parse as RFC3339 timestamp"),
            Self::HttpClientError { status } =>
                format!("HTTP client error (status {status})"),
            Self::SinkRejected { detail } => detail.clone(),
        }
    }
}

#[cfg(test)]
mod poison_reason_tests {
    use super::*;

    #[test]
    fn label_is_stable_per_variant() {
        assert_eq!(PoisonReason::NonJsonRecord.label(), "non_json_record");
        assert_eq!(
            PoisonReason::TypeMismatch {
                field: "x".into(), expected: "int".into(), got: "str".into()
            }.label(),
            "type_mismatch"
        );
        assert_eq!(
            PoisonReason::HttpClientError { status: 404 }.label(),
            "http_client_error"
        );
    }

    #[test]
    fn detail_includes_structured_fields() {
        let r = PoisonReason::TypeMismatch {
            field: "order_id".into(),
            expected: "bigint".into(),
            got: "string".into(),
        };
        let d = r.detail();
        assert!(d.contains("order_id"));
        assert!(d.contains("bigint"));
        assert!(d.contains("string"));
    }
}
