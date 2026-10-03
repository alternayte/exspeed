//! The plugin contract: what a source or sink implements, and the error
//! taxonomy every plugin maps its failures into.
//!
//! # Sources
//!
//! The framework drives a source like this:
//!
//! ```text
//! start(checkpoint) ─► loop { poll() ─► append via Log ─► persist checkpoint ─► ack() } ─► stop()
//! ```
//!
//! `poll()` must not acknowledge anything externally. `ack()` is called only
//! after every record of the batch is durably in the log **and** the batch's
//! checkpoint is persisted; it is the one place where a source may confirm an
//! LSN, ack an AMQP delivery or delete outbox rows. A crash anywhere before
//! `ack()` replays the batch (at-least-once).
//!
//! # Sinks
//!
//! ```text
//! start() ─► loop { write(batch) … flush() ─► commit offset } ─► flush() ─► commit ─► stop()
//! ```
//!
//! `write()` may buffer. `flush()` is a barrier: when it returns `Ok`, every
//! record accepted by earlier `write()` calls is durable in the target. The
//! framework commits the consumer position only after a successful flush.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Error taxonomy shared by every plugin.
///
/// | Variant      | Meaning                                   | Framework reaction |
/// |--------------|-------------------------------------------|--------------------|
/// | `Transient`  | may succeed if retried (timeout, 503, deadlock) | retry in place with `[retry]`, then `on_transient_exhausted` |
/// | `Connection` | the connection is gone (socket closed, server restart) | supervisor restart: `stop()` + `start()` with backoff |
/// | `Poison`     | this record can never be written          | DLQ (or drop + metric), continue |
/// | `Fatal`      | configuration/auth problem; retrying is pointless | connector → `failed` with `last_error` |
#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("transient error: {message}")]
    Transient {
        message: String,
        /// Minimum wait before retrying (e.g. HTTP `Retry-After`).
        retry_after: Option<Duration>,
    },

    #[error("connection error: {0}")]
    Connection(String),

    #[error("poison record: {0}")]
    Poison(PoisonReason),

    #[error("fatal error: {0}")]
    Fatal(String),
}

impl ConnectorError {
    pub fn transient(message: impl Into<String>) -> Self {
        Self::Transient {
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn connection(message: impl Into<String>) -> Self {
        Self::Connection(message.into())
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self::Fatal(message.into())
    }

    /// Shorthand for a configuration problem (a `Fatal` error).
    pub fn config(message: impl Into<String>) -> Self {
        Self::Fatal(message.into())
    }

    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Transient { .. } => ErrorKind::Transient,
            Self::Connection(_) => ErrorKind::Connection,
            Self::Poison(_) => ErrorKind::Poison,
            Self::Fatal(_) => ErrorKind::Fatal,
        }
    }

    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Transient { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

impl From<std::io::Error> for ConnectorError {
    fn from(e: std::io::Error) -> Self {
        Self::transient(format!("I/O error: {e}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Transient,
    Connection,
    Poison,
    Fatal,
}

// ---------------------------------------------------------------------------
// Source types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct SourceRecord {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub subject: String,
    pub headers: Vec<(String, String)>,
}

/// One poll's worth of records plus the position to resume from once they
/// are durable.
#[derive(Debug, Clone, Default)]
pub struct SourceBatch {
    pub records: Vec<SourceRecord>,
    /// Opaque resume position covering every record in this batch (and
    /// everything before it). `None` = no new position (e.g. a batch that
    /// stops mid-transaction, or a source without a durable cursor). An
    /// empty batch may still carry a checkpoint (e.g. an idle Postgres slot
    /// advancing on a keepalive).
    pub checkpoint: Option<String>,
}

impl SourceBatch {
    pub fn empty() -> Self {
        Self::default()
    }
}

/// A lag reading reported by a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lag {
    pub value: u64,
    pub unit: LagUnit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagUnit {
    Records,
    Bytes,
    Rows,
}

impl LagUnit {
    pub fn as_str(&self) -> &'static str {
        match self {
            LagUnit::Records => "records",
            LagUnit::Bytes => "bytes",
            LagUnit::Rows => "rows",
        }
    }
}

#[async_trait]
pub trait SourceConnector: Send {
    /// Connect and position at `checkpoint` (`None` = fresh start).
    async fn start(&mut self, checkpoint: Option<String>) -> Result<(), ConnectorError>;

    /// Return the next records. Must not acknowledge anything externally.
    ///
    /// A `Transient` error makes the runtime call `poll` again on the same
    /// instance, so it must leave the source able to return the same records
    /// again: it must not have consumed upstream state (advanced a stream,
    /// popped a queue, moved a cursor) that the failed call's records came
    /// from. When it has — e.g. a replication stream that already delivered
    /// part of the batch — return a `Connection` error instead; the runtime
    /// then restarts the connector from the last saved checkpoint.
    async fn poll(&mut self, max_batch: usize) -> Result<SourceBatch, ConnectorError>;

    /// Called after the last polled batch is durable and `checkpoint` (the
    /// batch's checkpoint, if any) is persisted. The only place external
    /// state may be acknowledged.
    async fn ack(&mut self, checkpoint: Option<&str>) -> Result<(), ConnectorError>;

    /// Disconnect. Must be idempotent and must release external resources
    /// (replication slots, consumers) before returning, so a restart never
    /// races the old instance.
    async fn stop(&mut self) -> Result<(), ConnectorError>;

    /// Latest lag reading, if the source can measure one.
    fn lag(&self) -> Option<Lag> {
        None
    }

    /// Fetch up to `max` sample records without side effects: no slot or
    /// publication creation, no ack. Used by `exspeed connector dry-run`.
    /// The default starts fresh, polls once and stops without acking.
    async fn dry_run(&mut self, max: usize) -> Result<Vec<SourceRecord>, ConnectorError> {
        self.start(None).await?;
        let polled = self.poll(max).await;
        let _ = self.stop().await;
        Ok(polled?.records.into_iter().take(max).collect())
    }

    /// Release external resources that outlive a single run (e.g. drop the
    /// replication slot). Called when the connector is deleted, after
    /// `stop()`. Default: nothing.
    async fn cleanup(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Sink types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct SinkRecord {
    pub offset: u64,
    pub timestamp: u64,
    pub subject: String,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, String)>,
}

impl SinkRecord {
    /// The record's idempotency key: its `x-idempotency-key` header if set,
    /// otherwise `<stream>:<offset>`.
    pub fn idempotency_key(&self, stream: &str) -> String {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("x-idempotency-key"))
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| format!("{stream}:{}", self.offset))
    }
}

/// Outcome of [`SinkConnector::write`].
#[derive(Debug)]
pub enum WriteResult {
    /// Every record was accepted (written, or buffered until `flush()`).
    Accepted,
    /// `records[..index]` were accepted; `records[index]` can never be
    /// written. The framework DLQs it and calls `write` again with
    /// `records[index + 1..]`.
    Poison { index: usize, reason: PoisonReason },
    /// `records[..accepted]` were accepted, then `error` happened. A
    /// `Poison` error here is treated like `WriteResult::Poison` at index
    /// `accepted`.
    Failed {
        accepted: usize,
        error: ConnectorError,
    },
}

#[async_trait]
pub trait SinkConnector: Send {
    async fn start(&mut self) -> Result<(), ConnectorError>;

    /// Write (or buffer) `records`. Records arrive in offset order.
    async fn write(&mut self, records: &[SinkRecord]) -> Result<WriteResult, ConnectorError>;

    /// Barrier: make everything accepted so far durable in the target.
    async fn flush(&mut self) -> Result<(), ConnectorError>;

    /// Disconnect. Called after a final `flush()` on graceful stop, and
    /// without one on restart after an error (the uncommitted records are
    /// re-read and re-written after the restart).
    async fn stop(&mut self) -> Result<(), ConnectorError>;

    /// Whether the sink's buffer is full and it wants `flush()` now.
    fn wants_flush(&self) -> bool {
        false
    }

    /// How often the framework flushes and commits when the connector
    /// config doesn't set `flush_interval_ms`. `Duration::ZERO` = after
    /// every written batch.
    fn default_flush_interval(&self) -> Duration {
        Duration::from_millis(1000)
    }
}

// ---------------------------------------------------------------------------
// Poison classification
// ---------------------------------------------------------------------------

/// Stable classification of why a single record was unrecoverable.
///
/// `label()` returns a low-cardinality metric-safe tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoisonReason {
    NonJsonRecord,
    TypeMismatch {
        field: String,
        expected: String,
        got: String,
    },
    MissingRequiredField {
        field: String,
    },
    TimestampParseFailed {
        field: String,
    },
    HttpClientError {
        status: u16,
    },
    InvalidRecord {
        detail: String,
    },
    SinkRejected {
        detail: String,
    },
    RetriesExhausted {
        detail: String,
    },
}

impl std::fmt::Display for PoisonReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.label(), self.detail())
    }
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
            Self::InvalidRecord { .. } => "invalid_record",
            Self::SinkRejected { .. } => "sink_rejected",
            Self::RetriesExhausted { .. } => "retries_exhausted",
        }
    }

    /// Human-readable detail string for DLQ headers / log lines.
    pub fn detail(&self) -> String {
        match self {
            Self::NonJsonRecord => "record body is not valid JSON".into(),
            Self::TypeMismatch {
                field,
                expected,
                got,
            } => format!("field '{field}': expected {expected}, got {got}"),
            Self::MissingRequiredField { field } => {
                format!("field '{field}': missing required value")
            }
            Self::TimestampParseFailed { field } => {
                format!("field '{field}': could not parse as RFC3339 timestamp")
            }
            Self::HttpClientError { status } => format!("HTTP client error (status {status})"),
            Self::InvalidRecord { detail }
            | Self::SinkRejected { detail }
            | Self::RetriesExhausted { detail } => detail.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_is_stable_per_variant() {
        assert_eq!(PoisonReason::NonJsonRecord.label(), "non_json_record");
        assert_eq!(
            PoisonReason::TypeMismatch {
                field: "x".into(),
                expected: "int".into(),
                got: "str".into()
            }
            .label(),
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

    #[test]
    fn error_kinds() {
        assert_eq!(ConnectorError::transient("x").kind(), ErrorKind::Transient);
        assert_eq!(
            ConnectorError::connection("x").kind(),
            ErrorKind::Connection
        );
        assert_eq!(ConnectorError::fatal("x").kind(), ErrorKind::Fatal);
        assert_eq!(
            ConnectorError::Poison(PoisonReason::NonJsonRecord).kind(),
            ErrorKind::Poison
        );
    }

    #[test]
    fn idempotency_key_prefers_header() {
        let mut r = SinkRecord {
            offset: 7,
            timestamp: 0,
            subject: String::new(),
            key: None,
            value: Bytes::new(),
            headers: vec![],
        };
        assert_eq!(r.idempotency_key("s"), "s:7");
        r.headers.push(("x-idempotency-key".into(), "abc".into()));
        assert_eq!(r.idempotency_key("s"), "abc");
    }
}
