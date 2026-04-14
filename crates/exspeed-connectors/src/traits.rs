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
