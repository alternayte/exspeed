//! Backend abstraction over "execute parameterized SQL against a pool".
//!
//! The `Dialect` trait produces SQL text; `SinkBackend` executes it. Existing
//! Postgres + MySQL paths live in `SqlxBackend`; MSSQL lives in `TiberiusBackend`
//! because `sqlx::Any` dropped MSSQL support in v0.6.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// A bound parameter, owned. We keep this owned (rather than borrowed with a
/// lifetime) because the extra `String` clones per record are negligible next
/// to the SQL round-trip, and the simpler API removes lifetime gymnastics
/// across the backend boundary.
#[derive(Debug, Clone)]
pub enum Param {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Timestamptz(DateTime<Utc>),
    /// Pre-serialized JSON text. Stored as the dialect's JSON column type
    /// (`JSONB` on Postgres, `JSON` on MySQL, `NVARCHAR(MAX)` on MSSQL).
    JsonText(String),
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// Connecting failed or the connection dropped.
    #[error("connection error: {0}")]
    Connection(String),
    /// A parameter could not be encoded (a bad value in this record).
    #[error("encode error: {0}")]
    Encode(String),
    /// A configuration problem in the driver (bad URL, TLS setup, ...).
    #[error("configuration error: {0}")]
    Config(String),
    /// The database rejected the statement. `code` is the dialect's error
    /// code: SQLSTATE for Postgres, the vendor error number for MySQL and
    /// SQL Server, the extended result code for SQLite.
    #[error("sql error ({code}): {message}")]
    Sql { code: String, message: String },
    #[error("{0}")]
    Other(String),
}

#[async_trait]
pub trait SinkBackend: Send + Sync {
    /// Execute a DDL statement (CREATE TABLE, etc.). No parameters.
    async fn execute_ddl(&self, sql: &str) -> Result<(), BackendError>;

    /// Execute a DML statement (single- or multi-row) with the given bound
    /// parameters, in positional order.
    async fn execute_row(&self, sql: &str, params: &[Param]) -> Result<(), BackendError>;

    /// Close the underlying pool. Idempotent.
    async fn close(&self);
}
