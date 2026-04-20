//! Error types for the replication module. Kept narrow — most failure
//! modes degrade to "drop the connection and retry" rather than
//! propagating up.

use exspeed_streams::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("version mismatch — leader speaks v{leader}, follower speaks v{follower}")]
    VersionSkew { leader: u16, follower: u16 },
    #[error("follower queue full — disconnecting")]
    QueueFull,
    #[error("apply error: {0}")]
    Apply(#[from] StorageError),
    #[error("auth denied: {0}")]
    AuthDenied(String),
    /// Typed bincode errors preserve the source for debugging.
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    /// Typed serde_json errors preserve the source for debugging.
    #[error("json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    /// String fallback for anything without a direct `From` impl.
    #[error("serialization: {0}")]
    Serde(String),
    /// Follower was asked to shut down (cancel token fired) while partway
    /// through a session. This is an expected path — log INFO at most,
    /// never WARN/ERROR; do not count it as a transient error or bump
    /// backoff. The outer loop will observe the cancel and exit.
    #[error("replication cancelled")]
    Cancelled,
}
