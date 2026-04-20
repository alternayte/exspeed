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
    #[error("serialization: {0}")]
    Serde(String),
}

impl From<bincode::Error> for ReplicationError {
    fn from(e: bincode::Error) -> Self {
        ReplicationError::Serde(e.to_string())
    }
}
