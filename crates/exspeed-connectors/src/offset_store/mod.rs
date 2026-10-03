//! Connector offsets (source checkpoints and sink positions).
//!
//! - [`log_store::LogOffsetStore`] (default, `EXSPEED_CONNECTOR_OFFSET_STORE=log`): records
//!   in the internal `__connector_offsets` stream, written through the
//!   broker `Log`, so offsets replicate with the data.
//! - [`file::FileOffsetStore`] (`EXSPEED_CONNECTOR_OFFSET_STORE=file`): one JSON file
//!   per connector under `{data_dir}/connector-offsets/`, written atomically.
//!
//! A load error is an error, never "no offset": the supervisor marks the
//! connector `failed` instead of silently restarting from scratch.

pub mod file;
pub mod log_store;

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use exspeed_broker::log::Log;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum OffsetStoreError {
    #[error("offset store I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("offset store write failed: {0}")]
    Write(String),
    #[error("offset store read failed: {0}")]
    Read(String),
    #[error("corrupt offset for connector '{connector}': {detail}")]
    Corrupt { connector: String, detail: String },
}

/// What is stored per connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoredOffset {
    /// Opaque source checkpoint (an LSN, a cursor, …).
    Source { checkpoint: String },
    /// Next stream offset a sink will read.
    Sink { offset: u64 },
}

#[async_trait]
pub trait OffsetStore: Send + Sync {
    async fn load(&self, connector: &str) -> Result<Option<StoredOffset>, OffsetStoreError>;
    async fn save(&self, connector: &str, offset: &StoredOffset) -> Result<(), OffsetStoreError>;
    async fn delete(&self, connector: &str) -> Result<(), OffsetStoreError>;

    async fn load_source(&self, connector: &str) -> Result<Option<String>, OffsetStoreError> {
        match self.load(connector).await? {
            None => Ok(None),
            Some(StoredOffset::Source { checkpoint }) => Ok(Some(checkpoint)),
            Some(StoredOffset::Sink { .. }) => Err(OffsetStoreError::Corrupt {
                connector: connector.into(),
                detail: "stored offset belongs to a sink, but the connector is a source".into(),
            }),
        }
    }

    async fn load_sink(&self, connector: &str) -> Result<Option<u64>, OffsetStoreError> {
        match self.load(connector).await? {
            None => Ok(None),
            Some(StoredOffset::Sink { offset }) => Ok(Some(offset)),
            Some(StoredOffset::Source { .. }) => Err(OffsetStoreError::Corrupt {
                connector: connector.into(),
                detail: "stored offset belongs to a source, but the connector is a sink".into(),
            }),
        }
    }

    async fn save_source(&self, connector: &str, checkpoint: &str) -> Result<(), OffsetStoreError> {
        self.save(
            connector,
            &StoredOffset::Source {
                checkpoint: checkpoint.to_string(),
            },
        )
        .await
    }

    async fn save_sink(&self, connector: &str, offset: u64) -> Result<(), OffsetStoreError> {
        self.save(connector, &StoredOffset::Sink { offset }).await
    }
}

/// Environment variable selecting the connector offset store.
pub const ENV_VAR: &str = "EXSPEED_CONNECTOR_OFFSET_STORE";

/// The backend name `from_env` will use.
pub fn backend_from_env() -> String {
    std::env::var(ENV_VAR).unwrap_or_else(|_| "log".to_string())
}

/// Build the offset store selected by `EXSPEED_CONNECTOR_OFFSET_STORE`
/// (`log`, the default, or `file`). `EXSPEED_OFFSET_STORE` is not consulted:
/// it selects the consumer/lease backends.
pub fn from_env(data_dir: &Path, log: Arc<Log>) -> Result<Arc<dyn OffsetStore>, String> {
    let backend = backend_from_env();
    match backend.as_str() {
        "" | "log" => Ok(Arc::new(log_store::LogOffsetStore::new(log))),
        "file" => Ok(Arc::new(file::FileOffsetStore::new(data_dir.to_path_buf()))),
        other => Err(format!(
            "unknown {ENV_VAR} '{other}' (expected 'log' or 'file')"
        )),
    }
}
