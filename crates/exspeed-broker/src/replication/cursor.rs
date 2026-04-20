//! Persistent `{stream → next_offset}` cursor for followers.
//!
//! Written via `fsync + rename` so a crash mid-write leaves either the
//! previous cursor or the new one — never a partial file. Corrupt /
//! missing cursor is treated as empty (fresh follower) + a warning;
//! the cursor is a resume hint, not authoritative state.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::replication::errors::ReplicationError;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Cursor {
    /// Next offset to fetch per stream. `None` in the map means "not yet
    /// tracked"; `Some(0)` means "tracked, expecting offset 0 next".
    #[serde(flatten)]
    inner: BTreeMap<String, u64>,
}

impl Cursor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn as_map(&self) -> &BTreeMap<String, u64> {
        &self.inner
    }

    pub fn get(&self, stream: &str) -> Option<u64> {
        self.inner.get(stream).copied()
    }

    pub fn set(&mut self, stream: impl Into<String>, offset: u64) {
        self.inner.insert(stream.into(), offset);
    }

    pub fn remove(&mut self, stream: &str) {
        self.inner.remove(stream);
    }

    /// Load from `path`. Missing file → empty cursor (OK). Corrupt file →
    /// warning + empty cursor (OK).
    pub fn load(path: &Path) -> Result<Self, ReplicationError> {
        match std::fs::read_to_string(path) {
            Ok(s) => match serde_json::from_str(&s) {
                Ok(c) => Ok(c),
                Err(e) => {
                    warn!(path = %path.display(), err = %e, "cursor file is corrupt — starting empty");
                    Ok(Self::default())
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(ReplicationError::Io(e)),
        }
    }

    /// Save atomically: write to `{path}.tmp`, fsync, then rename. If the
    /// process crashes before rename, the old file is untouched.
    pub fn save(&self, path: &Path) -> Result<(), ReplicationError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp_path = path.with_extension("tmp");
        {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            let buf = serde_json::to_vec_pretty(self)
                .map_err(|e| ReplicationError::Serde(e.to_string()))?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }
}
