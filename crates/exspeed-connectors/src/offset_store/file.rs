//! File-backed offset store: `{data_dir}/connector-offsets/<name>.json`,
//! written with tmp + fsync + rename + dir fsync. Local to one node; use the
//! default log store when running replicated.

use std::path::PathBuf;

use async_trait::async_trait;

use super::{OffsetStore, OffsetStoreError, StoredOffset};
use crate::config::write_atomic;

pub struct FileOffsetStore {
    dir: PathBuf,
}

impl FileOffsetStore {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            dir: data_dir.join("connector-offsets"),
        }
    }

    fn path(&self, connector: &str) -> PathBuf {
        self.dir.join(format!("{connector}.json"))
    }
}

#[async_trait]
impl OffsetStore for FileOffsetStore {
    async fn load(&self, connector: &str) -> Result<Option<StoredOffset>, OffsetStoreError> {
        let path = self.path(connector);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| OffsetStoreError::Corrupt {
                connector: connector.to_string(),
                detail: format!("{}: {e}", path.display()),
            })
    }

    async fn save(&self, connector: &str, offset: &StoredOffset) -> Result<(), OffsetStoreError> {
        let path = self.path(connector);
        let bytes =
            serde_json::to_vec(offset).map_err(|e| OffsetStoreError::Write(e.to_string()))?;
        tokio::task::spawn_blocking(move || write_atomic(&path, &bytes))
            .await
            .map_err(|e| OffsetStoreError::Write(e.to_string()))??;
        Ok(())
    }

    async fn delete(&self, connector: &str) -> Result<(), OffsetStoreError> {
        match tokio::fs::remove_file(self.path(connector)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn roundtrip_and_delete() {
        let dir = TempDir::new().unwrap();
        let store = FileOffsetStore::new(dir.path().to_path_buf());
        assert_eq!(store.load("c").await.unwrap(), None);
        store.save_source("c", "0/1A2B").await.unwrap();
        assert_eq!(
            store.load_source("c").await.unwrap().as_deref(),
            Some("0/1A2B")
        );
        store.save_sink("s", 42).await.unwrap();
        assert_eq!(store.load_sink("s").await.unwrap(), Some(42));
        assert!(
            store.load_source("s").await.is_err(),
            "kind mismatch is an error"
        );
        store.delete("c").await.unwrap();
        assert_eq!(store.load("c").await.unwrap(), None);
    }

    #[tokio::test]
    async fn corrupt_file_is_an_error_not_a_fresh_start() {
        let dir = TempDir::new().unwrap();
        let store = FileOffsetStore::new(dir.path().to_path_buf());
        std::fs::create_dir_all(dir.path().join("connector-offsets")).unwrap();
        std::fs::write(dir.path().join("connector-offsets/c.json"), b"{not json").unwrap();
        assert!(matches!(
            store.load("c").await,
            Err(OffsetStoreError::Corrupt { .. })
        ));
    }
}
