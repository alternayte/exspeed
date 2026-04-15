use std::path::PathBuf;

use async_trait::async_trait;

use super::{OffsetStore, OffsetStoreError};

pub struct FileOffsetStore {
    data_dir: PathBuf,
}

impl FileOffsetStore {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    fn offset_path(&self, connector: &str) -> PathBuf {
        self.data_dir
            .join("connectors")
            .join(format!("{}.offset.json", connector))
    }
}

#[async_trait]
impl OffsetStore for FileOffsetStore {
    async fn save_source_offset(
        &self,
        connector: &str,
        position: &str,
    ) -> Result<(), OffsetStoreError> {
        let path = self.offset_path(connector);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        crate::offset::save_offset(&path, position)?;
        Ok(())
    }

    async fn load_source_offset(
        &self,
        connector: &str,
    ) -> Result<Option<String>, OffsetStoreError> {
        let path = self.offset_path(connector);
        let result = crate::offset::load_offset(&path)?;
        Ok(result)
    }

    async fn save_sink_offset(
        &self,
        connector: &str,
        offset: u64,
    ) -> Result<(), OffsetStoreError> {
        let path = self.offset_path(connector);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        crate::offset::save_sink_offset(&path, offset)?;
        Ok(())
    }

    async fn load_sink_offset(&self, connector: &str) -> Result<u64, OffsetStoreError> {
        let path = self.offset_path(connector);
        let result = crate::offset::load_sink_offset(&path)?;
        Ok(result)
    }

    async fn delete(&self, connector: &str) -> Result<(), OffsetStoreError> {
        let path = self.offset_path(connector);
        crate::offset::delete_offset(&path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn source_offset_roundtrip() {
        let dir = TempDir::new().unwrap();
        let store = FileOffsetStore::new(dir.path().to_path_buf());
        store
            .save_source_offset("my-connector", "0/1A2B3C4")
            .await
            .unwrap();
        let loaded = store.load_source_offset("my-connector").await.unwrap();
        assert_eq!(loaded, Some("0/1A2B3C4".to_string()));
    }

    #[tokio::test]
    async fn source_offset_missing_returns_none() {
        let dir = TempDir::new().unwrap();
        let store = FileOffsetStore::new(dir.path().to_path_buf());
        let loaded = store.load_source_offset("nonexistent").await.unwrap();
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn sink_offset_roundtrip() {
        let dir = TempDir::new().unwrap();
        let store = FileOffsetStore::new(dir.path().to_path_buf());
        store
            .save_sink_offset("my-sink", 12345)
            .await
            .unwrap();
        let loaded = store.load_sink_offset("my-sink").await.unwrap();
        assert_eq!(loaded, 12345);
    }

    #[tokio::test]
    async fn sink_offset_missing_returns_zero() {
        let dir = TempDir::new().unwrap();
        let store = FileOffsetStore::new(dir.path().to_path_buf());
        let loaded = store.load_sink_offset("nonexistent").await.unwrap();
        assert_eq!(loaded, 0);
    }

    #[tokio::test]
    async fn delete_removes_offset() {
        let dir = TempDir::new().unwrap();
        let store = FileOffsetStore::new(dir.path().to_path_buf());
        store
            .save_source_offset("my-connector", "some-pos")
            .await
            .unwrap();
        store.delete("my-connector").await.unwrap();
        let loaded = store.load_source_offset("my-connector").await.unwrap();
        assert_eq!(loaded, None);
    }
}
