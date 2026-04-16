use std::path::PathBuf;

use async_trait::async_trait;

use crate::consumer_state::ConsumerConfig;
use crate::persistence;
use super::{ConsumerStore, ConsumerStoreError};

/// Consumer store backed by JSON files in `{data_dir}/consumers/`.
pub struct FileConsumerStore {
    data_dir: PathBuf,
}

impl FileConsumerStore {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }
}

#[async_trait]
impl ConsumerStore for FileConsumerStore {
    async fn save(&self, config: &ConsumerConfig) -> Result<(), ConsumerStoreError> {
        persistence::save_consumer(&self.data_dir, config)?;
        Ok(())
    }

    async fn load(&self, name: &str) -> Result<Option<ConsumerConfig>, ConsumerStoreError> {
        match persistence::load_consumer(&self.data_dir, name) {
            Ok(cfg) => Ok(Some(cfg)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(ConsumerStoreError::Io(e)),
        }
    }

    async fn load_all(&self) -> Result<Vec<ConsumerConfig>, ConsumerStoreError> {
        Ok(persistence::load_all_consumers(&self.data_dir)?)
    }

    async fn delete(&self, name: &str) -> Result<(), ConsumerStoreError> {
        match persistence::delete_consumer(&self.data_dir, name) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ConsumerStoreError::Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_config(name: &str) -> ConsumerConfig {
        ConsumerConfig {
            name: name.to_string(),
            stream: "orders".to_string(),
            group: "workers".to_string(),
            subject_filter: "order.>".to_string(),
            offset: 42,
        }
    }

    fn make_store() -> (FileConsumerStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = FileConsumerStore::new(dir.path().to_path_buf());
        (store, dir)
    }

    #[tokio::test]
    async fn save_and_load_roundtrip() {
        let (store, _dir) = make_store();
        let cfg = sample_config("c1");
        store.save(&cfg).await.unwrap();

        let loaded = store.load("c1").await.unwrap().unwrap();
        assert_eq!(loaded.name, "c1");
        assert_eq!(loaded.stream, "orders");
        assert_eq!(loaded.group, "workers");
        assert_eq!(loaded.subject_filter, "order.>");
        assert_eq!(loaded.offset, 42);
    }

    #[tokio::test]
    async fn load_missing_returns_none() {
        let (store, _dir) = make_store();
        let loaded = store.load("nonexistent").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn load_all_empty_returns_empty() {
        let (store, _dir) = make_store();
        let all = store.load_all().await.unwrap();
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn load_all_returns_sorted() {
        let (store, _dir) = make_store();
        store.save(&sample_config("beta")).await.unwrap();
        store.save(&sample_config("alpha")).await.unwrap();
        store.save(&sample_config("gamma")).await.unwrap();

        let all = store.load_all().await.unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].name, "alpha");
        assert_eq!(all[1].name, "beta");
        assert_eq!(all[2].name, "gamma");
    }

    #[tokio::test]
    async fn delete_removes_entry() {
        let (store, _dir) = make_store();
        store.save(&sample_config("to-delete")).await.unwrap();
        store.delete("to-delete").await.unwrap();
        assert!(store.load("to-delete").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_is_ok() {
        let (store, _dir) = make_store();
        store.delete("nothing-here").await.unwrap();
    }

    #[tokio::test]
    async fn save_updates_existing() {
        let (store, _dir) = make_store();
        let mut cfg = sample_config("updater");
        store.save(&cfg).await.unwrap();
        assert_eq!(store.load("updater").await.unwrap().unwrap().offset, 42);

        cfg.offset = 100;
        store.save(&cfg).await.unwrap();
        assert_eq!(store.load("updater").await.unwrap().unwrap().offset, 100);
    }
}
