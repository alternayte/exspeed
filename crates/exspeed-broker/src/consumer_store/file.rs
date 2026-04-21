use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::consumer_state::ConsumerConfig;
use crate::persistence;
use super::{ConsumerStore, ConsumerStoreError};

/// Default interval at which the background flusher drains pending
/// debounced saves. Acks received within this window for the same consumer
/// collapse into a single disk write. On crash, up to one window of acked
/// offsets can be lost.
const DEFAULT_DEBOUNCE_INTERVAL_MS: u64 = 100;

/// Consumer store backed by JSON files in `{data_dir}/consumers/`.
///
/// All disk I/O runs inside `tokio::task::spawn_blocking` so it does not
/// stall the tokio worker. `save_debounced` additionally coalesces rapid
/// per-consumer updates by enqueueing into a background flusher task that
/// writes at most once per consumer per `DEFAULT_DEBOUNCE_INTERVAL_MS`.
pub struct FileConsumerStore {
    data_dir: PathBuf,
    /// Sender for the debounce flusher. Dropping the FileConsumerStore
    /// closes the channel; the flusher observes that, performs a final
    /// flush of any pending saves, then exits.
    debouncer_tx: mpsc::UnboundedSender<ConsumerConfig>,
}

impl FileConsumerStore {
    pub fn new(data_dir: PathBuf) -> Self {
        Self::with_debounce(data_dir, Duration::from_millis(DEFAULT_DEBOUNCE_INTERVAL_MS))
    }

    /// Constructor for tests that need a deterministic debounce window.
    pub fn with_debounce(data_dir: PathBuf, interval: Duration) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(flusher(data_dir.clone(), rx, interval));
        Self { data_dir, debouncer_tx: tx }
    }
}

/// Background task that drains `rx`, coalescing latest-wins per consumer
/// name, and flushes the whole map every `interval` (or immediately on
/// channel close before exiting).
async fn flusher(
    data_dir: PathBuf,
    mut rx: mpsc::UnboundedReceiver<ConsumerConfig>,
    interval: Duration,
) {
    let mut pending: HashMap<String, ConsumerConfig> = HashMap::new();
    let mut ticker = tokio::time::interval(interval);
    // Skip the immediate first tick so we wait a full interval before the
    // first opportunistic flush.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if pending.is_empty() { continue; }
                let to_flush: Vec<ConsumerConfig> = std::mem::take(&mut pending).into_values().collect();
                let dd = data_dir.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    for cfg in &to_flush {
                        if let Err(e) = persistence::save_consumer(&dd, cfg) {
                            tracing::warn!(
                                consumer = %cfg.name,
                                error = %e,
                                "consumer_store: debounced save failed"
                            );
                        }
                    }
                }).await;
            }
            req = rx.recv() => {
                match req {
                    Some(cfg) => {
                        // Latest-wins coalescing: a burst of acks for the same
                        // consumer collapses to the latest config at flush time.
                        pending.insert(cfg.name.clone(), cfg);
                    }
                    None => {
                        // Channel closed — final flush then exit.
                        if !pending.is_empty() {
                            let to_flush: Vec<ConsumerConfig> = std::mem::take(&mut pending).into_values().collect();
                            let dd = data_dir.clone();
                            let _ = tokio::task::spawn_blocking(move || {
                                for cfg in &to_flush {
                                    let _ = persistence::save_consumer(&dd, cfg);
                                }
                            }).await;
                        }
                        return;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl ConsumerStore for FileConsumerStore {
    async fn save(&self, config: &ConsumerConfig) -> Result<(), ConsumerStoreError> {
        let data_dir = self.data_dir.clone();
        let config = config.clone();
        let result = tokio::task::spawn_blocking(move || persistence::save_consumer(&data_dir, &config))
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("spawn_blocking join failed: {e}")))?;
        result?;
        Ok(())
    }

    async fn save_debounced(&self, config: ConsumerConfig) -> Result<(), ConsumerStoreError> {
        // Fire-and-forget enqueue. If the channel is closed (shouldn't happen
        // during normal operation — the flusher outlives the store), fall
        // back to a synchronous save so data is not silently dropped.
        if self.debouncer_tx.send(config.clone()).is_err() {
            return self.save(&config).await;
        }
        Ok(())
    }

    async fn load(&self, name: &str) -> Result<Option<ConsumerConfig>, ConsumerStoreError> {
        let data_dir = self.data_dir.clone();
        let name = name.to_string();
        let result = tokio::task::spawn_blocking(move || persistence::load_consumer(&data_dir, &name))
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("spawn_blocking join failed: {e}")))?;
        match result {
            Ok(cfg) => Ok(Some(cfg)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(ConsumerStoreError::Io(e)),
        }
    }

    async fn load_all(&self) -> Result<Vec<ConsumerConfig>, ConsumerStoreError> {
        let data_dir = self.data_dir.clone();
        let result = tokio::task::spawn_blocking(move || persistence::load_all_consumers(&data_dir))
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("spawn_blocking join failed: {e}")))?;
        Ok(result?)
    }

    async fn delete(&self, name: &str) -> Result<(), ConsumerStoreError> {
        let data_dir = self.data_dir.clone();
        let name = name.to_string();
        let result = tokio::task::spawn_blocking(move || persistence::delete_consumer(&data_dir, &name))
            .await
            .map_err(|e| ConsumerStoreError::Connection(format!("spawn_blocking join failed: {e}")))?;
        match result {
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

    #[tokio::test]
    async fn save_debounced_eventually_persists() {
        let dir = TempDir::new().unwrap();
        // Short debounce window so the test finishes quickly.
        let store = FileConsumerStore::with_debounce(
            dir.path().to_path_buf(),
            Duration::from_millis(20),
        );
        let mut cfg = sample_config("debounced");

        // Fire many debounced saves for the same consumer in rapid succession.
        // Latest-wins coalescing should keep only the last offset.
        for i in 0..100 {
            cfg.offset = i;
            store.save_debounced(cfg.clone()).await.unwrap();
        }

        // Wait long enough for the flusher to drain.
        tokio::time::sleep(Duration::from_millis(80)).await;

        let loaded = store.load("debounced").await.unwrap().unwrap();
        assert_eq!(loaded.offset, 99, "expected last-write-wins coalescing");
    }
}
