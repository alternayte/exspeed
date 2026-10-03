//! Offsets stored as records in the internal `__connector_offsets` stream.
//!
//! Each save appends `{"connector": .., "offset": {..}}` keyed by the
//! connector name through the broker [`Log`], so offsets take the same
//! write path as data (leader check, replication). A delete appends a
//! tombstone (`"offset": null`).
//!
//! Loading reads backwards from the high watermark in windows and returns
//! the newest record for the key. This works on a plain stream (every save
//! retained) and on a compacted one (gaps in offsets), and costs only the
//! distance to the connector's last save.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use exspeed_broker::log::Log;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{ReadLimits, Record, StorageError, StreamConfig};
use serde::{Deserialize, Serialize};

use super::{OffsetStore, OffsetStoreError, StoredOffset};

pub const OFFSETS_STREAM: &str = "__connector_offsets";

/// Records scanned per backward step.
const WINDOW: u64 = 512;

#[derive(Debug, Serialize, Deserialize)]
struct OffsetRecord {
    connector: String,
    /// `None` = tombstone.
    offset: Option<StoredOffset>,
}

pub struct LogOffsetStore {
    log: Arc<Log>,
    stream: StreamName,
    /// Last value written per connector, to skip redundant appends.
    last_written: Mutex<HashMap<String, Option<StoredOffset>>>,
}

impl LogOffsetStore {
    pub fn new(log: Arc<Log>) -> Self {
        Self {
            log,
            stream: StreamName::try_from(OFFSETS_STREAM).expect("valid internal stream name"),
            last_written: Mutex::new(HashMap::new()),
        }
    }

    /// Retention must never drop a connector's only offset record, so the
    /// stream keeps data for ~100 years and is unbounded in size; compaction
    /// keeps just the latest record per connector.
    fn stream_config() -> StreamConfig {
        StreamConfig {
            max_age_secs: 100 * 365 * 24 * 3600,
            max_bytes: u64::MAX / 2,
            compaction: true,
            ..StreamConfig::default()
        }
    }

    async fn ensure_stream(&self) -> Result<(), OffsetStoreError> {
        match self.log.storage().stream_bounds(&self.stream).await {
            Ok(_) => Ok(()),
            Err(StorageError::StreamNotFound(_)) => {
                match self
                    .log
                    .create_stream(&self.stream, &Self::stream_config())
                    .await
                {
                    Ok(())
                    | Err(exspeed_broker::log::LogError::Storage(
                        StorageError::StreamAlreadyExists(_),
                    )) => Ok(()),
                    Err(e) => Err(OffsetStoreError::Write(e.to_string())),
                }
            }
            Err(e) => Err(OffsetStoreError::Read(e.to_string())),
        }
    }

    async fn append(
        &self,
        connector: &str,
        offset: Option<StoredOffset>,
    ) -> Result<(), OffsetStoreError> {
        {
            let last = self.last_written.lock().unwrap();
            if last.get(connector) == Some(&offset) {
                return Ok(());
            }
        }
        self.ensure_stream().await?;
        let value = serde_json::to_vec(&OffsetRecord {
            connector: connector.to_string(),
            offset: offset.clone(),
        })
        .map_err(|e| OffsetStoreError::Write(e.to_string()))?;
        let record = Record {
            key: Some(Bytes::copy_from_slice(connector.as_bytes())),
            value: Bytes::from(value),
            subject: "connector.offset".to_string(),
            headers: vec![],
            timestamp_ns: None,
        };
        self.log
            .append(&self.stream, record)
            .await
            .map_err(|e| OffsetStoreError::Write(e.to_string()))?;
        self.last_written
            .lock()
            .unwrap()
            .insert(connector.to_string(), offset);
        Ok(())
    }
}

#[async_trait]
impl OffsetStore for LogOffsetStore {
    async fn load(&self, connector: &str) -> Result<Option<StoredOffset>, OffsetStoreError> {
        let storage = self.log.storage();
        let (earliest, hwm) = match storage.stream_bounds(&self.stream).await {
            Ok(b) => b,
            Err(StorageError::StreamNotFound(_)) => return Ok(None),
            Err(e) => return Err(OffsetStoreError::Read(e.to_string())),
        };
        let key = connector.as_bytes();
        let mut upper = hwm.0;
        while upper > earliest.0 {
            let lower = upper.saturating_sub(WINDOW).max(earliest.0);
            // Newest match within [lower, upper).
            let mut found: Option<Bytes> = None;
            let mut from = lower;
            while from < upper {
                let batch = storage
                    .read_batch(
                        &self.stream,
                        Offset(from),
                        ReadLimits {
                            max_records: (upper - from) as usize,
                            max_bytes: 4 * 1024 * 1024,
                        },
                    )
                    .await
                    .map_err(|e| OffsetStoreError::Read(e.to_string()))?;
                if batch.records.is_empty() {
                    break;
                }
                for r in &batch.records {
                    if r.offset.0 >= upper {
                        break;
                    }
                    if r.key.as_deref() == Some(key) {
                        found = Some(r.value.clone());
                    }
                }
                if batch.next_offset.0 <= from {
                    break;
                }
                from = batch.next_offset.0;
            }
            if let Some(value) = found {
                let rec: OffsetRecord =
                    serde_json::from_slice(&value).map_err(|e| OffsetStoreError::Corrupt {
                        connector: connector.to_string(),
                        detail: e.to_string(),
                    })?;
                self.last_written
                    .lock()
                    .unwrap()
                    .insert(connector.to_string(), rec.offset.clone());
                return Ok(rec.offset);
            }
            upper = lower;
        }
        Ok(None)
    }

    async fn save(&self, connector: &str, offset: &StoredOffset) -> Result<(), OffsetStoreError> {
        self.append(connector, Some(offset.clone())).await
    }

    async fn delete(&self, connector: &str) -> Result<(), OffsetStoreError> {
        self.last_written.lock().unwrap().remove(connector);
        match self.log.storage().stream_bounds(&self.stream).await {
            Err(StorageError::StreamNotFound(_)) => Ok(()),
            _ => self.append(connector, None).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exspeed_broker::broker_append::BrokerAppend;
    use exspeed_common::Metrics;
    use exspeed_storage::memory::MemoryStorage;
    use exspeed_streams::StorageEngine;
    use std::sync::atomic::AtomicBool;

    fn log() -> Arc<Log> {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let dedup = Arc::new(BrokerAppend::new(storage.clone(), 300));
        let (metrics, _) = Metrics::new();
        Arc::new(Log::new(
            storage,
            dedup,
            Arc::new(metrics),
            Arc::new(AtomicBool::new(true)),
        ))
    }

    #[tokio::test]
    async fn latest_record_per_key_wins() {
        let log = log();
        let store = LogOffsetStore::new(log.clone());
        assert_eq!(store.load("a").await.unwrap(), None, "no stream yet");
        store.save_source("a", "1").await.unwrap();
        store.save_sink("b", 10).await.unwrap();
        store.save_source("a", "2").await.unwrap();
        // A fresh store (e.g. after failover) reads from the log.
        let fresh = LogOffsetStore::new(log.clone());
        assert_eq!(fresh.load_source("a").await.unwrap().as_deref(), Some("2"));
        assert_eq!(fresh.load_sink("b").await.unwrap(), Some(10));
        assert_eq!(fresh.load("missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn finds_old_records_across_many_windows() {
        let log = log();
        let store = LogOffsetStore::new(log.clone());
        store.save_sink("old", 7).await.unwrap();
        for i in 0..(WINDOW * 3 + 17) {
            store.save_sink("busy", i).await.unwrap();
        }
        let fresh = LogOffsetStore::new(log.clone());
        assert_eq!(fresh.load_sink("old").await.unwrap(), Some(7));
        assert_eq!(
            fresh.load_sink("busy").await.unwrap(),
            Some(WINDOW * 3 + 16)
        );
    }

    #[tokio::test]
    async fn identical_saves_are_not_rewritten() {
        let log = log();
        let store = LogOffsetStore::new(log.clone());
        store.save_sink("a", 1).await.unwrap();
        store.save_sink("a", 1).await.unwrap();
        store.save_sink("a", 1).await.unwrap();
        let s = StreamName::try_from(OFFSETS_STREAM).unwrap();
        let (_, next) = log.storage().stream_bounds(&s).await.unwrap();
        assert_eq!(next.0, 1);
    }

    #[tokio::test]
    async fn tombstone_hides_offset() {
        let log = log();
        let store = LogOffsetStore::new(log.clone());
        store.save_source("a", "x").await.unwrap();
        store.delete("a").await.unwrap();
        let fresh = LogOffsetStore::new(log.clone());
        assert_eq!(fresh.load("a").await.unwrap(), None);
        // Saving again after a delete works.
        store.save_source("a", "y").await.unwrap();
        assert_eq!(fresh.load_source("a").await.unwrap().as_deref(), Some("y"));
    }

    #[tokio::test]
    async fn corrupt_record_is_an_error() {
        let log = log();
        let store = LogOffsetStore::new(log.clone());
        store.save_source("a", "x").await.unwrap();
        let s = StreamName::try_from(OFFSETS_STREAM).unwrap();
        log.append(
            &s,
            Record {
                key: Some(Bytes::from_static(b"a")),
                value: Bytes::from_static(b"garbage"),
                subject: String::new(),
                headers: vec![],
                timestamp_ns: None,
            },
        )
        .await
        .unwrap();
        let fresh = LogOffsetStore::new(log);
        assert!(matches!(
            fresh.load("a").await,
            Err(OffsetStoreError::Corrupt { .. })
        ));
    }
}
