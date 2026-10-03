//! Consumer state persistence in the internal `__consumers` stream.
//!
//! Each consumer's latest [`Snapshot`] is a record keyed by the consumer
//! name; deleting a consumer writes a tombstone (empty value). Because the
//! state lives in an ordinary stream, it replicates with the log and is
//! restored by any node that becomes leader. Loading keeps the latest record
//! per key, so it is correct whether or not the stream has been compacted.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use exspeed_common::StreamName;
use exspeed_streams::{ReadLimits, Record, StorageError, StreamConfig};

use super::core::Snapshot;
use crate::log::{Log, LogError};

pub const CONSUMERS_STREAM: &str = "__consumers";

pub struct ConsumerStore {
    log: Arc<Log>,
    stream: StreamName,
}

impl ConsumerStore {
    pub fn new(log: Arc<Log>) -> Self {
        Self {
            log,
            stream: StreamName::try_from(CONSUMERS_STREAM).expect("valid internal name"),
        }
    }

    /// Settings for the internal stream: never age out (an idle consumer's
    /// only snapshot may be old); the store relies on compaction to bound
    /// its size.
    fn stream_config() -> StreamConfig {
        StreamConfig {
            max_age_secs: u64::MAX / 2,
            max_bytes: u64::MAX / 2,
            ..StreamConfig::default()
        }
    }

    async fn ensure(&self) -> Result<(), LogError> {
        match self.log.storage().stream_bounds(&self.stream).await {
            Ok(_) => Ok(()),
            Err(StorageError::StreamNotFound(_)) => {
                match self
                    .log
                    .create_stream(&self.stream, &Self::stream_config())
                    .await
                {
                    Ok(()) | Err(LogError::Storage(StorageError::StreamAlreadyExists(_))) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Latest snapshot of every live consumer.
    pub async fn load_all(&self) -> Result<HashMap<String, Snapshot>, LogError> {
        let storage = self.log.storage();
        let (earliest, hwm) = match storage.stream_bounds(&self.stream).await {
            Ok(b) => b,
            Err(StorageError::StreamNotFound(_)) => return Ok(HashMap::new()),
            Err(e) => return Err(e.into()),
        };
        let mut latest: HashMap<String, Option<Snapshot>> = HashMap::new();
        let mut from = earliest;
        while from.0 < hwm.0 {
            let batch = storage
                .read_batch(
                    &self.stream,
                    from,
                    ReadLimits {
                        max_records: 1000,
                        max_bytes: 4 * 1024 * 1024,
                    },
                )
                .await?;
            if batch.records.is_empty() {
                break;
            }
            for r in &batch.records {
                let Some(key) = r.key.as_ref() else { continue };
                let name = String::from_utf8_lossy(key).into_owned();
                if r.value.is_empty() {
                    latest.insert(name, None);
                    continue;
                }
                match serde_json::from_slice::<Snapshot>(&r.value) {
                    Ok(s) => {
                        latest.insert(name, Some(s));
                    }
                    Err(e) => {
                        tracing::error!(consumer = %name, offset = r.offset.0, error = %e,
                                        "unreadable consumer snapshot; keeping the previous one");
                    }
                }
            }
            from = batch.next_offset;
        }
        Ok(latest
            .into_iter()
            .filter_map(|(k, v)| v.map(|s| (k, s)))
            .collect())
    }

    pub async fn save(&self, snapshot: &Snapshot) -> Result<(), LogError> {
        self.ensure().await?;
        let value = serde_json::to_vec(snapshot).expect("snapshot serializes");
        self.write(&snapshot.spec.name, Bytes::from(value)).await
    }

    pub async fn delete(&self, name: &str) -> Result<(), LogError> {
        self.ensure().await?;
        self.write(name, Bytes::new()).await
    }

    async fn write(&self, name: &str, value: Bytes) -> Result<(), LogError> {
        let record = Record {
            key: Some(Bytes::copy_from_slice(name.as_bytes())),
            value,
            subject: "consumer.state".into(),
            headers: vec![],
            timestamp_ns: None,
        };
        self.log.append(&self.stream, record).await.map(|_| ())
    }
}
