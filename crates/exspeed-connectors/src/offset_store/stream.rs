use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{record::Record, traits::StorageEngine, StorageError};
use serde::{Deserialize, Serialize};

use super::{OffsetStore, OffsetStoreError};

const OFFSETS_STREAM: &str = "__exspeed_offsets";

#[derive(Debug, Serialize, Deserialize)]
struct OffsetRecord {
    offset_type: String,
    position: Option<String>,
    sink_offset: Option<u64>,
    #[serde(default)]
    tombstone: bool,
}

pub struct StreamOffsetStore {
    storage: Arc<dyn StorageEngine>,
    stream_name: StreamName,
}

impl StreamOffsetStore {
    pub async fn new(storage: Arc<dyn StorageEngine>) -> Result<Self, OffsetStoreError> {
        let stream_name = StreamName::try_from(OFFSETS_STREAM)
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;

        match storage.create_stream(&stream_name, 0, 0).await {
            Ok(_) => {}
            Err(StorageError::StreamAlreadyExists(_)) => {}
            Err(e) => return Err(OffsetStoreError::Connection(e.to_string())),
        }

        Ok(Self {
            storage,
            stream_name,
        })
    }

    async fn find_latest(&self, connector: &str) -> Option<OffsetRecord> {
        let connector_key = Bytes::copy_from_slice(connector.as_bytes());
        let batch_size = 100;
        let mut from = Offset(0);
        let mut latest: Option<OffsetRecord> = None;

        loop {
            let records = match self.storage.read(&self.stream_name, from, batch_size).await {
                Ok(r) => r,
                Err(_) => break,
            };

            if records.is_empty() {
                break;
            }

            let last_offset = records.last().map(|r| r.offset);

            for stored in records {
                if stored.key.as_ref() == Some(&connector_key) {
                    if let Ok(rec) = serde_json::from_slice::<OffsetRecord>(&stored.value) {
                        latest = Some(rec);
                    }
                }
            }

            match last_offset {
                Some(o) => from = Offset(o.0 + 1),
                None => break,
            }
        }

        // If the latest record is a tombstone, treat as absent
        match latest {
            Some(rec) if rec.tombstone => None,
            other => other,
        }
    }

    async fn append_offset_record(
        storage: &Arc<dyn StorageEngine>,
        stream_name: &StreamName,
        connector: &str,
        record: &OffsetRecord,
    ) -> Result<(), OffsetStoreError> {
        let value = serde_json::to_vec(record)
            .map_err(|e| OffsetStoreError::Serialization(e.to_string()))?;

        let r = Record {
            key: Some(Bytes::copy_from_slice(connector.as_bytes())),
            value: Bytes::from(value),
            subject: format!("offsets.{}", connector),
            headers: vec![],
        };

        storage
            .append(stream_name, &r)
            .await
            .map_err(|e| OffsetStoreError::Connection(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl OffsetStore for StreamOffsetStore {
    async fn save_source_offset(
        &self,
        connector: &str,
        position: &str,
    ) -> Result<(), OffsetStoreError> {
        let record = OffsetRecord {
            offset_type: "source".to_string(),
            position: Some(position.to_string()),
            sink_offset: None,
            tombstone: false,
        };
        StreamOffsetStore::append_offset_record(
            &self.storage,
            &self.stream_name,
            connector,
            &record,
        )
        .await
    }

    async fn load_source_offset(
        &self,
        connector: &str,
    ) -> Result<Option<String>, OffsetStoreError> {
        Ok(self.find_latest(connector).await.and_then(|r| r.position))
    }

    async fn save_sink_offset(
        &self,
        connector: &str,
        offset: u64,
    ) -> Result<(), OffsetStoreError> {
        let record = OffsetRecord {
            offset_type: "sink".to_string(),
            position: None,
            sink_offset: Some(offset),
            tombstone: false,
        };
        StreamOffsetStore::append_offset_record(
            &self.storage,
            &self.stream_name,
            connector,
            &record,
        )
        .await
    }

    async fn load_sink_offset(&self, connector: &str) -> Result<u64, OffsetStoreError> {
        Ok(self
            .find_latest(connector)
            .await
            .and_then(|r| r.sink_offset)
            .unwrap_or(0))
    }

    async fn delete(&self, connector: &str) -> Result<(), OffsetStoreError> {
        let record = OffsetRecord {
            offset_type: "tombstone".to_string(),
            position: None,
            sink_offset: None,
            tombstone: true,
        };
        StreamOffsetStore::append_offset_record(
            &self.storage,
            &self.stream_name,
            connector,
            &record,
        )
        .await
    }
}
