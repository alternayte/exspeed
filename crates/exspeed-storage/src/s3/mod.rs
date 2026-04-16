pub mod cache;
pub mod config;
pub mod manifest;
pub mod uploader;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{info, warn};

use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError, StoredRecord};

use crate::file::FileStorage;
use crate::s3::manifest::Manifest;

/// S3-backed tiered storage engine.
///
/// Writes always go to the local `FileStorage`.  When a segment is sealed
/// (rolled), it is uploaded to S3 in the background.  Reads first try local
/// storage; on a miss the engine checks the remote manifest for the stream,
/// downloads the relevant segment from S3, reloads the local partition, and
/// retries.
pub struct S3TieredStorage {
    local: FileStorage,
    bucket: Box<s3::Bucket>,
    prefix: String,
    manifests: Arc<RwLock<HashMap<String, Manifest>>>,
}

impl S3TieredStorage {
    /// Create a new `S3TieredStorage` that wraps `local` with S3 tiering.
    ///
    /// Sets up background tasks for uploading sealed segments and managing
    /// the local cache.  Loads existing manifests from S3 so that read-path
    /// download-on-miss works immediately.
    pub async fn new(
        local: FileStorage,
        bucket: Box<s3::Bucket>,
        prefix: String,
        local_max_bytes: u64,
    ) -> Result<Self, String> {
        // Channel: partition seal notifications -> uploader.
        let (seal_tx, seal_rx) = tokio::sync::mpsc::unbounded_channel();
        local.set_seal_notifier(seal_tx);

        // Channel: uploader confirmations -> cache manager.
        let (uploaded_tx, uploaded_rx) = tokio::sync::mpsc::unbounded_channel();

        // Spawn the background uploader task.
        uploader::spawn_uploader(seal_rx, bucket.clone(), prefix.clone(), uploaded_tx);

        // Spawn the background cache manager.
        cache::spawn_cache_manager(uploaded_rx, local_max_bytes);

        // Load all existing manifests from S3.
        let remote_manifests = uploader::load_all_manifests(&bucket, &prefix).await;
        let mut manifest_map = HashMap::new();
        for m in remote_manifests {
            info!(
                stream = %m.stream,
                segments = m.segments.len(),
                "loaded remote manifest"
            );
            manifest_map.insert(m.stream.clone(), m);
        }

        Ok(Self {
            local,
            bucket,
            prefix,
            manifests: Arc::new(RwLock::new(manifest_map)),
        })
    }

    /// Attempt to fetch a segment from S3 that contains `offset` for the given
    /// stream.  Downloads the segment files to the local partition directory and
    /// reloads the partition so the new files become visible.
    ///
    /// Returns `Ok(true)` if a segment was downloaded, `Ok(false)` if no
    /// matching segment exists in the manifest.
    async fn fetch_segment_from_s3(
        &self,
        stream: &StreamName,
        offset: u64,
    ) -> Result<bool, StorageError> {
        let base_offset = {
            let manifests = self.manifests.read().await;
            let manifest = match manifests.get(stream.as_str()) {
                Some(m) => m,
                None => return Ok(false),
            };
            match manifest.find_segment(offset) {
                Some(entry) => entry.base_offset,
                None => return Ok(false),
            }
        };

        let local_dir = self.local.partition_dir(stream.as_str(), 0);

        uploader::download_segment(&self.bucket, &self.prefix, stream.as_str(), 0, base_offset, &local_dir)
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;

        let local = self.local.clone();
        let stream_name = stream.as_str().to_string();
        tokio::task::spawn_blocking(move || local.reload_partition(&stream_name, 0))
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
            .map_err(StorageError::Io)?;

        Ok(true)
    }
}

#[async_trait]
impl StorageEngine for S3TieredStorage {
    async fn create_stream(
        &self,
        stream: &StreamName,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<(), StorageError> {
        self.local.create_stream(stream, max_age_secs, max_bytes).await
    }

    async fn append(&self, stream: &StreamName, record: &Record) -> Result<Offset, StorageError> {
        self.local.append(stream, record).await
    }

    async fn read(
        &self,
        stream: &StreamName,
        from: Offset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        match self.local.read(stream, from, max_records).await {
            Ok(records) if !records.is_empty() => Ok(records),
            Ok(_empty) => {
                // Local returned no records — check S3 for a segment containing this offset.
                info!(
                    stream = stream.as_str(),
                    offset = from.0,
                    "local read returned empty, checking S3"
                );
                let fetched = self.fetch_segment_from_s3(stream, from.0).await?;
                if fetched {
                    self.local.read(stream, from, max_records).await
                } else {
                    Ok(Vec::new())
                }
            }
            Err(StorageError::StreamNotFound(ref name)) => {
                // Stream doesn't exist locally — check if we have a manifest for it.
                let has_manifest = {
                    let manifests = self.manifests.read().await;
                    manifests.contains_key(name.as_str())
                };

                if !has_manifest {
                    return Err(StorageError::StreamNotFound(name.clone()));
                }

                info!(
                    stream = name.as_str(),
                    "stream not found locally but exists in S3, creating local stream"
                );

                // Create the local stream (ignore AlreadyExists in case of race).
                match self.local.create_stream(name, 0, 0).await {
                    Ok(()) => {}
                    Err(StorageError::StreamAlreadyExists(_)) => {}
                    Err(e) => return Err(e),
                }

                // Download the segment from S3 and retry.
                let fetched = self.fetch_segment_from_s3(name, from.0).await?;
                if fetched {
                    self.local.read(stream, from, max_records).await
                } else {
                    Ok(Vec::new())
                }
            }
            other => other,
        }
    }

    async fn seek_by_time(
        &self,
        stream: &StreamName,
        timestamp: u64,
    ) -> Result<Offset, StorageError> {
        // S3 segment download for seek is a future optimization.
        self.local.seek_by_time(stream, timestamp).await
    }

    async fn list_streams(&self) -> Result<Vec<StreamName>, StorageError> {
        let mut streams: Vec<StreamName> =
            StorageEngine::list_streams(&self.local).await?;

        let manifests = self.manifests.read().await;
        for stream_name in manifests.keys() {
            let already_listed = streams.iter().any(|s| s.as_str() == stream_name.as_str());
            if !already_listed {
                if let Ok(sn) = StreamName::try_from(stream_name.as_str()) {
                    streams.push(sn);
                } else {
                    warn!(
                        stream = stream_name.as_str(),
                        "skipping invalid stream name from S3 manifest"
                    );
                }
            }
        }

        streams.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(streams)
    }
}
