use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{oneshot, RwLock};
use tracing::{error, info, warn};

use exspeed_common::metrics::Metrics;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::record::Record;
use exspeed_streams::traits::StorageEngine;
use exspeed_streams::StorageError;

use crate::builtin;
use crate::config::ConnectorConfig;
use crate::offset;
use crate::traits::{SinkBatch, SinkRecord, WriteResult};

// ---------------------------------------------------------------------------
// Status types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ConnectorStatus {
    Running,
    Failed(String),
    Stopped,
}

impl std::fmt::Display for ConnectorStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorStatus::Running => write!(f, "running"),
            ConnectorStatus::Failed(msg) => write!(f, "failed: {msg}"),
            ConnectorStatus::Stopped => write!(f, "stopped"),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ConnectorInfo {
    pub name: String,
    pub connector_type: String,
    pub plugin: String,
    pub stream: String,
    pub status: String,
    pub uptime_secs: u64,
}

// ---------------------------------------------------------------------------
// RunningConnector
// ---------------------------------------------------------------------------

pub struct RunningConnector {
    pub config: ConnectorConfig,
    pub status: ConnectorStatus,
    pub cancel_tx: Option<oneshot::Sender<()>>,
    pub started_at: Instant,
}

// ---------------------------------------------------------------------------
// ConnectorManager
// ---------------------------------------------------------------------------

pub struct ConnectorManager {
    pub storage: Arc<dyn StorageEngine>,
    pub connectors: RwLock<HashMap<String, RunningConnector>>,
    pub data_dir: PathBuf,
    pub metrics: Arc<Metrics>,
}

impl ConnectorManager {
    pub fn new(storage: Arc<dyn StorageEngine>, data_dir: PathBuf, metrics: Arc<Metrics>) -> Self {
        Self {
            storage,
            connectors: RwLock::new(HashMap::new()),
            data_dir,
            metrics,
        }
    }

    // -- path helpers -------------------------------------------------------

    fn configs_dir(&self) -> PathBuf {
        self.data_dir.join("connectors")
    }

    fn config_path(&self, name: &str) -> PathBuf {
        self.configs_dir().join(format!("{name}.json"))
    }

    fn offset_path(&self, name: &str) -> PathBuf {
        self.configs_dir().join(format!("{name}.offset.json"))
    }

    fn connectors_d_dir(&self) -> PathBuf {
        self.data_dir.join("connectors.d")
    }

    // -- public API ---------------------------------------------------------

    /// Create and start a new connector. Persists config to disk.
    pub async fn create(&self, config: ConnectorConfig) -> Result<(), String> {
        // Validate name
        if config.name.is_empty() {
            return Err("connector name cannot be empty".into());
        }

        // Check for duplicates
        {
            let map = self.connectors.read().await;
            if map.contains_key(&config.name) {
                return Err(format!("connector '{}' already exists", config.name));
            }
        }

        // Validate connector_type
        if config.connector_type != "source" && config.connector_type != "sink" {
            return Err(format!(
                "invalid connector type '{}': must be 'source' or 'sink'",
                config.connector_type
            ));
        }

        // Validate stream name
        let stream_name = StreamName::try_from(config.stream.as_str())
            .map_err(|e| format!("invalid stream name: {e}"))?;

        // Auto-create stream if it doesn't exist
        self.ensure_stream_exists(&stream_name)?;

        // Persist config
        let dir = self.configs_dir();
        std::fs::create_dir_all(&dir).map_err(|e| format!("failed to create configs dir: {e}"))?;
        config
            .save_json(&self.config_path(&config.name))
            .map_err(|e| format!("failed to save config: {e}"))?;

        // Start the connector
        self.start_connector(config).await
    }

    /// Stop and delete a connector, removing config and offset files.
    pub async fn delete(&self, name: &str) -> Result<(), String> {
        self.stop_connector(name).await?;

        {
            let mut map = self.connectors.write().await;
            map.remove(name);
        }

        // Remove config file
        let config_path = self.config_path(name);
        if config_path.exists() {
            std::fs::remove_file(&config_path)
                .map_err(|e| format!("failed to remove config file: {e}"))?;
        }

        // Remove offset file
        let offset_path = self.offset_path(name);
        offset::delete_offset(&offset_path)
            .map_err(|e| format!("failed to remove offset file: {e}"))?;

        info!(connector = name, "connector deleted");
        Ok(())
    }

    /// Restart a connector: stop it, then start it again with the same config.
    pub async fn restart(&self, name: &str) -> Result<(), String> {
        let config = {
            let map = self.connectors.read().await;
            match map.get(name) {
                Some(rc) => rc.config.clone(),
                None => return Err(format!("connector '{name}' not found")),
            }
        };

        self.stop_connector(name).await?;

        {
            let mut map = self.connectors.write().await;
            map.remove(name);
        }

        self.start_connector(config).await
    }

    /// Get status info for a single connector.
    pub async fn get_status(&self, name: &str) -> Option<ConnectorInfo> {
        let map = self.connectors.read().await;
        map.get(name).map(|rc| ConnectorInfo {
            name: rc.config.name.clone(),
            connector_type: rc.config.connector_type.clone(),
            plugin: rc.config.plugin.clone(),
            stream: rc.config.stream.clone(),
            status: rc.status.to_string(),
            uptime_secs: rc.started_at.elapsed().as_secs(),
        })
    }

    /// List all connectors.
    pub async fn list(&self) -> Vec<ConnectorInfo> {
        let map = self.connectors.read().await;
        map.values()
            .map(|rc| ConnectorInfo {
                name: rc.config.name.clone(),
                connector_type: rc.config.connector_type.clone(),
                plugin: rc.config.plugin.clone(),
                stream: rc.config.stream.clone(),
                status: rc.status.to_string(),
                uptime_secs: rc.started_at.elapsed().as_secs(),
            })
            .collect()
    }

    /// Load all persisted connector configs (REST JSON + TOML from connectors.d/).
    pub async fn load_all(&self) -> Result<(), String> {
        self.load_json_configs().await?;
        self.load_toml_configs().await?;
        Ok(())
    }

    /// Load TOML configs from connectors.d/ directory.
    pub async fn load_toml_configs(&self) -> Result<(), String> {
        let dir = self.connectors_d_dir();
        if !dir.exists() {
            return Ok(());
        }

        let entries =
            std::fs::read_dir(&dir).map_err(|e| format!("failed to read connectors.d: {e}"))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("failed to read dir entry: {e}"))?;
            let path = entry.path();

            if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                continue;
            }

            match ConnectorConfig::load_toml(&path) {
                Ok(mut config) => {
                    config.resolve_env_vars();
                    info!(
                        connector = config.name.as_str(),
                        file = ?path,
                        "loaded TOML connector config"
                    );
                    if let Err(e) = self.create(config).await {
                        warn!(file = ?path, error = %e, "failed to start TOML connector");
                    }
                }
                Err(e) => {
                    warn!(file = ?path, error = %e, "failed to parse TOML connector config");
                }
            }
        }

        Ok(())
    }

    // -- internal -----------------------------------------------------------

    /// Load JSON configs from the connectors/ directory (configs created via REST).
    async fn load_json_configs(&self) -> Result<(), String> {
        let dir = self.configs_dir();
        if !dir.exists() {
            return Ok(());
        }

        let entries =
            std::fs::read_dir(&dir).map_err(|e| format!("failed to read connectors dir: {e}"))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("failed to read dir entry: {e}"))?;
            let path = entry.path();

            // Skip non-JSON files
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }

            // Skip offset files
            let filename = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if filename.ends_with(".offset.json") {
                continue;
            }

            match ConnectorConfig::load_json(&path) {
                Ok(config) => {
                    info!(
                        connector = config.name.as_str(),
                        file = ?path,
                        "loaded JSON connector config"
                    );
                    if let Err(e) = self.start_connector(config).await {
                        warn!(file = ?path, error = %e, "failed to start connector");
                    }
                }
                Err(e) => {
                    warn!(file = ?path, error = %e, "failed to parse connector config");
                }
            }
        }

        Ok(())
    }

    /// Ensure a stream exists, creating it if necessary.
    fn ensure_stream_exists(&self, stream: &StreamName) -> Result<(), String> {
        // Try reading from the stream. If it fails with StreamNotFound, create it.
        match self.storage.read(stream, Offset(0), 0) {
            Ok(_) => Ok(()),
            Err(StorageError::StreamNotFound(_)) => self
                .storage
                .create_stream(stream, 0, 0)
                .map_err(|e| format!("failed to create stream: {e}")),
            Err(e) => Err(format!("failed to check stream: {e}")),
        }
    }

    /// Start a connector based on its type (source or sink).
    async fn start_connector(&self, config: ConnectorConfig) -> Result<(), String> {
        match config.connector_type.as_str() {
            "source" => self.start_source(config).await,
            "sink" => self.start_sink(config).await,
            other => Err(format!("unknown connector type: {other}")),
        }
    }

    /// Start a source connector.
    async fn start_source(&self, config: ConnectorConfig) -> Result<(), String> {
        let name = config.name.clone();

        // HTTP webhook sources don't have a task loop — they're axum handlers.
        if config.plugin == "http_webhook" {
            let mut map = self.connectors.write().await;
            map.insert(
                name.clone(),
                RunningConnector {
                    config,
                    status: ConnectorStatus::Running,
                    cancel_tx: None,
                    started_at: Instant::now(),
                },
            );
            info!(
                connector = name.as_str(),
                "registered http_webhook source (no task loop)"
            );
            return Ok(());
        }

        // Create the source connector via plugin registry
        let mut source = builtin::create_source(&config.plugin, &config)
            .map_err(|e| format!("failed to create source: {e}"))?;

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        let storage = self.storage.clone();
        let metrics = self.metrics.clone();
        let offset_path = self.offset_path(&name);
        let stream_str = config.stream.clone();
        let batch_size = config.batch_size as usize;
        let poll_interval = std::time::Duration::from_millis(config.poll_interval_ms);
        let dedup_enabled = config.dedup_enabled;
        let dedup_window_secs = config.dedup_window_secs;
        let dedup_key_header = config.dedup_key.clone();
        let task_name = name.clone();

        // Insert into map before spawning
        {
            let mut map = self.connectors.write().await;
            map.insert(
                name.clone(),
                RunningConnector {
                    config,
                    status: ConnectorStatus::Running,
                    cancel_tx: Some(cancel_tx),
                    started_at: Instant::now(),
                },
            );
        }

        // Spawn source task
        tokio::spawn(async move {
            // Load last position
            let last_pos = match offset::load_offset(&offset_path) {
                Ok(pos) => pos,
                Err(e) => {
                    error!(connector = task_name.as_str(), error = %e, "failed to load offset");
                    None
                }
            };

            // Start the source
            if let Err(e) = source.start(last_pos).await {
                error!(connector = task_name.as_str(), error = %e, "source start failed");
                return;
            }

            let stream_name = match StreamName::try_from(stream_str.as_str()) {
                Ok(s) => s,
                Err(e) => {
                    error!(connector = task_name.as_str(), error = %e, "invalid stream name");
                    return;
                }
            };

            let mut dedup_cache = if dedup_enabled {
                Some(crate::dedup::DedupCache::new(dedup_window_secs))
            } else {
                None
            };
            let mut dedup_counter = 0u64;

            let mut cancel_rx = cancel_rx;

            loop {
                // Check for cancellation
                match cancel_rx.try_recv() {
                    Ok(()) | Err(oneshot::error::TryRecvError::Closed) => {
                        info!(connector = task_name.as_str(), "source task cancelled");
                        break;
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                }

                // Poll for records
                let batch = match source.poll(batch_size).await {
                    Ok(b) => b,
                    Err(e) => {
                        error!(connector = task_name.as_str(), error = %e, "source poll failed");
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                };

                if batch.records.is_empty() {
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }

                // Append each record to storage
                for record in &batch.records {
                    // Dedup check
                    if let Some(ref mut cache) = dedup_cache {
                        let key = if !dedup_key_header.is_empty() {
                            // Look up named header
                            record.headers.iter()
                                .find(|(k, _)| k == &dedup_key_header)
                                .map(|(_, v)| v.clone())
                        } else if let Some(ref k) = record.key {
                            // Use record key
                            Some(String::from_utf8_lossy(k).to_string())
                        } else {
                            // Content hash fallback
                            None
                        }.unwrap_or_else(|| crate::dedup::DedupCache::content_hash(&record.value));

                        if !cache.check_and_insert(&key) {
                            continue; // skip duplicate
                        }

                        // Periodic cleanup
                        dedup_counter += 1;
                        if dedup_counter % 1000 == 0 {
                            cache.cleanup();
                        }
                    }

                    let storage_record = Record {
                        key: record.key.clone(),
                        value: record.value.clone(),
                        subject: record.subject.clone(),
                        headers: record.headers.clone(),
                    };

                    let st = storage.clone();
                    let sn = stream_name.clone();
                    let result =
                        tokio::task::spawn_blocking(move || st.append(&sn, &storage_record)).await;

                    match result {
                        Ok(Ok(_offset)) => {
                            metrics.record_publish(stream_str.as_str());
                        }
                        Ok(Err(e)) => {
                            error!(
                                connector = task_name.as_str(),
                                error = %e,
                                "failed to append record to storage"
                            );
                        }
                        Err(e) => {
                            error!(
                                connector = task_name.as_str(),
                                error = %e,
                                "spawn_blocking panicked"
                            );
                        }
                    }
                }

                // Save offset and commit
                if let Some(ref pos) = batch.position {
                    if let Err(e) = offset::save_offset(&offset_path, pos) {
                        error!(connector = task_name.as_str(), error = %e, "failed to save offset");
                    }
                    if let Err(e) = source.commit(pos.clone()).await {
                        error!(connector = task_name.as_str(), error = %e, "source commit failed");
                    }
                }
            }

            // Graceful shutdown
            if let Err(e) = source.stop().await {
                error!(connector = task_name.as_str(), error = %e, "source stop failed");
            }
            info!(connector = task_name.as_str(), "source task exited");
        });

        info!(connector = name.as_str(), "source connector started");
        Ok(())
    }

    /// Start a sink connector.
    async fn start_sink(&self, config: ConnectorConfig) -> Result<(), String> {
        let name = config.name.clone();

        // Create the sink connector via plugin registry
        let mut sink = builtin::create_sink(&config.plugin, &config)
            .map_err(|e| format!("failed to create sink: {e}"))?;

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        let storage = self.storage.clone();
        let metrics = self.metrics.clone();
        let offset_path = self.offset_path(&name);
        let stream_str = config.stream.clone();
        let subject_filter = config.subject_filter.clone();
        let batch_size = config.batch_size as usize;
        let poll_interval = std::time::Duration::from_millis(config.poll_interval_ms);
        let task_name = name.clone();

        // Insert into map before spawning
        {
            let mut map = self.connectors.write().await;
            map.insert(
                name.clone(),
                RunningConnector {
                    config,
                    status: ConnectorStatus::Running,
                    cancel_tx: Some(cancel_tx),
                    started_at: Instant::now(),
                },
            );
        }

        // Spawn sink task
        tokio::spawn(async move {
            // Start the sink
            if let Err(e) = sink.start().await {
                error!(connector = task_name.as_str(), error = %e, "sink start failed");
                return;
            }

            // Load last sink offset
            let mut current_offset = match offset::load_sink_offset(&offset_path) {
                Ok(o) => o,
                Err(e) => {
                    error!(connector = task_name.as_str(), error = %e, "failed to load sink offset");
                    0
                }
            };

            let stream_name = match StreamName::try_from(stream_str.as_str()) {
                Ok(s) => s,
                Err(e) => {
                    error!(connector = task_name.as_str(), error = %e, "invalid stream name");
                    return;
                }
            };

            let mut cancel_rx = cancel_rx;

            loop {
                // Check for cancellation
                match cancel_rx.try_recv() {
                    Ok(()) | Err(oneshot::error::TryRecvError::Closed) => {
                        info!(connector = task_name.as_str(), "sink task cancelled");
                        break;
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                }

                // Read records from storage
                let read_offset = current_offset;
                let st = storage.clone();
                let sn = stream_name.clone();
                let records = tokio::task::spawn_blocking(move || {
                    st.read(&sn, Offset(read_offset), batch_size)
                })
                .await;

                let stored_records = match records {
                    Ok(Ok(recs)) => recs,
                    Ok(Err(e)) => {
                        error!(connector = task_name.as_str(), error = %e, "failed to read from storage");
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                    Err(e) => {
                        error!(connector = task_name.as_str(), error = %e, "spawn_blocking panicked");
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                };

                if stored_records.is_empty() {
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }

                // Compute the offset to advance to: past all records we read,
                // regardless of subject filtering.
                let new_offset = stored_records
                    .last()
                    .map(|r| r.offset.0 + 1)
                    .unwrap_or(current_offset);

                // Filter by subject if pattern is set
                let filtered: Vec<_> = stored_records
                    .into_iter()
                    .filter(|r| {
                        exspeed_common::subject::subject_matches(&r.subject, &subject_filter)
                    })
                    .collect();

                if !filtered.is_empty() {
                    // Convert to SinkRecords
                    let sink_records: Vec<SinkRecord> = filtered
                        .iter()
                        .map(|r| SinkRecord {
                            offset: r.offset.0,
                            timestamp: r.timestamp,
                            subject: r.subject.clone(),
                            key: r.key.clone(),
                            value: r.value.clone(),
                            headers: r.headers.clone(),
                        })
                        .collect();

                    let batch = SinkBatch {
                        records: sink_records,
                    };

                    match sink.write(batch).await {
                        Ok(WriteResult::AllSuccess) => {
                            metrics.record_consume(&stream_str, &task_name);
                        }
                        Ok(WriteResult::PartialSuccess {
                            last_successful_offset,
                        }) => {
                            warn!(
                                connector = task_name.as_str(),
                                last_offset = last_successful_offset,
                                "partial write success"
                            );
                            // Only advance to after the last successful record
                            let partial_offset = last_successful_offset + 1;
                            if let Err(e) = offset::save_sink_offset(&offset_path, partial_offset) {
                                error!(connector = task_name.as_str(), error = %e, "failed to save sink offset");
                            }
                            current_offset = partial_offset;
                            continue;
                        }
                        Ok(WriteResult::AllFailed(msg)) => {
                            error!(connector = task_name.as_str(), error = %msg, "all records failed to write");
                            tokio::time::sleep(poll_interval).await;
                            continue;
                        }
                        Err(e) => {
                            error!(connector = task_name.as_str(), error = %e, "sink write error");
                            tokio::time::sleep(poll_interval).await;
                            continue;
                        }
                    }
                }

                // Advance past all records we read (including filtered-out ones)
                current_offset = new_offset;

                // Persist sink offset
                if let Err(e) = offset::save_sink_offset(&offset_path, current_offset) {
                    error!(connector = task_name.as_str(), error = %e, "failed to save sink offset");
                }
            }

            // Graceful shutdown: flush + stop
            if let Err(e) = sink.flush().await {
                error!(connector = task_name.as_str(), error = %e, "sink flush failed");
            }
            if let Err(e) = sink.stop().await {
                error!(connector = task_name.as_str(), error = %e, "sink stop failed");
            }
            info!(connector = task_name.as_str(), "sink task exited");
        });

        info!(connector = name.as_str(), "sink connector started");
        Ok(())
    }

    /// Stop a connector by sending a cancel signal.
    async fn stop_connector(&self, name: &str) -> Result<(), String> {
        let mut map = self.connectors.write().await;
        match map.get_mut(name) {
            Some(rc) => {
                if let Some(tx) = rc.cancel_tx.take() {
                    let _ = tx.send(());
                }
                rc.status = ConnectorStatus::Stopped;
                info!(connector = name, "connector stopped");
                Ok(())
            }
            None => Err(format!("connector '{name}' not found")),
        }
    }
}
