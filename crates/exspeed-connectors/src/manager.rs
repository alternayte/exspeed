use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{oneshot, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use exspeed_common::metrics::Metrics;
use exspeed_common::{Offset, StreamName};
use exspeed_broker::broker_append::BrokerAppend;
use exspeed_broker::leadership::ClusterLeadership;
use exspeed_streams::record::Record;
use exspeed_streams::traits::StorageEngine;
use exspeed_streams::StorageError;

use crate::builtin;
use crate::config::ConnectorConfig;
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
    pub broker_append: Arc<BrokerAppend>,
    pub connectors: RwLock<HashMap<String, RunningConnector>>,
    pub data_dir: PathBuf,
    pub metrics: Arc<Metrics>,
    pub offset_store: Arc<dyn crate::offset_store::OffsetStore>,
    /// Tracks content hashes of TOML connector files for change detection.
    pub toml_hashes: RwLock<HashMap<String, u64>>,
    /// Cluster-leader lease wrapper. Used to check whether this pod is
    /// currently the leader before starting connector tasks, and to obtain
    /// the current leader `CancellationToken` for the spawned tasks.
    pub leadership: Arc<ClusterLeadership>,
}

impl ConnectorManager {
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        broker_append: Arc<BrokerAppend>,
        data_dir: PathBuf,
        metrics: Arc<Metrics>,
        offset_store: Arc<dyn crate::offset_store::OffsetStore>,
        leadership: Arc<ClusterLeadership>,
    ) -> Self {
        Self {
            storage,
            broker_append,
            connectors: RwLock::new(HashMap::new()),
            data_dir,
            metrics,
            offset_store,
            toml_hashes: RwLock::new(HashMap::new()),
            leadership,
        }
    }

    /// Hash the contents of a file for change detection.
    pub fn hash_file(path: &Path) -> Option<u64> {
        let content = std::fs::read(path).ok()?;
        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        Some(hasher.finish())
    }

    // -- path helpers -------------------------------------------------------

    fn configs_dir(&self) -> PathBuf {
        self.data_dir.join("connectors")
    }

    fn config_path(&self, name: &str) -> PathBuf {
        self.configs_dir().join(format!("{name}.json"))
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
        self.ensure_stream_exists(&stream_name).await?;

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

        // Remove offset
        if let Err(e) = self.offset_store.delete(name).await {
            warn!(connector = name, error = %e, "failed to delete offset");
        }

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
    /// Only parses and registers configs — does NOT start connector tasks.
    /// Starting is handled by the leader supervisor via `run_all(token)`.
    pub async fn load_all(&self) -> Result<(), String> {
        self.load_json_configs().await?;
        self.load_toml_configs().await?;
        Ok(())
    }

    /// Load TOML configs from connectors.d/ directory (config-only, no starting).
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
                    let connector_name = config.name.clone();
                    info!(
                        connector = config.name.as_str(),
                        file = ?path,
                        "loaded TOML connector config"
                    );
                    // Register config without starting (leader supervisor handles startup).
                    let mut map = self.connectors.write().await;
                    map.entry(connector_name.clone()).or_insert_with(|| RunningConnector {
                        config,
                        status: ConnectorStatus::Stopped,
                        cancel_tx: None,
                        started_at: Instant::now(),
                    });
                    drop(map);
                    // Record file hash for change detection.
                    if let Some(hash) = Self::hash_file(&path) {
                        self.toml_hashes.write().await.insert(connector_name, hash);
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
    /// Only registers configs — does NOT start connector tasks.
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
                    // Register config without starting (leader supervisor handles startup).
                    let name = config.name.clone();
                    let mut map = self.connectors.write().await;
                    map.entry(name).or_insert_with(|| RunningConnector {
                        config,
                        status: ConnectorStatus::Stopped,
                        cancel_tx: None,
                        started_at: Instant::now(),
                    });
                }
                Err(e) => {
                    warn!(file = ?path, error = %e, "failed to parse connector config");
                }
            }
        }

        Ok(())
    }

    /// Ensure a stream exists, creating it if necessary.
    async fn ensure_stream_exists(&self, stream: &StreamName) -> Result<(), String> {
        // Try reading from the stream. If it fails with StreamNotFound, create it.
        match self.storage.read(stream, Offset(0), 0).await {
            Ok(_) => Ok(()),
            Err(StorageError::StreamNotFound(_)) => self
                .storage
                .create_stream(stream, 0, 0)
                .await
                .map_err(|e| format!("failed to create stream: {e}")),
            Err(e) => Err(format!("failed to check stream: {e}")),
        }
    }

    /// Start a connector based on its type (source or sink). Called from REST
    /// API endpoints (POST /api/v1/connectors, restart, etc.) and the
    /// file-watcher hot-reload path.
    ///
    /// If this pod is not the leader the call returns an error immediately —
    /// the `leader_gate` middleware should have already returned 503 for REST
    /// callers, but we check here as defence-in-depth.
    pub async fn start_connector(&self, config: ConnectorConfig) -> Result<(), String> {
        if !self.leadership.is_currently_leader() {
            return Err("not leader; connector creation is handled by the leader".to_string());
        }
        let token = self.leadership.current_child_token().await;
        self.start_connector_under_token(config, token).await
    }

    /// Internal: dispatch by type but spawn under the provided token.
    async fn start_connector_under_token(
        &self,
        config: ConnectorConfig,
        token: CancellationToken,
    ) -> Result<(), String> {
        match config.connector_type.as_str() {
            "source" => self.start_source_under_token(config, token).await,
            "sink" => self.start_sink_under_token(config, token).await,
            other => Err(format!("unknown connector type: {other}")),
        }
    }

    /// Start every loaded source + sink connector under `token`. Called
    /// exactly once per leadership tenure by the leader supervisor in
    /// `crates/exspeed/src/cli/server.rs`. Returns when `token.cancelled()`
    /// fires — on demotion or shutdown.
    ///
    /// Callers should NOT call this method outside the leader supervisor;
    /// if you need to start a single connector at create-time (e.g., the
    /// `/api/v1/connectors` POST handler), use `start_connector` which
    /// checks the current leadership state.
    pub async fn run_all(&self, token: CancellationToken) {
        // Snapshot the config list under read-lock so we don't hold it
        // across await points that might try to take the write lock.
        let configs: Vec<ConnectorConfig> = {
            let conns = self.connectors.read().await;
            conns.values().map(|rc| rc.config.clone()).collect()
        };

        for cfg in configs {
            if let Err(e) = self.start_connector_under_token(cfg, token.clone()).await {
                warn!(error = %e, "failed to start connector under leader supervisor");
            }
        }

        token.cancelled().await;
        info!("connector manager leader tenure ended; connector tasks will drain");
    }

    /// Start a source connector under the given cancellation token.
    async fn start_source_under_token(
        &self,
        config: ConnectorConfig,
        token: CancellationToken,
    ) -> Result<(), String> {
        let name = config.name.clone();

        // HTTP webhook sources don't have a task loop — they're axum handlers
        // driven by inbound HTTP requests.
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
        let mut source = match builtin::create_source(&config.plugin, &config) {
            Ok(s) => s,
            Err(e) => {
                return Err(format!("failed to create source: {e}"));
            }
        };

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        // Derive a child token so that dropping cancel_tx (for manual stop /
        // restart) doesn't disturb sibling connector tasks that share the
        // parent token.
        let child_token = token.child_token();

        let broker_append = self.broker_append.clone();
        let metrics = self.metrics.clone();
        let offset_store = self.offset_store.clone();
        let stream_str = config.stream.clone();
        let batch_size = config.batch_size as usize;
        let poll_interval = std::time::Duration::from_millis(config.poll_interval_ms);
        let dedup_enabled = config.dedup_enabled;
        let dedup_window_secs = config.dedup_window_secs;
        let dedup_key_header = config.dedup_key.clone();
        let transform_sql = config.transform_sql.clone();
        let task_name = name.clone();
        let name_for_select = name.clone();

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

        let log_name = name.clone();

        // Spawn source task
        tokio::spawn(async move {
            let name = name_for_select;
            let work = async move {
            // Load last position
            let last_pos = match offset_store.load_source_offset(&task_name).await {
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

            // Compile transform once before the loop
            let transform = if !transform_sql.is_empty() {
                match crate::transform::Transform::compile(&transform_sql) {
                    Ok(t) => Some(t),
                    Err(e) => {
                        error!(
                            connector = task_name.as_str(),
                            "transform compile failed: {e}"
                        );
                        return;
                    }
                }
            } else {
                None
            };

            loop {
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
                            record
                                .headers
                                .iter()
                                .find(|(k, _)| k == &dedup_key_header)
                                .map(|(_, v)| v.clone())
                        } else {
                            // Use record key, or content hash fallback
                            record
                                .key
                                .as_ref()
                                .map(|k| String::from_utf8_lossy(k).to_string())
                        }
                        .unwrap_or_else(|| crate::dedup::DedupCache::content_hash(&record.value));

                        if !cache.check_and_insert(&key) {
                            continue; // skip duplicate
                        }

                        // Periodic cleanup
                        dedup_counter += 1;
                        if dedup_counter.is_multiple_of(1000) {
                            cache.cleanup();
                        }
                    }

                    // Apply transform (filter + projection)
                    let record = if let Some(ref t) = transform {
                        match t.apply(record) {
                            Some(r) => r,
                            None => continue, // filtered out
                        }
                    } else {
                        record.clone()
                    };

                    let storage_record = Record {
                        key: record.key.clone(),
                        value: record.value.clone(),
                        subject: record.subject.clone(),
                        headers: record.headers.clone(),
                    };

                    let result = broker_append.append(&stream_name, &storage_record).await;

                    match result {
                        Ok(exspeed_broker::broker_append::AppendResult::Written(_)) => {
                            metrics.record_publish(stream_str.as_str());
                        }
                        Ok(exspeed_broker::broker_append::AppendResult::Duplicate(_)) => {
                            // Silently skip — broker dedup caught it
                        }
                        Err(e) => {
                            error!(
                                connector = task_name.as_str(),
                                error = %e,
                                "failed to append record"
                            );
                        }
                    }
                }

                // Save offset and commit
                if let Some(ref pos) = batch.position {
                    if let Err(e) = offset_store.save_source_offset(&task_name, pos).await {
                        error!(connector = task_name.as_str(), error = %e, "failed to save offset");
                    }
                    if let Err(e) = source.commit(pos.clone()).await {
                        error!(connector = task_name.as_str(), error = %e, "source commit failed");
                    }
                }
            }
            }; // end of `work` async block

            tokio::select! {
                _ = work => {}
                _ = child_token.cancelled() => {
                    info!(connector = %name, "stopping source (leader token fired)");
                }
                _ = cancel_rx => {
                    info!(connector = %name, "stopping source (manual cancel)");
                }
            }
        });

        info!(connector = log_name.as_str(), "source connector started");
        Ok(())
    }

    /// Start a sink connector under the given cancellation token.
    async fn start_sink_under_token(
        &self,
        config: ConnectorConfig,
        token: CancellationToken,
    ) -> Result<(), String> {
        let name = config.name.clone();

        // Create the sink connector via plugin registry
        let mut sink = match builtin::create_sink(&config.plugin, &config) {
            Ok(s) => s,
            Err(e) => {
                return Err(format!("failed to create sink: {e}"));
            }
        };

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        // Derive a child token so that dropping cancel_tx (for manual stop /
        // restart) doesn't disturb sibling connector tasks that share the
        // parent token.
        let child_token = token.child_token();

        let storage = self.storage.clone();
        let metrics = self.metrics.clone();
        let offset_store = self.offset_store.clone();
        let stream_str = config.stream.clone();
        let subject_filter = config.subject_filter.clone();
        let batch_size = config.batch_size as usize;
        let poll_interval = std::time::Duration::from_millis(config.poll_interval_ms);
        let task_name = name.clone();
        let name_for_select = name.clone();

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

        let log_name = name.clone();

        // Spawn sink task
        tokio::spawn(async move {
            let name = name_for_select;
            let work = async move {
            // Start the sink
            if let Err(e) = sink.start().await {
                error!(connector = task_name.as_str(), error = %e, "sink start failed");
                return;
            }

            // Load last sink offset
            let mut current_offset = match offset_store.load_sink_offset(&task_name).await {
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

            loop {
                // Read records from storage
                let stored_records = match storage.read(&stream_name, Offset(current_offset), batch_size).await {
                    Ok(recs) => recs,
                    Err(e) => {
                        error!(connector = task_name.as_str(), error = %e, "failed to read from storage");
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
                            if let Err(e) = offset_store.save_sink_offset(&task_name, partial_offset).await {
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
                if let Err(e) = offset_store.save_sink_offset(&task_name, current_offset).await {
                    error!(connector = task_name.as_str(), error = %e, "failed to save sink offset");
                }
            }
            }; // end of `work` async block

            tokio::select! {
                _ = work => {}
                _ = child_token.cancelled() => {
                    info!(connector = %name, "stopping sink (leader token fired)");
                }
                _ = cancel_rx => {
                    info!(connector = %name, "stopping sink (manual cancel)");
                }
            }
        });

        info!(connector = log_name.as_str(), "sink connector started");
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
