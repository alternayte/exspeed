//! Connector lifecycle: registration, provenance (file vs API), leadership,
//! and one supervisor per connector (see [`crate::runtime`]).

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use exspeed_broker::leadership::ClusterLeadership;
use exspeed_broker::log::Log;
use exspeed_common::metrics::Metrics;
use exspeed_common::StreamName;
use exspeed_streams::traits::StorageEngine;

use crate::builtin::http_webhook::WebhookEndpoint;
use crate::config::{sanitized_name, ConnectorConfig, ConnectorType};
use crate::offset_store::OffsetStore;
use crate::registry::Registry;
use crate::runtime::{self, RunContext, RunHandle};
use crate::status::{ConnectorState, Status, StatusSnapshot};

/// How long `stop` waits for a connector (final flush included).
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Where a connector's definition lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// Created through the HTTP API; persisted as JSON under
    /// `{data_dir}/connectors/`.
    Api,
    /// Defined by `{data_dir}/connectors.d/<file>`; the file is the source
    /// of truth and nothing is persisted.
    File(String),
}

/// A connector config file in `connectors.d/` and what it currently defines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TomlFile {
    /// `[connector].name` from the file (need not match the file name).
    pub connector_name: String,
    /// Content hash used for change detection.
    pub hash: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectorInfo {
    pub name: String,
    pub connector_type: String,
    pub plugin: String,
    pub stream: String,
    /// `api` or `file`.
    pub origin: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(flatten)]
    pub state: StatusSnapshot,
}

#[derive(Debug, thiserror::Error)]
pub enum ManagerError {
    #[error("connector '{0}' not found")]
    NotFound(String),
    #[error("connector '{0}' already exists")]
    AlreadyExists(String),
    #[error("{0}")]
    Invalid(String),
    #[error("not the leader; connectors run on the leader")]
    NotLeader,
    #[error("connector '{name}' is defined by connectors.d/{file}; edit the file instead")]
    FileManaged { name: String, file: String },
    #[error("{0}")]
    Internal(String),
}

struct Entry {
    config: ConnectorConfig,
    origin: Origin,
    state: Arc<ConnectorState>,
    handle: Option<RunHandle>,
    /// Passive sources (webhooks): the parsed endpoint.
    webhook: Option<Arc<WebhookEndpoint>>,
}

pub struct ConnectorManager {
    pub storage: Arc<dyn StorageEngine>,
    /// The broker write path; every source record, DLQ write and offset goes here.
    pub log: Arc<Log>,
    pub data_dir: PathBuf,
    pub metrics: Arc<Metrics>,
    pub offset_store: Arc<dyn OffsetStore>,
    /// Connectors that came from `connectors.d/*.toml`, keyed by file name.
    /// The file watcher only ever reconciles these.
    pub toml_files: RwLock<HashMap<String, TomlFile>>,
    pub leadership: Arc<ClusterLeadership>,
    registry: Registry,
    entries: RwLock<HashMap<String, Entry>>,
    /// Serialises lifecycle operations (create/update/delete/restart).
    lifecycle: Mutex<()>,
    /// Token of the current leader tenure while `run_all` is active.
    tenure: std::sync::Mutex<Option<CancellationToken>>,
}

impl ConnectorManager {
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        log: Arc<Log>,
        data_dir: PathBuf,
        metrics: Arc<Metrics>,
        offset_store: Arc<dyn OffsetStore>,
        leadership: Arc<ClusterLeadership>,
    ) -> Self {
        Self {
            storage,
            log,
            data_dir,
            metrics,
            offset_store,
            toml_files: RwLock::new(HashMap::new()),
            leadership,
            registry: Registry::builtin(),
            entries: RwLock::new(HashMap::new()),
            lifecycle: Mutex::new(()),
            tenure: std::sync::Mutex::new(None),
        }
    }

    /// Replace the plugin registry (tests register fake plugins).
    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Hash the contents of a file for change detection.
    pub fn hash_file(path: &Path) -> Option<u64> {
        let content = std::fs::read(path).ok()?;
        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        Some(hasher.finish())
    }

    fn configs_dir(&self) -> PathBuf {
        self.data_dir.join("connectors")
    }

    fn config_path(&self, name: &str) -> PathBuf {
        self.configs_dir().join(format!("{name}.json"))
    }

    fn connectors_d_dir(&self) -> PathBuf {
        self.data_dir.join("connectors.d")
    }

    // -- validation -----------------------------------------------------------

    /// Full validation with the plugin registry (typed settings included).
    pub fn validate(&self, config: &ConnectorConfig, origin: &Origin) -> Result<(), ManagerError> {
        self.registry
            .validate(config, Self::resolves_env(origin), self.metrics.clone())
            .map_err(|e| ManagerError::Invalid(e.to_string()))
    }

    /// `${VAR}` substitution applies only to file-defined connectors; API
    /// callers can't read the server's environment.
    fn resolves_env(origin: &Origin) -> bool {
        matches!(origin, Origin::File(_))
    }

    fn check_collision(entries: &HashMap<String, Entry>, name: &str) -> Result<(), ManagerError> {
        let sanitized = sanitized_name(name);
        if let Some(other) = entries
            .keys()
            .find(|k| k.as_str() != name && sanitized_name(k) == sanitized)
        {
            return Err(ManagerError::Invalid(format!(
                "connector name '{name}' collides with existing connector '{other}' \
                 (both map to '{sanitized}' in external identifiers)"
            )));
        }
        Ok(())
    }

    // -- public API -------------------------------------------------------------

    /// Create a connector through the API: validate, persist, start.
    pub async fn create(&self, config: ConnectorConfig) -> Result<(), ManagerError> {
        let _g = self.lifecycle.lock().await;
        self.validate(&config, &Origin::Api)?;
        {
            let entries = self.entries.read().await;
            if entries.contains_key(&config.name) {
                return Err(ManagerError::AlreadyExists(config.name.clone()));
            }
            Self::check_collision(&entries, &config.name)?;
        }
        config
            .save_json(&self.config_path(&config.name))
            .map_err(|e| ManagerError::Internal(format!("failed to save config: {e}")))?;
        self.insert_and_start(config, Origin::Api).await;
        Ok(())
    }

    /// Register (and start, on the leader) a connector defined by a
    /// `connectors.d/` file. Nothing is persisted; secrets resolved from
    /// `${VAR}` never reach disk. An invalid config is still registered, in
    /// `failed` status with the validation error, so it is visible.
    pub async fn create_from_file(
        &self,
        config: ConnectorConfig,
        file_name: &str,
    ) -> Result<(), ManagerError> {
        let _g = self.lifecycle.lock().await;
        {
            let entries = self.entries.read().await;
            if entries.contains_key(&config.name) {
                return Err(ManagerError::AlreadyExists(config.name.clone()));
            }
            Self::check_collision(&entries, &config.name)?;
        }
        let origin = Origin::File(file_name.to_string());
        let invalid = self.validate(&config, &origin).err();
        self.insert_and_start(config, origin).await;
        invalid.map_or(Ok(()), Err)
    }

    /// Replace an API-created connector's config (`PUT`), keeping its
    /// offsets. File-defined connectors are read-only through the API.
    pub async fn update(&self, config: ConnectorConfig) -> Result<(), ManagerError> {
        {
            let entries = self.entries.read().await;
            match entries.get(&config.name) {
                None => return Err(ManagerError::NotFound(config.name.clone())),
                Some(Entry {
                    origin: Origin::File(file),
                    ..
                }) => {
                    return Err(ManagerError::FileManaged {
                        name: config.name.clone(),
                        file: file.clone(),
                    })
                }
                Some(_) => {}
            }
        }
        self.validate(&config, &Origin::Api)?;
        self.update_config(config, Origin::Api).await
    }

    /// Replace a connector's config and restart it, keeping its offsets.
    /// Creates it if it doesn't exist.
    pub async fn update_config(
        &self,
        config: ConnectorConfig,
        origin: Origin,
    ) -> Result<(), ManagerError> {
        let _g = self.lifecycle.lock().await;
        let name = config.name.clone();
        let old = self.take_handle(&name).await;
        if let Some(h) = old {
            h.stop(STOP_TIMEOUT).await;
        }
        {
            let entries = self.entries.read().await;
            Self::check_collision(&entries, &name)?;
        }
        let invalid = self.validate(&config, &origin).err();
        match &origin {
            Origin::Api => {
                if invalid.is_none() {
                    config.save_json(&self.config_path(&name)).map_err(|e| {
                        ManagerError::Internal(format!("failed to save config: {e}"))
                    })?;
                }
            }
            Origin::File(_) => {
                // The file is now the definition; drop any API copy.
                let _ = std::fs::remove_file(self.config_path(&name));
            }
        }
        self.insert_and_start(config, origin).await;
        invalid.map_or(Ok(()), Err)
    }

    /// Stop and delete a connector, its persisted config and its offsets.
    /// Plugin cleanup (e.g. `drop_slot_on_delete`) runs on the leader.
    pub async fn delete(&self, name: &str) -> Result<(), ManagerError> {
        let _g = self.lifecycle.lock().await;
        let entry = {
            let mut entries = self.entries.write().await;
            entries
                .remove(name)
                .ok_or_else(|| ManagerError::NotFound(name.to_string()))?
        };
        if let Some(h) = &entry.handle {
            h.stop(STOP_TIMEOUT).await;
        }
        entry.state.set_status(Status::Stopped);
        entry.state.retire();

        let path = self.config_path(name);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| {
                ManagerError::Internal(format!("failed to remove config file: {e}"))
            })?;
        }
        if self.log.can_write() {
            if let Err(e) = self.offset_store.delete(name).await {
                warn!(connector = name, error = %e, "failed to delete offset");
            }
            self.cleanup_plugin(&entry).await;
        }
        info!(connector = name, "connector deleted");
        Ok(())
    }

    async fn cleanup_plugin(&self, entry: &Entry) {
        if entry.config.connector_type != ConnectorType::Source
            || self.registry.is_passive(&entry.config.plugin)
        {
            return;
        }
        let init = match self.registry.init(
            &entry.config,
            Self::resolves_env(&entry.origin),
            self.metrics.clone(),
        ) {
            Ok(i) => i,
            Err(_) => return,
        };
        if let Ok(mut source) = self.registry.create_source(&init) {
            match tokio::time::timeout(Duration::from_secs(30), source.cleanup()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(connector = %entry.config.name, error = %e, "connector cleanup failed")
                }
                Err(_) => warn!(connector = %entry.config.name, "connector cleanup timed out"),
            }
        }
    }

    /// Restart a connector (also brings a `failed` connector back).
    pub async fn restart(&self, name: &str) -> Result<(), ManagerError> {
        let _g = self.lifecycle.lock().await;
        if !self.entries.read().await.contains_key(name) {
            return Err(ManagerError::NotFound(name.to_string()));
        }
        if !self.leadership.is_currently_leader() {
            return Err(ManagerError::NotLeader);
        }
        if let Some(h) = self.take_handle(name).await {
            h.stop(STOP_TIMEOUT).await;
        }
        let token = self.leader_token().await;
        self.start_entry(name, &token).await;
        Ok(())
    }

    pub async fn get_status(&self, name: &str) -> Option<ConnectorInfo> {
        let entries = self.entries.read().await;
        entries.get(name).map(Self::info)
    }

    pub async fn list(&self) -> Vec<ConnectorInfo> {
        let entries = self.entries.read().await;
        let mut v: Vec<_> = entries.values().map(Self::info).collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// The config of a connector, as defined (unresolved).
    pub async fn get_config(&self, name: &str) -> Option<ConnectorConfig> {
        self.entries
            .read()
            .await
            .get(name)
            .map(|e| e.config.clone())
    }

    fn info(e: &Entry) -> ConnectorInfo {
        ConnectorInfo {
            name: e.config.name.clone(),
            connector_type: e.config.connector_type.to_string(),
            plugin: e.config.plugin.clone(),
            stream: e.config.stream.clone(),
            origin: match e.origin {
                Origin::Api => "api",
                Origin::File(_) => "file",
            },
            file: match &e.origin {
                Origin::File(f) => Some(f.clone()),
                Origin::Api => None,
            },
            state: e.state.snapshot(),
        }
    }

    /// The webhook connector serving `path` (`/webhooks/<path>`), if any.
    pub async fn find_webhook(&self, path: &str) -> Option<Arc<WebhookEndpoint>> {
        let wanted = path.trim_matches('/');
        let entries = self.entries.read().await;
        entries
            .values()
            .filter_map(|e| e.webhook.clone())
            .find(|w| w.path() == wanted)
    }

    /// Record a problem with a connector's definition file (e.g. the edited
    /// file no longer parses; the previous config stays active).
    pub async fn note_config_error(&self, name: &str, error: &str) {
        if let Some(e) = self.entries.read().await.get(name) {
            e.state.note_error(format!("config file: {error}"));
        }
    }

    // -- startup / leadership -------------------------------------------------------

    /// Load persisted API configs and `connectors.d/*.toml`. Registers
    /// only; `run_all` starts them on the leader.
    pub async fn load_all(&self) -> Result<(), String> {
        self.load_json_configs().await?;
        self.load_toml_configs().await?;
        Ok(())
    }

    /// Load TOML configs from `connectors.d/` (register only).
    pub async fn load_toml_configs(&self) -> Result<(), String> {
        let dir = self.connectors_d_dir();
        if !dir.exists() {
            return Ok(());
        }
        let entries =
            std::fs::read_dir(&dir).map_err(|e| format!("failed to read connectors.d: {e}"))?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|n| n.to_str()).map(String::from)
            else {
                continue;
            };
            match ConnectorConfig::load_toml(&path) {
                Ok(config) => {
                    let name = config.name.clone();
                    // The file is the source of truth: it replaces any stale
                    // API copy.
                    let _ = std::fs::remove_file(self.config_path(&name));
                    self.entries.write().await.remove(&name);
                    if let Err(e) = self.register(config, Origin::File(file_name.clone())).await {
                        warn!(file = ?path, error = %e, "connector config registered as failed");
                    }
                    if let Some(hash) = Self::hash_file(&path) {
                        self.toml_files.write().await.insert(
                            file_name,
                            TomlFile {
                                connector_name: name,
                                hash,
                            },
                        );
                    }
                }
                Err(e) => warn!(file = ?path, error = %e, "failed to parse TOML connector config"),
            }
        }
        Ok(())
    }

    async fn load_json_configs(&self) -> Result<(), String> {
        let dir = self.configs_dir();
        if !dir.exists() {
            return Ok(());
        }
        let entries =
            std::fs::read_dir(&dir).map_err(|e| format!("failed to read connectors dir: {e}"))?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            match ConnectorConfig::load_json(&path) {
                Ok(config) => {
                    if let Err(e) = self.register(config, Origin::Api).await {
                        warn!(file = ?path, error = %e, "connector config registered as failed");
                    }
                }
                Err(e) => warn!(file = ?path, error = %e, "failed to parse connector config"),
            }
        }
        Ok(())
    }

    /// Register without starting. Invalid configs are registered `failed`.
    async fn register(&self, config: ConnectorConfig, origin: Origin) -> Result<(), ManagerError> {
        let invalid = self.validate(&config, &origin).err();
        let webhook = if invalid.is_none() {
            self.webhook_for(&config, &origin)
        } else {
            None
        };
        let mut entries = self.entries.write().await;
        // Keep an existing status object (restart count, history).
        let state = entries
            .get(&config.name)
            .map(|e| e.state.clone())
            .unwrap_or_else(|| ConnectorState::new(&config.name, self.metrics.clone()));
        match &invalid {
            Some(e) => state.set_error(Status::Failed, e.to_string()),
            None => state.set_status(Status::Stopped),
        }
        entries.insert(
            config.name.clone(),
            Entry {
                config,
                origin,
                state,
                handle: None,
                webhook,
            },
        );
        invalid.map_or(Ok(()), Err)
    }

    fn webhook_for(
        &self,
        config: &ConnectorConfig,
        origin: &Origin,
    ) -> Option<Arc<WebhookEndpoint>> {
        if config.connector_type != ConnectorType::Source || config.plugin != "http_webhook" {
            return None;
        }
        let init = self
            .registry
            .init(config, Self::resolves_env(origin), self.metrics.clone())
            .ok()?;
        WebhookEndpoint::from_init(&init).ok().map(Arc::new)
    }

    async fn insert_and_start(&self, config: ConnectorConfig, origin: Origin) {
        let name = config.name.clone();
        let valid = self.register(config, origin).await.is_ok();
        if valid && self.leadership.is_currently_leader() {
            let token = self.leader_token().await;
            self.start_entry(&name, &token).await;
        }
    }

    async fn leader_token(&self) -> CancellationToken {
        let tenure = self.tenure.lock().unwrap().clone();
        match tenure {
            Some(t) => t,
            None => self.leadership.current_child_token().await,
        }
    }

    async fn take_handle(&self, name: &str) -> Option<RunHandle> {
        self.entries
            .write()
            .await
            .get_mut(name)
            .and_then(|e| e.handle.take())
    }

    /// Start one registered connector under `token`. A still-running
    /// previous instance is stopped (and awaited) first.
    async fn start_entry(&self, name: &str, token: &CancellationToken) {
        if let Some(old) = self.take_handle(name).await {
            old.stop(STOP_TIMEOUT).await;
        }
        let mut entries = self.entries.write().await;
        let Some(entry) = entries.get_mut(name) else {
            return;
        };
        let origin = entry.origin.clone();
        if let Err(e) = self.validate(&entry.config, &origin) {
            entry.state.set_error(Status::Failed, e.to_string());
            return;
        }
        if self.registry.is_passive(&entry.config.plugin) {
            entry.webhook = self.webhook_for(&entry.config, &origin);
            entry.state.set_status(Status::Running);
            return;
        }
        let settings = match self.registry.init(
            &entry.config,
            Self::resolves_env(&origin),
            self.metrics.clone(),
        ) {
            Ok(i) => i.settings,
            Err(e) => {
                entry.state.set_error(Status::Failed, e.to_string());
                return;
            }
        };
        if StreamName::try_from(entry.config.stream.as_str()).is_err() {
            entry
                .state
                .set_error(Status::Failed, "invalid stream name".to_string());
            return;
        }
        let ctx = Arc::new(RunContext {
            config: entry.config.clone(),
            settings,
            log: self.log.clone(),
            storage: self.storage.clone(),
            offsets: self.offset_store.clone(),
            metrics: self.metrics.clone(),
            state: entry.state.clone(),
            registry: self.registry.clone(),
        });
        entry.handle = Some(runtime::spawn(ctx, token));
        info!(connector = name, "connector supervisor started");
    }

    /// Start every registered connector under `token` (one leader tenure).
    /// Returns after `token` is cancelled and every connector has stopped
    /// (final sink flushes included).
    pub async fn run_all(&self, token: CancellationToken) {
        *self.tenure.lock().unwrap() = Some(token.clone());
        let names: Vec<String> = self.entries.read().await.keys().cloned().collect();
        for name in names {
            self.start_entry(&name, &token).await;
        }
        token.cancelled().await;
        info!("connector manager: leader tenure ended; stopping connectors");
        *self.tenure.lock().unwrap() = None;
        self.stop_all().await;
    }

    /// Stop every connector and wait for them (graceful shutdown).
    pub async fn shutdown(&self) {
        *self.tenure.lock().unwrap() = None;
        self.stop_all().await;
    }

    async fn stop_all(&self) {
        let handles: Vec<(String, RunHandle, Arc<ConnectorState>)> = {
            let mut entries = self.entries.write().await;
            entries
                .iter_mut()
                .filter_map(|(n, e)| {
                    if e.handle.is_none() && e.webhook.is_some() {
                        e.state.set_status(Status::Stopped);
                    }
                    e.handle.take().map(|h| (n.clone(), h, e.state.clone()))
                })
                .collect()
        };
        let stops = handles.iter().map(|(name, h, _)| async move {
            if !h.stop(STOP_TIMEOUT).await {
                error!(connector = %name, "connector did not stop in time; aborted");
            }
        });
        futures_util::future::join_all(stops).await;
        for (_, _, state) in handles {
            if state.status() != Status::Failed {
                state.set_status(Status::Stopped);
            }
        }
    }
}
