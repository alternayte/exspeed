//! Plugin registry: maps a plugin name to a factory. The server, the API and
//! `exspeed connector validate` all build plugins through the same registry,
//! so validation and runtime agree on what a valid config is.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use exspeed_common::Metrics;

use crate::config::{ConnectorConfig, ConnectorType, Settings};
use crate::traits::{ConnectorError, SinkConnector, SourceConnector};

/// Everything a plugin constructor gets. `settings` are already resolved
/// (`${VAR}` substituted where allowed).
pub struct PluginInit {
    pub config: ConnectorConfig,
    pub settings: Settings,
    pub metrics: Arc<Metrics>,
}

pub type SourceFactory =
    Arc<dyn Fn(&PluginInit) -> Result<Box<dyn SourceConnector>, ConnectorError> + Send + Sync>;
pub type SinkFactory =
    Arc<dyn Fn(&PluginInit) -> Result<Box<dyn SinkConnector>, ConnectorError> + Send + Sync>;
/// Validates a passive plugin's settings (no task loop, e.g. webhooks).
pub type PassiveValidator = Arc<dyn Fn(&PluginInit) -> Result<(), ConnectorError> + Send + Sync>;

#[derive(Clone, Default)]
pub struct Registry {
    sources: HashMap<String, SourceFactory>,
    sinks: HashMap<String, SinkFactory>,
    passive_sources: HashMap<String, PassiveValidator>,
}

impl Registry {
    /// An empty registry (tests register fakes).
    pub fn empty() -> Self {
        Self::default()
    }

    /// The built-in plugins.
    pub fn builtin() -> Self {
        let mut r = Self::default();
        crate::builtin::register(&mut r);
        r
    }

    pub fn register_source(
        &mut self,
        name: &str,
        f: impl Fn(&PluginInit) -> Result<Box<dyn SourceConnector>, ConnectorError>
            + Send
            + Sync
            + 'static,
    ) {
        self.sources.insert(name.to_string(), Arc::new(f));
    }

    pub fn register_sink(
        &mut self,
        name: &str,
        f: impl Fn(&PluginInit) -> Result<Box<dyn SinkConnector>, ConnectorError>
            + Send
            + Sync
            + 'static,
    ) {
        self.sinks.insert(name.to_string(), Arc::new(f));
    }

    pub fn register_passive_source(
        &mut self,
        name: &str,
        f: impl Fn(&PluginInit) -> Result<(), ConnectorError> + Send + Sync + 'static,
    ) {
        self.passive_sources.insert(name.to_string(), Arc::new(f));
    }

    /// A passive source has no task loop (it is driven by inbound HTTP).
    pub fn is_passive(&self, plugin: &str) -> bool {
        self.passive_sources.contains_key(plugin)
    }

    pub fn source_names(&self) -> BTreeSet<String> {
        self.sources
            .keys()
            .chain(self.passive_sources.keys())
            .cloned()
            .collect()
    }

    pub fn sink_names(&self) -> BTreeSet<String> {
        self.sinks.keys().cloned().collect()
    }

    pub fn create_source(
        &self,
        init: &PluginInit,
    ) -> Result<Box<dyn SourceConnector>, ConnectorError> {
        let f = self.sources.get(&init.config.plugin).ok_or_else(|| {
            ConnectorError::config(format!(
                "unknown source plugin '{}' (known: {})",
                init.config.plugin,
                join(self.source_names())
            ))
        })?;
        f(init)
    }

    pub fn create_sink(&self, init: &PluginInit) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let f = self.sinks.get(&init.config.plugin).ok_or_else(|| {
            ConnectorError::config(format!(
                "unknown sink plugin '{}' (known: {})",
                init.config.plugin,
                join(self.sink_names())
            ))
        })?;
        f(init)
    }

    /// Full validation: common fields, then the plugin's typed settings
    /// (by constructing the plugin — constructors do no I/O).
    pub fn validate(
        &self,
        config: &ConnectorConfig,
        resolve_env: bool,
        metrics: Arc<Metrics>,
    ) -> Result<(), ConnectorError> {
        config.validate_common().map_err(ConnectorError::Fatal)?;
        let init = self.init(config, resolve_env, metrics)?;
        match config.connector_type {
            ConnectorType::Source => {
                if let Some(v) = self.passive_sources.get(&config.plugin) {
                    return v(&init);
                }
                self.create_source(&init).map(drop)
            }
            ConnectorType::Sink => self.create_sink(&init).map(drop),
        }
    }

    /// Build the constructor input, resolving `${VAR}` when `resolve_env`.
    pub fn init(
        &self,
        config: &ConnectorConfig,
        resolve_env: bool,
        metrics: Arc<Metrics>,
    ) -> Result<PluginInit, ConnectorError> {
        let settings = if resolve_env {
            config.resolved_settings()?
        } else {
            config.settings.clone()
        };
        Ok(PluginInit {
            config: config.clone(),
            settings,
            metrics,
        })
    }
}

fn join(names: BTreeSet<String>) -> String {
    names.into_iter().collect::<Vec<_>>().join(", ")
}
