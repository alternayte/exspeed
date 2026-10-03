//! Sources and sinks between Exspeed streams and external systems.
//!
//! - [`traits`]: the plugin contract and error taxonomy.
//! - [`runtime`]: the supervisor and the checkpoint protocol.
//! - [`manager`]: lifecycle, provenance and leadership.
//! - [`builtin`]: the shipped plugins.

pub mod builtin;
pub mod config;
pub mod dlq;
pub mod file_watcher;
pub mod manager;
pub mod offset_store;
pub mod registry;
pub mod retry;
pub mod runtime;
pub mod settings;
pub mod status;
pub mod subject;
pub mod traits;
pub mod transform;

pub use config::{ConnectorConfig, ConnectorType};
pub use manager::{ConnectorInfo, ConnectorManager, ManagerError, Origin};
pub use registry::{PluginInit, Registry};
pub use traits::*;
