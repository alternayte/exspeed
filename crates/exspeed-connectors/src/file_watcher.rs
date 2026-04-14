use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::{info, warn};

use crate::config::ConnectorConfig;
use crate::manager::ConnectorManager;

/// Spawn a background task that polls `connectors_dir` every 5 seconds.
///
/// New `.toml` files trigger `manager.create`; deleted files trigger `manager.delete`.
pub fn spawn_file_watcher(manager: Arc<ConnectorManager>, connectors_dir: PathBuf) {
    tokio::spawn(async move {
        let mut known: HashSet<String> = HashSet::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            interval.tick().await;

            // If the directory doesn't exist, skip this tick silently.
            if !connectors_dir.exists() {
                continue;
            }

            // Scan the directory for .toml files.
            let entries = match std::fs::read_dir(&connectors_dir) {
                Ok(e) => e,
                Err(err) => {
                    warn!(dir = ?connectors_dir, error = %err, "file_watcher: failed to read connectors.d");
                    continue;
                }
            };

            let mut current: HashSet<String> = HashSet::new();

            for entry in entries {
                let entry = match entry {
                    Ok(e) => e,
                    Err(err) => {
                        warn!(error = %err, "file_watcher: failed to read dir entry");
                        continue;
                    }
                };

                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                    continue;
                }

                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    current.insert(filename.to_string());
                }
            }

            // New files: present in current but not in known.
            for filename in &current {
                if !known.contains(filename) {
                    let path = connectors_dir.join(filename);
                    match ConnectorConfig::load_toml(&path) {
                        Ok(mut config) => {
                            config.resolve_env_vars();
                            info!(
                                connector = config.name.as_str(),
                                file = ?path,
                                "file_watcher: new TOML connector config detected"
                            );
                            if let Err(e) = manager.create(config).await {
                                warn!(file = ?path, error = %e, "file_watcher: failed to create connector from TOML");
                            }
                        }
                        Err(e) => {
                            warn!(file = ?path, error = %e, "file_watcher: failed to parse TOML connector config");
                        }
                    }
                }
            }

            // Deleted files: present in known but not in current.
            for filename in &known {
                if !current.contains(filename) {
                    // Derive connector name by stripping the ".toml" suffix.
                    let connector_name = filename
                        .strip_suffix(".toml")
                        .unwrap_or(filename.as_str())
                        .to_string();
                    info!(
                        connector = connector_name.as_str(),
                        file = filename.as_str(),
                        "file_watcher: TOML connector config removed"
                    );
                    if let Err(e) = manager.delete(&connector_name).await {
                        warn!(connector = connector_name.as_str(), error = %e, "file_watcher: failed to delete connector");
                    }
                }
            }

            // Update known set to reflect the current state.
            known = current;
        }
    });
}
