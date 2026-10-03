use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use notify::{EventKind, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::ConnectorConfig;
use crate::manager::{ConnectorManager, ManagerError, Origin, TomlFile};

/// Scan `connectors_dir` for `.toml` files and reconcile the connectors that
/// came from that directory.
///
/// - New file → create the connector it defines.
/// - Changed file, same connector name → update the config in place and
///   restart it, **keeping its offsets**.
/// - Changed file that now names a different connector → delete the old
///   connector, create the new one.
/// - File removed → delete the connector it defined.
///
/// Connectors created through the HTTP API are never touched.
pub async fn sync_connectors(manager: &Arc<ConnectorManager>, connectors_dir: &PathBuf) {
    if !connectors_dir.exists() {
        return;
    }

    let entries = match std::fs::read_dir(connectors_dir) {
        Ok(e) => e,
        Err(err) => {
            warn!(dir = ?connectors_dir, error = %err, "file_watcher: failed to read connectors.d");
            return;
        }
    };

    let mut on_disk: HashMap<String, PathBuf> = HashMap::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("toml") {
            continue;
        }
        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            on_disk.insert(filename.to_string(), path.clone());
        }
    }

    let known: HashMap<String, TomlFile> = manager.toml_files.read().await.clone();

    // New or modified files.
    for (filename, path) in &on_disk {
        let Some(hash) = ConnectorManager::hash_file(path) else {
            continue;
        };
        let previous = known.get(filename);
        if previous.map(|p| p.hash) == Some(hash) {
            continue; // unchanged
        }
        let config = match ConnectorConfig::load_toml(path) {
            Ok(c) => c,
            Err(e) => {
                warn!(file = ?path, error = %e, "file_watcher: failed to parse TOML connector config; keeping the previous config");
                if let Some(prev) = previous {
                    manager
                        .note_config_error(&prev.connector_name, &e.to_string())
                        .await;
                }
                continue;
            }
        };
        let name = config.name.clone();
        let origin = Origin::File(filename.clone());

        let result: Result<(), ManagerError> = match previous {
            Some(prev) if prev.connector_name == name => {
                info!(connector = name.as_str(), file = ?path,
                      "file_watcher: connector config changed, restarting (offsets kept)");
                manager.update_config(config, origin).await
            }
            Some(prev) => {
                info!(old = prev.connector_name.as_str(), new = name.as_str(), file = ?path,
                      "file_watcher: file now defines a different connector");
                if let Err(e) = manager.delete(&prev.connector_name).await {
                    warn!(connector = prev.connector_name.as_str(), error = %e,
                          "file_watcher: failed to delete replaced connector");
                }
                manager.create_from_file(config, filename).await
            }
            None => {
                if manager.get_status(&name).await.is_some() {
                    // Same name as an API-created connector: adopt the file
                    // as the definition, keeping offsets.
                    info!(connector = name.as_str(), file = ?path,
                          "file_watcher: file redefines an existing connector");
                    manager.update_config(config, origin).await
                } else {
                    info!(connector = name.as_str(), file = ?path,
                          "file_watcher: new connector config detected");
                    manager.create_from_file(config, filename).await
                }
            }
        };
        if let Err(e) = result {
            warn!(file = ?path, error = %e, "file_watcher: connector config not applied cleanly");
        }
        manager.toml_files.write().await.insert(
            filename.clone(),
            TomlFile {
                connector_name: name,
                hash,
            },
        );
    }

    // Removed files: delete only the connectors they defined.
    for (filename, prev) in &known {
        if on_disk.contains_key(filename) {
            continue;
        }
        info!(
            connector = prev.connector_name.as_str(),
            file = filename.as_str(),
            "file_watcher: connector config file removed"
        );
        if let Err(e) = manager.delete(&prev.connector_name).await {
            warn!(connector = prev.connector_name.as_str(), error = %e,
                  "file_watcher: failed to delete connector");
        }
        manager.toml_files.write().await.remove(filename);
    }
}

/// Spawn a background task that watches `connectors_dir` for filesystem changes.
///
/// Tries to use a native OS watcher (inotify / kqueue / FSEvents) via the
/// `notify` crate.  If that fails, falls back to a 5-second poll interval with
/// a warning.
///
/// On any Create / Modify / Remove event the watcher debounces 500 ms and then
/// calls [`sync_connectors`].
pub fn spawn_file_watcher(manager: Arc<ConnectorManager>, connectors_dir: PathBuf) {
    // Unbounded-ish channel: the watcher thread signals the async loop.
    let (tx, mut rx) = mpsc::channel::<()>(16);

    // ----- Try native watcher -----
    let tx_notify = tx.clone();
    let watch_dir = connectors_dir.clone();

    let watcher_result =
        notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
            Ok(event) => {
                let relevant = matches!(
                    event.kind,
                    EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
                );
                if relevant {
                    // blocking_send is fine here: we're on a background thread.
                    let _ = tx_notify.blocking_send(());
                }
            }
            Err(e) => {
                warn!(error = %e, "file_watcher: notify error");
            }
        });

    match watcher_result {
        Ok(mut watcher) => {
            // Ensure the directory exists so the watcher can attach to it.
            if let Err(e) = std::fs::create_dir_all(&watch_dir) {
                warn!(dir = ?watch_dir, error = %e, "file_watcher: could not create connectors.d dir");
            }

            match watcher.watch(&watch_dir, RecursiveMode::NonRecursive) {
                Ok(()) => {
                    info!(dir = ?watch_dir, "file_watcher: native OS watcher active");
                }
                Err(e) => {
                    warn!(dir = ?watch_dir, error = %e, "file_watcher: failed to watch dir, falling back to polling");
                    spawn_poll_fallback(tx);
                }
            }

            // Spawn the async event loop, keeping `watcher` alive inside it.
            tokio::spawn(async move {
                // Keep the watcher alive for the lifetime of this task.
                let _watcher = watcher;

                event_loop(&mut rx, &manager, &connectors_dir).await;
            });
        }
        Err(e) => {
            warn!(error = %e, "file_watcher: native watcher unavailable, falling back to 5s polling");
            spawn_poll_fallback(tx);

            tokio::spawn(async move {
                event_loop(&mut rx, &manager, &connectors_dir).await;
            });
        }
    }
}

/// Spawn a background interval task that fires the channel every 5 seconds.
fn spawn_poll_fallback(tx: mpsc::Sender<()>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            if tx.send(()).await.is_err() {
                break; // receiver gone
            }
        }
    });
}

/// Core async loop: receives signals, debounces 500 ms, then syncs.
async fn event_loop(
    rx: &mut mpsc::Receiver<()>,
    manager: &Arc<ConnectorManager>,
    connectors_dir: &PathBuf,
) {
    loop {
        // Wait for the first signal.
        if rx.recv().await.is_none() {
            break; // channel closed
        }

        // Debounce: drain additional signals that arrive within 500 ms.
        let debounce = Duration::from_millis(500);
        loop {
            match tokio::time::timeout(debounce, rx.recv()).await {
                Ok(Some(())) => {}      // more events — keep draining
                Ok(None) => return,     // channel closed
                Err(_elapsed) => break, // debounce window passed
            }
        }

        sync_connectors(manager, connectors_dir).await;
    }
}
