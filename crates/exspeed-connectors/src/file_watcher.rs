use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use notify::{EventKind, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::ConnectorConfig;
use crate::manager::ConnectorManager;

/// Scan `connectors_dir` for `.toml` files and reconcile with running connectors.
///
/// - Files present in the directory but not in `manager` → load TOML + create.
/// - Files absent from the directory but still running in `manager` → delete.
async fn sync_connectors(manager: &Arc<ConnectorManager>, connectors_dir: &PathBuf) {
    if !connectors_dir.exists() {
        return;
    }

    // Build the set of filenames currently on disk.
    let entries = match std::fs::read_dir(connectors_dir) {
        Ok(e) => e,
        Err(err) => {
            warn!(dir = ?connectors_dir, error = %err, "file_watcher: failed to read connectors.d");
            return;
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

    // Build the set of connector names currently running (from TOML-managed ones).
    // We match by deriving the expected filename: <name>.toml.
    let running_names: HashSet<String> = manager
        .list()
        .await
        .into_iter()
        .map(|info| format!("{}.toml", info.name))
        .collect();

    // New files: present on disk but not running.
    for filename in &current {
        if !running_names.contains(filename) {
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

    // Deleted files: running but no longer on disk.
    for filename in &running_names {
        if !current.contains(filename) {
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
