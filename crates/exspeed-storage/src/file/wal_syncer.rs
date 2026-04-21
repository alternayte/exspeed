//! Periodic fsync task for the async storage mode. Spawned per partition.
//! Owns a cloned `File` handle to the WAL so `sync_data` can run without
//! holding the per-partition `Mutex<Partition>` that serializes writes.
//!
//! Both the syncer's cloned handle and the `WalWriter`'s handle point to
//! the same kernel fd. `sync_data` on the syncer handle flushes every
//! buffered write that reached the fd before the syscall began — which is
//! exactly the async-mode semantic: "acked data is durable within the
//! next sync window." Writes that start mid-sync simply become durable on
//! the next tick.
//!
//! The returned `WalSyncerHandle` keeps the task alive. Dropping the
//! handle signals the task to stop (via a watch channel), performs one
//! final sync, and releases its `File`.

use std::fs::File;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::interval;

/// Owned handle for a spawned `WalSyncer` task.
///
/// Dropping this signals the background task to stop. The task performs a
/// final `sync_data` before exiting so no buffered writes are silently
/// abandoned on clean shutdown.
pub struct WalSyncerHandle {
    /// Dropping this sender signals `true` to the watcher, causing the
    /// task to break out of its `select!` loop.
    _shutdown: watch::Sender<bool>,
}

/// Spawn a periodic WAL-sync task that owns `wal_file` (cloned from the
/// `WalWriter` via `Partition::try_clone_wal_file`). The task fires every
/// `sync_interval`; the first tick is skipped so the first sync happens
/// after a full interval.
///
/// `stream` and `partition_id` are captured at spawn time purely for log
/// output — the task has no handle back to the `Partition` itself.
///
/// When the handle is dropped the task receives a shutdown signal,
/// performs one final `sync_data`, then exits.
pub fn spawn(
    wal_file: File,
    stream: String,
    partition_id: u32,
    sync_interval: Duration,
) -> WalSyncerHandle {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    // Wrap the File in an Arc<Mutex<File>> so the syncer task has exclusive
    // access to it across the `block_in_place` boundary without needing
    // unsafe or file-descriptor arithmetic. The Mutex is uncontended — only
    // the syncer task ever acquires it.
    let file = Arc::new(Mutex::new(wal_file));
    tokio::spawn(async move {
        let mut ticker = interval(sync_interval);
        // First tick fires immediately by default; skip it so we wait a
        // full interval before the first sync.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let result = tokio::task::block_in_place(|| {
                        let f = file.lock().expect("wal_syncer file mutex poisoned");
                        f.sync_data()
                    });
                    if let Err(e) = result {
                        tracing::warn!(
                            stream = %stream,
                            partition = partition_id,
                            error = %e,
                            "wal_syncer fsync failed"
                        );
                    }
                }
                _ = shutdown_rx.changed() => {
                    // Final sync before exit so buffered writes are not
                    // lost on clean shutdown.
                    let result = tokio::task::block_in_place(|| {
                        let f = file.lock().expect("wal_syncer file mutex poisoned");
                        f.sync_data()
                    });
                    if let Err(e) = result {
                        tracing::warn!(
                            stream = %stream,
                            partition = partition_id,
                            error = %e,
                            "wal_syncer final fsync on shutdown failed"
                        );
                    }
                    return;
                }
            }
        }
    });
    WalSyncerHandle { _shutdown: shutdown_tx }
}
