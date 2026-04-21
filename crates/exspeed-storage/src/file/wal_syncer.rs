//! Periodic fsync task for the async storage mode. Spawned per partition.
//! Calls `Partition::sync_wal_now` every `interval`.
//!
//! The returned `WalSyncerHandle` keeps the task alive. Dropping the handle
//! signals the task to stop (via a watch channel), performs one final sync,
//! and releases its `Arc<Mutex<Partition>>`.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex};
use tokio::time::interval;

use crate::file::partition::Partition;

/// Owned handle for a spawned `WalSyncer` task.
///
/// Dropping this signals the background task to stop. The task performs a
/// final `sync_wal_now` before exiting so no pending writes are silently
/// abandoned on clean shutdown.
pub struct WalSyncerHandle {
    /// Dropping this sender signals `true` to the watcher, causing the task
    /// to break out of its `select!` loop.
    _shutdown: watch::Sender<bool>,
}

/// Spawn a periodic WAL-sync task for `partition` and return a
/// [`WalSyncerHandle`]. The task fires every `sync_interval`; the first tick
/// is skipped so the first sync happens after a full interval.
///
/// When the handle is dropped the task receives a shutdown signal, performs
/// one final sync, then exits — releasing its `Arc<Mutex<Partition>>`.
pub fn spawn(partition: Arc<Mutex<Partition>>, sync_interval: Duration) -> WalSyncerHandle {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let mut ticker = interval(sync_interval);
        // First tick fires immediately by default; skip it so we wait
        // a full interval before the first sync.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let result = tokio::task::block_in_place(|| {
                        let mut p = partition.blocking_lock();
                        let stream = p.stream_name().to_string();
                        let part_id = p.partition_id();
                        let r = p.sync_wal_now();
                        (stream, part_id, r)
                    });
                    if let Err(e) = result.2 {
                        tracing::warn!(
                            stream = %result.0,
                            partition = result.1,
                            error = %e,
                            "wal_syncer fsync failed"
                        );
                    }
                }
                _ = shutdown_rx.changed() => {
                    // Final sync before exit so in-flight writes are not lost
                    // on clean shutdown.
                    let result = tokio::task::block_in_place(|| {
                        let mut p = partition.blocking_lock();
                        let stream = p.stream_name().to_string();
                        let part_id = p.partition_id();
                        let r = p.sync_wal_now();
                        (stream, part_id, r)
                    });
                    if let Err(e) = result.2 {
                        tracing::warn!(
                            stream = %result.0,
                            partition = result.1,
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
