//! Periodic fsync task for the async storage mode. Spawned per partition.
//! Owns a cloned `File` handle to the currently active segment so
//! `sync_data` can run without holding the per-partition
//! `Mutex<Partition>` that serializes writes.
//!
//! Both the syncer's cloned handle and the `SegmentWriter`'s handle point
//! to the same kernel fd. `sync_data` on the syncer handle flushes every
//! buffered write that reached the fd before the syscall began — which is
//! exactly the async-mode semantic: "acked data is durable within the
//! next sync window." Writes that start mid-sync simply become durable on
//! the next tick.
//!
//! The `File` is held in an `Arc<std::sync::Mutex<File>>` that is shared
//! between the spawned task and the returned `SegmentSyncerHandle`. When
//! `Partition::roll_segment` seals the old active segment and creates a
//! new one, it calls `SegmentSyncerHandle::set_active_file` to swap the
//! cloned fd inside the slot so subsequent ticks fsync the new segment.
//! Without that swap the syncer would keep fsyncing a sealed (unchanging)
//! file and the new active segment would receive no periodic fsync.
//!
//! The returned `SegmentSyncerHandle` keeps the task alive. Dropping the
//! handle signals the task to stop (via a watch channel), performs one
//! final sync, and releases its `File`.

use std::fs::File;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::interval;

/// Owned handle for a spawned `SegmentSyncer` task.
///
/// Dropping this signals the background task to stop. The task performs a
/// final `sync_data` before exiting so no buffered writes are silently
/// abandoned on clean shutdown.
///
/// The handle also exposes `set_active_file` so that `Partition::roll_segment`
/// can swap the file the syncer task is fsyncing when the active segment
/// rolls.
pub struct SegmentSyncerHandle {
    /// Dropping this sender signals `true` to the watcher, causing the
    /// task to break out of its `select!` loop.
    _shutdown: watch::Sender<bool>,
    /// Shared slot holding the `File` the spawned task fsyncs on each tick.
    /// `set_active_file` swaps the file inside this slot; the old `File`
    /// is dropped when the new one replaces it.
    file_slot: Arc<Mutex<File>>,
}

impl SegmentSyncerHandle {
    /// Replace the file that the syncer task fsyncs on each tick. Called by
    /// `Partition::roll_segment` after the old active segment has been sealed
    /// (and fsync'd by the roll path itself). The old `File` inside the slot
    /// is dropped when the new one replaces it.
    pub(crate) fn set_active_file(&self, new_file: File) {
        let mut guard = self
            .file_slot
            .lock()
            .expect("segment_syncer file_slot poisoned");
        *guard = new_file;
    }
}

/// Spawn a periodic segment-sync task that owns `active_file` (cloned from
/// the `SegmentWriter` via `Partition::try_clone_active_segment_file`).
/// The task fires every `sync_interval`; the first tick is skipped so the
/// first sync happens after a full interval.
///
/// `stream` and `partition_id` are captured at spawn time purely for log
/// output — the task has no handle back to the `Partition` itself.
///
/// When the handle is dropped the task receives a shutdown signal,
/// performs one final `sync_data`, then exits.
pub fn spawn(
    active_file: File,
    stream: String,
    partition_id: u32,
    sync_interval: Duration,
) -> SegmentSyncerHandle {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    // Wrap the File in an Arc<Mutex<File>> so the syncer task and the
    // returned handle share a single slot. The handle can swap the File
    // via `set_active_file` on segment roll, and the task sees the
    // replacement on its next tick. The Mutex is effectively uncontended
    // outside of roll events — only the syncer task acquires it on each
    // tick, and roll events are infrequent.
    let file_slot = Arc::new(Mutex::new(active_file));
    let file_for_task = file_slot.clone();
    tokio::spawn(async move {
        let mut ticker = interval(sync_interval);
        // First tick fires immediately by default; skip it so we wait a
        // full interval before the first sync.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let result = tokio::task::block_in_place(|| {
                        let f = file_for_task
                            .lock()
                            .expect("segment_syncer file_slot poisoned");
                        f.sync_data()
                    });
                    if let Err(e) = result {
                        tracing::warn!(
                            stream = %stream,
                            partition = partition_id,
                            error = %e,
                            "segment_syncer fsync failed"
                        );
                    }
                }
                _ = shutdown_rx.changed() => {
                    // Final sync before exit so buffered writes are not
                    // lost on clean shutdown.
                    let result = tokio::task::block_in_place(|| {
                        let f = file_for_task
                            .lock()
                            .expect("segment_syncer file_slot poisoned");
                        f.sync_data()
                    });
                    if let Err(e) = result {
                        tracing::warn!(
                            stream = %stream,
                            partition = partition_id,
                            error = %e,
                            "segment_syncer final fsync on shutdown failed"
                        );
                    }
                    return;
                }
            }
        }
    });
    SegmentSyncerHandle {
        _shutdown: shutdown_tx,
        file_slot,
    }
}
