use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use exspeed_storage::file::FileStorage;
use exspeed_streams::StorageEngine;

use crate::replication::ReplicationCoordinator;

/// Run retention enforcement until `token` is cancelled. Ticks every
/// 60 seconds, with a 10-second initial delay before the first check.
/// Called by the leader supervisor in `exspeed/src/cli/server.rs`.
///
/// `coordinator` — when `Some(_)`, every successful trim that advances a
/// stream's earliest offset emits a `RetentionTrimmed` replication event
/// so followers can apply the same trim locally.
pub async fn run(
    storage: Arc<FileStorage>,
    token: CancellationToken,
    coordinator: Option<Arc<ReplicationCoordinator>>,
) {
    // Initial delay before first check.
    tokio::select! {
        _ = time::sleep(Duration::from_secs(10)) => {}
        _ = token.cancelled() => {
            info!("retention task stopped before first tick (leader token fired)");
            return;
        }
    }

    let mut interval = time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                enforce_once(&storage, coordinator.as_ref()).await;
            }
            _ = token.cancelled() => {
                info!("retention task stopping (leader token fired)");
                return;
            }
        }
    }
}

/// One pass of retention enforcement. Snapshots per-stream earliest
/// offsets before and after `enforce_all_retention`; when a coordinator
/// is attached and a stream's earliest moved forward, emits a
/// `RetentionTrimmed` event so followers apply the same trim.
///
/// Exposed as `pub` (not `pub(crate)`) so the replication-retention
/// integration test suite can drive it directly — otherwise the only
/// way to trigger a pass is the 60s background tick, which would make
/// the test slow and flaky.
pub async fn enforce_once(
    storage: &Arc<FileStorage>,
    coordinator: Option<&Arc<ReplicationCoordinator>>,
) {
    // Snapshot pre-enforce earliest offsets (only when a coordinator is attached —
    // otherwise we save the extra work on the single-pod hot path).
    let pre: Vec<(exspeed_common::StreamName, u64)> = if coordinator.is_some() {
        let storage_trait: Arc<dyn StorageEngine> = storage.clone();
        match storage_trait.list_streams().await {
            Ok(streams) => {
                let mut out: Vec<(exspeed_common::StreamName, u64)> =
                    Vec::with_capacity(streams.len());
                for s in streams {
                    match storage_trait.stream_bounds(&s).await {
                        Ok((earliest, _)) => out.push((s, earliest.0)),
                        Err(e) => {
                            tracing::debug!(stream = %s, err = %e, "stream_bounds failed pre-retention");
                        }
                    }
                }
                out
            }
            Err(e) => {
                tracing::debug!(err = %e, "list_streams failed pre-retention");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let storage_clone = storage.clone();
    let result = tokio::task::spawn_blocking(move || storage_clone.enforce_all_retention()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            error!("retention enforcement failed: {}", e);
            return;
        }
        Err(e) => {
            error!("retention task panicked: {}", e);
            return;
        }
    }

    // Emit RetentionTrimmed events for every stream whose earliest offset moved forward.
    let Some(coord) = coordinator else { return };
    let storage_trait: Arc<dyn StorageEngine> = storage.clone();
    for (stream_name, pre_earliest) in pre {
        match storage_trait.stream_bounds(&stream_name).await {
            Ok((post_earliest, _)) if post_earliest.0 > pre_earliest => {
                use exspeed_protocol::messages::replicate::RetentionTrimmedEvent;
                coord.emit(crate::replication::ReplicationEvent::RetentionTrimmed(
                    RetentionTrimmedEvent {
                        stream: stream_name.as_str().to_string(),
                        new_earliest_offset: post_earliest.0,
                    },
                ));
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(stream = %stream_name, err = %e, "stream_bounds failed post-retention");
            }
        }
    }
}

/// Legacy entrypoint — calls `run` with a token that is never cancelled
/// and no replication coordinator. Kept for any caller that hasn't been
/// migrated yet. Remove once all callers pass their own token.
pub fn spawn_retention_task(storage: Arc<FileStorage>) {
    let token = CancellationToken::new();
    tokio::spawn(async move { run(storage, token, None).await });
}
