use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use exspeed_storage::file::FileStorage;

/// Run retention enforcement until `token` is cancelled. Ticks every
/// 60 seconds, with a 10-second initial delay before the first check.
/// Called by the leader supervisor in `exspeed/src/cli/server.rs`.
pub async fn run(storage: Arc<FileStorage>, token: CancellationToken) {
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
                let storage_clone = storage.clone();
                let result = tokio::task::spawn_blocking(
                    move || storage_clone.enforce_all_retention(),
                ).await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => error!("retention enforcement failed: {}", e),
                    Err(e) => error!("retention task panicked: {}", e),
                }
            }
            _ = token.cancelled() => {
                info!("retention task stopping (leader token fired)");
                return;
            }
        }
    }
}

/// Legacy entrypoint — calls `run` with a token that is never cancelled.
/// Kept for any caller that hasn't been migrated yet. Remove once all
/// callers pass their own token.
pub fn spawn_retention_task(storage: Arc<FileStorage>) {
    let token = CancellationToken::new();
    tokio::spawn(async move { run(storage, token).await });
}
