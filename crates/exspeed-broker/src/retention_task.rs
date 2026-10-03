use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use exspeed_storage::file::FileStorage;

/// Run retention enforcement until `token` is cancelled. Ticks every
/// 60 seconds, with a 10-second initial delay before the first check.
/// Runs on the leader only (the leader supervisor in
/// `exspeed/src/cli/server.rs`); followers mirror the trims through
/// replication, which carries each stream's earliest offset.
pub async fn run(storage: Arc<FileStorage>, token: CancellationToken) {
    tokio::select! {
        _ = time::sleep(Duration::from_secs(10)) => {}
        _ = token.cancelled() => return,
    }
    let mut interval = time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = interval.tick() => enforce_once(&storage).await,
            _ = token.cancelled() => {
                info!("retention task stopping (leader token fired)");
                return;
            }
        }
    }
}

/// One pass of retention enforcement over every stream.
pub async fn enforce_once(storage: &Arc<FileStorage>) {
    let storage = storage.clone();
    match tokio::task::spawn_blocking(move || storage.enforce_all_retention()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!("retention enforcement failed: {}", e),
        Err(e) => error!("retention task panicked: {}", e),
    }
}
