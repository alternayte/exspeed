use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::broker_append::BrokerAppend;

pub fn spawn_dedup_snapshot_task(
    broker_append: Arc<BrokerAppend>,
    data_dir: PathBuf,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(60));
        ticker.tick().await; // Skip immediate first tick.
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let _ = broker_append.snapshot_all(&data_dir).await;
                }
                _ = shutdown.cancelled() => {
                    let _ = broker_append.snapshot_all(&data_dir).await;
                    break;
                }
            }
        }
    })
}
