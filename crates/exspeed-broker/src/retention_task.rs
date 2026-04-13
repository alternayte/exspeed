use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::error;

use exspeed_storage::file::FileStorage;

/// Spawn a background task that enforces retention every 60 seconds.
pub fn spawn_retention_task(storage: Arc<FileStorage>) {
    tokio::spawn(async move {
        // Initial delay before first check
        time::sleep(Duration::from_secs(10)).await;

        let mut interval = time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;

            let storage_clone = storage.clone();
            let result = tokio::task::spawn_blocking(move || {
                storage_clone.enforce_all_retention()
            })
            .await;

            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!("retention enforcement failed: {}", e),
                Err(e) => error!("retention task panicked: {}", e),
            }
        }
    });
}
