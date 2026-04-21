//! Periodic fsync task for the async storage mode. Spawned per partition.
//! Calls `Partition::sync_wal_now` every `interval`.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;

use crate::file::partition::Partition;

pub fn spawn(partition: Arc<Mutex<Partition>>, sync_interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = interval(sync_interval);
        // First tick fires immediately by default; skip it so we wait
        // a full interval before the first sync.
        ticker.tick().await;
        loop {
            ticker.tick().await;
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
    });
}
