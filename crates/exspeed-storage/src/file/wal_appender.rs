//! Per-partition writer task that batches incoming append requests for
//! group commit. Sits between FileStorage::append (async caller) and
//! Partition::append_batch (sync). One task per (stream, partition).

use std::sync::Arc;
use std::time::Duration;

use exspeed_common::Offset;
use exspeed_streams::record::Record;
use exspeed_streams::StorageError;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Instant;

use crate::file::partition::Partition;

/// Whether the appender should call `sync_data` after each batch flush.
///
/// `Sync` — group commit + fsync per batch (default, strongest durability).
/// `Async` — write without fsync; the `WalSyncer` task handles periodic fsyncs.
#[derive(Debug, Clone, Copy)]
pub enum AppenderMode {
    Sync,
    Async,
}

/// Tunables for the appender's batching behavior. Defaults match the spec
/// (Section 4): 500µs window, 256 records, 1 MiB.
#[derive(Debug, Clone, Copy)]
pub struct AppenderConfig {
    pub flush_window: Duration,
    pub flush_threshold_records: usize,
    pub flush_threshold_bytes: usize,
}

impl Default for AppenderConfig {
    fn default() -> Self {
        Self {
            flush_window: Duration::from_micros(500),
            flush_threshold_records: 256,
            flush_threshold_bytes: 1024 * 1024,
        }
    }
}

/// One append request, sent from FileStorage::append to the writer task.
pub struct AppendRequest {
    pub record: Record,
    pub respond_to: oneshot::Sender<Result<(Offset, u64), StorageError>>,
}

/// Handle to a per-partition writer task: holds its mpsc sender.
#[derive(Clone)]
pub struct AppenderHandle {
    tx: mpsc::Sender<AppendRequest>,
}

impl AppenderHandle {
    pub async fn append(&self, record: Record) -> Result<(Offset, u64), StorageError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(AppendRequest {
                record,
                respond_to: resp_tx,
            })
            .await
            .map_err(|_| StorageError::ChannelClosed)?;
        resp_rx.await.map_err(|_| StorageError::ChannelClosed)?
    }
}

/// Spawn a writer task that owns shared `partition`. Returns a handle the
/// caller uses to send append requests. The task runs until the handle (and
/// all clones) are dropped — which closes the channel.
///
/// `mode` controls whether each batch flush calls `sync_data` on the WAL.
/// In `Async` mode the `WalSyncer` task handles periodic fsyncs separately.
pub fn spawn(
    partition: Arc<Mutex<Partition>>,
    config: AppenderConfig,
    mode: AppenderMode,
) -> AppenderHandle {
    let (tx, mut rx) = mpsc::channel::<AppendRequest>(1024);

    tokio::spawn(async move {
        let mut batch: Vec<AppendRequest> = Vec::with_capacity(config.flush_threshold_records);
        let mut batch_bytes: usize = 0;
        let mut batch_started: Option<Instant> = None;

        loop {
            // Compute time to wait for the next request.
            let wait = match batch_started {
                None => Duration::from_secs(60), // long wait — no batch in flight
                Some(start) => config.flush_window.saturating_sub(start.elapsed()),
            };

            tokio::select! {
                req = rx.recv() => {
                    match req {
                        None => {
                            // Channel closed and empty; flush whatever we have, then exit.
                            if !batch.is_empty() {
                                flush_batch(&partition, &mut batch, mode).await;
                            }
                            return;
                        }
                        Some(req) => {
                            let bytes = req.record.value.len();
                            batch.push(req);
                            batch_bytes += bytes;
                            if batch_started.is_none() {
                                batch_started = Some(Instant::now());
                            }
                            if batch.len() >= config.flush_threshold_records
                                || batch_bytes >= config.flush_threshold_bytes
                            {
                                flush_batch(&partition, &mut batch, mode).await;
                                batch_bytes = 0;
                                batch_started = None;
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(wait), if batch_started.is_some() => {
                    flush_batch(&partition, &mut batch, mode).await;
                    batch_bytes = 0;
                    batch_started = None;
                }
            }
        }
    });

    AppenderHandle { tx }
}

async fn flush_batch(
    partition: &Arc<Mutex<Partition>>,
    batch: &mut Vec<AppendRequest>,
    mode: AppenderMode,
) {
    if batch.is_empty() {
        return;
    }
    // Collect records for the batch call.
    let records: Vec<Record> = batch.iter().map(|r| r.record.clone()).collect();

    // Determine whether to sync the WAL now or defer to WalSyncer.
    let sync_now = matches!(mode, AppenderMode::Sync);

    // Lock the partition and call append_batch. This is sync I/O under the
    // tokio Mutex — acceptable for the short duration of a WAL write (and
    // fsync in Sync mode).
    let result = {
        let mut p = partition.lock().await;
        p.append_batch(&records, sync_now)
    };

    match result {
        Ok(assignments) => {
            for (req, (offset, ts)) in batch.drain(..).zip(assignments) {
                let _ = req.respond_to.send(Ok((offset, ts)));
            }
        }
        Err(e) => {
            // Broadcast the io::Error message to all waiters in this batch.
            let err_msg = e.to_string();
            for req in batch.drain(..) {
                let _ = req
                    .respond_to
                    .send(Err(StorageError::Io(std::io::Error::other(err_msg.clone()))));
            }
        }
    }
}
