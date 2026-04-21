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

/// One append request, sent from FileStorage::append / append_batch to the
/// writer task.
pub enum AppendRequest {
    Single {
        record: Record,
        respond_to: oneshot::Sender<Result<(Offset, u64), StorageError>>,
    },
    Batch {
        records: Vec<Record>,
        respond_to: oneshot::Sender<Result<Vec<(Offset, u64)>, StorageError>>,
    },
}

/// Handle to a per-partition writer task: holds its mpsc sender.
#[derive(Clone)]
pub struct AppenderHandle {
    tx: mpsc::Sender<AppendRequest>,
}

impl AppenderHandle {
    pub async fn append(&self, record: Record) -> Result<(Offset, u64), StorageError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(AppendRequest::Single { record, respond_to: tx })
            .await
            .map_err(|_| StorageError::ChannelClosed)?;
        rx.await.map_err(|_| StorageError::ChannelClosed)?
    }

    pub async fn append_batch(
        &self,
        records: Vec<Record>,
    ) -> Result<Vec<(Offset, u64)>, StorageError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(AppendRequest::Batch { records, respond_to: tx })
            .await
            .map_err(|_| StorageError::ChannelClosed)?;
        rx.await.map_err(|_| StorageError::ChannelClosed)?
    }
}

/// Spawn a writer task that owns shared `partition`. Returns a handle the
/// caller uses to send append requests. The task runs until the handle (and
/// all clones) are dropped — which closes the channel.
///
/// `mode` controls whether each batch flush calls `sync_data` on the active
/// segment. In `Async` mode the `WalSyncer` task handles periodic fsyncs
/// separately.
pub fn spawn(
    partition: Arc<Mutex<Partition>>,
    config: AppenderConfig,
    mode: AppenderMode,
) -> AppenderHandle {
    let (tx, mut rx) = mpsc::channel::<AppendRequest>(1024);

    tokio::spawn(async move {
        let mut pending_singles: Vec<PendingSingle> = Vec::with_capacity(config.flush_threshold_records);
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
                            if !pending_singles.is_empty() {
                                flush_singles(&partition, &mut pending_singles, mode).await;
                            }
                            return;
                        }
                        Some(AppendRequest::Single { record, respond_to }) => {
                            let bytes = record.value.len();
                            pending_singles.push(PendingSingle { record, respond_to });
                            batch_bytes += bytes;
                            if batch_started.is_none() {
                                batch_started = Some(Instant::now());
                            }
                            if pending_singles.len() >= config.flush_threshold_records
                                || batch_bytes >= config.flush_threshold_bytes
                            {
                                flush_singles(&partition, &mut pending_singles, mode).await;
                                batch_bytes = 0;
                                batch_started = None;
                            }
                        }
                        Some(AppendRequest::Batch { records, respond_to }) => {
                            // Flush in-flight singles first to preserve FIFO ordering
                            // across variants.
                            if !pending_singles.is_empty() {
                                flush_singles(&partition, &mut pending_singles, mode).await;
                                batch_bytes = 0;
                                batch_started = None;
                            }
                            flush_explicit_batch(&partition, records, respond_to, mode).await;
                        }
                    }
                }
                _ = tokio::time::sleep(wait), if batch_started.is_some() => {
                    flush_singles(&partition, &mut pending_singles, mode).await;
                    batch_bytes = 0;
                    batch_started = None;
                }
            }
        }
    });

    AppenderHandle { tx }
}

/// An accumulated single-record request waiting to be group-committed.
struct PendingSingle {
    record: Record,
    respond_to: oneshot::Sender<Result<(Offset, u64), StorageError>>,
}

/// Flush the accumulated singles as one group commit, dispatching individual
/// results back to each caller.
async fn flush_singles(
    partition: &Arc<Mutex<Partition>>,
    batch: &mut Vec<PendingSingle>,
    mode: AppenderMode,
) {
    if batch.is_empty() {
        return;
    }

    let mut records: Vec<Record> = Vec::with_capacity(batch.len());
    let mut responders: Vec<oneshot::Sender<Result<(Offset, u64), StorageError>>> =
        Vec::with_capacity(batch.len());
    for p in batch.drain(..) {
        records.push(p.record);
        responders.push(p.respond_to);
    }

    let sync_now = matches!(mode, AppenderMode::Sync);
    let result = {
        let mut p = partition.lock().await;
        p.append_batch(&records, sync_now)
    };

    match result {
        Ok(assignments) => {
            for (tx, (offset, ts)) in responders.into_iter().zip(assignments.into_iter()) {
                let _ = tx.send(Ok((offset, ts)));
            }
        }
        Err(e) => {
            let err_msg = e.to_string();
            for tx in responders {
                let _ = tx.send(Err(StorageError::Io(std::io::Error::other(err_msg.clone()))));
            }
        }
    }
}

/// Flush an explicit batch request (from `AppenderHandle::append_batch`),
/// returning a `Vec` of `(Offset, u64)` to the caller.
async fn flush_explicit_batch(
    partition: &Arc<Mutex<Partition>>,
    records: Vec<Record>,
    respond_to: oneshot::Sender<Result<Vec<(Offset, u64)>, StorageError>>,
    mode: AppenderMode,
) {
    let sync_now = matches!(mode, AppenderMode::Sync);
    let result = {
        let mut p = partition.lock().await;
        p.append_batch(&records, sync_now)
    };
    let _ = respond_to.send(
        result.map_err(|e| StorageError::Io(std::io::Error::other(format!("{e}")))),
    );
}
