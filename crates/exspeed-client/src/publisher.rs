//! A pipelined, coalescing publisher.
//!
//! Concurrent [`Publisher::publish`] calls are gathered for up to
//! `batch_window` (or `max_batch_records`) and sent as `PublishBatch`
//! requests, one per stream, in arrival order. Many batches can be in flight
//! at once, bounded by `max_in_flight` records; each caller gets its own
//! record's offset back. Order on the wire, and therefore in the stream,
//! matches the order in which `publish` calls were enqueued.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::{Client, Error, PublishAck, PublishRecord, Request, Response, Result};

struct Queued {
    stream: String,
    record: PublishRecord,
    reply: oneshot::Sender<Result<PublishAck>>,
    _permit: OwnedSemaphorePermit,
}

pub struct PublisherBuilder {
    client: Client,
    batch_window: Duration,
    max_batch_records: usize,
    max_in_flight: usize,
}

impl PublisherBuilder {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            batch_window: Duration::from_micros(100),
            max_batch_records: 512,
            max_in_flight: 4096,
        }
    }

    /// How long to wait for more records before sending a batch. Zero sends
    /// every record on its own.
    pub fn batch_window(mut self, d: Duration) -> Self {
        self.batch_window = d;
        self
    }

    pub fn max_batch_records(mut self, n: usize) -> Self {
        self.max_batch_records = n.max(1);
        self
    }

    /// Records accepted but not yet acknowledged; `publish` waits when full.
    pub fn max_in_flight(mut self, n: usize) -> Self {
        self.max_in_flight = n.max(1);
        self
    }

    pub fn build(self) -> Publisher {
        let (tx, rx) = mpsc::unbounded_channel();
        let sem = Arc::new(Semaphore::new(self.max_in_flight));
        tokio::spawn(flusher(
            self.client.clone(),
            rx,
            self.batch_window,
            self.max_batch_records,
        ));
        Publisher {
            tx,
            sem,
            capacity: self.max_in_flight as u32,
        }
    }
}

/// See the [module docs](self). Cheap to clone; all clones share one queue.
#[derive(Clone)]
pub struct Publisher {
    tx: mpsc::UnboundedSender<Queued>,
    sem: Arc<Semaphore>,
    capacity: u32,
}

impl Publisher {
    pub async fn publish(&self, stream: &str, record: PublishRecord) -> Result<PublishAck> {
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Queued {
                stream: stream.to_string(),
                record,
                reply,
                _permit: permit,
            })
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Wait until every accepted record has been acknowledged.
    pub async fn flush(&self) -> Result<()> {
        let all = self
            .sem
            .acquire_many(self.capacity)
            .await
            .map_err(|_| Error::Closed)?;
        drop(all);
        Ok(())
    }

    /// Flush, then stop accepting records.
    pub async fn close(self) -> Result<()> {
        self.flush().await?;
        self.sem.close();
        Ok(())
    }
}

async fn flusher(
    client: Client,
    mut rx: mpsc::UnboundedReceiver<Queued>,
    window: Duration,
    max_batch: usize,
) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        if !window.is_zero() {
            let deadline = tokio::time::Instant::now() + window;
            while batch.len() < max_batch {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(q)) => batch.push(q),
                    Ok(None) | Err(_) => break,
                }
            }
        }
        // Drain anything already queued, without waiting.
        while batch.len() < max_batch {
            match rx.try_recv() {
                Ok(q) => batch.push(q),
                Err(_) => break,
            }
        }
        // Split into runs of the same stream, keeping arrival order.
        let mut runs: Vec<Vec<Queued>> = Vec::new();
        for q in batch {
            match runs.last_mut() {
                Some(run) if run[0].stream == q.stream => run.push(q),
                _ => runs.push(vec![q]),
            }
        }
        for run in runs {
            send_run(&client, run).await;
        }
    }
}

/// Send one run (same stream) and spawn a task to deliver the results. The
/// frame is enqueued before returning so wire order matches arrival order.
async fn send_run(client: &Client, run: Vec<Queued>) {
    let stream = run[0].stream.clone();
    let (records, waiters): (Vec<PublishRecord>, Vec<_>) = run
        .into_iter()
        .map(|q| (q.record, (q.reply, q._permit)))
        .unzip();
    let single = records.len() == 1;
    let req = if single {
        Request::Publish {
            stream,
            record: records.into_iter().next().unwrap(),
        }
    } else {
        Request::PublishBatch { stream, records }
    };
    let sent = client.send_request(req).await;
    let client = client.clone();
    tokio::spawn(async move {
        let result = match sent {
            Ok((corr, rx)) => {
                let timeout = client.inner.opts.request_timeout;
                client.await_response(corr, rx, timeout).await
            }
            Err(e) => Err(e),
        };
        match result {
            Ok(Response::PublishOk { offset, duplicate }) if single => {
                let (reply, _permit) = waiters.into_iter().next().unwrap();
                let _ = reply.send(Ok(PublishAck { offset, duplicate }));
            }
            Ok(Response::PublishBatchOk { results }) if results.len() == waiters.len() => {
                for ((reply, _permit), (offset, duplicate)) in waiters.into_iter().zip(results) {
                    let _ = reply.send(Ok(PublishAck { offset, duplicate }));
                }
            }
            Ok(other) => {
                let e = Error::Unexpected(format!("{:?}", other.opcode()));
                for (reply, _permit) in waiters {
                    let _ = reply.send(Err(e.duplicate()));
                }
            }
            Err(e) => {
                for (reply, _permit) in waiters {
                    let _ = reply.send(Err(e.duplicate()));
                }
            }
        }
    });
}
