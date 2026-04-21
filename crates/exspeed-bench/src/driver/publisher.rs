//! Pipelined Publisher with transparent coalescing.
//!
//! One Publisher owns one TCP connection. A background flusher task coalesces
//! concurrent `publish()` calls into `PublishBatch` frames — at most one frame
//! per `batch_window` (default 100µs) or when `max_batch_records` is reached.
//!
//! The reader task demultiplexes `PublishBatchOk` frames back to the correct
//! per-record oneshot senders via correlation IDs. Multiple `publish()` calls
//! can be in flight concurrently, bounded by the semaphore.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, Semaphore};
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_common::Offset;
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::publish::PublishRequest;
use exspeed_protocol::messages::publish_batch::{
    BatchResult, PublishBatchOkResponse, PublishBatchRecord, PublishBatchRequest,
};
use exspeed_protocol::opcodes::OpCode;

// ─── Error ───────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    #[error("connection lost (in_flight: {in_flight})")]
    ConnectionLost { in_flight: usize },
    #[error("server error: code={code:#06x} message={message}")]
    ServerError { code: u16, message: String },
    #[error("publisher closed")]
    Closed,
    #[error("io: {0}")]
    Io(String),
}

// ─── Internal types ───────────────────────────────────────────────────────────

/// One entry queued inside the coalescing queue.
struct QueuedPublish {
    req: PublishRequest,
    respond_to: oneshot::Sender<Result<Offset, PublisherError>>,
}

/// A corr-ID slot that holds one oneshot per record in the batch.
struct PendingBatch {
    responders: Vec<oneshot::Sender<Result<Offset, PublisherError>>>,
}

enum WriteCmd {
    Frame(Frame),
    Close,
}

// ─── Builder ─────────────────────────────────────────────────────────────────

pub struct PublisherBuilder {
    addr: String,
    max_in_flight: usize,
    batch_window: std::time::Duration,
    max_batch_records: usize,
}

impl PublisherBuilder {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_owned(),
            max_in_flight: 1024,
            batch_window: std::time::Duration::from_micros(100),
            max_batch_records: 256,
        }
    }

    pub fn max_in_flight(mut self, n: usize) -> Self {
        self.max_in_flight = n.max(1);
        self
    }

    pub fn batch_window(mut self, d: std::time::Duration) -> Self {
        self.batch_window = d;
        self
    }

    pub fn max_batch_records(mut self, n: usize) -> Self {
        self.max_batch_records = n.max(1);
        self
    }

    pub async fn build(self) -> Result<Publisher> {
        Publisher::with_config(
            &self.addr,
            self.max_in_flight,
            self.batch_window,
            self.max_batch_records,
        )
        .await
    }
}

// ─── Publisher ───────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Publisher {
    inner: Arc<PublisherInner>,
}

struct PublisherInner {
    write_tx: mpsc::Sender<WriteCmd>,
    pending: Mutex<HashMap<u32, PendingBatch>>,
    permits: Arc<Semaphore>,
    next_corr: Mutex<u32>,
    batch_window: std::time::Duration,
    max_batch_records: usize,
    coalesce_queue: Mutex<Vec<QueuedPublish>>,
    flush_notify: Notify,
}

impl Publisher {
    /// Convenience constructor — default settings (max_in_flight=1024, window=100µs, max_batch=256).
    pub async fn new(addr: &str) -> Result<Self> {
        Self::with_config(
            addr,
            1024,
            std::time::Duration::from_micros(100),
            256,
        )
        .await
    }

    pub async fn with_config(
        addr: &str,
        max_in_flight: usize,
        batch_window: std::time::Duration,
        max_batch_records: usize,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true).ok();
        let (reader, writer) = stream.into_split();
        let mut framed_reader = FramedRead::new(reader, ExspeedCodec::new());
        let mut framed_writer = FramedWrite::new(writer, ExspeedCodec::new());

        // CONNECT handshake (correlation_id = 1).
        let req = ConnectRequest {
            client_id: "exspeed-bench-publisher".into(),
            auth_type: AuthType::None,
            auth_payload: bytes::Bytes::new(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        framed_writer
            .send(Frame::new(OpCode::Connect, 1, buf.freeze()))
            .await?;
        let resp = framed_reader
            .next()
            .await
            .ok_or_else(|| anyhow!("connect closed"))??;
        if resp.opcode != OpCode::ConnectOk {
            return Err(anyhow!("expected ConnectOk, got {:?}", resp.opcode));
        }

        let inner = Arc::new(PublisherInner {
            write_tx: spawn_writer(framed_writer),
            pending: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(max_in_flight)),
            next_corr: Mutex::new(2), // 0 = push, 1 = CONNECT
            batch_window,
            max_batch_records,
            coalesce_queue: Mutex::new(Vec::new()),
            flush_notify: Notify::new(),
        });

        spawn_reader(framed_reader, inner.clone());
        spawn_flusher(inner.clone());

        Ok(Self { inner })
    }

    /// Enqueue a single record into the coalescing queue. The record will be
    /// sent in the next batch flush (timer-driven or size-driven). Awaits the
    /// per-record result from the server.
    ///
    /// When `batch_window` is zero, notify the flusher immediately so the
    /// record is flushed without delay (no timer accumulation).
    pub async fn publish(&self, req: PublishRequest) -> Result<Offset, PublisherError> {
        let permit = self
            .inner
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PublisherError::Closed)?;
        let (tx, rx) = oneshot::channel();
        {
            let mut q = self.inner.coalesce_queue.lock().await;
            q.push(QueuedPublish { req, respond_to: tx });
            let notify = q.len() >= self.inner.max_batch_records
                || self.inner.batch_window.is_zero();
            if notify {
                self.inner.flush_notify.notify_one();
            }
        }
        let result = rx
            .await
            .map_err(|_| PublisherError::ConnectionLost { in_flight: 0 })?;
        drop(permit);
        result
    }

    /// Explicit batch publish — bypasses the coalescing queue and sends a
    /// single `PublishBatch` frame immediately. All records must target the
    /// same stream (CDC-style flush).
    pub async fn publish_batch(
        &self,
        reqs: Vec<PublishRequest>,
    ) -> Vec<Result<Offset, PublisherError>> {
        if reqs.is_empty() {
            return vec![];
        }
        let stream = reqs[0].stream.clone();
        if !reqs.iter().all(|r| r.stream == stream) {
            return reqs
                .into_iter()
                .map(|_| {
                    Err(PublisherError::Io(
                        "publish_batch requires all records to target the same stream".into(),
                    ))
                })
                .collect();
        }

        let records: Vec<PublishBatchRecord> = reqs
            .iter()
            .map(|r| PublishBatchRecord {
                subject: r.subject.clone(),
                key: r.key.clone(),
                msg_id: r.msg_id.clone(),
                value: r.value.clone(),
                headers: r.headers.clone(),
            })
            .collect();

        let corr = next_corr_id(&self.inner).await;

        let mut receivers: Vec<oneshot::Receiver<Result<Offset, PublisherError>>> =
            Vec::with_capacity(reqs.len());
        let mut senders: Vec<oneshot::Sender<Result<Offset, PublisherError>>> =
            Vec::with_capacity(reqs.len());
        for _ in 0..reqs.len() {
            let (tx, rx) = oneshot::channel();
            senders.push(tx);
            receivers.push(rx);
        }

        {
            let mut p = self.inner.pending.lock().await;
            p.insert(corr, PendingBatch { responders: senders });
        }

        let batch_req = PublishBatchRequest { stream, records };
        let mut buf = BytesMut::new();
        batch_req.encode(&mut buf);
        if self
            .inner
            .write_tx
            .send(WriteCmd::Frame(Frame::new(
                OpCode::PublishBatch,
                corr,
                buf.freeze(),
            )))
            .await
            .is_err()
        {
            // Writer gone — fail all.
            let mut p = self.inner.pending.lock().await;
            if let Some(batch) = p.remove(&corr) {
                return batch
                    .responders
                    .into_iter()
                    .map(|_| Err(PublisherError::Closed))
                    .collect();
            }
        }

        let mut results = Vec::with_capacity(receivers.len());
        for rx in receivers {
            results.push(rx.await.unwrap_or(Err(PublisherError::Closed)));
        }
        results
    }

    /// Wait until the coalescing queue is empty AND the pending-ack map is
    /// empty (i.e., all in-flight records have been acknowledged).
    pub async fn flush(&self) -> Result<(), PublisherError> {
        loop {
            let queue_empty = self.inner.coalesce_queue.lock().await.is_empty();
            let pending_empty = self.inner.pending.lock().await.is_empty();
            if queue_empty && pending_empty {
                break;
            }
            // Poke the flusher so it drains the queue if anything is queued.
            self.inner.flush_notify.notify_one();
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        Ok(())
    }

    /// Flush all pending records and then close the TCP connection.
    pub async fn close(self) -> Result<(), PublisherError> {
        self.flush().await?;
        let _ = self.inner.write_tx.send(WriteCmd::Close).await;
        Ok(())
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

async fn next_corr_id(inner: &PublisherInner) -> u32 {
    let mut g = inner.next_corr.lock().await;
    let c = *g;
    *g = match g.wrapping_add(1) {
        0 | 1 => 2,
        n => n,
    };
    c
}

// ─── Background tasks ─────────────────────────────────────────────────────────

fn spawn_writer(mut writer: FramedWrite<OwnedWriteHalf, ExspeedCodec>) -> mpsc::Sender<WriteCmd> {
    let (tx, mut rx) = mpsc::channel::<WriteCmd>(1024);
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                WriteCmd::Frame(f) => {
                    if writer.send(f).await.is_err() {
                        break;
                    }
                }
                WriteCmd::Close => break,
            }
        }
    });
    tx
}

fn spawn_flusher(inner: Arc<PublisherInner>) {
    tokio::spawn(async move {
        loop {
            // When batch_window is zero we rely solely on notify_one() from
            // publish() to wake the flusher; use a long fallback sleep so we
            // don't busy-spin. When batch_window > 0 the timer drives batching.
            let sleep_dur = if inner.batch_window.is_zero() {
                std::time::Duration::from_secs(3600)
            } else {
                inner.batch_window
            };

            tokio::select! {
                _ = inner.flush_notify.notified() => {}
                _ = tokio::time::sleep(sleep_dur) => {}
            }

            let drained: Vec<QueuedPublish> = {
                let mut q = inner.coalesce_queue.lock().await;
                if q.is_empty() {
                    continue;
                }
                std::mem::take(&mut *q)
            };

            // Group by stream so each stream gets exactly one frame.
            let mut by_stream: HashMap<String, Vec<QueuedPublish>> = HashMap::new();
            for qp in drained {
                by_stream
                    .entry(qp.req.stream.clone())
                    .or_default()
                    .push(qp);
            }

            for (stream, group) in by_stream {
                let records: Vec<PublishBatchRecord> = group
                    .iter()
                    .map(|qp| PublishBatchRecord {
                        subject: qp.req.subject.clone(),
                        key: qp.req.key.clone(),
                        msg_id: qp.req.msg_id.clone(),
                        value: qp.req.value.clone(),
                        headers: qp.req.headers.clone(),
                    })
                    .collect();

                let corr = next_corr_id(&inner).await;

                let batch_req = PublishBatchRequest { stream, records };
                let mut buf = BytesMut::new();
                batch_req.encode(&mut buf);

                let responders: Vec<oneshot::Sender<Result<Offset, PublisherError>>> =
                    group.into_iter().map(|qp| qp.respond_to).collect();
                {
                    let mut p = inner.pending.lock().await;
                    p.insert(corr, PendingBatch { responders });
                }

                if inner
                    .write_tx
                    .send(WriteCmd::Frame(Frame::new(
                        OpCode::PublishBatch,
                        corr,
                        buf.freeze(),
                    )))
                    .await
                    .is_err()
                {
                    // Writer gone — clean up and stop.
                    let mut p = inner.pending.lock().await;
                    if let Some(batch) = p.remove(&corr) {
                        for tx in batch.responders {
                            let _ = tx.send(Err(PublisherError::Closed));
                        }
                    }
                    return;
                }
            }
        }
    });
}

fn spawn_reader(mut reader: FramedRead<OwnedReadHalf, ExspeedCodec>, inner: Arc<PublisherInner>) {
    tokio::spawn(async move {
        use bytes::Buf;
        while let Some(frame_res) = reader.next().await {
            let frame = match frame_res {
                Ok(f) => f,
                Err(_) => break,
            };

            if frame.opcode == OpCode::PublishBatchOk {
                let resp = match PublishBatchOkResponse::decode(frame.payload) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let mut map = inner.pending.lock().await;
                if let Some(batch) = map.remove(&frame.correlation_id) {
                    for (tx, result) in batch
                        .responders
                        .into_iter()
                        .zip(resp.results.into_iter())
                    {
                        let _ = tx.send(match result {
                            BatchResult::Written { offset } => Ok(Offset(offset)),
                            BatchResult::Duplicate { offset, .. } => Ok(Offset(offset)),
                            BatchResult::Error { code, message } => {
                                Err(PublisherError::ServerError { code, message })
                            }
                        });
                    }
                }
            } else if frame.opcode == OpCode::Error {
                let mut p = frame.payload.clone();
                let code = if p.remaining() >= 2 { p.get_u16_le() } else { 0 };
                let ml = if p.remaining() >= 2 {
                    p.get_u16_le() as usize
                } else {
                    0
                };
                let msg = if p.remaining() >= ml {
                    String::from_utf8_lossy(&p.slice(..ml)).into_owned()
                } else {
                    String::new()
                };
                let mut map = inner.pending.lock().await;
                if let Some(batch) = map.remove(&frame.correlation_id) {
                    for tx in batch.responders {
                        let _ = tx.send(Err(PublisherError::ServerError {
                            code,
                            message: msg.clone(),
                        }));
                    }
                }
            }
            // Push-delivered records (corr=0) and other server opcodes are
            // silently ignored on a publish-only connection.
        }

        // Connection dropped — fail all pending callers.
        let mut map = inner.pending.lock().await;
        let in_flight: usize = map.values().map(|b| b.responders.len()).sum();
        for (_, batch) in map.drain() {
            for tx in batch.responders {
                let _ = tx.send(Err(PublisherError::ConnectionLost { in_flight }));
            }
        }
    });
}
