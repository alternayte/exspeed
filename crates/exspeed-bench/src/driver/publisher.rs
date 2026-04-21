//! Pipelined Publisher. One Publisher owns one TCP connection. The
//! background reader task demultiplexes server responses to the right
//! caller via correlation IDs. Multiple `publish()` calls can be in
//! flight concurrently.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_common::Offset;
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::publish::PublishRequest;
use exspeed_protocol::opcodes::OpCode;

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

struct Pending {
    respond_to: oneshot::Sender<Result<Offset, PublisherError>>,
}

pub struct PublisherBuilder {
    addr: String,
    max_in_flight: usize,
}

impl PublisherBuilder {
    pub fn new(addr: &str) -> Self {
        Self { addr: addr.to_owned(), max_in_flight: 1024 }
    }
    pub fn max_in_flight(mut self, n: usize) -> Self {
        self.max_in_flight = n.max(1);
        self
    }
    pub async fn build(self) -> Result<Publisher> {
        Publisher::with_capacity(&self.addr, self.max_in_flight).await
    }
}

#[derive(Clone)]
pub struct Publisher {
    inner: Arc<PublisherInner>,
}

struct PublisherInner {
    write_tx: mpsc::Sender<WriteCmd>,
    pending: Mutex<HashMap<u32, Pending>>,
    permits: Arc<Semaphore>,
    next_corr: Mutex<u32>,
}

enum WriteCmd {
    Frame(Frame),
    Close,
}

impl Publisher {
    pub async fn new(addr: &str) -> Result<Self> {
        Self::with_capacity(addr, 1024).await
    }

    pub async fn with_capacity(addr: &str, max_in_flight: usize) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true).ok();
        let (reader, writer) = stream.into_split();
        let mut reader = FramedRead::new(reader, ExspeedCodec::new());
        let mut writer = FramedWrite::new(writer, ExspeedCodec::new());

        // CONNECT handshake (correlation_id = 1).
        let req = ConnectRequest {
            client_id: "exspeed-bench-publisher".into(),
            auth_type: AuthType::None,
            auth_payload: bytes::Bytes::new(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        writer.send(Frame::new(OpCode::Connect, 1, buf.freeze())).await?;
        let resp = reader.next().await.ok_or_else(|| anyhow!("connect closed"))??;
        if resp.opcode != OpCode::ConnectOk {
            return Err(anyhow!("expected ConnectOk, got {:?}", resp.opcode));
        }

        let inner = Arc::new(PublisherInner {
            write_tx: spawn_writer(writer),
            pending: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(max_in_flight)),
            next_corr: Mutex::new(2), // 0 = push, 1 = CONNECT
        });

        spawn_reader(reader, inner.clone());

        Ok(Self { inner })
    }

    pub async fn publish(&self, req: PublishRequest) -> Result<Offset, PublisherError> {
        let permit = self.inner.permits.clone().acquire_owned().await
            .map_err(|_| PublisherError::Closed)?;
        let corr = {
            let mut g = self.inner.next_corr.lock().await;
            let c = *g;
            // Skip 0 (push) and 1 (CONNECT) on wrap.
            *g = match g.wrapping_add(1) {
                0 | 1 => 2,
                n => n,
            };
            c
        };
        let (tx, rx) = oneshot::channel();
        {
            let mut p = self.inner.pending.lock().await;
            p.insert(corr, Pending { respond_to: tx });
        }
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        if self.inner.write_tx
            .send(WriteCmd::Frame(Frame::new(OpCode::Publish, corr, buf.freeze())))
            .await
            .is_err()
        {
            let mut p = self.inner.pending.lock().await;
            p.remove(&corr);
            drop(permit);
            return Err(PublisherError::Closed);
        }
        let result = rx.await.map_err(|_| PublisherError::ConnectionLost { in_flight: 0 })?;
        drop(permit);
        result
    }

    pub async fn flush(&self) -> Result<(), PublisherError> {
        // Coarse: poll until pending map is empty.
        loop {
            let empty = self.inner.pending.lock().await.is_empty();
            if empty { break; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok(())
    }

    pub async fn close(self) -> Result<(), PublisherError> {
        self.flush().await?;
        let _ = self.inner.write_tx.send(WriteCmd::Close).await;
        Ok(())
    }
}

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

fn spawn_reader(mut reader: FramedRead<OwnedReadHalf, ExspeedCodec>, inner: Arc<PublisherInner>) {
    tokio::spawn(async move {
        use bytes::Buf;
        while let Some(frame_res) = reader.next().await {
            let frame = match frame_res {
                Ok(f) => f,
                Err(_) => break,
            };
            if frame.opcode == OpCode::PublishOk {
                // PublishOk payload: u64 LE offset (8 bytes) + u8 duplicate flag (1 byte).
                let mut p = frame.payload.clone();
                let offset = if p.remaining() >= 8 {
                    Offset(p.get_u64_le())
                } else {
                    Offset(0)
                };
                // Consume the duplicate byte if present (we don't surface it here).
                let _duplicate = if p.remaining() >= 1 { p.get_u8() != 0 } else { false };
                let mut map = inner.pending.lock().await;
                if let Some(pending) = map.remove(&frame.correlation_id) {
                    let _ = pending.respond_to.send(Ok(offset));
                }
            } else if frame.opcode == OpCode::Error {
                let mut p = frame.payload.clone();
                let code = if p.remaining() >= 2 { p.get_u16_le() } else { 0 };
                let msg_len = if p.remaining() >= 2 { p.get_u16_le() as usize } else { 0 };
                let msg = if p.remaining() >= msg_len {
                    String::from_utf8_lossy(&p.slice(..msg_len)).into_owned()
                } else {
                    String::new()
                };
                let mut map = inner.pending.lock().await;
                if let Some(pending) = map.remove(&frame.correlation_id) {
                    let _ = pending.respond_to.send(Err(PublisherError::ServerError {
                        code,
                        message: msg,
                    }));
                }
            }
        }
        // Connection dropped — fail all pending callers.
        let mut map = inner.pending.lock().await;
        let in_flight = map.len();
        for (_, p) in map.drain() {
            let _ = p.respond_to.send(Err(PublisherError::ConnectionLost { in_flight }));
        }
    });
}
