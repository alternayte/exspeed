//! Async Rust client for Exspeed (client protocol v2).
//!
//! One [`Client`] is one TCP (or TLS) connection. Requests are multiplexed
//! by correlation id, so a `Client` can be cloned and used from many tasks
//! at once; a long pull or long-poll read never blocks other requests.
//!
//! ```no_run
//! # async fn demo() -> Result<(), exspeed_client::Error> {
//! use exspeed_client::{Client, ConnectOptions, ConsumerSpec, PublishRecord, StreamSpec};
//!
//! let client = Client::connect("127.0.0.1:5933", ConnectOptions::default()).await?;
//! client.create_stream(StreamSpec::named("orders")).await?;
//! client.publish("orders", PublishRecord::new("orders.placed", r#"{"id":1}"#)).await?;
//!
//! client.create_consumer(ConsumerSpec::new("billing", "orders")).await?;
//! let mut sub = client.subscribe("billing", 256).await?;
//! while let Some(msg) = sub.next().await {
//!     println!("{} {:?}", msg.record.offset, msg.record.value);
//!     msg.ack().await?;
//! }
//! # Ok(()) }
//! ```
//!
//! Not (yet) included: automatic reconnection and leader redirects. A
//! closed connection fails pending requests with [`Error::Closed`]; create
//! a new client to continue.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;

pub use exspeed_protocol::client::{
    code, AckPolicy, ConsumerSpec, DeliverPolicy, PublishRecord, Request, Response, SeekTo,
    StreamSpec, WireRecord,
};

/// Default time to wait for a response (on top of any server-side wait the
/// request itself asks for).
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Interval of keepalive pings; the server closes connections idle for
/// 120 s.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("server error {code}: {message}")]
    Server {
        code: u16,
        message: String,
        detail: Option<serde_json::Value>,
    },
    #[error("connection closed")]
    Closed,
    #[error("request timed out")]
    Timeout,
    #[error("unexpected response: {0}")]
    Unexpected(String),
}

impl Error {
    /// The server's error code, if this is a server error.
    pub fn code(&self) -> Option<u16> {
        match self {
            Error::Server { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// `detail.leader` from a 503 "not the leader" error.
    pub fn leader_hint(&self) -> Option<String> {
        match self {
            Error::Server {
                detail: Some(d), ..
            } => d.get("leader").and_then(|v| v.as_str()).map(str::to_string),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct ConnectOptions {
    pub client_id: String,
    /// Bearer token; `None` when the server runs without auth.
    pub token: Option<String>,
    pub request_timeout: Duration,
    /// `None` disables keepalive pings.
    pub keepalive: Option<Duration>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            client_id: "exspeed-rust".to_string(),
            token: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            keepalive: Some(KEEPALIVE_INTERVAL),
        }
    }
}

impl ConnectOptions {
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }
}

/// Information from the server's handshake reply.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub server_version: String,
    pub node_id: String,
    /// Leader address when the connected node is not the leader.
    pub leader: Option<String>,
}

/// Result of a single publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishAck {
    pub offset: u64,
    /// True when `msg_id` matched an earlier publish; nothing was written.
    pub duplicate: bool,
}

/// Result of a stateless [`Client::read`].
#[derive(Debug, Clone)]
pub struct ReadResult {
    pub records: Vec<WireRecord>,
    /// Pass as `from` to continue.
    pub next_offset: u64,
    pub high_watermark: u64,
}

enum SubMsg {
    Records(Vec<WireRecord>),
    Ended { code: u16, message: String },
}

#[derive(Default)]
struct Routes {
    pending: HashMap<u32, oneshot::Sender<Response>>,
    subs: HashMap<u32, mpsc::UnboundedSender<SubMsg>>,
    /// Receivers created by the reader when it sees `SubscribeOk`, waiting
    /// for the subscribing call to pick them up. Creating them in the reader
    /// guarantees no `Deliver` that follows `SubscribeOk` is lost.
    new_subs: HashMap<u32, mpsc::UnboundedReceiver<SubMsg>>,
    closed: bool,
}

struct Inner {
    out: mpsc::Sender<Frame>,
    routes: Mutex<Routes>,
    next_corr: AtomicU32,
    opts: ConnectOptions,
    info: ServerInfo,
    shutdown: tokio_util::sync::CancellationToken,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// A connection to an Exspeed server. Cheap to clone.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    /// Connect over plain TCP.
    pub async fn connect(addr: &str, opts: ConnectOptions) -> Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        socket.set_nodelay(true)?;
        Self::connect_with(socket, opts).await
    }

    /// Connect over TLS. `server_name` is checked against the certificate.
    pub async fn connect_tls(
        addr: &str,
        server_name: &str,
        config: Arc<tokio_rustls::rustls::ClientConfig>,
        opts: ConnectOptions,
    ) -> Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        socket.set_nodelay(true)?;
        let name = tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|e| Error::Protocol(format!("invalid server name: {e}")))?;
        let tls = tokio_rustls::TlsConnector::from(config)
            .connect(name, socket)
            .await?;
        Self::connect_with(tls, opts).await
    }

    /// Run the handshake over an already-established stream.
    pub async fn connect_with<S>(stream: S, opts: ConnectOptions) -> Result<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (r, w) = tokio::io::split(stream);
        let mut reader = FramedRead::new(r, ExspeedCodec::new());
        let mut writer = FramedWrite::new(w, ExspeedCodec::new());

        writer
            .send(
                Request::Connect {
                    client_id: opts.client_id.clone(),
                    token: opts.token.clone(),
                }
                .into_frame(1),
            )
            .await
            .map_err(proto)?;
        let frame = tokio::time::timeout(opts.request_timeout, reader.next())
            .await
            .map_err(|_| Error::Timeout)?
            .ok_or(Error::Closed)?
            .map_err(proto)?;
        let info = match Response::from_frame(&frame).map_err(proto)? {
            Response::ConnectOk {
                server_version,
                node_id,
                leader,
            } => ServerInfo {
                server_version,
                node_id,
                leader,
            },
            other => return Err(into_error(other)),
        };

        let (out_tx, mut out_rx) = mpsc::channel::<Frame>(1024);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let inner = Arc::new(Inner {
            out: out_tx,
            routes: Mutex::new(Routes::default()),
            next_corr: AtomicU32::new(2),
            opts,
            info,
            shutdown: shutdown.clone(),
        });

        // Writer task.
        let wshutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = wshutdown.cancelled() => break,
                    f = out_rx.recv() => match f {
                        Some(f) => if writer.send(f).await.is_err() { break },
                        None => break,
                    },
                }
            }
            let _ = writer.close().await;
        });

        // Reader task. Holds only a weak reference so dropping every
        // `Client` closes the connection.
        let weak = Arc::downgrade(&inner);
        let rshutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                let frame = tokio::select! {
                    _ = rshutdown.cancelled() => break,
                    f = reader.next() => match f {
                        Some(Ok(f)) => f,
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "exspeed connection: bad frame");
                            break;
                        }
                        None => break,
                    },
                };
                let Some(inner) = weak.upgrade() else { break };
                inner.route(frame);
            }
            rshutdown.cancel();
            if let Some(inner) = weak.upgrade() {
                inner.close_routes();
            }
        });

        // Keepalive.
        if let Some(every) = inner.opts.keepalive {
            let weak = Arc::downgrade(&inner);
            let kshutdown = shutdown.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(every);
                tick.tick().await;
                loop {
                    tokio::select! {
                        _ = kshutdown.cancelled() => break,
                        _ = tick.tick() => {}
                    }
                    let Some(inner) = weak.upgrade() else { break };
                    let client = Client { inner };
                    let _ = client.ping().await;
                }
            });
        }

        Ok(Client { inner })
    }

    pub fn server_info(&self) -> &ServerInfo {
        &self.inner.info
    }

    pub fn is_closed(&self) -> bool {
        self.inner.shutdown.is_cancelled()
    }

    /// Close the connection. Pending requests fail with [`Error::Closed`].
    pub fn close(&self) {
        self.inner.shutdown.cancel();
    }

    // ---- request plumbing -------------------------------------------------

    async fn request_with_timeout(&self, req: Request, timeout: Duration) -> Result<Response> {
        let corr = self.inner.next_corr();
        let (tx, rx) = oneshot::channel();
        {
            let mut routes = self.inner.routes.lock().unwrap();
            if routes.closed {
                return Err(Error::Closed);
            }
            routes.pending.insert(corr, tx);
        }
        if self.inner.out.send(req.into_frame(corr)).await.is_err() {
            self.inner.routes.lock().unwrap().pending.remove(&corr);
            return Err(Error::Closed);
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Response::Error {
                code,
                message,
                detail,
            })) => Err(Error::Server {
                code,
                message,
                detail: detail.and_then(|d| serde_json::from_slice(&d).ok()),
            }),
            Ok(Ok(r)) => Ok(r),
            Ok(Err(_)) => Err(Error::Closed),
            Err(_) => {
                self.inner.routes.lock().unwrap().pending.remove(&corr);
                Err(Error::Timeout)
            }
        }
    }

    async fn request(&self, req: Request) -> Result<Response> {
        self.request_with_timeout(req, self.inner.opts.request_timeout)
            .await
    }

    async fn request_ok(&self, req: Request) -> Result<()> {
        match self.request(req).await? {
            Response::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn request_json<T: DeserializeOwned>(&self, req: Request) -> Result<T> {
        self.request_json_timeout(req, self.inner.opts.request_timeout)
            .await
    }

    async fn request_json_timeout<T: DeserializeOwned>(
        &self,
        req: Request,
        timeout: Duration,
    ) -> Result<T> {
        match self.request_with_timeout(req, timeout).await? {
            Response::Json(b) => {
                serde_json::from_slice(&b).map_err(|e| Error::Protocol(format!("bad JSON: {e}")))
            }
            other => Err(unexpected(other)),
        }
    }

    /// Send without waiting for a reply (correlation id 0). The server only
    /// answers if the request fails, and that error is logged.
    async fn send_nowait(&self, req: Request) -> Result<()> {
        self.inner
            .out
            .send(req.into_frame(0))
            .await
            .map_err(|_| Error::Closed)
    }

    // ---- basics -----------------------------------------------------------

    pub async fn ping(&self) -> Result<()> {
        match self.request(Request::Ping).await? {
            Response::Pong => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// `{node_id, is_leader, leader, server_version}`.
    pub async fn metadata(&self) -> Result<serde_json::Value> {
        self.request_json(Request::Metadata).await
    }

    // ---- publishing -------------------------------------------------------

    pub async fn publish(&self, stream: &str, record: PublishRecord) -> Result<PublishAck> {
        match self
            .request(Request::Publish {
                stream: stream.to_string(),
                record,
            })
            .await?
        {
            Response::PublishOk { offset, duplicate } => Ok(PublishAck { offset, duplicate }),
            other => Err(unexpected(other)),
        }
    }

    /// Publish several records atomically with respect to ordering; one
    /// result per record, in order.
    pub async fn publish_batch(
        &self,
        stream: &str,
        records: Vec<PublishRecord>,
    ) -> Result<Vec<PublishAck>> {
        match self
            .request(Request::PublishBatch {
                stream: stream.to_string(),
                records,
            })
            .await?
        {
            Response::PublishBatchOk { results } => Ok(results
                .into_iter()
                .map(|(offset, duplicate)| PublishAck { offset, duplicate })
                .collect()),
            other => Err(unexpected(other)),
        }
    }

    // ---- streams ----------------------------------------------------------

    /// Create a stream. Idempotent when the stream exists with the same
    /// settings; 409 when the settings differ.
    pub async fn create_stream(&self, spec: StreamSpec) -> Result<()> {
        self.request_ok(Request::CreateStream(spec)).await
    }

    pub async fn update_stream(&self, spec: StreamSpec) -> Result<()> {
        self.request_ok(Request::UpdateStream(spec)).await
    }

    pub async fn delete_stream(&self, name: &str) -> Result<()> {
        self.request_ok(Request::DeleteStream {
            name: name.to_string(),
        })
        .await
    }

    pub async fn stream_info(&self, name: &str) -> Result<serde_json::Value> {
        self.request_json(Request::StreamInfo {
            name: name.to_string(),
        })
        .await
    }

    pub async fn list_streams(&self) -> Result<Vec<serde_json::Value>> {
        self.request_json(Request::ListStreams).await
    }

    // ---- SQL --------------------------------------------------------------

    /// Run a bounded query: `{columns, rows, row_count, execution_time_ms}`.
    pub async fn query(&self, sql: &str) -> Result<serde_json::Value> {
        self.request_json(Request::Query {
            sql: sql.to_string(),
        })
        .await
    }

    // ---- consumers --------------------------------------------------------

    /// Create a durable (or, with `spec.ephemeral`, connection-scoped)
    /// consumer. Idempotent for an identical spec. Returns consumer info.
    pub async fn create_consumer(&self, spec: ConsumerSpec) -> Result<serde_json::Value> {
        self.request_json(Request::CreateConsumer(spec)).await
    }

    pub async fn delete_consumer(&self, name: &str) -> Result<()> {
        self.request_ok(Request::DeleteConsumer {
            name: name.to_string(),
        })
        .await
    }

    pub async fn consumer_info(&self, name: &str) -> Result<serde_json::Value> {
        self.request_json(Request::ConsumerInfo {
            name: name.to_string(),
        })
        .await
    }

    pub async fn list_consumers(&self, stream: Option<&str>) -> Result<Vec<serde_json::Value>> {
        self.request_json(Request::ListConsumers {
            stream: stream.map(str::to_string),
        })
        .await
    }

    pub async fn seek(&self, consumer: &str, to: SeekTo) -> Result<()> {
        self.request_ok(Request::SeekConsumer {
            consumer: consumer.to_string(),
            to,
        })
        .await
    }

    /// Start push delivery. `window` is how many unacknowledged-by-the-app
    /// records the server may have in flight to this subscription; the
    /// client tops it up as you take records from the [`Subscription`].
    /// Several subscriptions (on any connections, any app instances) to the
    /// same consumer share its records.
    pub async fn subscribe(&self, consumer: &str, window: u32) -> Result<Subscription> {
        let window = window.max(1);
        let sub_id = match self
            .request(Request::Subscribe {
                consumer: consumer.to_string(),
                credits: window,
            })
            .await?
        {
            Response::SubscribeOk { sub_id } => sub_id,
            other => return Err(unexpected(other)),
        };
        let rx = self
            .inner
            .routes
            .lock()
            .unwrap()
            .new_subs
            .remove(&sub_id)
            .ok_or_else(|| Error::Protocol("subscription receiver missing".into()))?;
        Ok(Subscription {
            client: self.clone(),
            consumer: consumer.to_string(),
            sub_id,
            rx,
            buffered: std::collections::VecDeque::new(),
            window,
            consumed: 0,
            ended: None,
        })
    }

    /// Fetch up to `max_messages` records, waiting up to `expires` for at
    /// least one. Returns an empty vec on timeout.
    pub async fn pull(
        &self,
        consumer: &str,
        max_messages: u32,
        expires: Duration,
    ) -> Result<Vec<WireRecord>> {
        let expires_ms = expires.as_millis().min(u32::MAX as u128) as u32;
        match self
            .request_with_timeout(
                Request::Pull {
                    consumer: consumer.to_string(),
                    max_messages,
                    max_bytes: 0,
                    expires_ms,
                },
                self.inner.opts.request_timeout + expires,
            )
            .await?
        {
            Response::Messages { records } => Ok(records),
            other => Err(unexpected(other)),
        }
    }

    pub async fn ack(&self, consumer: &str, offsets: Vec<u64>) -> Result<()> {
        self.request_ok(Request::Ack {
            consumer: consumer.to_string(),
            offsets,
        })
        .await
    }

    /// Acknowledge without waiting for the server's reply.
    pub async fn ack_nowait(&self, consumer: &str, offsets: Vec<u64>) -> Result<()> {
        self.send_nowait(Request::Ack {
            consumer: consumer.to_string(),
            offsets,
        })
        .await
    }

    /// Ask for redelivery, after `delay` (or the consumer's backoff when
    /// zero).
    pub async fn nack(&self, consumer: &str, offset: u64, delay: Duration) -> Result<()> {
        self.request_ok(Request::Nack {
            consumer: consumer.to_string(),
            offset,
            delay_ms: delay.as_millis().min(u32::MAX as u128) as u32,
        })
        .await
    }

    /// Stop redelivering this record and dead-letter it (if the consumer has
    /// a DLQ stream).
    pub async fn term(&self, consumer: &str, offset: u64, reason: &str) -> Result<()> {
        self.request_ok(Request::Term {
            consumer: consumer.to_string(),
            offset,
            reason: reason.to_string(),
        })
        .await
    }

    /// Reset the ack timer for records still being worked on.
    pub async fn in_progress(&self, consumer: &str, offsets: Vec<u64>) -> Result<()> {
        self.request_ok(Request::InProgress {
            consumer: consumer.to_string(),
            offsets,
        })
        .await
    }

    // ---- stateless reads --------------------------------------------------

    /// Read records from `from` without a consumer. Waits up to `wait` for
    /// new data when caught up. `filter` is a subject filter ("" = all).
    pub async fn read(
        &self,
        stream: &str,
        from: u64,
        max_records: u32,
        wait: Duration,
        filter: &str,
    ) -> Result<ReadResult> {
        let wait_ms = wait.as_millis().min(u32::MAX as u128) as u32;
        match self
            .request_with_timeout(
                Request::Read {
                    stream: stream.to_string(),
                    from,
                    max_records,
                    max_bytes: 0,
                    wait_ms,
                    filter: filter.to_string(),
                },
                self.inner.opts.request_timeout + wait,
            )
            .await?
        {
            Response::ReadResult {
                next_offset,
                high_watermark,
                records,
            } => Ok(ReadResult {
                records,
                next_offset,
                high_watermark,
            }),
            other => Err(unexpected(other)),
        }
    }

    /// Send a raw request and return the raw response (error responses are
    /// returned as `Err(Error::Server)`).
    pub async fn raw(&self, req: Request) -> Result<Response> {
        self.request(req).await
    }
}

impl Inner {
    fn next_corr(&self) -> u32 {
        loop {
            let c = self.next_corr.fetch_add(1, Ordering::Relaxed);
            if c != 0 {
                return c;
            }
        }
    }

    fn route(&self, frame: Frame) {
        let corr = frame.correlation_id;
        let resp = match Response::from_frame(&frame) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, corr, "exspeed: undecodable response");
                return;
            }
        };
        let mut routes = self.routes.lock().unwrap();
        match resp {
            Response::Deliver { sub_id, records } => {
                if let Some(tx) = routes.subs.get(&sub_id) {
                    let _ = tx.send(SubMsg::Records(records));
                }
            }
            Response::SubscriptionEnded {
                sub_id,
                code,
                message,
            } => {
                if let Some(tx) = routes.subs.remove(&sub_id) {
                    let _ = tx.send(SubMsg::Ended { code, message });
                }
            }
            Response::SubscribeOk { sub_id } if corr != 0 => {
                let (tx, rx) = mpsc::unbounded_channel();
                routes.subs.insert(sub_id, tx);
                routes.new_subs.insert(sub_id, rx);
                if let Some(p) = routes.pending.remove(&corr) {
                    let _ = p.send(Response::SubscribeOk { sub_id });
                } else {
                    routes.subs.remove(&sub_id);
                    routes.new_subs.remove(&sub_id);
                }
            }
            Response::Error { code, message, .. } if corr == 0 => {
                tracing::warn!(code, %message, "exspeed: fire-and-forget request failed");
            }
            other => {
                if let Some(p) = routes.pending.remove(&corr) {
                    let _ = p.send(other);
                }
            }
        }
    }

    fn close_routes(&self) {
        let mut routes = self.routes.lock().unwrap();
        routes.closed = true;
        routes.pending.clear();
        for (_, tx) in routes.subs.drain() {
            let _ = tx.send(SubMsg::Ended {
                code: code::UNAVAILABLE,
                message: "connection closed".into(),
            });
        }
    }
}

/// A delivered record plus the means to settle it.
pub struct Message {
    pub record: WireRecord,
    client: Client,
    consumer: String,
}

impl Message {
    pub fn consumer(&self) -> &str {
        &self.consumer
    }

    /// Parse the value as JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.record.value)
            .map_err(|e| Error::Protocol(format!("bad JSON: {e}")))
    }

    pub async fn ack(&self) -> Result<()> {
        self.client
            .ack(&self.consumer, vec![self.record.offset])
            .await
    }

    pub async fn nack(&self, delay: Duration) -> Result<()> {
        self.client
            .nack(&self.consumer, self.record.offset, delay)
            .await
    }

    pub async fn term(&self, reason: &str) -> Result<()> {
        self.client
            .term(&self.consumer, self.record.offset, reason)
            .await
    }

    pub async fn in_progress(&self) -> Result<()> {
        self.client
            .in_progress(&self.consumer, vec![self.record.offset])
            .await
    }
}

/// A push subscription. Yields messages in delivery order; ends when the
/// server ends it (see [`Subscription::end_reason`]) or the connection
/// closes. Dropping it unsubscribes.
pub struct Subscription {
    client: Client,
    consumer: String,
    sub_id: u32,
    rx: mpsc::UnboundedReceiver<SubMsg>,
    buffered: std::collections::VecDeque<WireRecord>,
    window: u32,
    consumed: u32,
    ended: Option<(u16, String)>,
}

impl Subscription {
    pub fn id(&self) -> u32 {
        self.sub_id
    }

    pub fn consumer(&self) -> &str {
        &self.consumer
    }

    /// Why the server ended the subscription, once [`next`](Self::next)
    /// has returned `None`.
    pub fn end_reason(&self) -> Option<(u16, &str)> {
        self.ended.as_ref().map(|(c, m)| (*c, m.as_str()))
    }

    /// Next message, or `None` when the subscription has ended.
    pub async fn next(&mut self) -> Option<Message> {
        loop {
            if let Some(record) = self.buffered.pop_front() {
                self.on_consumed().await;
                return Some(Message {
                    record,
                    client: self.client.clone(),
                    consumer: self.consumer.clone(),
                });
            }
            if self.ended.is_some() {
                return None;
            }
            match self.rx.recv().await {
                Some(SubMsg::Records(rs)) => self.buffered.extend(rs),
                Some(SubMsg::Ended { code, message }) => self.ended = Some((code, message)),
                None => {
                    self.ended = Some((code::UNAVAILABLE, "connection closed".into()));
                }
            }
        }
    }

    /// Like [`next`](Self::next) but gives up after `timeout`.
    pub async fn next_timeout(&mut self, timeout: Duration) -> Option<Message> {
        tokio::time::timeout(timeout, self.next()).await.ok().flatten()
    }

    /// Return credit to the server once half the window has been consumed,
    /// so delivery continues without a round trip per record.
    async fn on_consumed(&mut self) {
        self.consumed += 1;
        if self.consumed >= (self.window / 2).max(1) {
            let credits = std::mem::take(&mut self.consumed);
            let _ = self
                .client
                .send_nowait(Request::Credit {
                    sub_id: self.sub_id,
                    credits,
                })
                .await;
        }
    }

    /// Stop delivery and wait for the server to confirm.
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.client
            .inner
            .routes
            .lock()
            .unwrap()
            .subs
            .remove(&self.sub_id);
        self.ended = Some((0, "unsubscribed".into()));
        self.client
            .request_ok(Request::Unsubscribe {
                sub_id: self.sub_id,
            })
            .await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.ended.is_some() {
            return;
        }
        self.client
            .inner
            .routes
            .lock()
            .unwrap()
            .subs
            .remove(&self.sub_id);
        let _ = self.client.inner.out.try_send(
            Request::Unsubscribe {
                sub_id: self.sub_id,
            }
            .into_frame(0),
        );
    }
}

fn proto(e: impl std::fmt::Display) -> Error {
    Error::Protocol(e.to_string())
}

fn into_error(r: Response) -> Error {
    match r {
        Response::Error {
            code,
            message,
            detail,
        } => Error::Server {
            code,
            message,
            detail: detail.and_then(|d| serde_json::from_slice(&d).ok()),
        },
        other => unexpected(other),
    }
}

fn unexpected(r: Response) -> Error {
    Error::Unexpected(format!("{:?}", r.opcode()))
}
