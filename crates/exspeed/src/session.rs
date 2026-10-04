//! One TCP client connection speaking the client protocol (v2).
//!
//! Structure:
//! - a **writer task** owns the socket's write half; everything that sends
//!   a frame (responses, subscription pushes) goes through one channel, so
//!   a slow long-poll never blocks other responses and pushes never
//!   interleave mid-frame;
//! - the **reader loop** decodes requests and dispatches them. Requests that
//!   may wait (pull, long-poll read, query) run in their own tasks, bounded
//!   per connection;
//! - each **subscription** has a forwarder task turning consumer events into
//!   `Deliver` / `SubscriptionEnded` frames. All of them, and any ephemeral
//!   consumers this connection created, are cleaned up when the connection
//!   ends — however it ends.
//!
//! The handshake must complete within [`HANDSHAKE_TIMEOUT`]; an
//! authenticated connection that sends nothing (not even `Ping`) for
//! [`IDLE_TIMEOUT`] is closed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use exspeed_broker::broker_append::{AppendResult, IDEMPOTENCY_HEADER};
use exspeed_broker::consumer::{ConsumerError, SubEvent};
use exspeed_broker::kv::{BucketConfig, KvEntry, KvError};
use exspeed_broker::log::LogError;
use exspeed_broker::pubsub::{BusError, CoreEvent, CoreMessage, CORE_SUB_ID_BIT};
use exspeed_broker::Broker;
use exspeed_common::auth::{Action, CredentialStore, Identity, Permission, StreamGlob};
use exspeed_common::record_format;
use exspeed_common::{Metrics, Offset, StreamName, SubjectFilter};
use exspeed_processing::ExqlEngine;
use exspeed_protocol::client::{
    code, ConsumerSpec, EncodedRecords, PublishRecord, Request, Response, StreamSpec,
};
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::OutFrame;
use exspeed_protocol::ProtocolError;
use exspeed_streams::{ReadLimits, Record, StorageError, StreamConfig};

/// The first frame must be `Connect` and arrive within this time.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Close connections that send nothing for this long. Clients ping well
/// within it (the SDKs every 15–30 s).
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// Concurrent waiting requests (pull, long-poll read, query) per connection.
const MAX_CONCURRENT_WAITS: usize = 64;
/// Outbound frames buffered per connection before the reader stalls.
const OUTBOUND_BUFFER: usize = 1024;

/// Everything a session needs from the server.
pub struct SessionContext {
    /// The handshake must complete within this (default [`HANDSHAKE_TIMEOUT`]).
    pub handshake_timeout: Duration,
    /// A connection with no frame for this long is closed (default
    /// [`IDLE_TIMEOUT`]).
    pub idle_timeout: Duration,
    pub broker: Arc<Broker>,
    pub exql: Arc<ExqlEngine>,
    /// `None` = authentication disabled (every client gets full access).
    pub credential_store: Option<Arc<CredentialStore>>,
    pub metrics: Arc<Metrics>,
    pub node_id: String,
    /// Current leader's client address when this node is not the leader.
    pub leader_hint: Arc<dyn Fn() -> Option<String> + Send + Sync>,
}

fn anonymous_identity() -> Identity {
    Identity {
        name: "anonymous".to_string(),
        permissions: vec![Permission {
            streams: StreamGlob::compile("*", "anonymous").expect("* is a valid glob"),
            actions: Action::Publish | Action::Subscribe | Action::Admin,
        }],
        subject_permissions: Vec::new(),
    }
}

/// Outbound frame sink shared by the reader loop, waiting tasks and
/// subscription forwarders.
#[derive(Clone)]
struct Out(mpsc::Sender<OutFrame>);

impl Out {
    async fn send(&self, corr: u32, resp: Response) {
        let _ = self.0.send(resp.into_frame(corr).into()).await;
    }

    async fn send_frame(&self, frame: OutFrame) {
        let _ = self.0.send(frame).await;
    }
}

/// Write buffer of the connection's writer task. Record bytes in chunks at
/// least this large go to the socket without being copied into it.
const WRITE_BUFFER: usize = 64 * 1024;

/// Write one frame: header, head, then the body chunks (record bytes
/// straight from a segment read).
async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut BufWriter<W>,
    f: &OutFrame,
) -> std::io::Result<()> {
    // Read/pull/push batches are budgeted so a frame never exceeds the
    // limit; should one ever do, every decoder would drop the connection,
    // so answer with an error instead.
    if let Err(e) = f.check_size() {
        tracing::error!(opcode = ?f.opcode, error = %e, "response exceeds the frame limit");
        let err: OutFrame = Response::error(code::INTERNAL, format!("response not sent: {e}"))
            .into_frame(f.correlation_id)
            .into();
        return write_frame_parts(w, &err).await;
    }
    write_frame_parts(w, f).await
}

async fn write_frame_parts<W: AsyncWrite + Unpin>(
    w: &mut BufWriter<W>,
    f: &OutFrame,
) -> std::io::Result<()> {
    w.write_all(&f.header()).await?;
    w.write_all(&f.head).await?;
    for chunk in &f.body {
        w.write_all(chunk).await?;
    }
    Ok(())
}

struct SubEntry {
    consumer: String,
    forwarder: tokio::task::JoinHandle<()>,
}

/// Core messages queued for one connection before newer ones are dropped
/// (a slow subscriber loses messages instead of slowing everyone down).
const CORE_QUEUE: usize = 65_536;

/// Per-connection state that must be cleaned up when the connection ends.
struct ConnState {
    ctx: Arc<SessionContext>,
    subs: HashMap<u32, SubEntry>,
    /// Core subscription ids, and the queue (plus its forwarder) that
    /// carries their messages to this connection.
    core_subs: Vec<u32>,
    core_tx: Option<mpsc::Sender<CoreEvent>>,
    core_forwarder: Option<tokio::task::JoinHandle<()>>,
    ephemeral: Vec<String>,
    /// This connection's publish pipeline (started on the first publish).
    publishes: Option<mpsc::Sender<PublishJob>>,
    /// Cancelled when the connection ends, so waiting requests (queries,
    /// pulls, long-poll reads) stop instead of running on for nobody.
    closed: CancellationToken,
}

impl Drop for ConnState {
    fn drop(&mut self) {
        self.closed.cancel();
        for id in self.core_subs.drain(..) {
            self.ctx.broker.bus.unsubscribe(id);
        }
        if let Some(f) = self.core_forwarder.take() {
            f.abort();
        }
        let consumers = self.ctx.broker.consumers.clone();
        let subs: Vec<(u32, SubEntry)> = self.subs.drain().collect();
        let ephemeral = std::mem::take(&mut self.ephemeral);
        if subs.is_empty() && ephemeral.is_empty() {
            return;
        }
        // Drop can't await; finish the cleanup in the background.
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                for (sub_id, e) in subs {
                    e.forwarder.abort();
                    consumers.unsubscribe(&e.consumer, sub_id).await;
                }
                for name in ephemeral {
                    let _ = consumers.delete(&name).await;
                }
            });
        }
    }
}

/// Serve one client connection until it closes, errors, idles out, or
/// `cancel` fires.
pub async fn run<S>(
    socket: S,
    peer: SocketAddr,
    ctx: Arc<SessionContext>,
    cancel: CancellationToken,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    run_with_cert(socket, peer, ctx, cancel, None).await
}

/// [`run`] for a TLS connection whose client certificate (verified against
/// `tls.client_ca`) stands for `cert_name`: a Connect without a token gets
/// the credential bound to that name (`cert_cn`).
pub async fn run_with_cert<S>(
    socket: S,
    peer: SocketAddr,
    ctx: Arc<SessionContext>,
    cancel: CancellationToken,
    cert_name: Option<String>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(socket);
    let mut frames = FramedRead::new(reader, ExspeedCodec::new());
    let mut sink = FramedWrite::new(writer, ExspeedCodec::new());

    // --- Handshake ---------------------------------------------------------
    let first = match tokio::time::timeout(ctx.handshake_timeout, frames.next()).await {
        Err(_) => {
            warn!(%peer, "handshake timed out");
            return Ok(());
        }
        Ok(None) => return Ok(()),
        Ok(Some(Ok(f))) => f,
        Ok(Some(Err(e))) => {
            // Undecodable first frame (old client, wrong port, garbage): say
            // why in a v2 Error frame before closing instead of hanging up
            // silently.
            let message = match &e {
                ProtocolError::UnsupportedVersion(v) => format!(
                    "unsupported protocol version {v}; this server speaks {}",
                    exspeed_common::PROTOCOL_VERSION
                ),
                other => other.to_string(),
            };
            warn!(%peer, error = %e, "undecodable handshake frame");
            let _ = sink
                .send(Response::error(code::BAD_REQUEST, message).into_frame(0))
                .await;
            return Err(e.into());
        }
    };
    let corr = first.correlation_id;
    let (client_id, token) = match Request::from_frame(&first) {
        Ok(Request::Connect { client_id, token }) => (client_id, token),
        Ok(_) | Err(_) => {
            let _ = sink
                .send(
                    Response::error(code::UNAUTHORIZED, "first frame must be Connect")
                        .into_frame(corr),
                )
                .await;
            return Ok(());
        }
    };
    let identity: Arc<Identity> = match ctx.credential_store.as_ref() {
        None => Arc::new(anonymous_identity()),
        Some(store) => {
            let found = match (&token, &cert_name) {
                (Some(t), _) => {
                    let digest: [u8; 32] = Sha256::digest(t.as_bytes()).into();
                    store.lookup(&digest)
                }
                (None, Some(name)) => store.lookup_cert(name),
                (None, None) => None,
            };
            match found {
                Some(id) => id,
                None => {
                    ctx.metrics.auth_denied("unauthorized", "tcp", "Connect");
                    warn!(%peer, %client_id, "CONNECT rejected");
                    let _ = sink
                        .send(Response::error(code::UNAUTHORIZED, "unauthorized").into_frame(corr))
                        .await;
                    return Ok(());
                }
            }
        }
    };
    tracing::Span::current().record("identity", identity.name.as_str());
    info!(%peer, %client_id, identity = %identity.name, "CONNECT authenticated");
    sink.send(
        Response::ConnectOk {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            node_id: ctx.node_id.clone(),
            leader: if ctx.broker.log.can_write() {
                None
            } else {
                (ctx.leader_hint)()
            },
        }
        .into_frame(corr),
    )
    .await?;

    // --- Writer task -------------------------------------------------------
    // Frames queued together are written with one flush.
    let (out_tx, mut out_rx) = mpsc::channel::<OutFrame>(OUTBOUND_BUFFER);
    let mut w = BufWriter::with_capacity(WRITE_BUFFER, sink.into_inner());
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            if write_frame(&mut w, &frame).await.is_err() {
                return;
            }
            while let Ok(frame) = out_rx.try_recv() {
                if write_frame(&mut w, &frame).await.is_err() {
                    return;
                }
            }
            if w.flush().await.is_err() {
                return;
            }
        }
        let _ = w.shutdown().await;
    });
    let out = Out(out_tx);

    // --- Request loop ------------------------------------------------------
    let mut state = ConnState {
        ctx: ctx.clone(),
        subs: HashMap::new(),
        core_subs: Vec::new(),
        core_tx: None,
        core_forwarder: None,
        ephemeral: Vec::new(),
        publishes: None,
        closed: cancel.child_token(),
    };
    let waits = Arc::new(Semaphore::new(MAX_CONCURRENT_WAITS));
    let result = loop {
        let next = tokio::select! {
            _ = cancel.cancelled() => break Ok(()),
            r = tokio::time::timeout(ctx.idle_timeout, frames.next()) => r,
        };
        let frame = match next {
            Err(_) => {
                info!(%peer, "closing idle connection");
                break Ok(());
            }
            Ok(None) => break Ok(()),
            Ok(Some(Err(e))) => {
                // Tell the client why before closing (bad version/opcode/size).
                out.send(0, Response::error(code::BAD_REQUEST, e.to_string()))
                    .await;
                break Err(e.into());
            }
            Ok(Some(Ok(f))) => f,
        };
        let corr = frame.correlation_id;
        let req = match Request::from_frame(&frame) {
            Ok(r) => r,
            Err(e) => {
                out.send(corr, Response::error(code::BAD_REQUEST, e.to_string()))
                    .await;
                continue;
            }
        };
        dispatch(req, corr, &identity, &mut state, &out, &waits).await;
    };

    // Dropping `state` unsubscribes and deletes ephemeral consumers; dropping
    // `out` lets the writer flush and exit.
    drop(state);
    drop(out);
    let _ = tokio::time::timeout(Duration::from_secs(5), writer_task).await;
    result
}

/// Check `action` on a bucket's stream.
fn authorize_bucket(
    ctx: &SessionContext,
    identity: &Identity,
    bucket: &str,
    action: Action,
) -> Result<(), Response> {
    let stream = exspeed_broker::kv::bucket_stream(bucket)
        .map_err(|e| Response::error(code::BAD_REQUEST, e.to_string()))?;
    if identity.authorize(action, &stream) {
        Ok(())
    } else {
        ctx.metrics.auth_denied("forbidden", "tcp", "Kv");
        Err(Response::error(code::FORBIDDEN, "forbidden"))
    }
}

fn kv_error_response(ctx: &SessionContext, e: KvError) -> Response {
    match e {
        KvError::Invalid(_) | KvError::NotABucket(_) => {
            Response::error(code::BAD_REQUEST, e.to_string())
        }
        KvError::BucketNotFound(_) => Response::error(code::NOT_FOUND, e.to_string()),
        KvError::WrongRevision { current, .. } => Response::error_with(
            code::CONFLICT,
            e.to_string(),
            serde_json::json!({ "current_revision": current }),
        ),
        KvError::Log(e) => log_error_response(ctx, e),
    }
}

/// A KV revision as a wire record (offset = revision, subject = key).
fn kv_wire(e: KvEntry) -> exspeed_protocol::client::WireRecord {
    let r = e.record;
    exspeed_protocol::client::WireRecord {
        offset: r.offset.0,
        timestamp_ns: r.timestamp,
        delivery_count: 0,
        subject: r.subject,
        key: r.key,
        value: r.value,
        headers: r.headers,
    }
}

/// Parse a subject a core message is published to: one concrete subject
/// (no wildcards, no empty tokens).
fn check_core_subject(subject: &str, what: &str) -> Result<SubjectFilter, Response> {
    let bad = |m: String| Response::error(code::BAD_REQUEST, m);
    let f = SubjectFilter::parse(subject).map_err(|e| bad(format!("{what}: {e}")))?;
    if !f.is_literal() {
        return Err(bad(format!(
            "{what} '{subject}' must be a concrete subject (no wildcards, not empty)"
        )));
    }
    Ok(f)
}

/// Write a connection's core messages to its socket.
async fn forward_core(mut rx: mpsc::Receiver<CoreEvent>, out: Out) {
    while let Some(ev) = rx.recv().await {
        let resp = match ev {
            CoreEvent::Message { sub_id, msg } => Response::CoreMsg {
                sub_id,
                subject: msg.subject.clone(),
                reply_to: msg.reply_to.clone(),
                headers: msg.headers.clone(),
                value: msg.value.clone(),
            },
            CoreEvent::Ended {
                sub_id,
                code,
                message,
            } => Response::SubscriptionEnded {
                sub_id,
                code,
                message,
            },
        };
        if out.0.send(resp.into_frame(0).into()).await.is_err() {
            return;
        }
    }
}

/// Respond with a 403 and count it.
async fn forbid(ctx: &SessionContext, out: &Out, corr: u32, op: &'static str) {
    ctx.metrics.auth_denied("forbidden", "tcp", op);
    out.send(corr, Response::error(code::FORBIDDEN, "forbidden"))
        .await;
}

fn stream_name(name: &str) -> Result<StreamName, Response> {
    StreamName::try_from(name)
        .map_err(|e| Response::error(code::BAD_REQUEST, format!("invalid stream name: {e}")))
}

fn not_leader(ctx: &SessionContext) -> Response {
    match (ctx.leader_hint)() {
        Some(l) => Response::error_with(
            code::UNAVAILABLE,
            "not the leader",
            serde_json::json!({ "leader": l }),
        ),
        None => Response::error(code::UNAVAILABLE, "not the leader"),
    }
}

pub fn log_error_response(ctx: &SessionContext, e: LogError) -> Response {
    match e {
        LogError::NotLeader => not_leader(ctx),
        LogError::DedupNotReady | LogError::ReplicationTimeout => {
            Response::error(code::UNAVAILABLE, e.to_string())
        }
        LogError::NotEnoughReplicas { in_sync, required } => Response::error_with(
            code::UNAVAILABLE,
            e.to_string(),
            serde_json::json!({ "in_sync": in_sync, "required": required }),
        ),
        LogError::InvalidRecord(_) | LogError::InvalidConfig(_) => {
            Response::error(code::BAD_REQUEST, e.to_string())
        }
        LogError::Storage(StorageError::KeyCollision { stored_offset }) => Response::error_with(
            code::CONFLICT,
            "msg_id already used for a different body",
            serde_json::json!({ "stored_offset": stored_offset }),
        ),
        LogError::Storage(StorageError::DedupMapFull { retry_after_secs }) => Response::error_with(
            code::TOO_MANY_REQUESTS,
            "dedup map full; retry later",
            serde_json::json!({ "retry_after_secs": retry_after_secs }),
        ),
        LogError::Storage(StorageError::StreamNotFound(s)) => {
            Response::error(code::NOT_FOUND, format!("stream '{s}' not found"))
        }
        LogError::Storage(e @ StorageError::StreamFull { .. }) => {
            Response::error(code::TOO_MANY_REQUESTS, e.to_string())
        }
        LogError::Storage(StorageError::StreamAlreadyExists(s)) => {
            Response::error(code::CONFLICT, format!("stream '{s}' already exists"))
        }
        LogError::Storage(StorageError::Io(io))
            if exspeed_storage::file::io_errors::is_storage_full(&io) =>
        {
            Response::error(
                code::INSUFFICIENT_STORAGE,
                format!("the server's disk is full; nothing was written: {io}"),
            )
        }
        LogError::Storage(e) => Response::error(code::INTERNAL, e.to_string()),
    }
}

fn consumer_error_response(ctx: &SessionContext, e: ConsumerError) -> Response {
    match e {
        ConsumerError::NotLeader => not_leader(ctx),
        other => Response::error(other.code(), other.to_string()),
    }
}

fn to_record(r: PublishRecord) -> Record {
    let mut headers = r.headers;
    if let Some(id) = r.msg_id {
        headers.retain(|(k, _)| k != IDEMPOTENCY_HEADER);
        headers.push((IDEMPOTENCY_HEADER.to_string(), id));
    }
    Record {
        key: r.key,
        value: r.value,
        subject: r.subject,
        headers,
        timestamp_ns: None,
    }
}

fn stream_config(s: &StreamSpec, default_window_secs: u64) -> StreamConfig {
    let mut cfg = StreamConfig::from_request_with_window(
        s.max_age_secs,
        s.max_bytes,
        s.dedup_window_secs,
        s.dedup_max_entries,
        default_window_secs,
    );
    cfg.compaction = s.compaction;
    cfg.dedup_window_secs = cfg.dedup_window_secs.min(cfg.max_age_secs);
    cfg.with_limits(&s.limits)
}

/// Resolve the consumer's stream and check `action` on it.
async fn authorize_consumer(
    ctx: &SessionContext,
    identity: &Identity,
    consumer: &str,
    action: Action,
    management: bool,
) -> Result<(), Response> {
    let Some(stream) = ctx.broker.consumers.stream_of(consumer).await else {
        return Err(if ctx.broker.log.can_write() {
            Response::error(code::NOT_FOUND, format!("consumer '{consumer}' not found"))
        } else {
            not_leader(ctx)
        });
    };
    let name = stream_name(&stream)?;
    // Admin on the stream also covers consumer management (info, seek).
    let admin_ok =
        action == Action::Subscribe && management && identity.authorize(Action::Admin, &name);
    if identity.authorize(action, &name) || admin_ok {
        Ok(())
    } else {
        Err(Response::error(code::FORBIDDEN, "forbidden"))
    }
}

/// One publish request waiting in a connection's publish pipeline.
struct PublishJob {
    corr: u32,
    stream: StreamName,
    records: Vec<Record>,
    /// `Publish` (one record, `PublishOk`) rather than `PublishBatch`.
    single: bool,
    start: std::time::Instant,
}

impl PublishJob {
    fn new(corr: u32, stream: StreamName, records: Vec<Record>, single: bool) -> Self {
        Self {
            corr,
            stream,
            records,
            single,
            start: std::time::Instant::now(),
        }
    }

    fn bytes(&self) -> usize {
        self.records
            .iter()
            .map(|r| r.value.len() + r.subject.len() + r.key.as_ref().map_or(0, |k| k.len()))
            .sum()
    }

    fn respond(&self, ctx: &SessionContext, results: Vec<AppendResult>) -> Response {
        if self.single {
            ctx.metrics
                .record_publish_latency(self.stream.as_str(), self.start.elapsed().as_secs_f64());
            let (offset, duplicate) = match results.into_iter().next() {
                Some(AppendResult::Written(o, _)) => (o.0, false),
                Some(AppendResult::Duplicate(o)) => (o.0, true),
                None => {
                    return Response::error(code::INTERNAL, "publish returned no result");
                }
            };
            Response::PublishOk { offset, duplicate }
        } else {
            Response::PublishBatchOk {
                results: results
                    .into_iter()
                    .map(|r| match r {
                        AppendResult::Written(o, _) => (o.0, false),
                        AppendResult::Duplicate(o) => (o.0, true),
                    })
                    .collect(),
            }
        }
    }
}

/// Requests a connection may queue in its publish pipeline before the read
/// loop waits (backpressure).
const PUBLISH_QUEUE: usize = 1024;
/// Most records / bytes appended together from queued publish requests.
const PUBLISH_COALESCE_RECORDS: usize = 4096;
const PUBLISH_COALESCE_BYTES: usize = 8 * 1024 * 1024;

impl ConnState {
    /// Queue a publish in this connection's pipeline. Publishes are applied
    /// in arrival order, but the read loop doesn't wait for them, so a
    /// pipelining client's requests share fsyncs instead of paying one
    /// each.
    async fn publish(&mut self, out: &Out, job: PublishJob) {
        let tx = self.publishes.get_or_insert_with(|| {
            let (tx, rx) = mpsc::channel(PUBLISH_QUEUE);
            tokio::spawn(publish_pipeline(self.ctx.clone(), out.clone(), rx));
            tx
        });
        if let Err(mpsc::error::SendError(job)) = tx.send(job).await {
            out.send(
                job.corr,
                Response::error(code::UNAVAILABLE, "connection closing"),
            )
            .await;
        }
    }
}

/// Applies one connection's publishes in order. Requests already queued
/// for the same stream are appended together (one storage batch, one
/// fsync); each request still gets its own reply.
async fn publish_pipeline(ctx: Arc<SessionContext>, out: Out, mut rx: mpsc::Receiver<PublishJob>) {
    let mut next: Option<PublishJob> = None;
    loop {
        let first = match next.take() {
            Some(j) => j,
            None => match rx.recv().await {
                Some(j) => j,
                None => return,
            },
        };
        let mut records = first.records.len();
        let mut bytes = first.bytes();
        let mut group = vec![first];
        while records < PUBLISH_COALESCE_RECORDS && bytes < PUBLISH_COALESCE_BYTES {
            match rx.try_recv() {
                Ok(j) if j.stream == group[0].stream => {
                    records += j.records.len();
                    bytes += j.bytes();
                    group.push(j);
                }
                Ok(j) => {
                    next = Some(j);
                    break;
                }
                Err(_) => break,
            }
        }
        append_group(&ctx, &out, group).await;
    }
}

async fn append_group(ctx: &SessionContext, out: &Out, mut group: Vec<PublishJob>) {
    let log = &ctx.broker.log;
    if group.len() == 1 {
        let job = group.pop().expect("one job");
        return append_one(ctx, out, job).await;
    }
    let stream = group[0].stream.clone();
    let all: Vec<Record> = group
        .iter()
        .flat_map(|j| j.records.iter().cloned())
        .collect();
    match log.append_batch(&stream, all).await {
        Ok(results) => {
            let mut results = results.into_iter();
            for job in group {
                let mine: Vec<AppendResult> = results.by_ref().take(job.records.len()).collect();
                out.send(job.corr, job.respond(ctx, mine)).await;
            }
        }
        // Rejected before anything was written, because of one request's
        // records: apply the requests one by one so only that one fails.
        Err(LogError::InvalidRecord(_))
        | Err(LogError::Storage(StorageError::KeyCollision { .. })) => {
            for job in group {
                append_one(ctx, out, job).await;
            }
        }
        // Anything else (not leader, storage, replication) is what each
        // request would have seen on its own.
        Err(e) => {
            let resp = log_error_response(ctx, e);
            for job in group {
                out.send(job.corr, resp.clone()).await;
            }
        }
    }
}

async fn append_one(ctx: &SessionContext, out: &Out, mut job: PublishJob) {
    let log = &ctx.broker.log;
    let result = if job.single {
        let record = job.records.pop().expect("one record");
        log.append(&job.stream, record).await.map(|r| vec![r])
    } else {
        log.append_batch(&job.stream, std::mem::take(&mut job.records))
            .await
    };
    let resp = match result {
        Ok(results) => job.respond(ctx, results),
        Err(e) => log_error_response(ctx, e),
    };
    out.send(job.corr, resp).await;
}

async fn dispatch(
    req: Request,
    corr: u32,
    identity: &Arc<Identity>,
    state: &mut ConnState,
    out: &Out,
    waits: &Arc<Semaphore>,
) {
    let ctx = state.ctx.clone();
    let broker = &ctx.broker;
    // `corr == 0` marks fire-and-forget requests: only errors are reported.
    let reply_ok = |corr: u32| async move {
        if corr != 0 {
            out.send(corr, Response::Ok).await;
        }
    };

    match req {
        Request::Connect { .. } => {
            out.send(
                corr,
                Response::error(code::BAD_REQUEST, "already connected"),
            )
            .await;
        }
        Request::Ping => out.send(corr, Response::Pong).await,
        Request::Metadata => {
            let leader = broker.log.can_write();
            out.send(
                corr,
                Response::json(&serde_json::json!({
                    "node_id": ctx.node_id,
                    "is_leader": leader,
                    "leader": if leader { None } else { (ctx.leader_hint)() },
                    "server_version": env!("CARGO_PKG_VERSION"),
                })),
            )
            .await;
        }

        // ---- Publishing ----------------------------------------------------
        Request::Publish { stream, record } => {
            let name = match stream_name(&stream) {
                Ok(n) => n,
                Err(r) => return out.send(corr, r).await,
            };
            if name.is_internal() || !identity.authorize(Action::Publish, &name) {
                return forbid(&ctx, out, corr, "Publish").await;
            }
            state
                .publish(
                    out,
                    PublishJob::new(corr, name, vec![to_record(record)], true),
                )
                .await;
        }
        Request::PublishBatch { stream, records } => {
            let name = match stream_name(&stream) {
                Ok(n) => n,
                Err(r) => return out.send(corr, r).await,
            };
            if name.is_internal() || !identity.authorize(Action::Publish, &name) {
                return forbid(&ctx, out, corr, "PublishBatch").await;
            }
            let records = records.into_iter().map(to_record).collect();
            state
                .publish(out, PublishJob::new(corr, name, records, false))
                .await;
        }

        // ---- Streams -------------------------------------------------------
        Request::CreateStream(spec) | Request::UpdateStream(spec)
            if spec
                .name
                .starts_with(exspeed_common::INTERNAL_STREAM_PREFIX) =>
        {
            forbid(&ctx, out, corr, "CreateStream").await;
        }
        Request::CreateStream(spec) => {
            let name = match stream_name(&spec.name) {
                Ok(n) => n,
                Err(r) => return out.send(corr, r).await,
            };
            if !identity.authorize(Action::Admin, &name) {
                return forbid(&ctx, out, corr, "CreateStream").await;
            }
            let cfg = stream_config(&spec, broker.log.default_dedup_window_secs());
            let resp = match broker.log.create_stream(&name, &cfg).await {
                Ok(()) => Response::Ok,
                // Idempotent: same config → Ok, different → 409.
                Err(LogError::Storage(StorageError::StreamAlreadyExists(_))) => {
                    match broker.storage.stream_config(&name).await {
                        Ok(existing) if existing == cfg => Response::Ok,
                        _ => Response::error(
                            code::CONFLICT,
                            format!("stream '{name}' exists with a different config"),
                        ),
                    }
                }
                Err(e) => log_error_response(&ctx, e),
            };
            out.send(corr, resp).await;
        }
        Request::UpdateStream(spec) => {
            let name = match stream_name(&spec.name) {
                Ok(n) => n,
                Err(r) => return out.send(corr, r).await,
            };
            if !identity.authorize(Action::Admin, &name) {
                return forbid(&ctx, out, corr, "UpdateStream").await;
            }
            let resp = match broker
                .log
                .update_stream_config(
                    &name,
                    &stream_config(&spec, broker.log.default_dedup_window_secs()),
                )
                .await
            {
                Ok(()) => Response::Ok,
                Err(e) => log_error_response(&ctx, e),
            };
            out.send(corr, resp).await;
        }
        Request::DeleteStream { name } => {
            let name = match stream_name(&name) {
                Ok(n) => n,
                Err(r) => return out.send(corr, r).await,
            };
            if name.is_internal() || !identity.authorize(Action::Admin, &name) {
                return forbid(&ctx, out, corr, "DeleteStream").await;
            }
            let users = broker.consumers.consumers_of(name.as_str()).await;
            let resp = if !users.is_empty() {
                Response::error_with(
                    code::CONFLICT,
                    "stream has consumers; delete them first",
                    serde_json::json!({ "consumers": users }),
                )
            } else {
                match broker.delete_stream(&name).await {
                    Ok(()) => Response::Ok,
                    Err(e) => log_error_response(&ctx, e),
                }
            };
            out.send(corr, resp).await;
        }
        Request::StreamInfo { name } => {
            let name = match stream_name(&name) {
                Ok(n) => n,
                Err(r) => return out.send(corr, r).await,
            };
            if !identity.authorize(Action::Admin, &name)
                && !identity.authorize(Action::Subscribe, &name)
            {
                return forbid(&ctx, out, corr, "StreamInfo").await;
            }
            let resp = match stream_info(&ctx, &name).await {
                Ok(v) => Response::json(&v),
                Err(e) => log_error_response(&ctx, e.into()),
            };
            out.send(corr, resp).await;
        }
        Request::ListStreams => {
            let resp = match broker.storage.list_streams().await {
                Ok(mut names) => {
                    names.sort_by(|a, b| a.as_str().cmp(b.as_str()));
                    let mut infos = Vec::new();
                    for n in names {
                        if n.is_internal() && !identity.has_global_admin() {
                            continue;
                        }
                        if identity.authorize(Action::Admin, &n)
                            || identity.authorize(Action::Subscribe, &n)
                            || identity.authorize(Action::Publish, &n)
                        {
                            if let Ok(v) = stream_info(&ctx, &n).await {
                                infos.push(v);
                            }
                        }
                    }
                    Response::json(&infos)
                }
                Err(e) => log_error_response(&ctx, e.into()),
            };
            out.send(corr, resp).await;
        }

        // ---- SQL -----------------------------------------------------------
        Request::Query { sql } => {
            // A query can read any stream (and registered external
            // databases), so it needs global admin.
            if !identity.has_global_admin() {
                return forbid(&ctx, out, corr, "Query").await;
            }
            let Ok(permit) = waits.clone().try_acquire_owned() else {
                return out
                    .send(
                        corr,
                        Response::error(code::TOO_MANY_REQUESTS, "too many concurrent requests"),
                    )
                    .await;
            };
            let out = out.clone();
            let ctx = ctx.clone();
            let closed = state.closed.clone();
            tokio::spawn(async move {
                let _permit = permit;
                // Dropping the query future on disconnect cancels execution.
                tokio::select! {
                    resp = run_query(&ctx, &sql) => out.send(corr, resp).await,
                    _ = closed.cancelled() => {}
                }
            });
        }

        // ---- Consumers -----------------------------------------------------
        Request::CreateConsumer(spec) => {
            let resp = create_consumer(&ctx, identity, state, spec).await;
            out.send(corr, resp).await;
        }
        Request::DeleteConsumer { name } => {
            if let Err(r) = authorize_consumer(&ctx, identity, &name, Action::Admin, false).await {
                return out.send(corr, r).await;
            }
            let resp = match broker.consumers.delete(&name).await {
                Ok(()) => Response::Ok,
                Err(e) => consumer_error_response(&ctx, e),
            };
            state.ephemeral.retain(|n| n != &name);
            out.send(corr, resp).await;
        }
        Request::ConsumerInfo { name } => {
            if let Err(r) = authorize_consumer(&ctx, identity, &name, Action::Subscribe, true).await
            {
                return out.send(corr, r).await;
            }
            let resp = match broker.consumers.info(&name).await {
                Ok(i) => Response::json(&i),
                Err(e) => consumer_error_response(&ctx, e),
            };
            out.send(corr, resp).await;
        }
        Request::ListConsumers { stream } => {
            let resp = match broker.consumers.list(stream.as_deref()).await {
                Ok(list) => {
                    let visible: Vec<_> = list
                        .into_iter()
                        .filter(|i| {
                            StreamName::try_from(i.spec.stream.as_str())
                                .map(|n| {
                                    identity.authorize(Action::Subscribe, &n)
                                        || identity.authorize(Action::Admin, &n)
                                })
                                .unwrap_or(false)
                        })
                        .collect();
                    Response::json(&visible)
                }
                Err(e) => consumer_error_response(&ctx, e),
            };
            out.send(corr, resp).await;
        }
        Request::SeekConsumer { consumer, to } => {
            if let Err(r) =
                authorize_consumer(&ctx, identity, &consumer, Action::Subscribe, true).await
            {
                return out.send(corr, r).await;
            }
            let resp = match broker.consumers.seek(&consumer, to).await {
                Ok(()) => Response::Ok,
                Err(e) => consumer_error_response(&ctx, e),
            };
            out.send(corr, resp).await;
        }
        Request::Subscribe { consumer, credits } => {
            if let Err(r) =
                authorize_consumer(&ctx, identity, &consumer, Action::Subscribe, false).await
            {
                return out.send(corr, r).await;
            }
            let mut sub = match broker.consumers.subscribe(&consumer, credits).await {
                Ok(s) => s,
                Err(e) => return out.send(corr, consumer_error_response(&ctx, e)).await,
            };
            let sub_id = sub.sub_id;
            // Reply before the forwarder starts so SubscribeOk precedes any
            // Deliver frame on the wire.
            out.send(corr, Response::SubscribeOk { sub_id }).await;
            let fwd_out = out.clone();
            let forwarder = tokio::spawn(async move {
                while let Some(ev) = sub.events.recv().await {
                    let frame = match ev {
                        SubEvent::Deliver(records) => Response::deliver_frame(sub_id, records),
                        SubEvent::Ended { code, message } => {
                            fwd_out
                                .send(
                                    0,
                                    Response::SubscriptionEnded {
                                        sub_id,
                                        code,
                                        message,
                                    },
                                )
                                .await;
                            return;
                        }
                    };
                    if fwd_out.0.send(frame).await.is_err() {
                        return;
                    }
                }
            });
            state.subs.insert(
                sub_id,
                SubEntry {
                    consumer,
                    forwarder,
                },
            );
        }
        Request::Credit { sub_id, credits } => {
            let Some(entry) = state.subs.get(&sub_id) else {
                return out
                    .send(
                        corr,
                        Response::error(code::NOT_FOUND, "unknown subscription"),
                    )
                    .await;
            };
            match broker
                .consumers
                .credit(&entry.consumer, sub_id, credits)
                .await
            {
                Ok(()) => reply_ok(corr).await,
                Err(e) => out.send(corr, consumer_error_response(&ctx, e)).await,
            }
        }
        Request::Unsubscribe { sub_id } => {
            if sub_id & CORE_SUB_ID_BIT != 0 {
                if let Some(i) = state.core_subs.iter().position(|&s| s == sub_id) {
                    state.core_subs.swap_remove(i);
                    broker.bus.unsubscribe(sub_id);
                }
            } else if let Some(entry) = state.subs.remove(&sub_id) {
                entry.forwarder.abort();
                broker.consumers.unsubscribe(&entry.consumer, sub_id).await;
            }
            reply_ok(corr).await;
        }

        // ---- Key-value buckets ---------------------------------------------
        Request::KvCreateBucket {
            bucket,
            history,
            ttl_ms,
            max_bytes,
        } => {
            if let Err(r) = authorize_bucket(&ctx, identity, &bucket, Action::Admin) {
                return out.send(corr, r).await;
            }
            let cfg = BucketConfig {
                history: history.max(1),
                ttl_ms,
                max_bytes,
            };
            match broker.kv.create_bucket(&bucket, &cfg).await {
                Ok(()) => out.send(corr, Response::Ok).await,
                Err(e) => out.send(corr, kv_error_response(&ctx, e)).await,
            }
        }
        Request::KvPut {
            bucket,
            key,
            value,
            expected_revision,
            ttl_ms,
        } => {
            if let Err(r) = authorize_bucket(&ctx, identity, &bucket, Action::Publish) {
                return out.send(corr, r).await;
            }
            match broker
                .kv
                .put(&bucket, &key, value, expected_revision, ttl_ms)
                .await
            {
                Ok(rev) => {
                    out.send(
                        corr,
                        Response::PublishOk {
                            offset: rev,
                            duplicate: false,
                        },
                    )
                    .await
                }
                Err(e) => out.send(corr, kv_error_response(&ctx, e)).await,
            }
        }
        Request::KvDelete {
            bucket,
            key,
            purge,
            expected_revision,
        } => {
            if let Err(r) = authorize_bucket(&ctx, identity, &bucket, Action::Publish) {
                return out.send(corr, r).await;
            }
            match broker
                .kv
                .delete(&bucket, &key, purge, expected_revision)
                .await
            {
                Ok(rev) => {
                    out.send(
                        corr,
                        Response::PublishOk {
                            offset: rev,
                            duplicate: false,
                        },
                    )
                    .await
                }
                Err(e) => out.send(corr, kv_error_response(&ctx, e)).await,
            }
        }
        Request::KvGet {
            bucket,
            key,
            revision,
        } => {
            if let Err(r) = authorize_bucket(&ctx, identity, &bucket, Action::Subscribe) {
                return out.send(corr, r).await;
            }
            match broker.kv.get(&bucket, &key, revision).await {
                Ok(Some(e)) => {
                    out.send(
                        corr,
                        Response::Messages {
                            records: vec![kv_wire(e)],
                        },
                    )
                    .await
                }
                Ok(None) => {
                    out.send(
                        corr,
                        Response::error(code::NOT_FOUND, format!("key '{key}' not found")),
                    )
                    .await
                }
                Err(e) => out.send(corr, kv_error_response(&ctx, e)).await,
            }
        }
        Request::KvKeys { bucket, filter } => {
            if let Err(r) = authorize_bucket(&ctx, identity, &bucket, Action::Subscribe) {
                return out.send(corr, r).await;
            }
            match broker.kv.keys(&bucket, &filter).await {
                Ok(keys) => out.send(corr, Response::json(&keys)).await,
                Err(e) => out.send(corr, kv_error_response(&ctx, e)).await,
            }
        }
        Request::KvHistory { bucket, key } => {
            if let Err(r) = authorize_bucket(&ctx, identity, &bucket, Action::Subscribe) {
                return out.send(corr, r).await;
            }
            match broker.kv.history(&bucket, &key).await {
                Ok(entries) => {
                    out.send(
                        corr,
                        Response::Messages {
                            records: entries.into_iter().map(kv_wire).collect(),
                        },
                    )
                    .await
                }
                Err(e) => out.send(corr, kv_error_response(&ctx, e)).await,
            }
        }

        // ---- Core messaging (non-persistent) -------------------------------
        Request::CoreSubscribe { subject, queue } => {
            let filter = match SubjectFilter::parse(&subject) {
                Ok(f) if !subject.is_empty() => f,
                Ok(_) => {
                    return out
                        .send(
                            corr,
                            Response::error(code::BAD_REQUEST, "a subject filter is required"),
                        )
                        .await
                }
                Err(e) => return out.send(corr, Response::error(code::BAD_REQUEST, e)).await,
            };
            if !identity.authorize_subject(Action::Subscribe, &filter) {
                return forbid(&ctx, out, corr, "CoreSubscribe").await;
            }
            if queue.as_ref().is_some_and(|q| q.len() > 256) {
                return out
                    .send(
                        corr,
                        Response::error(code::BAD_REQUEST, "queue group name over 256 bytes"),
                    )
                    .await;
            }
            let tx = state.core_tx.get_or_insert_with(|| {
                let (tx, rx) = mpsc::channel(CORE_QUEUE);
                state.core_forwarder = Some(tokio::spawn(forward_core(rx, out.clone())));
                tx
            });
            match broker.bus.subscribe(filter, queue, tx.clone()) {
                Ok(sub_id) => {
                    state.core_subs.push(sub_id);
                    out.send(corr, Response::SubscribeOk { sub_id }).await;
                }
                Err(BusError::NotLeader) => out.send(corr, not_leader(&ctx)).await,
                Err(e) => {
                    out.send(corr, Response::error(code::INTERNAL, e.to_string()))
                        .await
                }
            }
        }
        Request::CorePublish {
            subject,
            reply_to,
            headers,
            value,
        } => {
            let filter = match check_core_subject(&subject, "subject").and_then(|f| match &reply_to
            {
                Some(r) => check_core_subject(r, "reply_to").map(|_| f),
                None => Ok(f),
            }) {
                Ok(f) => f,
                Err(r) => return out.send(corr, r).await,
            };
            if !identity.authorize_subject(Action::Publish, &filter) {
                return forbid(&ctx, out, corr, "CorePublish").await;
            }
            let probe = Record {
                key: None,
                value: value.clone(),
                subject: subject.clone(),
                headers: headers.clone(),
                timestamp_ns: None,
            };
            if let Err(e) = broker.log.limits().check(&probe) {
                return out.send(corr, Response::error(code::BAD_REQUEST, e)).await;
            }
            let msg = CoreMessage {
                subject,
                reply_to,
                headers,
                value,
            };
            match broker.bus.publish(msg) {
                Ok(_) => reply_ok(corr).await,
                Err(BusError::NotLeader) => out.send(corr, not_leader(&ctx)).await,
                Err(e @ BusError::NoResponders(_)) => {
                    out.send(corr, Response::error(code::NOT_FOUND, e.to_string()))
                        .await
                }
            }
        }
        Request::Pull {
            consumer,
            max_messages,
            max_bytes,
            expires_ms,
        } => {
            if let Err(r) =
                authorize_consumer(&ctx, identity, &consumer, Action::Subscribe, false).await
            {
                return out.send(corr, r).await;
            }
            let Ok(permit) = waits.clone().try_acquire_owned() else {
                return out
                    .send(
                        corr,
                        Response::error(code::TOO_MANY_REQUESTS, "too many concurrent requests"),
                    )
                    .await;
            };
            let out = out.clone();
            let ctx = ctx.clone();
            let closed = state.closed.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let pull = ctx.broker.consumers.pull(
                    &consumer,
                    max_messages,
                    max_bytes,
                    Duration::from_millis(expires_ms as u64),
                );
                // On disconnect, dropping the pull drops its waiter; the
                // consumer skips waiters whose caller is gone, so no records
                // are stranded (any already handed out come back after
                // ack_wait).
                let frame = tokio::select! {
                    r = pull => match r {
                        Ok(records) => Response::messages_frame(corr, records),
                        Err(e) => consumer_error_response(&ctx, e).into_frame(corr).into(),
                    },
                    _ = closed.cancelled() => return,
                };
                out.send_frame(frame).await;
            });
        }
        Request::Ack { consumer, offsets } => {
            if let Err(r) =
                authorize_consumer(&ctx, identity, &consumer, Action::Subscribe, false).await
            {
                return out.send(corr, r).await;
            }
            match broker.consumers.ack(&consumer, offsets).await {
                Ok(()) => reply_ok(corr).await,
                Err(e) => out.send(corr, consumer_error_response(&ctx, e)).await,
            }
        }
        Request::Nack {
            consumer,
            offset,
            delay_ms,
        } => {
            if let Err(r) =
                authorize_consumer(&ctx, identity, &consumer, Action::Subscribe, false).await
            {
                return out.send(corr, r).await;
            }
            match broker.consumers.nack(&consumer, offset, delay_ms).await {
                Ok(()) => reply_ok(corr).await,
                Err(e) => out.send(corr, consumer_error_response(&ctx, e)).await,
            }
        }
        Request::Term {
            consumer,
            offset,
            reason,
        } => {
            if let Err(r) =
                authorize_consumer(&ctx, identity, &consumer, Action::Subscribe, false).await
            {
                return out.send(corr, r).await;
            }
            match broker.consumers.term(&consumer, offset, reason).await {
                Ok(()) => reply_ok(corr).await,
                Err(e) => out.send(corr, consumer_error_response(&ctx, e)).await,
            }
        }
        Request::InProgress { consumer, offsets } => {
            if let Err(r) =
                authorize_consumer(&ctx, identity, &consumer, Action::Subscribe, false).await
            {
                return out.send(corr, r).await;
            }
            match broker.consumers.in_progress(&consumer, offsets).await {
                Ok(()) => reply_ok(corr).await,
                Err(e) => out.send(corr, consumer_error_response(&ctx, e)).await,
            }
        }

        // ---- Stateless read ------------------------------------------------
        Request::Read {
            stream,
            from,
            max_records,
            max_bytes,
            wait_ms,
            filter,
        } => {
            let name = match stream_name(&stream) {
                Ok(n) => n,
                Err(r) => return out.send(corr, r).await,
            };
            if !identity.authorize(Action::Subscribe, &name) {
                return forbid(&ctx, out, corr, "Read").await;
            }
            let filter = match SubjectFilter::parse(&filter) {
                Ok(f) => f,
                Err(e) => return out.send(corr, Response::error(code::BAD_REQUEST, e)).await,
            };
            let Ok(permit) = waits.clone().try_acquire_owned() else {
                return out
                    .send(
                        corr,
                        Response::error(code::TOO_MANY_REQUESTS, "too many concurrent requests"),
                    )
                    .await;
            };
            let out = out.clone();
            let ctx = ctx.clone();
            let closed = state.closed.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let resp = read(
                    &ctx,
                    corr,
                    &name,
                    from,
                    max_records,
                    max_bytes,
                    Duration::from_millis(wait_ms as u64),
                    &filter,
                );
                tokio::select! {
                    frame = resp => out.send_frame(frame).await,
                    _ = closed.cancelled() => {}
                }
            });
        }
    }
}

async fn create_consumer(
    ctx: &SessionContext,
    identity: &Identity,
    state: &mut ConnState,
    spec: ConsumerSpec,
) -> Response {
    let stream = match stream_name(&spec.stream) {
        Ok(n) => n,
        Err(r) => return r,
    };
    if !identity.authorize(Action::Subscribe, &stream)
        && !identity.authorize(Action::Admin, &stream)
    {
        ctx.metrics
            .auth_denied("forbidden", "tcp", "CreateConsumer");
        return Response::error(code::FORBIDDEN, "forbidden");
    }
    // Dead-lettering writes to another stream on the caller's behalf.
    if let Some(dlq) = &spec.dlq_stream {
        match StreamName::try_from(dlq.as_str()) {
            Ok(d) if !d.is_internal() && identity.authorize(Action::Publish, &d) => {}
            Ok(_) => {
                ctx.metrics
                    .auth_denied("forbidden", "tcp", "CreateConsumer");
                return Response::error(code::FORBIDDEN, "no publish permission on dlq_stream");
            }
            Err(e) => return Response::error(code::BAD_REQUEST, format!("dlq_stream: {e}")),
        }
    }
    let ephemeral = spec.ephemeral;
    let name = spec.name.clone();
    match ctx.broker.consumers.create(spec).await {
        Ok(info) => {
            if ephemeral && !state.ephemeral.contains(&name) {
                state.ephemeral.push(name);
            }
            Response::json(&info)
        }
        Err(e) => consumer_error_response(ctx, e),
    }
}

async fn stream_info(
    ctx: &SessionContext,
    name: &StreamName,
) -> Result<serde_json::Value, StorageError> {
    let storage = &ctx.broker.storage;
    let (earliest, next) = storage.stream_bounds(name).await?;
    let config = storage.stream_config(name).await?;
    Ok(serde_json::json!({
        "name": name.as_str(),
        "earliest_offset": earliest.0,
        "next_offset": next.0,
        "records": next.0.saturating_sub(earliest.0),
        "config": config,
        "internal": name.is_internal(),
    }))
}

async fn run_query(ctx: &SessionContext, sql: &str) -> Response {
    match ctx.exql.execute_bounded(sql).await {
        Ok(rs) => Response::json(&rs.to_json()),
        Err(e) => Response::Error {
            code: e.http_status(),
            message: e.to_string(),
            detail: Some(Bytes::from(e.to_json().to_string())),
        },
    }
}

/// Stateless read with subject filtering and long-polling.
///
/// Records go from the segment to the socket without being decoded: the
/// storage returns them in wire encoding ([`exspeed_streams::RawBatch`]) and
/// the reply carries zero-copy slices of that buffer. With a subject filter
/// only each record's subject is parsed (in place), and runs of matching
/// records become the reply's chunks.
#[allow(clippy::too_many_arguments)]
async fn read(
    ctx: &SessionContext,
    corr: u32,
    name: &StreamName,
    from: u64,
    max_records: u32,
    max_bytes: u32,
    wait: Duration,
    filter: &SubjectFilter,
) -> OutFrame {
    let storage = &ctx.broker.storage;
    let limits = ReadLimits {
        max_records: (max_records as usize).clamp(1, 10_000),
        max_bytes: if max_bytes == 0 {
            1024 * 1024
        } else {
            (max_bytes as usize).min(exspeed_common::MAX_RECORDS_BYTES_PER_FRAME)
        },
    };
    let deadline = tokio::time::Instant::now() + wait.min(Duration::from_secs(300));
    let mut watch = storage.watch_appends(name);
    let mut cursor = Offset(from);
    loop {
        // Scan forward (bounded) until something matches the filter or we
        // reach the end of the log.
        let mut matched = EncodedRecords::new();
        let mut high_watermark = cursor;
        for _ in 0..16 {
            let batch = match storage.read_raw(name, cursor, limits).await {
                Ok(b) => b,
                Err(e) => return log_error_response(ctx, e.into()).into_frame(corr).into(),
            };
            high_watermark = batch.high_watermark;
            cursor = batch.next_offset;
            let count = batch.count;
            let bytes = batch.bytes.freeze();
            if filter.is_all() {
                matched.push_chunk(bytes, count as u32);
            } else {
                take_matching(
                    &bytes,
                    filter,
                    limits.max_records,
                    &mut matched,
                    &mut cursor,
                );
            }
            if !matched.is_empty() || count == 0 || cursor >= high_watermark {
                break;
            }
        }
        let caught_up = cursor >= high_watermark;
        if !matched.is_empty() || !caught_up || tokio::time::Instant::now() >= deadline {
            return Response::read_result_frame(corr, cursor.0, high_watermark.0, matched);
        }
        // Long-poll until new data or the deadline.
        let woke = match watch.as_mut() {
            Some(w) => tokio::time::timeout_at(deadline, w.changed()).await.is_ok(),
            None => {
                let step = tokio::time::Instant::now() + Duration::from_millis(25);
                tokio::time::sleep_until(step.min(deadline)).await;
                true
            }
        };
        if !woke && tokio::time::Instant::now() >= deadline {
            return Response::read_result_frame(
                corr,
                cursor.0,
                high_watermark.0,
                EncodedRecords::new(),
            );
        }
    }
}

/// Append the records of `bytes` whose subject matches `filter` to `out`
/// as zero-copy slices (one per run of consecutive matches), stopping at
/// `max_records`. When it stops early, `cursor` moves to just after the
/// last record taken.
fn take_matching(
    bytes: &Bytes,
    filter: &SubjectFilter,
    max_records: usize,
    out: &mut EncodedRecords,
    cursor: &mut Offset,
) {
    let mut run: Option<(usize, usize, u32)> = None;
    for p in record_format::iter(bytes) {
        let Ok(p) = p else { break };
        let rec = &bytes[p.range()];
        if record_format::subject(rec).is_ok_and(|s| filter.matches(s)) {
            run = match run {
                Some((start, end, n)) if end == p.start => Some((start, p.end(), n + 1)),
                other => {
                    if let Some((start, end, n)) = other {
                        out.push_chunk(bytes.slice(start..end), n);
                    }
                    Some((p.start, p.end(), 1))
                }
            };
            let pending = run.map_or(0, |r| r.2);
            if out.count() as usize + pending as usize >= max_records {
                // Stopped mid-batch: resume right after this record.
                *cursor = Offset(p.offset + 1);
                break;
            }
        } else if let Some((start, end, n)) = run.take() {
            out.push_chunk(bytes.slice(start..end), n);
        }
    }
    if let Some((start, end, n)) = run {
        out.push_chunk(bytes.slice(start..end), n);
    }
}
