//! A core NATS protocol listener: NATS clients (any language) publish,
//! subscribe, use queue groups and request-reply against the same core
//! message bus as Exspeed's own `CorePublish` / `CoreSubscribe`, so the two
//! protocols see each other's messages.
//!
//! Supported: `INFO`, `CONNECT` (token, user/password as token, client
//! certificates), `PUB`, `HPUB`, `SUB` (queue groups), `UNSUB` (with an
//! auto-unsubscribe count), `MSG`, `HMSG`, `PING`/`PONG` (and server pings
//! to find dead connections), `+OK` in verbose mode, `echo: false`, and the
//! `no_responders` 503 status for requests nobody is subscribed to.
//! JetStream, accounts, clustering protocols and TLS-first handshakes are
//! not supported.
//!
//! Core messaging runs on the leader. A standby closes NATS connections
//! right after `INFO`, and when leadership moves every NATS connection is
//! closed, so clients reconnect to another server in their list.

pub mod proto;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio_rustls::rustls::ServerConfig;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, info_span, warn, Instrument};

use exspeed_broker::capture::{Capture, CaptureJob};
use exspeed_broker::pubsub::{BusError, CoreEvent, CoreMessage};
use exspeed_common::auth::{Action, Identity};
use exspeed_common::SubjectFilter;
use exspeed_streams::Record;

use crate::session::{anonymous_identity, SessionContext};
use proto::{encode_err, encode_headers, encode_msg, ClientOp, ConnectOptions};

/// How often the server pings an idle client.
const PING_INTERVAL: Duration = Duration::from_secs(60);
/// Unanswered pings before the connection is considered stale.
const MAX_PINGS_OUT: u32 = 2;
/// Core messages queued for one connection before new ones are dropped.
const CORE_QUEUE: usize = 65_536;
/// Encoded chunks queued for the socket writer.
const OUTBOUND: usize = 1024;
const READ_CHUNK: usize = 64 * 1024;
/// Core messages encoded into one write at most.
const DELIVERY_BATCH: usize = 4096;

/// Connection ids, unique per process (also the `origin` of published core
/// messages, for `echo: false`).
static NEXT_CID: AtomicU64 = AtomicU64::new(1);

/// What the listener needs besides the session context.
pub struct NatsServer {
    pub ctx: Arc<SessionContext>,
    pub tls: Option<Arc<ServerConfig>>,
    /// Client certificates are required (`tls.client_ca`).
    pub tls_verify: bool,
    pub conn_sem: Arc<Semaphore>,
}

impl NatsServer {
    /// Accept NATS connections until `cancel` fires.
    pub async fn serve(self: Arc<Self>, listener: TcpListener, cancel: CancellationToken) {
        loop {
            let (socket, peer) = tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                r = listener.accept() => match r {
                    Ok(v) => v,
                    Err(e) => {
                        error!("NATS accept error: {e}");
                        continue;
                    }
                },
            };
            let Ok(permit) = self.conn_sem.clone().try_acquire_owned() else {
                self.ctx.metrics.connection_rejected();
                warn!(%peer, "NATS connection rejected: max_connections reached");
                continue;
            };
            let _ = socket.set_nodelay(true);
            self.ctx.metrics.connection_opened();
            let this = self.clone();
            let token = cancel.child_token();
            let span = info_span!("nats", %peer, identity = tracing::field::Empty);
            tokio::spawn(
                async move {
                    let _permit = permit;
                    if let Err(e) = this.handle(socket, peer, token).await {
                        debug!(%peer, "NATS connection error: {e}");
                    }
                    this.ctx.metrics.connection_closed();
                }
                .instrument(span),
            );
        }
    }

    async fn handle(
        &self,
        mut socket: TcpStream,
        peer: SocketAddr,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let cid = NEXT_CID.fetch_add(1, Ordering::Relaxed);
        let local = socket.local_addr()?;
        let limits = *self.ctx.broker.log.limits();
        let max_payload = limits.max_value_bytes;
        let info = json!({
            "server_id": self.ctx.node_id,
            "server_name": format!("exspeed-{}", self.ctx.node_id),
            "version": env!("CARGO_PKG_VERSION"),
            "proto": 1,
            "go": "exspeed",
            "host": local.ip().to_string(),
            "port": local.port(),
            "headers": true,
            "max_payload": max_payload,
            "client_id": cid,
            "client_ip": peer.ip().to_string(),
            "auth_required": self.ctx.credential_store.is_some(),
            "tls_required": self.tls.is_some(),
            "tls_verify": self.tls_verify,
        });
        socket
            .write_all(format!("INFO {info}\r\n").as_bytes())
            .await?;
        if !self.ctx.broker.bus.is_open() {
            // A standby: the client moves on to the next server in its list.
            info!(%peer, "NATS connection to a standby closed; core messaging runs on the leader");
            return Ok(());
        }
        let conn = Conn {
            ctx: self.ctx.clone(),
            cid,
            peer,
            // Headers plus their framing may come on top of the payload.
            max_body: max_payload + limits.max_total_header_bytes + 4 * limits.max_headers + 16,
        };
        match &self.tls {
            Some(cfg) => {
                let acceptor = tokio_rustls::TlsAcceptor::from(cfg.clone());
                let tls = tokio::time::timeout(self.ctx.handshake_timeout, acceptor.accept(socket))
                    .await
                    .map_err(|_| anyhow::anyhow!("TLS handshake timed out"))??;
                let cert_name =
                    crate::cli::server_tls::client_cert_name(tls.get_ref().1.peer_certificates());
                conn.run(tls, cert_name, cancel).await
            }
            None => conn.run(socket, None, cancel).await,
        }
    }
}

/// One subscription of a connection.
struct Sub {
    sid: String,
    bus_id: u32,
    /// Auto-unsubscribe after this many messages (`UNSUB <sid> <max>`).
    max: Option<u64>,
    delivered: u64,
}

struct Conn {
    ctx: Arc<SessionContext>,
    cid: u64,
    peer: SocketAddr,
    max_body: usize,
}

/// Why the session ends.
enum Close {
    /// Clean end (client closed, cancel, leadership moved).
    Quiet,
    /// Send this `-ERR` first.
    Err(String),
}

struct State {
    opts: ConnectOptions,
    identity: Option<Arc<Identity>>,
    subs: HashMap<String, Sub>,
    by_bus: HashMap<u32, String>,
    core_tx: mpsc::Sender<CoreEvent>,
    /// Appends captured messages in publish order (started on first use).
    capture_tx: Option<mpsc::Sender<CaptureJob>>,
    pings_out: u32,
}

impl State {
    fn remove(&mut self, bus: &exspeed_broker::pubsub::CoreBus, sid: &str) {
        if let Some(s) = self.subs.remove(sid) {
            self.by_bus.remove(&s.bus_id);
            bus.unsubscribe(s.bus_id);
        }
    }
}

impl Conn {
    async fn run<S>(
        self,
        stream: S,
        cert_name: Option<String>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut reader, writer) = tokio::io::split(stream);
        let (out_tx, mut out_rx) = mpsc::channel::<Bytes>(OUTBOUND);
        let writer_task = tokio::spawn(async move {
            let mut w = BufWriter::with_capacity(64 * 1024, writer);
            while let Some(b) = out_rx.recv().await {
                if w.write_all(&b).await.is_err() {
                    return;
                }
                while let Ok(b) = out_rx.try_recv() {
                    if w.write_all(&b).await.is_err() {
                        return;
                    }
                }
                if w.flush().await.is_err() {
                    return;
                }
            }
            let _ = w.shutdown().await;
        });

        let (core_tx, mut core_rx) = mpsc::channel::<CoreEvent>(CORE_QUEUE);
        let mut st = State {
            opts: ConnectOptions::default(),
            identity: None,
            subs: HashMap::new(),
            by_bus: HashMap::new(),
            core_tx,
            capture_tx: None,
            pings_out: 0,
        };
        let mut buf = BytesMut::with_capacity(READ_CHUNK);
        let connect_deadline = tokio::time::sleep(self.ctx.handshake_timeout);
        tokio::pin!(connect_deadline);
        let mut ping =
            tokio::time::interval_at(tokio::time::Instant::now() + PING_INTERVAL, PING_INTERVAL);

        let close = 'session: loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    // Lame-duck notice: clients reconnect elsewhere at once.
                    let _ = out_tx.send(Bytes::from_static(b"INFO {\"ldm\":true}\r\n")).await;
                    break Close::Quiet;
                }
                _ = &mut connect_deadline, if st.identity.is_none() => {
                    break Close::Err("Authentication Timeout".into());
                }
                _ = ping.tick(), if st.identity.is_some() => {
                    if st.pings_out >= MAX_PINGS_OUT {
                        break Close::Err("Stale Connection".into());
                    }
                    st.pings_out += 1;
                    let _ = out_tx.send(Bytes::from_static(b"PING\r\n")).await;
                }
                // Deliveries go first, in batches: a client publishing
                // faster than its subscriptions are written waits (TCP
                // backpressure) instead of its subscriptions losing messages.
                ev = core_rx.recv() => {
                    let mut chunk = BytesMut::new();
                    let mut next = ev;
                    let mut taken = 0;
                    let ended = loop {
                        match next {
                            None => break true,
                            Some(CoreEvent::Message { sub_id, msg }) => {
                                self.deliver(&mut st, sub_id, &msg, &mut chunk);
                            }
                            // Leadership moved: let the client reconnect.
                            Some(CoreEvent::Ended { .. }) => break true,
                        }
                        taken += 1;
                        if taken >= DELIVERY_BATCH || chunk.len() >= READ_CHUNK {
                            break false;
                        }
                        match core_rx.try_recv() {
                            Ok(ev) => next = Some(ev),
                            Err(_) => break false,
                        }
                    };
                    if !chunk.is_empty() && out_tx.send(chunk.freeze()).await.is_err() {
                        break Close::Quiet;
                    }
                    if ended {
                        break Close::Quiet;
                    }
                }
                n = reader.read_buf(&mut buf) => {
                    match n {
                        Ok(0) | Err(_) => break Close::Quiet,
                        Ok(_) => {}
                    }
                    loop {
                        let op = match proto::parse(&mut buf, self.max_body) {
                            Ok(Some(op)) => op,
                            Ok(None) => break,
                            Err(e) => break 'session Close::Err(e.message),
                        };
                        match self.handle_op(&mut st, op, cert_name.as_deref(), &out_tx).await {
                            Ok(()) => {}
                            Err(c) => break 'session c,
                        }
                    }
                    if buf.capacity() - buf.len() < 4096 {
                        buf.reserve(READ_CHUNK);
                    }
                }
            }
        };

        let bus = &self.ctx.broker.bus;
        for s in st.subs.values() {
            bus.unsubscribe(s.bus_id);
        }
        if let Close::Err(m) = &close {
            debug!(peer = %self.peer, "closing NATS connection: {m}");
            let _ = out_tx.send(encode_err(m)).await;
        }
        drop(out_tx);
        let _ = tokio::time::timeout(Duration::from_secs(5), writer_task).await;
        Ok(())
    }

    /// Encode a core message for a subscription into `out` (skipped for an
    /// unknown subscription or an echo the client opted out of).
    fn deliver(&self, st: &mut State, bus_id: u32, msg: &CoreMessage, out: &mut BytesMut) {
        if !st.opts.echo && msg.origin == self.cid {
            return;
        }
        let Some(sid) = st.by_bus.get(&bus_id).cloned() else {
            return;
        };
        let headers = (st.opts.headers && !msg.headers.is_empty())
            .then(|| encode_headers(&msg.headers, None));
        encode_msg(
            out,
            &msg.subject,
            &sid,
            msg.reply_to.as_deref(),
            headers.as_deref(),
            &msg.value,
        );
        self.count_delivery(st, &sid);
    }

    /// Count a delivery and auto-unsubscribe at the subscription's max.
    fn count_delivery(&self, st: &mut State, sid: &str) {
        let done = match st.subs.get_mut(sid) {
            Some(s) => {
                s.delivered += 1;
                s.max.is_some_and(|m| s.delivered >= m)
            }
            None => false,
        };
        if done {
            st.remove(&self.ctx.broker.bus, sid);
        }
    }

    async fn handle_op(
        &self,
        st: &mut State,
        op: ClientOp,
        cert_name: Option<&str>,
        out: &mpsc::Sender<Bytes>,
    ) -> Result<(), Close> {
        let send = |b: Bytes| async move { out.send(b).await.map_err(|_| Close::Quiet) };
        let verbose = st.opts.verbose;
        let ok = || async move {
            if verbose {
                out.send(Bytes::from_static(b"+OK\r\n"))
                    .await
                    .map_err(|_| Close::Quiet)?;
            }
            Ok::<(), Close>(())
        };
        if st.identity.is_none() {
            let ClientOp::Connect(opts) = op else {
                return Err(Close::Err("Authorization Violation".into()));
            };
            let identity = self.authenticate(&opts, cert_name)?;
            tracing::Span::current().record("identity", identity.name.as_str());
            info!(
                peer = %self.peer,
                name = opts.name.as_deref().unwrap_or(""),
                lang = opts.lang.as_deref().unwrap_or(""),
                identity = %identity.name,
                "NATS client connected"
            );
            st.opts = *opts;
            st.identity = Some(identity);
            if st.opts.verbose {
                send(Bytes::from_static(b"+OK\r\n")).await?;
            }
            return Ok(());
        }
        let identity = st.identity.clone().expect("connected");
        let bus = &self.ctx.broker.bus;
        match op {
            ClientOp::Connect(_) => ok().await,
            ClientOp::Ignored => Ok(()),
            ClientOp::Ping => send(Bytes::from_static(b"PONG\r\n")).await,
            ClientOp::Pong => {
                st.pings_out = 0;
                Ok(())
            }
            ClientOp::Sub {
                subject,
                queue,
                sid,
            } => {
                let filter = match SubjectFilter::parse(&subject) {
                    Ok(f) if !subject.is_empty() => f,
                    _ => return send(encode_err("Invalid Subject")).await,
                };
                if !identity.authorize_subject(Action::Subscribe, &filter) {
                    self.ctx.metrics.auth_denied("forbidden", "nats", "SUB");
                    return send(encode_err(&format!(
                        "Permissions Violation for Subscription to \"{subject}\""
                    )))
                    .await;
                }
                if st.subs.contains_key(&sid) {
                    return ok().await;
                }
                match bus.subscribe(filter, queue, st.core_tx.clone()) {
                    Ok(bus_id) => {
                        st.by_bus.insert(bus_id, sid.clone());
                        st.subs.insert(
                            sid.clone(),
                            Sub {
                                sid,
                                bus_id,
                                max: None,
                                delivered: 0,
                            },
                        );
                        ok().await
                    }
                    Err(_) => Err(Close::Quiet),
                }
            }
            ClientOp::Unsub { sid, max } => {
                let remove_now = match st.subs.get_mut(&sid) {
                    Some(s) => match max {
                        Some(m) if s.delivered < m => {
                            s.max = Some(m);
                            false
                        }
                        _ => true,
                    },
                    None => false,
                };
                if remove_now {
                    st.remove(bus, &sid);
                }
                ok().await
            }
            ClientOp::Pub {
                subject,
                reply_to,
                headers,
                payload,
            } => {
                let literal = |s: &str| SubjectFilter::parse(s).ok().filter(|f| f.is_literal());
                let Some(filter) = literal(&subject) else {
                    return send(encode_err("Invalid Publish Subject")).await;
                };
                if reply_to.as_deref().is_some_and(|r| literal(r).is_none()) {
                    return send(encode_err("Invalid Publish Subject")).await;
                }
                if !identity.authorize_subject(Action::Publish, &filter) {
                    self.ctx.metrics.auth_denied("forbidden", "nats", "PUB");
                    return send(encode_err(&format!(
                        "Permissions Violation for Publish to \"{subject}\""
                    )))
                    .await;
                }
                let probe = Record {
                    key: None,
                    value: payload.clone(),
                    subject: subject.clone(),
                    headers: headers.clone(),
                    timestamp_ns: None,
                };
                if let Err(e) = self.ctx.broker.log.limits().check(&probe) {
                    return Err(Close::Err(format!("Maximum Payload Violation: {e}")));
                }
                let captured = match self.ctx.broker.capture.target(&subject).await {
                    Some(stream) => {
                        if !identity.authorize(Action::Publish, &stream) {
                            self.ctx.metrics.auth_denied("forbidden", "nats", "PUB");
                            return send(encode_err(&format!(
                                "Permissions Violation for Publish to \"{subject}\""
                            )))
                            .await;
                        }
                        let tx = st.capture_tx.get_or_insert_with(|| {
                            Capture::pipeline(self.ctx.broker.log.clone(), bus.clone())
                        });
                        let job = CaptureJob::new(
                            stream,
                            &subject,
                            reply_to.clone(),
                            headers.clone(),
                            payload.clone(),
                        );
                        tx.send(job).await.map_err(|_| Close::Quiet)?;
                        true
                    }
                    None => false,
                };
                let reply = reply_to.clone();
                let msg = CoreMessage {
                    subject,
                    reply_to,
                    headers,
                    value: payload,
                    origin: self.cid,
                };
                match bus.publish(msg) {
                    Ok(_) => ok().await,
                    Err(BusError::NotLeader) => Err(Close::Quiet),
                    Err(BusError::NoResponders(_)) => {
                        ok().await?;
                        // A captured request is answered by its stream.
                        if !captured && st.opts.headers && st.opts.no_responders {
                            if let Some(chunk) = self.no_responders(st, reply.as_deref()) {
                                send(chunk).await?;
                            }
                        }
                        Ok(())
                    }
                }
            }
        }
    }

    /// The 503 status message a request with no responders gets, sent to
    /// this connection's subscription that matches the reply subject.
    fn no_responders(&self, st: &mut State, reply: Option<&str>) -> Option<Bytes> {
        let reply = reply?;
        let bus = &self.ctx.broker.bus;
        let sid = st
            .subs
            .values()
            .find(|s| bus.filter_of(s.bus_id).is_some_and(|f| f.matches(reply)))?
            .sid
            .clone();
        let mut out = BytesMut::new();
        encode_msg(
            &mut out,
            reply,
            &sid,
            None,
            Some(&encode_headers(&[], Some(503))),
            b"",
        );
        self.count_delivery(st, &sid);
        Some(out.freeze())
    }

    fn authenticate(
        &self,
        opts: &ConnectOptions,
        cert_name: Option<&str>,
    ) -> Result<Arc<Identity>, Close> {
        let Some(store) = self.ctx.credential_store.as_ref() else {
            return Ok(Arc::new(anonymous_identity()));
        };
        let token = opts
            .auth_token
            .as_deref()
            .or(opts.pass.as_deref())
            .filter(|t| !t.is_empty());
        let found = match (token, cert_name) {
            (Some(t), _) => {
                let digest: [u8; 32] = Sha256::digest(t.as_bytes()).into();
                store.lookup(&digest)
            }
            (None, Some(name)) => store.lookup_cert(name),
            (None, None) => None,
        };
        found.ok_or_else(|| {
            self.ctx
                .metrics
                .auth_denied("unauthorized", "nats", "CONNECT");
            warn!(peer = %self.peer, "NATS CONNECT rejected");
            Close::Err("Authorization Violation".into())
        })
    }
}
