//! Leader-side TCP listener for cluster replication.
//!
//! One accept loop, one per-follower task. Each connection goes through:
//!   1. `Connect` frame (reuse of the data-plane auth handshake), enforcing
//!      `Action::Replicate` on the returned Identity.
//!   2. `ReplicateResume` frame — follower's resume cursor.
//!   3. Register with the coordinator *first*, so any live event emitted
//!      from this point on is buffered in our mpsc queue. Doing this before
//!      building the manifest closes the "event lost during bootstrap"
//!      race: an event emitted between manifest snapshot and registration
//!      would otherwise be neither in the manifest's tail nor in the channel.
//!   4. `ClusterManifest` frame out — the leader's current stream bounds.
//!   5. `StreamReseedEvent` frames for every stream whose follower-cursor
//!      is earlier than the leader's `earliest_offset`.
//!   6. Catch-up: batched `RecordsAppended` frames up to each stream's tail.
//!   7. Live fan-out: `tokio::select!` across the coordinator channel, a
//!      heartbeat timer, and the server-wide cancel token. A closed channel
//!      (e.g. coordinator dropped us because our queue was full) tears
//!      the connection down cleanly.
//!
//! Invariant (bootstrap overlap): events buffered in the channel during
//! catch-up MAY overlap offsets that catch-up also streams (e.g. an append
//! landed after the manifest snapshot but before catch-up for that stream
//! started). This is fine — the follower's `append` path de-dupes via
//! `msg_id`, and its storage rejects offsets it already has. Duplicates
//! are not a correctness problem; losing events would be.
//!
//! Any I/O or protocol error at steps 2-7 closes the socket — a well-behaved
//! follower reconnects and redrives from cursor. The coordinator's
//! slow-follower drop (Wave 2) already covers the runtime backpressure case.
//!
//! NOTE: the Connect handshake here duplicates the data-plane handshake at
//! `crates/exspeed/src/cli/server.rs:853-906`. Keep them in sync; a future
//! ticket (TODO post-plan-g) should extract a shared helper in
//! `exspeed-broker::auth`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use exspeed_common::auth::{Action, CredentialStore, Identity};
use exspeed_common::{Metrics, Offset, StreamName};
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest, ConnectResponse};
use exspeed_protocol::messages::replicate::{
    ClusterManifest, RecordsAppended, ReplicateResume, ReplicatedRecord, StreamReseedEvent,
    StreamSummary, REPLICATION_WIRE_VERSION,
};
use exspeed_protocol::messages::WIRE_VERSION;
use exspeed_protocol::opcodes::OpCode;
use exspeed_streams::{StorageEngine, StorageError};
use opentelemetry::KeyValue;

use crate::replication::errors::ReplicationError;
use crate::replication::wire::encode_event;
use crate::replication::ReplicationCoordinator;

/// Default number of records per catch-up batch. Override via
/// `EXSPEED_REPLICATION_BATCH_RECORDS`.
const DEFAULT_BATCH_RECORDS: usize = 1000;

/// Default heartbeat interval when the connection is otherwise idle.
/// Override via `EXSPEED_REPLICATION_HEARTBEAT_SECS`.
///
/// Follower invariant: the follower's `idle_timeout` MUST be at least
/// `3 × DEFAULT_HEARTBEAT_SECS` so a single dropped heartbeat doesn't
/// tear down a healthy connection. Wave 4's follower client is expected
/// to read this constant.
const DEFAULT_HEARTBEAT_SECS: u64 = 5;

fn batch_records_from_env() -> usize {
    std::env::var("EXSPEED_REPLICATION_BATCH_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(DEFAULT_BATCH_RECORDS)
}

fn heartbeat_interval_from_env() -> Duration {
    let secs = std::env::var("EXSPEED_REPLICATION_HEARTBEAT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
        .unwrap_or(DEFAULT_HEARTBEAT_SECS);
    Duration::from_secs(secs)
}

/// Leader-side TCP server. Cheap to clone — all fields are `Arc`s /
/// primitives. Wave 5 starts and stops the server across leadership
/// transitions; binding the listener once at spawn time and re-using it
/// across tenures is the whole reason the `TcpListener` is `Arc`'d.
#[derive(Clone)]
pub struct ReplicationServer {
    listener: Arc<TcpListener>,
    coordinator: Arc<ReplicationCoordinator>,
    storage: Arc<dyn StorageEngine>,
    credential_store: Option<Arc<CredentialStore>>,
    leader_holder_id: Uuid,
    metrics: Arc<Metrics>,
}

impl ReplicationServer {
    /// Bind the cluster port. Returns the server handle; call `run` to
    /// start accepting.
    pub async fn bind(
        addr: SocketAddr,
        coordinator: Arc<ReplicationCoordinator>,
        storage: Arc<dyn StorageEngine>,
        credential_store: Option<Arc<CredentialStore>>,
        leader_holder_id: Uuid,
        metrics: Arc<Metrics>,
    ) -> Result<Self, ReplicationError> {
        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        info!(%local, "cluster replication listener bound");
        Ok(Self {
            listener: Arc::new(listener),
            coordinator,
            storage,
            credential_store,
            leader_holder_id,
            metrics,
        })
    }

    /// Local listening address. Useful for tests that bind on `:0`.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept loop. Runs until `cancel` fires; each accepted socket
    /// spawns a per-follower task.
    pub async fn run(&self, cancel: CancellationToken) {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    info!("replication server shutting down");
                    return;
                }
                accept = self.listener.accept() => {
                    match accept {
                        Ok((socket, peer)) => {
                            let server = self.clone();
                            let child = cancel.child_token();
                            tokio::spawn(async move {
                                if let Err(e) = handle_follower(server, socket, peer, child).await {
                                    warn!(%peer, error = %e, "replication connection ended");
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "replication accept failed");
                        }
                    }
                }
            }
        }
    }
}

/// Per-connection state machine. Returns `Ok` on graceful shutdown,
/// `Err` on any protocol/IO/auth failure; the caller logs at WARN.
async fn handle_follower(
    server: ReplicationServer,
    socket: TcpStream,
    peer: SocketAddr,
    cancel: CancellationToken,
) -> Result<(), ReplicationError> {
    let (reader, writer) = tokio::io::split(socket);
    let mut framed_read = FramedRead::new(reader, ExspeedCodec::new());
    let mut framed_write = FramedWrite::new(writer, ExspeedCodec::new());

    // ---- Step 1: Connect + authorize Replicate ------------------------------
    let identity = read_connect_and_authorize(
        &mut framed_read,
        &mut framed_write,
        &server.credential_store,
        &server.metrics,
        &peer,
    )
    .await?;

    // ---- Step 2: ReplicateResume -------------------------------------------
    let resume = read_replicate_resume(&mut framed_read, &peer).await?;
    info!(
        %peer,
        follower_id = %resume.follower_id,
        identity = %identity.name,
        cursor_streams = resume.cursor.len(),
        "follower handshake accepted"
    );

    // ---- Step 3: Register FIRST so live events buffer during bootstrap ----
    // Any event emitted between manifest snapshot and this point would be
    // silently dropped (not in the snapshot's tail offsets, not yet in any
    // channel). Register up front so the channel catches everything; the
    // catch-up range may overlap buffered events, which is harmless — see
    // the "bootstrap overlap" invariant in the module docstring.
    let (follower_id, mut rx) = server.coordinator.register_follower();

    // Hoist the rest of bootstrap + fan-out into a helper so we can
    // guarantee `deregister_follower` runs on every exit path (I/O error,
    // cancel, version skew, channel close, happy path).
    let result =
        run_bootstrap_and_fanout(&server, &mut framed_write, &resume, &mut rx, cancel).await;

    server.coordinator.deregister_follower(follower_id);
    result
}

/// Everything after registration: build manifest, reseed-detect, catch up,
/// then loop on the rx + heartbeat + cancel. Factored out so the caller can
/// always `deregister_follower` regardless of how we exit.
async fn run_bootstrap_and_fanout<W>(
    server: &ReplicationServer,
    framed_write: &mut FramedWrite<W, ExspeedCodec>,
    resume: &ReplicateResume,
    rx: &mut tokio::sync::mpsc::Receiver<crate::replication::ReplicationEvent>,
    cancel: CancellationToken,
) -> Result<(), ReplicationError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    // ---- Step 4: ClusterManifest -------------------------------------------
    let (manifest, manifest_bytes) =
        build_cluster_manifest(&server.storage, server.leader_holder_id).await?;
    write_frame(
        framed_write,
        OpCode::ClusterManifest,
        0,
        manifest_bytes,
        &server.metrics,
    )
    .await?;

    // ---- Step 5: Reseed detect + Step 6: Catch-up --------------------------
    // Local cursor we'll advance as we stream; never mutates `resume`.
    let mut cursor = resume.cursor.clone();
    for summary in &manifest.streams {
        let follower_next = cursor.get(&summary.name).copied().unwrap_or(0);

        // Reseed-detect: follower is behind the leader's retention window.
        if follower_next < summary.earliest_offset {
            let ev = StreamReseedEvent {
                stream: summary.name.clone(),
                new_earliest_offset: summary.earliest_offset,
            };
            let bytes = bincode::serialize(&ev)?;
            write_frame(
                framed_write,
                OpCode::StreamReseedEvent,
                0,
                Bytes::from(bytes),
                &server.metrics,
            )
            .await?;
            server.metrics.inc_replication_reseed(&summary.name);
            cursor.insert(summary.name.clone(), summary.earliest_offset);
        }

        // Catch-up: pump records until we reach the current tail.
        let final_next = stream_catchup(
            &server.storage,
            framed_write,
            &summary.name,
            &mut cursor,
            summary.latest_offset,
            &server.metrics,
        )
        .await?;
        // Observability: if catch-up exited below the manifest tail, a
        // retention trim / stream delete raced with us. That's recoverable
        // — the buffered `RetentionTrimmed` / `StreamDeleted` event in the
        // channel will correct the follower's cursor — but it's rare
        // enough that we want it grep-able when it happens.
        if final_next != summary.latest_offset {
            warn!(
                stream = %summary.name,
                reached = final_next,
                tail = summary.latest_offset,
                gap = summary.latest_offset.saturating_sub(final_next),
                follower = %resume.follower_id,
                "catch-up exited below manifest tail — expecting live event to correct"
            );
        }
    }

    // ---- Step 7: Live fan-out ----------------------------------------------
    fan_out_loop(server, framed_write, rx, cancel).await
}

/// Steady-state loop: forward every `ReplicationEvent` from `rx` out as a
/// frame, emit heartbeats on timer, exit cleanly on cancel or closed channel.
async fn fan_out_loop<W>(
    server: &ReplicationServer,
    framed_write: &mut FramedWrite<W, ExspeedCodec>,
    rx: &mut tokio::sync::mpsc::Receiver<crate::replication::ReplicationEvent>,
    cancel: CancellationToken,
) -> Result<(), ReplicationError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let heartbeat_every = heartbeat_interval_from_env();
    let mut heartbeat = tokio::time::interval(heartbeat_every);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate tick so we don't emit a heartbeat at t=0.
    heartbeat.tick().await;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                return Ok(());
            }
            maybe_ev = rx.recv() => {
                let Some(ev) = maybe_ev else {
                    // Coordinator dropped us — slow-follower queue overflow,
                    // or we were deregistered externally. Close cleanly.
                    return Ok(());
                };
                let encoded = encode_event(&ev)?;
                write_frame(
                    framed_write,
                    encoded.opcode,
                    0,
                    Bytes::from(encoded.bytes),
                    &server.metrics,
                )
                .await?;
            }
            _ = heartbeat.tick() => {
                write_frame(
                    framed_write,
                    OpCode::ReplicationHeartbeat,
                    0,
                    Bytes::new(),
                    &server.metrics,
                )
                .await?;
            }
        }
    }
}

/// Read first frame, expect Connect, authenticate, and enforce the
/// Replicate verb. Returns the authenticated Identity or an error after
/// the 401/403 error frame has already been sent.
async fn read_connect_and_authorize<R, W>(
    framed_read: &mut FramedRead<R, ExspeedCodec>,
    framed_write: &mut FramedWrite<W, ExspeedCodec>,
    credential_store: &Option<Arc<CredentialStore>>,
    metrics: &Metrics,
    peer: &SocketAddr,
) -> Result<Arc<Identity>, ReplicationError>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let frame = framed_read
        .next()
        .await
        .ok_or_else(|| ReplicationError::Protocol("connection closed before Connect".into()))?
        .map_err(|e| ReplicationError::Protocol(format!("connect decode: {e}")))?;

    if frame.opcode != OpCode::Connect {
        send_error(framed_write, frame.correlation_id, 401, "expected Connect").await?;
        return Err(ReplicationError::Protocol(format!(
            "expected Connect first, got {:?}",
            frame.opcode
        )));
    }

    let correlation_id = frame.correlation_id;
    let req = ConnectRequest::decode(frame.payload)
        .map_err(|e| ReplicationError::Protocol(format!("connect payload: {e}")))?;

    // Resolve identity exactly like the data plane handshake:
    //   * no credential store → anonymous identity with full privileges
    //     (single-pod dev mode; auth disabled globally)
    //   * otherwise Token auth required, sha256 lookup
    let identity: Arc<Identity> = match credential_store.as_ref() {
        Some(store) => {
            if req.auth_type != AuthType::Token {
                metrics.auth_denied("unauthorized", "tcp", "Connect");
                send_error(framed_write, correlation_id, 401, "unauthorized").await?;
                return Err(ReplicationError::AuthDenied("non-token auth".into()));
            }
            let digest: [u8; 32] = Sha256::digest(&req.auth_payload).into();
            match store.lookup(&digest) {
                Some(id) => id,
                None => {
                    metrics.auth_denied("unauthorized", "tcp", "Connect");
                    send_error(framed_write, correlation_id, 401, "unauthorized").await?;
                    return Err(ReplicationError::AuthDenied("unknown credential".into()));
                }
            }
        }
        None => Arc::new(anonymous_identity()),
    };

    // Enforce the Replicate verb. Replicate is cluster-wide — any
    // permission with Replicate in its action set satisfies it.
    if !identity
        .permissions
        .iter()
        .any(|p| p.actions.contains(Action::Replicate))
    {
        metrics.auth_denied("forbidden", "tcp", "replicate");
        send_error(framed_write, correlation_id, 403, "forbidden").await?;
        return Err(ReplicationError::AuthDenied(format!(
            "identity {} lacks Replicate permission",
            identity.name
        )));
    }

    // ACK the Connect with a ConnectOk frame so the follower can move on.
    let mut payload = BytesMut::new();
    ConnectResponse {
        server_version: WIRE_VERSION,
    }
    .encode(&mut payload);
    framed_write
        .send(Frame::new(
            OpCode::ConnectOk,
            correlation_id,
            payload.freeze(),
        ))
        .await
        .map_err(|e| ReplicationError::Protocol(format!("send ConnectOk: {e}")))?;

    info!(%peer, identity = %identity.name, "replication connect authenticated");
    Ok(identity)
}

async fn read_replicate_resume<R>(
    framed_read: &mut FramedRead<R, ExspeedCodec>,
    peer: &SocketAddr,
) -> Result<ReplicateResume, ReplicationError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let frame = framed_read
        .next()
        .await
        .ok_or_else(|| {
            ReplicationError::Protocol("connection closed before ReplicateResume".into())
        })?
        .map_err(|e| ReplicationError::Protocol(format!("resume decode: {e}")))?;

    if frame.opcode != OpCode::ReplicateResume {
        return Err(ReplicationError::Protocol(format!(
            "expected ReplicateResume, got {:?}",
            frame.opcode
        )));
    }

    let resume: ReplicateResume = bincode::deserialize(&frame.payload)?;
    if resume.wire_version != REPLICATION_WIRE_VERSION {
        warn!(
            %peer,
            leader = REPLICATION_WIRE_VERSION,
            follower = resume.wire_version,
            "replication wire-version mismatch"
        );
        return Err(ReplicationError::VersionSkew {
            leader: REPLICATION_WIRE_VERSION,
            follower: resume.wire_version,
        });
    }
    Ok(resume)
}

async fn build_cluster_manifest(
    storage: &Arc<dyn StorageEngine>,
    leader_holder_id: Uuid,
) -> Result<(ClusterManifest, Bytes), ReplicationError> {
    let names = storage
        .list_streams()
        .await
        .map_err(ReplicationError::Apply)?;

    let mut summaries = Vec::with_capacity(names.len());
    for name in names {
        match storage.stream_bounds(&name).await {
            Ok((earliest, next)) => {
                // Wave-3 limitation: `max_age_secs` / `max_bytes` come from
                // future `RetentionUpdated` events (Wave 4+); neither the
                // `StorageEngine` trait nor `Broker` exposes per-stream
                // retention config today. Zero here means "unknown" and the
                // follower mirrors via the event stream rather than the
                // manifest. This is flagged as a DONE-WITH-CONCERNS item.
                summaries.push(StreamSummary {
                    name: name.as_str().to_string(),
                    max_age_secs: 0,
                    max_bytes: 0,
                    earliest_offset: earliest.0,
                    latest_offset: next.0,
                });
            }
            Err(e) => {
                warn!(stream = %name, error = %e, "stream_bounds failed in manifest build");
            }
        }
    }

    let manifest = ClusterManifest {
        streams: summaries,
        leader_holder_id,
    };
    let bytes = bincode::serialize(&manifest)?;
    Ok((manifest, Bytes::from(bytes)))
}

/// Pump records from `cursor[stream]` up to `tail_offset`, writing one
/// `RecordsAppended` frame per storage batch. Updates `cursor` in-place.
/// Returns the final `next` offset we reached — callers compare it to
/// `tail_offset` to detect and log catch-up gaps.
async fn stream_catchup<W>(
    storage: &Arc<dyn StorageEngine>,
    framed_write: &mut FramedWrite<W, ExspeedCodec>,
    stream: &str,
    cursor: &mut std::collections::BTreeMap<String, u64>,
    tail_offset: u64,
    metrics: &Metrics,
) -> Result<u64, ReplicationError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let batch_size = batch_records_from_env();
    let name = match StreamName::try_from(stream) {
        Ok(n) => n,
        Err(_) => {
            warn!(stream = %stream, "skipping catch-up for stream with invalid name");
            return Ok(cursor.get(stream).copied().unwrap_or(0));
        }
    };

    let mut next = cursor.get(stream).copied().unwrap_or(0);
    while next < tail_offset {
        let records = storage
            .read(&name, Offset(next), batch_size)
            .await
            .map_err(ReplicationError::Apply)?;
        if records.is_empty() {
            // Storage returned nothing even though tail_offset > next.
            // Classify the race before giving up:
            //   * stream deleted → log + exit; buffered StreamDeleted
            //     event in the channel will land on the follower.
            //   * stream trimmed past `next` (retention) → log + exit;
            //     buffered RetentionTrimmed event will correct cursor.
            //   * anything else → still safe to exit because, post-C1,
            //     the follower channel was registered before manifest
            //     snapshot, so any catch-up corrections are queued.
            match storage.stream_bounds(&name).await {
                Err(StorageError::StreamNotFound(_)) => {
                    warn!(
                        stream = %stream,
                        next,
                        tail = tail_offset,
                        "catch-up empty read: stream deleted mid-bootstrap; \
                         expecting StreamDeleted event via fan-out"
                    );
                }
                Ok((earliest, tail)) if earliest.0 > next => {
                    warn!(
                        stream = %stream,
                        next,
                        earliest = earliest.0,
                        tail = tail.0,
                        "catch-up empty read: retention advanced past cursor; \
                         expecting RetentionTrimmed event via fan-out"
                    );
                }
                Ok((earliest, tail)) => {
                    // Bounds unchanged but read returned nothing — unusual;
                    // log with full context so it's easy to spot in logs.
                    warn!(
                        stream = %stream,
                        next,
                        earliest = earliest.0,
                        tail = tail.0,
                        manifest_tail = tail_offset,
                        "catch-up empty read with bounds unchanged; bailing out"
                    );
                }
                Err(e) => {
                    warn!(stream = %stream, error = %e, "stream_bounds re-check failed");
                }
            }
            break;
        }
        let base_offset = records[0].offset.0;
        let replicated: Vec<ReplicatedRecord> = records
            .iter()
            .map(|r| {
                // msg_id rehydrated from the x-idempotency-key header,
                // mirroring the leader's publish path so follower dedup
                // stays in sync.
                let msg_id = r
                    .headers
                    .iter()
                    .find(|(k, _)| k == "x-idempotency-key")
                    .map(|(_, v)| v.clone());
                ReplicatedRecord {
                    subject: r.subject.clone(),
                    payload: r.value.to_vec(),
                    headers: r.headers.clone(),
                    timestamp_ms: r.timestamp / 1_000_000,
                    msg_id,
                }
            })
            .collect();

        let last_offset = records.last().expect("non-empty").offset.0;
        let batch = RecordsAppended {
            stream: stream.to_string(),
            base_offset,
            records: replicated,
        };
        let bytes = bincode::serialize(&batch)?;
        write_frame(
            framed_write,
            OpCode::RecordsAppended,
            0,
            Bytes::from(bytes),
            metrics,
        )
        .await?;

        next = last_offset + 1;
        cursor.insert(stream.to_string(), next);
    }
    Ok(next)
}

/// Build + send a Frame and bump outbound bytes counter.
async fn write_frame<W>(
    framed_write: &mut FramedWrite<W, ExspeedCodec>,
    opcode: OpCode,
    correlation_id: u32,
    payload: Bytes,
    metrics: &Metrics,
) -> Result<(), ReplicationError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let payload_len = payload.len();
    let frame = Frame::new(opcode, correlation_id, payload);
    framed_write
        .send(frame)
        .await
        .map_err(|e| ReplicationError::Protocol(format!("send frame: {e}")))?;
    // Count the payload bytes; 10 frame-header bytes are negligible and
    // keep the accounting simple (matches how the replication coordinator
    // would count payload sizes in Wave 5 bandwidth dashboards).
    metrics.replication_bytes_total.add(
        payload_len as u64,
        &[KeyValue::new("direction", "out")],
    );
    Ok(())
}

async fn send_error<W>(
    framed_write: &mut FramedWrite<W, ExspeedCodec>,
    correlation_id: u32,
    code: u16,
    message: &str,
) -> Result<(), ReplicationError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use exspeed_protocol::messages::ServerMessage;
    let frame = ServerMessage::Error {
        code,
        message: message.into(),
    }
    .into_frame(correlation_id);
    framed_write
        .send(frame)
        .await
        .map_err(|e| ReplicationError::Protocol(format!("send error: {e}")))
}

/// Synthetic all-access identity used when auth is globally disabled.
/// Mirrors the data-plane connection handshake so replication behavior
/// matches the rest of the broker in that mode.
fn anonymous_identity() -> Identity {
    use enumset::EnumSet;
    use exspeed_common::auth::{Permission, StreamGlob};
    let mut actions = EnumSet::<Action>::new();
    actions |= Action::Publish;
    actions |= Action::Subscribe;
    actions |= Action::Admin;
    actions |= Action::Replicate;
    Identity {
        name: "anonymous".to_string(),
        permissions: vec![Permission {
            streams: StreamGlob::compile("*", "anonymous").expect("* is a valid glob"),
            actions,
        }],
    }
}
