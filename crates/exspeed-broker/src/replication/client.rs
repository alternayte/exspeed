//! Follower-side replication client.
//!
//! Dials the leader's cluster port, handshakes, and applies a continuous
//! stream of replication events to local storage. One client per running
//! follower process; Wave 5's role-transition supervisor starts/stops it
//! across leadership transitions.
//!
//! # Per-session lifecycle (see `run`):
//!
//!   1. Resolve the leader's `replication_endpoint` via `endpoint_getter`
//!      (lease-backed). `None` → backoff + retry.
//!   2. Dial the leader and send a `Connect` frame (Token auth, same shape
//!      as the data-plane handshake). Read `ConnectOk`; on `Error` return
//!      `AuthDenied`.
//!   3. Send `ReplicateResume { follower_id, wire_version, cursor }`.
//!   4. Expect `ClusterManifest` as the FIRST post-handshake frame
//!      (Wave 3's register-first ordering guarantee).
//!   5. Divergent-history check: for every stream in the manifest where
//!      our cursor > leader's `latest_offset` (post-failover divergence),
//!      call `storage.truncate_from`, update cursor, save. Also
//!      pre-create any manifest stream unknown locally (retention values
//!      from the manifest).
//!   6. Apply events (`RecordsAppended`, metadata events, heartbeats)
//!      until session ends. `idle_timeout` (default 30s) tears down
//!      connections that go silent.
//!   7. On any exit — I/O error, idle timeout, cancel, auth rejection —
//!      close socket, backoff, outer loop reconnects.
//!
//! # Idle-timeout invariant
//!
//! The follower's `idle_timeout` MUST be at least `3 × leader heartbeat`
//! (default leader heartbeat is 5s, default follower idle is 30s → 6×).
//! A single dropped heartbeat should not tear down a healthy connection.
//! See `server.rs::DEFAULT_HEARTBEAT_SECS`.
//!
//! # Crash durability (truncate_from)
//!
//! `FileStorage::truncate_from` is not atomic with cursor-save: if we
//! crash between storage rewrite and cursor save, on restart the cursor
//! is stale (old offsets) but storage is at the new state. Next session's
//! divergent-history check fires again → `truncate_from` is called →
//! idempotent (new truncate range is empty). Retry semantic is correct.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use exspeed_common::{Metrics, Offset, StreamName};
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest, ConnectResponse};
use exspeed_protocol::messages::replicate::{
    ClusterManifest, RecordsAppended, ReplicateResume, RetentionTrimmedEvent,
    RetentionUpdatedEvent, StreamCreatedEvent, StreamDeletedEvent, StreamReseedEvent,
    REPLICATION_WIRE_VERSION,
};
use exspeed_streams::{Record, StorageEngine, StorageError};
use opentelemetry::KeyValue;

use crate::replication::cursor::Cursor;
use crate::replication::errors::ReplicationError;

/// Default idle timeout (no frame received for this long → tear down).
/// Must be >= 3x leader heartbeat (default leader heartbeat = 5s).
/// Override via `EXSPEED_REPLICATION_IDLE_TIMEOUT_SECS`.
const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 30;

/// Initial reconnect backoff. Doubles each attempt up to the cap.
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Maximum reconnect backoff.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

fn idle_timeout_from_env() -> Duration {
    let secs = std::env::var("EXSPEED_REPLICATION_IDLE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
        .unwrap_or(DEFAULT_IDLE_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

/// Outcome of a single replication session, recording whether the
/// session reached the post-handshake manifest boundary. The outer loop
/// reads this to decide backoff: errors after the manifest are treated
/// as transient (handshake succeeded, so no point compounding backoff
/// against a flapping leader); errors before the manifest back off
/// harder (dial/auth/version errors that justify slowing retries).
enum SessionOutcome {
    /// EOF or cancel — no error, reset backoff.
    Clean,
    /// Error after `ClusterManifest` was successfully received. Handshake
    /// worked; treat as a transient mid-session failure and reset backoff.
    ErrAfterManifest(ReplicationError),
    /// Error during dial / Connect / ReplicateResume / manifest-read.
    /// Doubles backoff so we don't tight-loop against a bad leader.
    ErrBeforeManifest(ReplicationError),
}

/// Follower-side replication client. Holds the local storage handle and
/// the persistent cursor; loops dialing the leader and applying events.
pub struct ReplicationClient {
    storage: Arc<dyn StorageEngine>,
    cursor_path: PathBuf,
    cursor: Arc<Mutex<Cursor>>,
    follower_id: Uuid,
    metrics: Arc<Metrics>,
}

impl ReplicationClient {
    /// Load the persistent cursor (creating an empty one if missing / corrupt)
    /// and build a client. Cursor I/O errors surface here; corrupt cursor is
    /// treated as empty (per `Cursor::load`).
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        cursor_path: PathBuf,
        metrics: Arc<Metrics>,
    ) -> Result<Self, ReplicationError> {
        let cursor = Cursor::load(&cursor_path)?;
        Ok(Self {
            storage,
            cursor_path,
            cursor: Arc::new(Mutex::new(cursor)),
            follower_id: Uuid::new_v4(),
            metrics,
        })
    }

    /// This follower's identifier. Stable for the lifetime of the process
    /// (re-created after restart). Primarily for tests / diagnostics.
    pub fn follower_id(&self) -> Uuid {
        self.follower_id
    }

    /// Main loop. Resolves the leader endpoint, runs a session until it
    /// ends, then reconnects with exponential backoff until `cancel` fires.
    pub async fn run<F, Fut>(
        &self,
        endpoint_getter: F,
        connect_bearer: String,
        cancel: CancellationToken,
    ) where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Option<String>> + Send,
    {
        let mut backoff = INITIAL_BACKOFF;
        loop {
            if cancel.is_cancelled() {
                info!("replication client cancelled — exiting");
                return;
            }

            // ---- Step 1: Resolve leader endpoint --------------------------
            let endpoint = match endpoint_getter().await {
                Some(addr) => addr,
                None => {
                    debug!(
                        backoff_secs = backoff.as_secs(),
                        "no leader endpoint yet — backing off"
                    );
                    if sleep_or_cancel(backoff, &cancel).await {
                        return;
                    }
                    backoff = next_backoff(backoff);
                    continue;
                }
            };

            // ---- Step 2–7: Run one session --------------------------------
            //
            // Backoff rules:
            //   * Clean exit (EOF, cancel)            → reset to INITIAL.
            //   * Error AFTER manifest received       → reset to INITIAL —
            //     handshake succeeded, so a flapping leader won't compound
            //     backoff across sessions.
            //   * Error BEFORE manifest received      → double (transient
            //     network / dial path — real backoff pressure is useful).
            //   * Auth denied / version skew          → pin at MAX_BACKOFF.
            match self
                .run_session(&endpoint, &connect_bearer, cancel.clone())
                .await
            {
                SessionOutcome::Clean => {
                    info!(endpoint = %endpoint, "replication session ended cleanly");
                    backoff = INITIAL_BACKOFF;
                }
                SessionOutcome::ErrAfterManifest(e) => {
                    warn!(
                        endpoint = %endpoint,
                        error = %e,
                        "replication session ended post-manifest — resetting backoff"
                    );
                    backoff = INITIAL_BACKOFF;
                }
                SessionOutcome::ErrBeforeManifest(ReplicationError::AuthDenied(msg)) => {
                    error!(endpoint = %endpoint, error = %msg, "replication auth denied");
                    backoff = MAX_BACKOFF;
                }
                SessionOutcome::ErrBeforeManifest(ReplicationError::VersionSkew {
                    leader,
                    follower,
                }) => {
                    error!(
                        endpoint = %endpoint,
                        leader,
                        follower,
                        "replication wire-version mismatch — retrying at max backoff"
                    );
                    backoff = MAX_BACKOFF;
                }
                SessionOutcome::ErrBeforeManifest(e) => {
                    warn!(endpoint = %endpoint, error = %e, "replication session ended with error");
                }
            }

            if sleep_or_cancel(backoff, &cancel).await {
                return;
            }
            backoff = next_backoff(backoff);
        }
    }

    /// One session: dial, handshake, manifest, divergent-history check,
    /// apply event loop. Returns a `SessionOutcome` describing whether
    /// the session reached the manifest boundary — the outer loop uses
    /// that to decide whether a transient error should reset backoff.
    ///
    /// Cancellation is checked at every major handshake boundary. A
    /// cancel mid-handshake returns `SessionOutcome::Clean`, not an
    /// error — the process is shutting down as intended, not failing.
    async fn run_session(
        &self,
        endpoint: &str,
        connect_bearer: &str,
        cancel: CancellationToken,
    ) -> SessionOutcome {
        // ── Boundary: pre-dial. ────────────────────────────────────────
        if cancel.is_cancelled() {
            return SessionOutcome::Clean;
        }

        // Dial.
        let socket = match TcpStream::connect(endpoint).await {
            Ok(s) => {
                self.metrics.record_replication_connect_attempt(true);
                info!(endpoint = %endpoint, "replication TCP connected");
                s
            }
            Err(e) => {
                self.metrics.record_replication_connect_attempt(false);
                return SessionOutcome::ErrBeforeManifest(ReplicationError::Io(e));
            }
        };

        let (reader, writer) = tokio::io::split(socket);
        let mut framed_read = FramedRead::new(reader, ExspeedCodec::new());
        let mut framed_write = FramedWrite::new(writer, ExspeedCodec::new());

        // ── Boundary: pre-Connect. ────────────────────────────────────
        if cancel.is_cancelled() {
            return SessionOutcome::Clean;
        }

        // Handshake: Connect + ConnectOk, then ReplicateResume.
        if let Err(e) = send_connect(&mut framed_write, connect_bearer).await {
            return SessionOutcome::ErrBeforeManifest(e);
        }
        if let Err(e) = read_connect_ok(&mut framed_read).await {
            return SessionOutcome::ErrBeforeManifest(e);
        }

        // ── Boundary: pre-ReplicateResume. ────────────────────────────
        if cancel.is_cancelled() {
            return SessionOutcome::Clean;
        }

        if let Err(e) = send_replicate_resume(&mut framed_write, self.follower_id, &self.cursor).await {
            return SessionOutcome::ErrBeforeManifest(e);
        }

        // ── Boundary: pre-ClusterManifest wait. ───────────────────────
        // The manifest read blocks on the leader; race it against cancel
        // so shutdown isn't held up by a leader that never responds.
        let manifest = tokio::select! {
            biased;
            _ = cancel.cancelled() => return SessionOutcome::Clean,
            res = read_cluster_manifest(&mut framed_read, &self.metrics) => match res {
                Ok(m) => m,
                Err(e) => return SessionOutcome::ErrBeforeManifest(e),
            },
        };
        info!(
            streams = manifest.streams.len(),
            leader_holder_id = %manifest.leader_holder_id,
            "cluster manifest received"
        );

        // From here on, any error is "post-manifest" — the handshake
        // succeeded, so the outer loop resets backoff.
        if let Err(e) = self.reconcile_manifest(&manifest).await {
            return SessionOutcome::ErrAfterManifest(e);
        }

        match self.apply_loop(&mut framed_read, cancel).await {
            Ok(()) => SessionOutcome::Clean,
            Err(e) => SessionOutcome::ErrAfterManifest(e),
        }
    }

    /// For every stream in the manifest:
    ///   * if follower cursor > leader latest_offset → `truncate_from`
    ///     (divergent history after failover).
    ///   * if follower has no local stream → create it with the manifest's
    ///     retention config so the follower mirrors leader's config.
    async fn reconcile_manifest(
        &self,
        manifest: &ClusterManifest,
    ) -> Result<(), ReplicationError> {
        let mut cursor = self.cursor.lock().await;
        let mut mutated = false;

        for summary in &manifest.streams {
            let name = match StreamName::try_from(summary.name.as_str()) {
                Ok(n) => n,
                Err(e) => {
                    warn!(stream = %summary.name, error = %e, "manifest has invalid stream name — skipping");
                    continue;
                }
            };

            // Pre-create locally if unknown. We use the manifest's retention
            // so followers' stream config matches the leader's. If the
            // stream already exists locally, `StreamAlreadyExists` is the
            // expected race — swallow it.
            match self
                .storage
                .create_stream(&name, summary.max_age_secs, summary.max_bytes)
                .await
            {
                Ok(()) => {}
                Err(StorageError::StreamAlreadyExists(_)) => {}
                Err(e) => {
                    warn!(stream = %summary.name, error = %e, "pre-create from manifest failed");
                }
            }

            // Divergent-history check. `cursor` stores `next_offset` per
            // stream; leader reports `latest_offset` as its NEXT offset
            // (matches StreamSummary contract).
            let follower_next = cursor.get(&summary.name).unwrap_or(0);
            if follower_next > summary.latest_offset {
                let dropped = follower_next - summary.latest_offset;
                // NOTE: `FileStorage::truncate_from` is non-atomic with the
                // cursor save below. If we crash between the two, the next
                // session's divergent-history check will re-run, and
                // `truncate_from` is idempotent (range is empty). Correct.
                self.storage
                    .truncate_from(&name, Offset(summary.latest_offset))
                    .await?;
                self.metrics
                    .inc_replication_truncated_records(&summary.name, dropped);
                warn!(
                    stream = %summary.name,
                    dropped,
                    from_offset = summary.latest_offset,
                    "exspeed.replication: divergent-history truncation"
                );
                cursor.set(&summary.name, summary.latest_offset);
                mutated = true;
            }
        }

        if mutated {
            cursor.save(&self.cursor_path)?;
        }
        Ok(())
    }

    /// Steady-state: read frames, apply events, reset idle timer each
    /// frame. Exits on idle timeout / I/O error / cancel.
    async fn apply_loop<R>(
        &self,
        framed_read: &mut FramedRead<R, ExspeedCodec>,
        cancel: CancellationToken,
    ) -> Result<(), ReplicationError>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let idle = idle_timeout_from_env();
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    return Ok(());
                }
                maybe_frame = tokio::time::timeout(idle, framed_read.next()) => {
                    let frame_result = maybe_frame.map_err(|_| {
                        warn!(idle_secs = idle.as_secs(), "replication idle timeout — tearing down");
                        ReplicationError::Protocol("idle timeout".into())
                    })?;
                    let frame = match frame_result {
                        Some(Ok(f)) => f,
                        Some(Err(e)) => {
                            return Err(ReplicationError::Protocol(format!("decode: {e}")));
                        }
                        None => {
                            info!("replication connection closed by leader");
                            return Ok(());
                        }
                    };
                    self.metrics.replication_bytes_total.add(
                        frame.payload.len() as u64,
                        &[KeyValue::new("direction", "in")],
                    );
                    self.apply_frame(frame).await?;
                }
            }
        }
    }

    /// Dispatch a single frame to the right apply-handler. Any handler
    /// error propagates out to tear the session down (the outer loop
    /// reconnects and the manifest check re-runs on resume).
    async fn apply_frame(&self, frame: Frame) -> Result<(), ReplicationError> {
        use exspeed_protocol::opcodes::OpCode;

        match frame.opcode {
            OpCode::ReplicationHeartbeat => {
                // No-op; the outer `timeout` already reset the idle timer.
                Ok(())
            }
            OpCode::StreamCreatedEvent => {
                let ev: StreamCreatedEvent = bincode::deserialize(&frame.payload)?;
                self.apply_stream_created(ev).await
            }
            OpCode::StreamDeletedEvent => {
                let ev: StreamDeletedEvent = bincode::deserialize(&frame.payload)?;
                self.apply_stream_deleted(ev).await
            }
            OpCode::RetentionUpdatedEvent => {
                let ev: RetentionUpdatedEvent = bincode::deserialize(&frame.payload)?;
                // TODO(wave 6): the `StorageEngine` trait has no per-stream
                // retention-config update method. For now, log and skip —
                // the follower's retention config drifts from the leader's
                // until the next `StreamCreatedEvent` for that stream.
                warn!(
                    stream = %ev.stream,
                    max_age_secs = ev.max_age_secs,
                    max_bytes = ev.max_bytes,
                    "RetentionUpdatedEvent: storage trait lacks retention update — skipping"
                );
                Ok(())
            }
            OpCode::RetentionTrimmedEvent => {
                let ev: RetentionTrimmedEvent = bincode::deserialize(&frame.payload)?;
                self.apply_retention_trimmed(ev).await
            }
            OpCode::StreamReseedEvent => {
                let ev: StreamReseedEvent = bincode::deserialize(&frame.payload)?;
                self.apply_stream_reseed(ev).await
            }
            OpCode::RecordsAppended => {
                let batch: RecordsAppended = bincode::deserialize(&frame.payload)?;
                self.apply_records_appended(batch).await
            }
            other => {
                self.metrics.replication_apply_errors_total.add(
                    1,
                    &[KeyValue::new("reason", "unknown_opcode")],
                );
                Err(ReplicationError::Protocol(format!(
                    "unexpected opcode in apply loop: {other:?}"
                )))
            }
        }
    }

    async fn apply_stream_created(
        &self,
        ev: StreamCreatedEvent,
    ) -> Result<(), ReplicationError> {
        let name = StreamName::try_from(ev.name.as_str())
            .map_err(|e| ReplicationError::Protocol(format!("invalid stream name {}: {e}", ev.name)))?;
        match self
            .storage
            .create_stream(&name, ev.max_age_secs, ev.max_bytes)
            .await
        {
            Ok(()) => {
                info!(stream = %ev.name, "created stream from StreamCreatedEvent");
                Ok(())
            }
            Err(StorageError::StreamAlreadyExists(_)) => {
                // Idempotent — bootstrap ordering may overlap with live
                // events (e.g., we pre-created from manifest).
                Ok(())
            }
            Err(e) => Err(ReplicationError::Apply(e)),
        }
    }

    async fn apply_stream_deleted(
        &self,
        ev: StreamDeletedEvent,
    ) -> Result<(), ReplicationError> {
        let name = StreamName::try_from(ev.name.as_str())
            .map_err(|e| ReplicationError::Protocol(format!("invalid stream name {}: {e}", ev.name)))?;
        self.storage.delete_stream(&name).await?;
        let mut cursor = self.cursor.lock().await;
        cursor.remove(&ev.name);
        cursor.save(&self.cursor_path)?;
        info!(stream = %ev.name, "deleted stream from StreamDeletedEvent");
        Ok(())
    }

    async fn apply_retention_trimmed(
        &self,
        ev: RetentionTrimmedEvent,
    ) -> Result<(), ReplicationError> {
        let name = StreamName::try_from(ev.stream.as_str())
            .map_err(|e| ReplicationError::Protocol(format!("invalid stream name {}: {e}", ev.stream)))?;
        self.storage
            .trim_up_to(&name, Offset(ev.new_earliest_offset))
            .await?;

        // If retention advanced past our cursor, log it (shouldn't normally
        // happen — we should have been consuming faster than retention) and
        // bump the cursor so we don't try to read already-deleted records.
        let mut cursor = self.cursor.lock().await;
        let follower_next = cursor.get(&ev.stream).unwrap_or(0);
        if follower_next < ev.new_earliest_offset {
            warn!(
                stream = %ev.stream,
                follower_next,
                new_earliest = ev.new_earliest_offset,
                "retention advanced past follower cursor — bumping cursor"
            );
            cursor.set(&ev.stream, ev.new_earliest_offset);
            cursor.save(&self.cursor_path)?;
        }
        debug!(stream = %ev.stream, new_earliest = ev.new_earliest_offset, "applied RetentionTrimmedEvent");
        Ok(())
    }

    async fn apply_stream_reseed(
        &self,
        ev: StreamReseedEvent,
    ) -> Result<(), ReplicationError> {
        let name = StreamName::try_from(ev.stream.as_str())
            .map_err(|e| ReplicationError::Protocol(format!("invalid stream name {}: {e}", ev.stream)))?;
        // Drop + recreate empty. Retention goes to 0/0 here because the
        // manifest already seeded retention on the pre-create; a subsequent
        // `RetentionUpdatedEvent` (Wave 6) would correct any drift.
        self.storage.delete_stream(&name).await?;
        self.storage.create_stream(&name, 0, 0).await?;

        let mut cursor = self.cursor.lock().await;
        cursor.set(&ev.stream, ev.new_earliest_offset);
        cursor.save(&self.cursor_path)?;
        self.metrics.inc_replication_reseed(&ev.stream);
        warn!(
            stream = %ev.stream,
            new_earliest = ev.new_earliest_offset,
            "applied StreamReseedEvent — local stream wiped"
        );
        Ok(())
    }

    async fn apply_records_appended(
        &self,
        batch: RecordsAppended,
    ) -> Result<(), ReplicationError> {
        let name = StreamName::try_from(batch.stream.as_str())
            .map_err(|e| ReplicationError::Protocol(format!("invalid stream name {}: {e}", batch.stream)))?;

        let batch_len = batch.records.len();
        if batch_len == 0 {
            return Ok(());
        }

        // Ensure the stream exists locally (first-seen case — manifest
        // pre-create above already handles the happy path). If it already
        // exists, swallow the `AlreadyExists` error.
        match self.storage.create_stream(&name, 0, 0).await {
            Ok(()) => {}
            Err(StorageError::StreamAlreadyExists(_)) => {}
            Err(e) => return Err(ReplicationError::Apply(e)),
        }

        let base = batch.base_offset;
        for (i, rec) in batch.records.into_iter().enumerate() {
            // Rehydrate msg_id back into headers so the follower's dedup
            // map matches the leader's. The leader's publish + catch-up
            // paths strip `x-idempotency-key` from headers into the
            // dedicated `msg_id` field before emitting, so a
            // pre-existing header here would be a protocol violation —
            // no per-record `retain()` dedup is needed.  The debug
            // assertion guards against regressions on the leader side.
            let mut headers = rec.headers;
            debug_assert!(
                !headers
                    .iter()
                    .any(|(k, _)| k.eq_ignore_ascii_case("x-idempotency-key")),
                "leader should have stripped x-idempotency-key before emit; found one in replicated headers"
            );
            if let Some(msg_id) = rec.msg_id {
                headers.push(("x-idempotency-key".into(), msg_id));
            }

            let record = Record {
                key: None,
                value: Bytes::from(rec.payload),
                subject: rec.subject,
                headers,
                // Preserve the leader's persisted timestamp so that
                // `seek_by_time` returns identical offsets on leader and
                // follower. The wire format carries ms granularity
                // (`ReplicatedRecord::timestamp_ms`); we round-trip it
                // back to ns for storage. `saturating_mul` defends
                // against pathological far-future values that could
                // overflow (records > year 2554).
                timestamp_ns: Some(rec.timestamp_ms.saturating_mul(1_000_000)),
            };
            let (assigned, _ts) = self.storage.append(&name, &record).await?;

            let expected = base + i as u64;
            if assigned.0 != expected {
                // Offset drift → follower diverged from leader in a way the
                // manifest check didn't catch. Tear down; on reconnect, the
                // divergent-history check will either truncate or reseed.
                error!(
                    stream = %batch.stream,
                    expected,
                    assigned = assigned.0,
                    "replicated record offset mismatch — tearing down session"
                );
                self.metrics.replication_apply_errors_total.add(
                    1,
                    &[KeyValue::new("reason", "offset_mismatch")],
                );
                return Err(ReplicationError::Protocol(format!(
                    "offset mismatch on stream {}: expected {expected}, got {}",
                    batch.stream, assigned.0
                )));
            }
        }

        // Single cursor save per batch — fsync+rename is millisecond-scale
        // on SSD but not free. Per-batch is a reasonable latency/durability
        // trade-off; per-record would multiply fsyncs by batch size.
        let new_next = base + batch_len as u64;
        let mut cursor = self.cursor.lock().await;
        cursor.set(&batch.stream, new_next);
        cursor.save(&self.cursor_path)?;
        debug!(
            stream = %batch.stream,
            base_offset = base,
            count = batch_len,
            new_next,
            "applied RecordsAppended batch"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Handshake helpers — separate functions so tests can stub the wire.
// ---------------------------------------------------------------------------

async fn send_connect<W>(
    framed_write: &mut FramedWrite<W, ExspeedCodec>,
    bearer: &str,
) -> Result<(), ReplicationError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    // Mirror the data-plane Connect shape exactly — Token auth carries
    // the raw bearer as `auth_payload`; the server sha256's + looks it up.
    // See `server.rs::read_connect_and_authorize`.
    let req = ConnectRequest {
        client_id: "exspeed-follower".to_string(),
        auth_type: AuthType::Token,
        auth_payload: Bytes::from(bearer.as_bytes().to_vec()),
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload);
    framed_write
        .send(Frame::new(
            exspeed_protocol::opcodes::OpCode::Connect,
            1,
            payload.freeze(),
        ))
        .await
        .map_err(|e| ReplicationError::Protocol(format!("send Connect: {e}")))
}

async fn read_connect_ok<R>(
    framed_read: &mut FramedRead<R, ExspeedCodec>,
) -> Result<(), ReplicationError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use exspeed_protocol::opcodes::OpCode;

    let frame = framed_read
        .next()
        .await
        .ok_or_else(|| ReplicationError::Protocol("connection closed before ConnectOk".into()))?
        .map_err(|e| ReplicationError::Protocol(format!("ConnectOk decode: {e}")))?;

    match frame.opcode {
        OpCode::ConnectOk => {
            let _ = ConnectResponse::decode(frame.payload)
                .map_err(|e| ReplicationError::Protocol(format!("ConnectOk payload: {e}")))?;
            Ok(())
        }
        OpCode::Error => {
            // Payload: [code(u16 le)][msg_len(u16 le)][msg bytes]
            use bytes::Buf;
            let mut payload = frame.payload.clone();
            let code = if payload.remaining() >= 2 {
                payload.get_u16_le()
            } else {
                0
            };
            let msg_len = if payload.remaining() >= 2 {
                payload.get_u16_le() as usize
            } else {
                0
            };
            let msg = if payload.remaining() >= msg_len {
                let bytes = payload.split_to(msg_len);
                String::from_utf8_lossy(&bytes).into_owned()
            } else {
                "<truncated>".to_string()
            };
            Err(ReplicationError::AuthDenied(format!(
                "leader rejected Connect: {code} {msg}"
            )))
        }
        other => Err(ReplicationError::Protocol(format!(
            "expected ConnectOk, got {other:?}"
        ))),
    }
}

async fn send_replicate_resume<W>(
    framed_write: &mut FramedWrite<W, ExspeedCodec>,
    follower_id: Uuid,
    cursor: &Arc<Mutex<Cursor>>,
) -> Result<(), ReplicationError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let snapshot: BTreeMap<String, u64> = {
        let guard = cursor.lock().await;
        guard.as_map().clone()
    };
    let resume = ReplicateResume {
        follower_id,
        wire_version: REPLICATION_WIRE_VERSION,
        cursor: snapshot,
    };
    let bytes = bincode::serialize(&resume)?;
    framed_write
        .send(Frame::new(
            exspeed_protocol::opcodes::OpCode::ReplicateResume,
            1,
            Bytes::from(bytes),
        ))
        .await
        .map_err(|e| ReplicationError::Protocol(format!("send ReplicateResume: {e}")))
}

async fn read_cluster_manifest<R>(
    framed_read: &mut FramedRead<R, ExspeedCodec>,
    metrics: &Metrics,
) -> Result<ClusterManifest, ReplicationError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use exspeed_protocol::opcodes::OpCode;

    let frame = framed_read
        .next()
        .await
        .ok_or_else(|| ReplicationError::Protocol("closed before ClusterManifest".into()))?
        .map_err(|e| ReplicationError::Protocol(format!("manifest decode: {e}")))?;

    if frame.opcode != OpCode::ClusterManifest {
        return Err(ReplicationError::Protocol(format!(
            "expected ClusterManifest first, got {:?}",
            frame.opcode
        )));
    }
    metrics.replication_bytes_total.add(
        frame.payload.len() as u64,
        &[KeyValue::new("direction", "in")],
    );
    let manifest: ClusterManifest = bincode::deserialize(&frame.payload)?;
    Ok(manifest)
}

/// Sleep for `dur` or until `cancel` fires. Returns `true` if cancelled.
async fn sleep_or_cancel(dur: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(dur) => false,
        _ = cancel.cancelled() => true,
    }
}

fn next_backoff(current: Duration) -> Duration {
    let doubled = current.saturating_mul(2);
    if doubled > MAX_BACKOFF {
        MAX_BACKOFF
    } else {
        doubled
    }
}

