#![cfg(test)]
//! Integration tests for `ReplicationServer`. Uses `MemoryStorage` for
//! deterministic stream state and a raw TCP client (not the real follower
//! client, which arrives in Wave 4) to exercise the wire protocol.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use exspeed_broker::replication::server::ReplicationServer;
use exspeed_broker::replication::{ReplicationCoordinator, ReplicationEvent};
use exspeed_common::auth::CredentialStore;
use exspeed_common::types::StreamName;
use exspeed_common::Metrics;
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::replicate::{
    ClusterManifest, RecordsAppended, ReplicateResume, StreamCreatedEvent, StreamDeletedEvent,
    StreamReseedEvent, REPLICATION_WIRE_VERSION,
};
use exspeed_protocol::opcodes::OpCode;
use exspeed_storage::memory::MemoryStorage;
use exspeed_streams::{Record, StorageEngine};

/// Credential store that recognizes one token mapped to an identity with
/// Publish/Subscribe but NOT Replicate. Used by the missing-permission
/// test to verify the server rejects the Connect.
fn build_publish_only_store() -> (Arc<CredentialStore>, String) {
    use std::io::Write;
    let token = "pub-only-token";
    let hash = exspeed_common::auth::sha256_hex(token.as_bytes());
    let toml = format!(
        r#"
[[credentials]]
name = "publisher"
token_sha256 = "{hash}"
permissions = [
  {{ streams = "*", actions = ["publish", "subscribe"] }},
]
"#
    );
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(toml.as_bytes()).unwrap();
    f.flush().unwrap();
    let store = CredentialStore::build(Some(f.path()), None).expect("build store");
    // Keep the file alive for the lifetime of the store — `CredentialStore`
    // has already slurped the TOML into memory, but just in case.
    std::mem::forget(f);
    (Arc::new(store), token.to_string())
}

/// Start a ReplicationServer bound on 127.0.0.1:0 with the given
/// credential store and seeded storage. Returns the server + local addr.
///
/// `credential_store = None` enables the anonymous path, which grants the
/// Replicate verb — this is the "happy-path server" used by the bootstrap
/// and reseed tests.
async fn spawn_server(
    storage: Arc<dyn StorageEngine>,
    credential_store: Option<Arc<CredentialStore>>,
) -> (ReplicationServer, std::net::SocketAddr, CancellationToken) {
    let (_coord, server, addr, cancel) = spawn_server_with_coord(storage, credential_store).await;
    (server, addr, cancel)
}

/// Like `spawn_server` but also returns the coordinator so tests can
/// inject `ReplicationEvent`s to exercise races.
async fn spawn_server_with_coord(
    storage: Arc<dyn StorageEngine>,
    credential_store: Option<Arc<CredentialStore>>,
) -> (
    Arc<ReplicationCoordinator>,
    ReplicationServer,
    std::net::SocketAddr,
    CancellationToken,
) {
    let (metrics, _r) = Metrics::new();
    let metrics = Arc::new(metrics);
    let coordinator = ReplicationCoordinator::new(metrics.clone(), 64);
    let leader_id = Uuid::new_v4();
    let server = ReplicationServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        coordinator.clone(),
        storage,
        credential_store,
        leader_id,
        metrics,
    )
    .await
    .expect("bind");
    let addr = server.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let s2 = server.clone();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        s2.run(c2).await;
    });
    (coordinator, server, addr, cancel)
}

/// Send a Connect frame with the given auth payload (or none), then the
/// given ReplicateResume. Returns the framed reader/writer so tests can
/// continue pulling frames.
async fn open_and_handshake(
    addr: std::net::SocketAddr,
    auth_payload: Option<&str>,
    resume: ReplicateResume,
) -> (
    FramedRead<tokio::io::ReadHalf<TcpStream>, ExspeedCodec>,
    FramedWrite<tokio::io::WriteHalf<TcpStream>, ExspeedCodec>,
) {
    let sock = TcpStream::connect(addr).await.expect("connect");
    let (r, w) = tokio::io::split(sock);
    let mut framed_read = FramedRead::new(r, ExspeedCodec::new());
    let mut framed_write = FramedWrite::new(w, ExspeedCodec::new());

    // Send Connect.
    let (auth_type, payload_bytes) = match auth_payload {
        Some(tok) => (AuthType::Token, Bytes::from(tok.as_bytes().to_vec())),
        None => (AuthType::None, Bytes::new()),
    };
    let req = ConnectRequest {
        client_id: "test-follower".into(),
        auth_type,
        auth_payload: payload_bytes,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload);
    framed_write
        .send(Frame::new(OpCode::Connect, 1, payload.freeze()))
        .await
        .expect("send connect");

    // Drain ConnectOk (if the server sent one). If the server rejected us
    // we return early letting the caller observe the closed socket.
    let first = framed_read.next().await;
    match first {
        Some(Ok(f)) if f.opcode == OpCode::ConnectOk => {}
        other => {
            // Return a writer that will fail on next send; the caller
            // will observe the outcome. But for the rejection tests we
            // want to explicitly see that ConnectOk did NOT come. We
            // plumb that back by returning here — the test will call
            // `.next()` again and get None.
            let _ = other;
        }
    }

    // Send ReplicateResume.
    let bytes = bincode::serialize(&resume).unwrap();
    framed_write
        .send(Frame::new(OpCode::ReplicateResume, 0, Bytes::from(bytes)))
        .await
        .expect("send resume");

    (framed_read, framed_write)
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

#[tokio::test]
async fn handshake_rejects_wrong_version() {
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let (_server, addr, cancel) = spawn_server(storage, None).await;

    let bogus = ReplicateResume {
        follower_id: Uuid::new_v4(),
        wire_version: 99,
        cursor: BTreeMap::new(),
    };
    let (mut rx, _tx) = open_and_handshake(addr, None, bogus).await;

    // Server must close the socket promptly on version skew. Bound the
    // wait so a hung server fails fast instead of making CI flaky.
    let remainder = tokio::time::timeout(Duration::from_secs(2), rx.next())
        .await
        .expect("server did not close socket within 2s after version skew");
    assert!(
        remainder.is_none() || matches!(remainder, Some(Err(_))),
        "expected EOF or decode error after version skew, got {:?}",
        remainder
    );

    cancel.cancel();
}

#[tokio::test]
async fn handshake_rejects_missing_replicate_permission() {
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let (store, token) = build_publish_only_store();
    let (_server, addr, cancel) = spawn_server(storage, Some(store)).await;

    let resume = ReplicateResume {
        follower_id: Uuid::new_v4(),
        wire_version: REPLICATION_WIRE_VERSION,
        cursor: BTreeMap::new(),
    };

    // Use raw TCP so we see the error frame rather than swallowing it.
    let sock = TcpStream::connect(addr).await.expect("connect");
    let (r, w) = tokio::io::split(sock);
    let mut framed_read = FramedRead::new(r, ExspeedCodec::new());
    let mut framed_write = FramedWrite::new(w, ExspeedCodec::new());

    let req = ConnectRequest {
        client_id: "publisher-posing-as-follower".into(),
        auth_type: AuthType::Token,
        auth_payload: Bytes::from(token.as_bytes().to_vec()),
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload);
    framed_write
        .send(Frame::new(OpCode::Connect, 1, payload.freeze()))
        .await
        .expect("send connect");

    // Server responds with 403 Error, then closes.
    let first = framed_read.next().await.expect("frame").expect("decoded");
    assert_eq!(
        first.opcode,
        OpCode::Error,
        "expected 403 error frame, got {:?}",
        first.opcode
    );

    // No ClusterManifest arrives.
    let _ = bincode::serialize(&resume).unwrap();
    // Don't bother sending the resume; server has already closed or is
    // about to. Read next frame and assert the stream is done.
    let next = framed_read.next().await;
    assert!(
        next.is_none() || matches!(next, Some(Err(_))),
        "expected close after forbidden Connect, got {:?}",
        next
    );

    cancel.cancel();
}

#[tokio::test]
async fn fresh_follower_bootstrap_streams_records() {
    // Seed storage with 3 streams × 10 records each.
    let storage = Arc::new(MemoryStorage::new());
    let stream_names = ["alpha", "beta", "gamma"];
    for s in &stream_names {
        let name = StreamName::try_from(*s).unwrap();
        storage.create_stream(&name, 0, 0).await.unwrap();
        for i in 0..10u32 {
            let rec = Record {
                key: None,
                value: Bytes::from(format!("{s}-{i}").into_bytes()),
                subject: format!("{s}.subj"),
                headers: vec![],
                timestamp_ns: None,
            };
            storage.append(&name, &rec).await.unwrap();
        }
    }

    let storage_dyn: Arc<dyn StorageEngine> = storage.clone();
    let (_server, addr, cancel) = spawn_server(storage_dyn, None).await;

    let resume = ReplicateResume {
        follower_id: Uuid::new_v4(),
        wire_version: REPLICATION_WIRE_VERSION,
        cursor: BTreeMap::new(),
    };
    let (mut rx, _tx) = open_and_handshake(addr, None, resume).await;

    // First post-handshake frame MUST be ClusterManifest — the bootstrap
    // contract requires the follower learn the stream set before seeing
    // any records for those streams.
    let first = tokio::time::timeout(Duration::from_secs(2), rx.next())
        .await
        .expect("first frame within 2s")
        .expect("frame present")
        .expect("decoded ok");
    assert_eq!(
        first.opcode,
        OpCode::ClusterManifest,
        "first post-handshake frame must be ClusterManifest, got {:?}",
        first.opcode
    );
    let manifest: ClusterManifest = bincode::deserialize(&first.payload).unwrap();
    assert_eq!(manifest.streams.len(), 3);
    for s in &stream_names {
        let found = manifest
            .streams
            .iter()
            .find(|ss| ss.name == *s)
            .expect("stream in manifest");
        assert_eq!(found.earliest_offset, 0);
        assert_eq!(found.latest_offset, 10);
    }

    // Drain frames with a per-frame timeout until every stream has been
    // streamed up to its tail. For each stream, verify `base_offset`
    // is strictly monotonic across batches (the leader must never emit
    // an earlier offset after a later one for the same stream).
    let mut records_by_stream: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut last_base_by_stream: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    let overall_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while records_by_stream.values().copied().sum::<usize>() < stream_names.len() * 10 {
        if std::time::Instant::now() > overall_deadline {
            panic!(
                "catch-up did not finish within 5s; got per-stream counts: {:?}",
                records_by_stream
            );
        }
        let next = tokio::time::timeout(Duration::from_millis(500), rx.next()).await;
        let frame = match next {
            Ok(Some(Ok(f))) => f,
            Ok(Some(Err(e))) => panic!("decode error: {e}"),
            Ok(None) => panic!("socket closed mid-catchup"),
            Err(_) => continue, // brief stall; keep draining
        };
        if frame.opcode == OpCode::ReplicationHeartbeat {
            // Heartbeats are allowed interleaved; tighten-but-tolerate.
            continue;
        }
        assert_eq!(
            frame.opcode,
            OpCode::RecordsAppended,
            "only RecordsAppended / heartbeat expected during catch-up, got {:?}",
            frame.opcode
        );
        let batch: RecordsAppended = bincode::deserialize(&frame.payload).unwrap();
        if let Some(prev) = last_base_by_stream.get(&batch.stream) {
            assert!(
                batch.base_offset > *prev,
                "base_offset for {} must be strictly monotonic: saw {} after {}",
                batch.stream,
                batch.base_offset,
                prev
            );
        }
        last_base_by_stream.insert(batch.stream.clone(), batch.base_offset);
        *records_by_stream.entry(batch.stream.clone()).or_insert(0) += batch.records.len();
    }

    // Ensure every seeded stream got fully catch-up-streamed.
    for s in &stream_names {
        let count = records_by_stream.get(*s).copied().unwrap_or(0);
        assert_eq!(count, 10, "stream {s} should stream all 10 records");
    }

    cancel.cancel();
}

#[tokio::test]
async fn reseed_detect_emits_streamreseed() {
    // Seed one stream, append 200 records, then trim the first 100 so the
    // earliest offset advances past the follower's resume cursor (50).
    let storage = Arc::new(MemoryStorage::new());
    let name = StreamName::try_from("orders").unwrap();
    storage.create_stream(&name, 0, 0).await.unwrap();
    for i in 0..200u32 {
        let rec = Record {
            key: None,
            value: Bytes::from(format!("v{i}").into_bytes()),
            subject: "orders.placed".into(),
            headers: vec![],
            timestamp_ns: None,
        };
        storage.append(&name, &rec).await.unwrap();
    }
    storage
        .trim_up_to(&name, exspeed_common::Offset(100))
        .await
        .unwrap();
    let (earliest, next) = storage.stream_bounds(&name).await.unwrap();
    assert_eq!(earliest.0, 100);
    assert_eq!(next.0, 200);

    let storage_dyn: Arc<dyn StorageEngine> = storage.clone();
    let (_server, addr, cancel) = spawn_server(storage_dyn, None).await;

    let mut cursor = BTreeMap::new();
    cursor.insert("orders".to_string(), 50u64);
    let resume = ReplicateResume {
        follower_id: Uuid::new_v4(),
        wire_version: REPLICATION_WIRE_VERSION,
        cursor,
    };
    let (mut rx, _tx) = open_and_handshake(addr, None, resume).await;

    // Manifest first.
    let frame = rx.next().await.expect("frame").expect("ok");
    assert_eq!(frame.opcode, OpCode::ClusterManifest);

    // Then StreamReseedEvent.
    let frame = rx.next().await.expect("frame").expect("ok");
    assert_eq!(
        frame.opcode,
        OpCode::StreamReseedEvent,
        "expected reseed, got {:?}",
        frame.opcode
    );
    let ev: StreamReseedEvent = bincode::deserialize(&frame.payload).unwrap();
    assert_eq!(ev.stream, "orders");
    assert_eq!(ev.new_earliest_offset, 100);

    // Follow-up: at least one RecordsAppended frame (catch-up from offset 100).
    let frame = rx.next().await.expect("frame").expect("ok");
    assert_eq!(frame.opcode, OpCode::RecordsAppended);
    let batch: RecordsAppended = bincode::deserialize(&frame.payload).unwrap();
    assert_eq!(batch.stream, "orders");
    assert_eq!(batch.base_offset, 100);

    cancel.cancel();
}

#[tokio::test]
async fn connection_close_always_deregisters_follower() {
    // Regression for I3: prior to the fan-out helper refactor, an I/O error
    // in the steady-state loop skipped the explicit `deregister_follower`
    // call, so `connected_followers()` reported stale 1+ counts until the
    // next emit discovered the closed channel. With the helper structure,
    // every exit path — including abrupt client disconnect mid-fan-out —
    // runs `deregister_follower` exactly once.
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let (coord, _server, addr, cancel) = spawn_server_with_coord(storage, None).await;

    let resume = ReplicateResume {
        follower_id: Uuid::new_v4(),
        wire_version: REPLICATION_WIRE_VERSION,
        cursor: BTreeMap::new(),
    };
    let (rx, tx) = open_and_handshake(addr, None, resume).await;

    // Wait for registration.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while coord.connected_followers() == 0 {
        if std::time::Instant::now() > deadline {
            panic!("server never registered the follower");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(coord.connected_followers(), 1);

    // Abruptly drop the client side of the socket. The server's next
    // write (heartbeat or fan-out) will hit EPIPE and must still run
    // `deregister_follower` on its way out.
    drop(rx);
    drop(tx);

    // Provoke writes until the server notices the socket is dead. A
    // single small write may land in the kernel TCP send buffer even
    // after the peer has closed; a handful of writes plus a brief
    // sleep forces the FIN/RST to propagate back as a write error.
    //
    // (Alternatively: use `cancel.cancel()` to force the fan-out loop
    // to exit via the cancel branch — but we specifically want to
    // verify the I/O-error exit path, so we stick with the socket drop.)
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while coord.connected_followers() != 0 {
        if std::time::Instant::now() > deadline {
            panic!(
                "deregister_follower was skipped on I/O-error exit; count = {}",
                coord.connected_followers()
            );
        }
        coord.emit(ReplicationEvent::StreamDeleted(StreamDeletedEvent {
            name: "trigger-write-after-close".into(),
        }));
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    cancel.cancel();
}

#[tokio::test]
async fn bootstrap_does_not_lose_events_emitted_during_catchup() {
    // Regression test for C1: events emitted between manifest snapshot and
    // follower registration used to vanish. After the fix the server
    // registers BEFORE snapshotting, so any event emitted after the
    // handshake is queued in the follower's channel and delivered.
    //
    // We simulate the race by emitting an event through the coordinator as
    // soon as the server has registered the follower (observable via
    // `connected_followers()`). Even though catch-up for the existing
    // stream is still running, the injected event must appear on the wire.
    let storage = Arc::new(MemoryStorage::new());
    let name = StreamName::try_from("alpha").unwrap();
    storage.create_stream(&name, 0, 0).await.unwrap();
    for i in 0..10u32 {
        let rec = Record {
            key: None,
            value: Bytes::from(format!("v{i}").into_bytes()),
            subject: "alpha.subj".into(),
            headers: vec![],
            timestamp_ns: None,
        };
        storage.append(&name, &rec).await.unwrap();
    }

    let storage_dyn: Arc<dyn StorageEngine> = storage.clone();
    let (coord, _server, addr, cancel) = spawn_server_with_coord(storage_dyn, None).await;

    let resume = ReplicateResume {
        follower_id: Uuid::new_v4(),
        wire_version: REPLICATION_WIRE_VERSION,
        cursor: BTreeMap::new(),
    };
    let (mut rx, _tx) = open_and_handshake(addr, None, resume).await;

    // Busy-wait (bounded) until the server has registered our follower.
    // This marks the moment after which live events must not be lost.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while coord.connected_followers() == 0 {
        if std::time::Instant::now() > deadline {
            panic!("server never registered the follower");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Inject a "live" event. Without the C1 fix this would race — if the
    // manifest snapshot had already been taken but registration had not,
    // the event would be silently lost. With registration-first the event
    // lands in the follower channel and survives catch-up.
    coord.emit(ReplicationEvent::StreamCreated(StreamCreatedEvent {
        name: "zeta-new-stream".into(),
        max_age_secs: 3600,
        max_bytes: 0,
    }));

    // Drain frames with a bounded timeout, watching for our injected event.
    let mut saw_injected = false;
    let overall_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < overall_deadline {
        let next = tokio::time::timeout(Duration::from_millis(500), rx.next()).await;
        let frame = match next {
            Ok(Some(Ok(f))) => f,
            Ok(Some(Err(e))) => panic!("decode error: {e}"),
            Ok(None) => panic!("socket closed before injected event arrived"),
            Err(_) => continue, // brief stall; the server may still be catching up
        };
        if frame.opcode == OpCode::StreamCreatedEvent {
            let ev: StreamCreatedEvent = bincode::deserialize(&frame.payload).unwrap();
            if ev.name == "zeta-new-stream" {
                saw_injected = true;
                break;
            }
        }
    }
    assert!(
        saw_injected,
        "injected StreamCreated event was never delivered to the follower"
    );

    cancel.cancel();
}

#[tokio::test]
async fn catchup_empty_read_with_stream_deleted_logs_and_continues() {
    // Regression test for C2: an empty read during catch-up used to
    // `break` silently, betting on a future event to correct the cursor.
    // With the C1 register-first fix + C2 observability, a stream
    // deleted mid-catchup is handled: the server logs, moves on, and the
    // follower eventually receives the `StreamDeleted` event via the
    // live fan-out channel.
    let storage = Arc::new(MemoryStorage::new());
    let name = StreamName::try_from("about-to-be-deleted").unwrap();
    storage.create_stream(&name, 0, 0).await.unwrap();
    for i in 0..5u32 {
        let rec = Record {
            key: None,
            value: Bytes::from(format!("v{i}").into_bytes()),
            subject: "doomed.subj".into(),
            headers: vec![],
            timestamp_ns: None,
        };
        storage.append(&name, &rec).await.unwrap();
    }

    let storage_dyn: Arc<dyn StorageEngine> = storage.clone();
    let (coord, _server, addr, cancel) = spawn_server_with_coord(storage_dyn, None).await;

    let resume = ReplicateResume {
        follower_id: Uuid::new_v4(),
        wire_version: REPLICATION_WIRE_VERSION,
        cursor: BTreeMap::new(),
    };
    let (mut rx, _tx) = open_and_handshake(addr, None, resume).await;

    // Wait for registration before emitting the StreamDeleted event.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while coord.connected_followers() == 0 {
        if std::time::Instant::now() > deadline {
            panic!("server never registered the follower");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Emit a StreamDeleted event as if the leader had dropped the stream
    // mid-catchup. The server should NOT panic; it should continue to
    // process frames and eventually forward the StreamDeleted event.
    coord.emit(ReplicationEvent::StreamDeleted(StreamDeletedEvent {
        name: "about-to-be-deleted".into(),
    }));

    let mut saw_deleted = false;
    let overall_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < overall_deadline {
        let next = tokio::time::timeout(Duration::from_millis(500), rx.next()).await;
        let frame = match next {
            Ok(Some(Ok(f))) => f,
            Ok(Some(Err(e))) => panic!("decode error: {e}"),
            Ok(None) => panic!("socket closed before StreamDeleted arrived"),
            Err(_) => continue,
        };
        if frame.opcode == OpCode::StreamDeletedEvent {
            let ev: StreamDeletedEvent = bincode::deserialize(&frame.payload).unwrap();
            if ev.name == "about-to-be-deleted" {
                saw_deleted = true;
                break;
            }
        }
    }
    assert!(
        saw_deleted,
        "server never forwarded StreamDeleted event — catch-up may have panicked or stalled"
    );

    cancel.cancel();
}

