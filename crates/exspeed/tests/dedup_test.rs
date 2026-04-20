/// End-to-end integration tests for the idempotent-publish feature.
///
/// All tests go through the real TCP wire protocol: they spin up a genuine
/// server (FileStorage + Broker + API), connect with `FramedRead`/`FramedWrite`
/// pairs, and decode server responses using `ServerMessage::from_frame`.
///
/// Restart tests (tests 3 & 4) use `run_with_shutdown` with a `CancellationToken`
/// so the server can be stopped gracefully, then restarted on the same `data_dir`.
use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use std::path::{Path, PathBuf};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::publish::PublishRequest;
use exspeed_protocol::messages::stream_mgmt::CreateStreamRequest;
use exspeed_protocol::messages::{ServerMessage, ERR_DEDUP_MAP_FULL, ERR_KEY_COLLISION};
use exspeed_protocol::opcodes::OpCode;

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

type FramedReader = FramedRead<tokio::net::tcp::OwnedReadHalf, ExspeedCodec>;
type FramedWriter = FramedWrite<tokio::net::tcp::OwnedWriteHalf, ExspeedCodec>;

// ---------------------------------------------------------------------------
// Server helpers
// ---------------------------------------------------------------------------

/// Start a server with a temporary data directory.
/// Returns `(tcp_addr, http_addr, data_dir, cancel_token)`.
/// `data_dir` is a `PathBuf` owned by the caller; `cancel_token` triggers shutdown.
/// The tempdir is kept alive by the spawned task.
async fn start_server_temp() -> (String, String, PathBuf, CancellationToken) {
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let http_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{}", tcp_port);
    let http_addr = format!("127.0.0.1:{}", http_port);
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let cancel = CancellationToken::new();
    let args = exspeed::cli::server::ServerArgs {
        bind: tcp_addr.clone(),
        api_bind: http_addr.clone(),
        data_dir: data_dir.clone(),
        auth_token: None,
        tls_cert: None,
        tls_key: None,
    };
    let cancel_for_server = cancel.clone();
    tokio::spawn(async move {
        let _tmp = tmp; // keep temp dir alive for the lifetime of this task
        // Move the token into the async block; use an owned future so the
        // reference doesn't escape the block's lifetime.
        let shutdown_fut = async move { cancel_for_server.cancelled().await };
        exspeed::cli::server::run_with_shutdown(args, shutdown_fut).await.ok();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    (tcp_addr, http_addr, data_dir, cancel)
}

/// Start a server at an explicit `data_dir` path.
/// Returns `(tcp_addr, http_addr, cancel_token)`.
async fn start_server_at(data_dir: PathBuf) -> (String, String, CancellationToken) {
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let http_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{}", tcp_port);
    let http_addr = format!("127.0.0.1:{}", http_port);

    let cancel = CancellationToken::new();
    let args = exspeed::cli::server::ServerArgs {
        bind: tcp_addr.clone(),
        api_bind: http_addr.clone(),
        data_dir,
        auth_token: None,
        tls_cert: None,
        tls_key: None,
    };
    let cancel_for_server = cancel.clone();
    tokio::spawn(async move {
        let shutdown_fut = async move { cancel_for_server.cancelled().await };
        exspeed::cli::server::run_with_shutdown(args, shutdown_fut).await.ok();
    });

    // Wait for the server to become ready by polling the TCP port.
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if TcpStream::connect(&tcp_addr).await.is_ok() {
            break;
        }
    }
    (tcp_addr, http_addr, cancel)
}

/// Gracefully shut down a server by cancelling its token.
///
/// Because `run_with_shutdown` leaks the data-dir lock file handle via
/// `Box::leak` (intentional for production), the flock on `.exspeed.lock`
/// outlives the task within the same OS process.  To allow a second server to
/// start on the same data_dir in a test, we delete the lock file after
/// cancellation.  On unix the old (now-deleted) inode keeps its flock on the
/// in-memory fd, but a new `create` of the same path creates a fresh inode
/// the second server can lock successfully.
async fn shutdown(cancel: CancellationToken, data_dir: &Path) {
    cancel.cancel();

    // Give the server's TCP drain + snapshot task time to finish.
    // With no active connections the drain exits in <50ms; the snapshot write
    // is typically <5ms for small test maps.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Remove the lock file so the next server can create a fresh one.
    let lock_path = data_dir.join(".exspeed.lock");
    let _ = std::fs::remove_file(&lock_path); // ignore "not found"
}

// ---------------------------------------------------------------------------
// TCP helpers
// ---------------------------------------------------------------------------

async fn connect_to(addr: &str) -> (FramedReader, FramedWriter) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, writer) = stream.into_split();
    (
        FramedRead::new(reader, ExspeedCodec::new()),
        FramedWrite::new(writer, ExspeedCodec::new()),
    )
}

async fn send_recv(writer: &mut FramedWriter, reader: &mut FramedReader, frame: Frame) -> Frame {
    writer.send(frame).await.unwrap();
    timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout waiting for response")
        .unwrap()
        .unwrap()
}

fn connect_frame(corr: u32) -> Frame {
    let req = ConnectRequest {
        client_id: "dedup-test".into(),
        auth_type: AuthType::None,
        auth_payload: Bytes::new(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Connect, corr, buf.freeze())
}

fn create_stream_frame(stream_name: &str, corr: u32) -> Frame {
    let req = CreateStreamRequest {
        stream_name: stream_name.into(),
        max_age_secs: 0,
        max_bytes: 0,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::CreateStream, corr, buf.freeze())
}

fn publish_frame(
    stream: &str,
    subject: &str,
    body: &[u8],
    msg_id: Option<&str>,
    headers: Vec<(&str, &str)>,
    corr: u32,
) -> Frame {
    let req = PublishRequest {
        stream: stream.into(),
        subject: subject.into(),
        key: None,
        msg_id: msg_id.map(|s| s.to_string()),
        value: Bytes::copy_from_slice(body),
        headers: headers
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Publish, corr, buf.freeze())
}

/// Decode a PublishOk frame and return `(offset, duplicate)`.
fn decode_publish_ok(frame: Frame) -> (u64, bool) {
    assert_eq!(
        frame.opcode,
        OpCode::PublishOk,
        "expected PublishOk, got {:?}",
        frame.opcode
    );
    match ServerMessage::from_frame(frame).unwrap() {
        ServerMessage::PublishOk { offset, duplicate } => (offset, duplicate),
        other => panic!("expected PublishOk variant, got {other:?}"),
    }
}

/// Decode a frame known to be an Error and assert its error code.
/// Returns the full decoded `ServerMessage` for further inspection.
fn decode_error_frame(frame: Frame, expected_code: u16) -> ServerMessage {
    assert_eq!(
        frame.opcode,
        OpCode::Error,
        "expected Error frame, got {:?}",
        frame.opcode
    );
    let msg = ServerMessage::from_frame(frame).unwrap();
    match &msg {
        ServerMessage::KeyCollision { .. } => {
            assert_eq!(expected_code, ERR_KEY_COLLISION);
        }
        ServerMessage::DedupMapFull { .. } => {
            assert_eq!(expected_code, ERR_DEDUP_MAP_FULL);
        }
        ServerMessage::Error { code, .. } => {
            assert_eq!(*code, expected_code, "unexpected error code");
        }
        other => panic!("expected an Error variant, got {other:?}"),
    }
    msg
}

// ---------------------------------------------------------------------------
// HTTP helper — create a stream with per-stream dedup config
// ---------------------------------------------------------------------------

async fn http_create_stream_with_dedup(
    http_base: &str,
    name: &str,
    dedup_window_secs: u64,
    dedup_max_entries: u64,
) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{http_base}/api/v1/streams"))
        .json(&serde_json::json!({
            "name": name,
            "dedup_window_secs": dedup_window_secs,
            "dedup_max_entries": dedup_max_entries,
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 409,
        "create stream returned {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// Test 1: duplicate same key + body → duplicate:true + same offset
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dedup_same_key_and_body_returns_duplicate() {
    let (tcp_addr, _http_addr, _data_dir, _cancel) = start_server_temp().await;

    let (mut reader, mut writer) = connect_to(&tcp_addr).await;
    send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    send_recv(&mut writer, &mut reader, create_stream_frame("orders", 2)).await;

    // First publish
    let r1 = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("orders", "o.created", b"body", Some("msg-1"), vec![], 10),
    )
    .await;
    let (o1, dup1) = decode_publish_ok(r1);
    assert!(!dup1, "first publish should not be duplicate");

    // Retry — identical key + body
    let r2 = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("orders", "o.created", b"body", Some("msg-1"), vec![], 11),
    )
    .await;
    let (o2, dup2) = decode_publish_ok(r2);
    assert!(dup2, "retry should be duplicate");
    assert_eq!(o1, o2, "duplicate must return the same offset");
}

// ---------------------------------------------------------------------------
// Test 2: same key + different body → KeyCollision error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dedup_different_body_same_key_returns_collision() {
    let (tcp_addr, _http_addr, _data_dir, _cancel) = start_server_temp().await;

    let (mut reader, mut writer) = connect_to(&tcp_addr).await;
    send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    send_recv(&mut writer, &mut reader, create_stream_frame("coll-stream", 2)).await;

    // First publish
    let r1 = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("coll-stream", "evt", b"body-A", Some("msg-A"), vec![], 10),
    )
    .await;
    let (stored_offset, _) = decode_publish_ok(r1);

    // Second publish: same key, different body
    let r2 = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("coll-stream", "evt", b"body-B", Some("msg-A"), vec![], 11),
    )
    .await;
    let msg = decode_error_frame(r2, ERR_KEY_COLLISION);
    match msg {
        ServerMessage::KeyCollision { stored_offset: so } => {
            assert_eq!(so, stored_offset, "collision must point to the original offset");
        }
        other => panic!("expected KeyCollision, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Test 3: restart via snapshot path retains dedup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restart_via_snapshot_path_retains_dedup() {
    // Use a persistent (non-temp) dir so we can restart on the same path.
    let dir = tempfile::TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    // --- First server lifecycle ---
    let (tcp_addr, http_addr, cancel) = start_server_at(data_dir.clone()).await;

    // Create a stream via the HTTP API (easier for named streams)
    {
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{http_addr}/api/v1/streams"))
            .json(&serde_json::json!({"name": "snap-stream"}))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
    }

    // Publish 10 records with distinct msg_ids
    {
        let (mut reader, mut writer) = connect_to(&tcp_addr).await;
        send_recv(&mut writer, &mut reader, connect_frame(1)).await;

        for i in 0u32..10 {
            let msg_id = format!("msg-{i}");
            let body = format!("body-{i}");
            let frame = publish_frame(
                "snap-stream",
                "evt",
                body.as_bytes(),
                Some(&msg_id),
                vec![],
                10 + i,
            );
            let resp = send_recv(&mut writer, &mut reader, frame).await;
            let (_, dup) = decode_publish_ok(resp);
            assert!(!dup, "first publish of msg-{i} must not be duplicate");
        }
    }

    // Trigger snapshot + graceful shutdown
    shutdown(cancel, &data_dir).await;

    // --- Second server lifecycle on same data_dir ---
    let (tcp_addr2, _http_addr2, cancel2) = start_server_at(data_dir.clone()).await;

    {
        let (mut reader, mut writer) = connect_to(&tcp_addr2).await;
        send_recv(&mut writer, &mut reader, connect_frame(1)).await;

        for i in 0u32..10 {
            let msg_id = format!("msg-{i}");
            let body = format!("body-{i}");
            let frame = publish_frame(
                "snap-stream",
                "evt",
                body.as_bytes(),
                Some(&msg_id),
                vec![],
                10 + i,
            );
            let resp = send_recv(&mut writer, &mut reader, frame).await;
            let (_offset, dup) = decode_publish_ok(resp);
            assert!(dup, "retry of msg-{i} after restart must be duplicate");
        }
    }

    cancel2.cancel();
}

// ---------------------------------------------------------------------------
// Test 4: full-scan fallback when snapshot is deleted before restart
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restart_full_scan_fallback_when_snapshot_missing() {
    let dir = tempfile::TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    // --- First server lifecycle ---
    let (tcp_addr, http_addr, cancel) = start_server_at(data_dir.clone()).await;

    {
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{http_addr}/api/v1/streams"))
            .json(&serde_json::json!({"name": "scan-stream"}))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
    }

    {
        let (mut reader, mut writer) = connect_to(&tcp_addr).await;
        send_recv(&mut writer, &mut reader, connect_frame(1)).await;

        for i in 0u32..10 {
            let msg_id = format!("msg-{i}");
            let body = format!("body-{i}");
            let frame = publish_frame(
                "scan-stream",
                "evt",
                body.as_bytes(),
                Some(&msg_id),
                vec![],
                10 + i,
            );
            let resp = send_recv(&mut writer, &mut reader, frame).await;
            let (_, dup) = decode_publish_ok(resp);
            assert!(!dup, "first publish of msg-{i} must not be duplicate");
        }
    }

    shutdown(cancel, &data_dir).await;

    // Delete the snapshot file so the server must fall back to full log scan.
    let snapshot_path = data_dir
        .join("streams")
        .join("scan-stream")
        .join("dedup_snapshot.bin");
    if snapshot_path.exists() {
        std::fs::remove_file(&snapshot_path).unwrap();
    }

    // --- Second server lifecycle ---
    let (tcp_addr2, _http_addr2, cancel2) = start_server_at(data_dir.clone()).await;

    {
        let (mut reader, mut writer) = connect_to(&tcp_addr2).await;
        send_recv(&mut writer, &mut reader, connect_frame(1)).await;

        for i in 0u32..10 {
            let msg_id = format!("msg-{i}");
            let body = format!("body-{i}");
            let frame = publish_frame(
                "scan-stream",
                "evt",
                body.as_bytes(),
                Some(&msg_id),
                vec![],
                10 + i,
            );
            let resp = send_recv(&mut writer, &mut reader, frame).await;
            let (_offset, dup) = decode_publish_ok(resp);
            assert!(
                dup,
                "retry of msg-{i} after restart + missing snapshot must be duplicate"
            );
        }
    }

    cancel2.cancel();
}

// ---------------------------------------------------------------------------
// Test 5: cap hit, then eviction, then retry succeeds
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cap_hit_recovers_after_eviction() {
    let (tcp_addr, http_addr, _data_dir, _cancel) = start_server_temp().await;

    // Create stream with tight cap + very short window via HTTP API
    http_create_stream_with_dedup(&format!("http://{http_addr}"), "cap-stream", 1, 2).await;

    let (mut reader, mut writer) = connect_to(&tcp_addr).await;
    send_recv(&mut writer, &mut reader, connect_frame(1)).await;

    // Fill the cap: publish k1 and k2 (cap=2)
    for (i, key) in ["k1", "k2"].iter().enumerate() {
        let frame = publish_frame(
            "cap-stream",
            "evt",
            format!("body-{i}").as_bytes(),
            Some(key),
            vec![],
            10 + i as u32,
        );
        let resp = send_recv(&mut writer, &mut reader, frame).await;
        decode_publish_ok(resp);
    }

    // Third publish with a new key: cap is full → DedupMapFull
    let r3 = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("cap-stream", "evt", b"body-c", Some("k3"), vec![], 20),
    )
    .await;
    let msg = decode_error_frame(r3, ERR_DEDUP_MAP_FULL);
    assert!(
        matches!(msg, ServerMessage::DedupMapFull { .. }),
        "expected DedupMapFull, got {msg:?}"
    );

    // Wait for window to expire (window=1s, so 1.5s is enough)
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Retry k3 — entries are expired, should succeed as a new write
    let r4 = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("cap-stream", "evt", b"body-c", Some("k3"), vec![], 21),
    )
    .await;
    let (_offset, dup) = decode_publish_ok(r4);
    assert!(!dup, "after eviction, k3 must succeed as a fresh write");
}

// ---------------------------------------------------------------------------
// Test 6: non-msg_id publishes are unaffected by a full cap
// ---------------------------------------------------------------------------

#[tokio::test]
async fn no_msg_id_publishes_unaffected_by_cap() {
    let (tcp_addr, http_addr, _data_dir, _cancel) = start_server_temp().await;

    // Cap = 1
    http_create_stream_with_dedup(&format!("http://{http_addr}"), "pass-stream", 300, 1).await;

    let (mut reader, mut writer) = connect_to(&tcp_addr).await;
    send_recv(&mut writer, &mut reader, connect_frame(1)).await;

    // Fill the single slot
    let r1 = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("pass-stream", "evt", b"body-a", Some("k1"), vec![], 10),
    )
    .await;
    decode_publish_ok(r1);

    // Now the cap is full.  Publish without msg_id — should go through (fast path).
    let r2 = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("pass-stream", "evt", b"plain-body", None, vec![], 11),
    )
    .await;
    let (_, dup) = decode_publish_ok(r2);
    assert!(!dup, "pass-through publish must succeed even when dedup cap is full");
}

// ---------------------------------------------------------------------------
// Test 7: legacy x-idempotency-key header path still works
// ---------------------------------------------------------------------------

#[tokio::test]
async fn legacy_header_path_still_works() {
    let (tcp_addr, _http_addr, _data_dir, _cancel) = start_server_temp().await;

    let (mut reader, mut writer) = connect_to(&tcp_addr).await;
    send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    send_recv(&mut writer, &mut reader, create_stream_frame("legacy-stream", 2)).await;

    // First publish using the header, not the msg_id field
    let r1 = send_recv(
        &mut writer,
        &mut reader,
        publish_frame(
            "legacy-stream",
            "evt",
            b"body",
            None, // no msg_id field
            vec![("x-idempotency-key", "hdr-key-1")],
            10,
        ),
    )
    .await;
    let (o1, dup1) = decode_publish_ok(r1);
    assert!(!dup1, "first publish via header must not be duplicate");

    // Retry with the same header
    let r2 = send_recv(
        &mut writer,
        &mut reader,
        publish_frame(
            "legacy-stream",
            "evt",
            b"body",
            None,
            vec![("x-idempotency-key", "hdr-key-1")],
            11,
        ),
    )
    .await;
    let (o2, dup2) = decode_publish_ok(r2);
    assert!(dup2, "retry via same header must be duplicate");
    assert_eq!(o1, o2, "duplicate must return the same offset");
}
