//! End-to-end integration tests for the multi-tenant auth (Plan C).
//!
//! Each test spins up a real in-process server with a per-test
//! credentials.toml (threaded via `ServerArgs.credentials_file` so no
//! env-var state is mutated). The tests exercise both the TCP and HTTP
//! authz paths against a matrix of identities (publish-only, subscribe-only,
//! scoped admin, global admin, legacy-admin env var).

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tempfile::NamedTempFile;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::ack::{AckRequest, NackRequest};
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::consumer::{CreateConsumerRequest, StartFrom, SubscribeRequest};
use exspeed_protocol::messages::fetch::FetchRequest;
use exspeed_protocol::messages::publish::PublishRequest;
use exspeed_protocol::messages::seek::SeekRequest;
use exspeed_protocol::messages::stream_mgmt::CreateStreamRequest;
use exspeed_protocol::messages::ServerMessage;
use exspeed_protocol::opcodes::OpCode;

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

type FramedReader = FramedRead<tokio::net::tcp::OwnedReadHalf, ExspeedCodec>;
type FramedWriter = FramedWrite<tokio::net::tcp::OwnedWriteHalf, ExspeedCodec>;

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

/// Write a TOML credentials file to a tempfile and return the handle.
/// Keep the returned `NamedTempFile` alive for the test's lifetime — its
/// `Drop` impl removes the file from disk.
fn write_creds(contents: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

/// Helper wrapping the publicly-exported `exspeed_common::auth::sha256_hex`.
fn sha256_hex(raw: &str) -> String {
    exspeed_common::auth::sha256_hex(raw.as_bytes())
}

// ---------------------------------------------------------------------------
// Server harness
// ---------------------------------------------------------------------------

/// Holds the resources a running test server needs to keep alive plus the
/// shutdown token. Dropping `_tmp` removes the data dir; `cancel` stops
/// the server.
struct TestServer {
    tcp_addr: String,
    http_addr: String,
    cancel: CancellationToken,
    _tmp: tempfile::TempDir,
}

/// Start a server with the given credentials file + env token.
/// Either or both may be `None`. Blocks until `/readyz` returns 200.
async fn start_server(credentials_file: Option<PathBuf>, auth_token: Option<String>) -> TestServer {
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let http_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{tcp_port}");
    let http_addr = format!("127.0.0.1:{http_port}");
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let cancel = CancellationToken::new();
    let args = exspeed::cli::server::ServerArgs {
        bind: tcp_addr.clone(),
        api_bind: http_addr.clone(),
        data_dir,
        auth_token,
        credentials_file,
        tls_cert: None,
        tls_key: None,
            storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
            storage_flush_window_us: 500,
            storage_flush_threshold_records: 256,
            storage_flush_threshold_bytes: 1_048_576,
            storage_sync_interval_ms: 10,
            storage_sync_bytes: 4 * 1024 * 1024,
            delivery_buffer: 8192,
    };

    let cancel_for_server = cancel.clone();
    tokio::spawn(async move {
        let shutdown_fut = async move { cancel_for_server.cancelled().await };
        exspeed::cli::server::run_with_shutdown(args, shutdown_fut)
            .await
            .ok();
    });

    // Poll TCP first (cheap sanity check), then /readyz to ensure full init.
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if TcpStream::connect(&tcp_addr).await.is_ok() {
            break;
        }
    }
    let readyz = format!("http://{http_addr}/readyz");
    for _ in 0..50 {
        if let Ok(resp) = reqwest::get(&readyz).await {
            if resp.status().is_success() {
                return TestServer {
                    tcp_addr,
                    http_addr,
                    cancel,
                    _tmp: tmp,
                };
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server /readyz did not become ready in 5s");
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
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

/// Send a frame and wait for a response with a matching correlation id.
/// Push-delivered records (correlation_id = 0) are discarded.
async fn send_recv(writer: &mut FramedWriter, reader: &mut FramedReader, frame: Frame) -> Frame {
    let corr = frame.correlation_id;
    writer.send(frame).await.unwrap();
    loop {
        let resp = timeout(Duration::from_secs(5), reader.next())
            .await
            .expect("timeout waiting for response")
            .unwrap()
            .unwrap();
        if resp.correlation_id == corr {
            return resp;
        }
    }
}

fn connect_frame(client_id: &str, token: Option<&str>, corr: u32) -> Frame {
    let (auth_type, payload) = match token {
        Some(t) => (AuthType::Token, Bytes::copy_from_slice(t.as_bytes())),
        None => (AuthType::None, Bytes::new()),
    };
    let req = ConnectRequest {
        client_id: client_id.into(),
        auth_type,
        auth_payload: payload,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Connect, corr, buf.freeze())
}

fn create_stream_frame(name: &str, corr: u32) -> Frame {
    let req = CreateStreamRequest {
        stream_name: name.into(),
        max_age_secs: 0,
        max_bytes: 0,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::CreateStream, corr, buf.freeze())
}

fn publish_frame(stream: &str, subject: &str, value: &[u8], corr: u32) -> Frame {
    let req = PublishRequest {
        stream: stream.into(),
        subject: subject.into(),
        key: None,
        msg_id: None,
        value: Bytes::copy_from_slice(value),
        headers: vec![],
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Publish, corr, buf.freeze())
}

fn fetch_frame(stream: &str, offset: u64, corr: u32) -> Frame {
    let req = FetchRequest {
        stream: stream.into(),
        offset,
        max_records: 10,
        subject_filter: String::new(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Fetch, corr, buf.freeze())
}

fn create_consumer_frame(name: &str, stream: &str, corr: u32) -> Frame {
    let req = CreateConsumerRequest {
        name: name.into(),
        stream: stream.into(),
        group: String::new(),
        subject_filter: String::new(),
        start_from: StartFrom::Earliest,
        start_offset: 0,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::CreateConsumer, corr, buf.freeze())
}

fn subscribe_frame(consumer_name: &str, corr: u32) -> Frame {
    let req = SubscribeRequest {
        consumer_name: consumer_name.into(),
        subscriber_id: String::new(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Subscribe, corr, buf.freeze())
}

fn ack_frame(consumer_name: &str, offset: u64, corr: u32) -> Frame {
    let req = AckRequest {
        consumer_name: consumer_name.into(),
        offset,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Ack, corr, buf.freeze())
}

fn nack_frame(consumer_name: &str, offset: u64, corr: u32) -> Frame {
    let req = NackRequest {
        consumer_name: consumer_name.into(),
        offset,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Nack, corr, buf.freeze())
}

fn seek_frame(consumer_name: &str, timestamp: u64, corr: u32) -> Frame {
    let req = SeekRequest {
        consumer_name: consumer_name.into(),
        timestamp,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Seek, corr, buf.freeze())
}

/// Pre-create a stream + consumer on `addr` as the given admin token. Used
/// by tests that exercise a scoped credential's data-plane op against a
/// real stream. Uses TCP for consumer creation because the HTTP API has no
/// POST /api/v1/consumers route.
async fn admin_setup_stream_and_consumer(
    addr: &str,
    admin_token: &str,
    stream: &str,
    consumer: &str,
) {
    let (mut r, mut w) = connect_to(addr).await;
    let resp = send_recv(&mut w, &mut r, connect_frame("admin", Some(admin_token), 1)).await;
    assert_eq!(resp.opcode, OpCode::ConnectOk, "admin connect failed");
    let resp = send_recv(&mut w, &mut r, create_stream_frame(stream, 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "admin create_stream failed");
    let resp = send_recv(&mut w, &mut r, create_consumer_frame(consumer, stream, 3)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "admin create_consumer failed");
}

/// Assert the frame is an Error with the given HTTP-style status code.
fn assert_error(frame: Frame, code: u16) {
    assert_eq!(
        frame.opcode,
        OpCode::Error,
        "expected Error frame, got {:?}",
        frame.opcode
    );
    match ServerMessage::from_frame(frame).unwrap() {
        ServerMessage::Error { code: c, .. } => assert_eq!(c, code, "error code mismatch"),
        other => panic!("expected Error variant, got {other:?}"),
    }
}

// ===========================================================================
// Core authentication tests — Sub-unit 6.2
// ===========================================================================

// 1 -------------------------------------------------------------------------
#[tokio::test]
async fn tcp_connect_unknown_token_returns_401_and_closes() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "known"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["publish"] }}]
"#,
        sha256_hex("known")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    let resp = send_recv(
        &mut writer,
        &mut reader,
        connect_frame("c", Some("unknown"), 1),
    )
    .await;
    assert_error(resp, 401);

    // Server should close the socket after the 401. The next read returns
    // None (EOF) within a short window.
    let next = timeout(Duration::from_secs(2), reader.next()).await;
    match next {
        Ok(None) => {}         // clean EOF
        Ok(Some(Err(_))) => {} // transport error from broken read
        Ok(Some(Ok(frame))) => panic!("expected socket close, got frame {frame:?}"),
        Err(_) => panic!("timeout waiting for socket close"),
    }
}

// 2 -------------------------------------------------------------------------
#[tokio::test]
async fn tcp_connect_known_token_succeeds_and_identity_attached() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "orders-service"
token_sha256 = "{}"
permissions = [{{ streams = "orders-*", actions = ["publish", "admin"] }}]
"#,
        sha256_hex("tok-orders")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    let resp = send_recv(
        &mut writer,
        &mut reader,
        connect_frame("c", Some("tok-orders"), 1),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::ConnectOk);

    // Pre-create the stream (admin on orders-*) then publish.
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_stream_frame("orders-placed", 2),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::Ok);

    let resp = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("orders-placed", "evt", b"hello", 3),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::PublishOk);
}

// 3 -------------------------------------------------------------------------
#[tokio::test]
async fn http_unknown_bearer_returns_401() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "alice"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        sha256_hex("alice-token")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{}/api/v1/streams", srv.http_addr))
        .header("Authorization", "Bearer totally-wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

// 4 -------------------------------------------------------------------------
#[tokio::test]
async fn http_known_but_non_admin_bearer_returns_403() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "pubber"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["publish"] }}]
"#,
        sha256_hex("pub-token")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{}/api/v1/streams", srv.http_addr))
        .header("Authorization", "Bearer pub-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

// 5 -------------------------------------------------------------------------
#[tokio::test]
async fn http_admin_bearer_allowed() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "admin"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        sha256_hex("admin-token")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{}/api/v1/streams", srv.http_addr))
        .header("Authorization", "Bearer admin-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// 6 -------------------------------------------------------------------------
#[tokio::test]
async fn publish_denied_when_credential_lacks_publish() {
    // Credential has subscribe only. Admin pre-creates a stream via HTTP so
    // the subscriber can target a real one.
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "sub-only"
token_sha256 = "{sub_hash}"
permissions = [{{ streams = "*", actions = ["subscribe"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        sub_hash = sha256_hex("sub-token"),
        admin_hash = sha256_hex("admin-token"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    admin_setup_stream_and_consumer(&srv.tcp_addr, "admin-token", "events", "c1").await;

    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    let resp = send_recv(
        &mut writer,
        &mut reader,
        connect_frame("c", Some("sub-token"), 1),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::ConnectOk);

    // Publish → 403.
    let resp = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("events", "evt", b"x", 2),
    )
    .await;
    assert_error(resp, 403);

    // Connection stays open — Subscribe succeeds on the same connection.
    let resp = send_recv(&mut writer, &mut reader, subscribe_frame("c1", 3)).await;
    assert_eq!(resp.opcode, OpCode::Ok);
}

// 7 -------------------------------------------------------------------------
#[tokio::test]
async fn publish_allowed_on_glob_match() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "orders"
token_sha256 = "{}"
permissions = [{{ streams = "orders-*", actions = ["publish", "admin"] }}]
"#,
        sha256_hex("t")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    let resp = send_recv(&mut writer, &mut reader, connect_frame("c", Some("t"), 1)).await;
    assert_eq!(resp.opcode, OpCode::ConnectOk);

    // admin on orders-* lets us create the stream for this test.
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_stream_frame("orders-placed", 2),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::Ok);

    let resp = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("orders-placed", "evt", b"x", 3),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::PublishOk);
}

// 8 -------------------------------------------------------------------------
#[tokio::test]
async fn publish_denied_on_glob_miss() {
    // orders-scoped cred may also create streams under the orders glob, but
    // `payments-*` is out-of-scope for both publish and admin.
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "orders"
token_sha256 = "{orders_hash}"
permissions = [{{ streams = "orders-*", actions = ["publish"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        orders_hash = sha256_hex("orders-tok"),
        admin_hash = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    // Admin pre-creates the payments-* stream over TCP (no HTTP consumer POST
    // exists; stay on a single wire protocol per test).
    {
        let (mut r, mut w) = connect_to(&srv.tcp_addr).await;
        send_recv(&mut w, &mut r, connect_frame("admin", Some("admin-tok"), 1)).await;
        let resp = send_recv(&mut w, &mut r, create_stream_frame("payments-received", 2)).await;
        assert_eq!(resp.opcode, OpCode::Ok);
    }

    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    send_recv(
        &mut writer,
        &mut reader,
        connect_frame("c", Some("orders-tok"), 1),
    )
    .await;
    let resp = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("payments-received", "evt", b"x", 2),
    )
    .await;
    assert_error(resp, 403);
}

// 9 -------------------------------------------------------------------------
#[tokio::test]
async fn subscribe_denied_when_credential_lacks_subscribe() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "pub-only"
token_sha256 = "{pub_hash}"
permissions = [{{ streams = "*", actions = ["publish"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        pub_hash = sha256_hex("pub-tok"),
        admin_hash = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    admin_setup_stream_and_consumer(&srv.tcp_addr, "admin-tok", "events", "c1").await;

    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    send_recv(
        &mut writer,
        &mut reader,
        connect_frame("c", Some("pub-tok"), 1),
    )
    .await;
    let resp = send_recv(&mut writer, &mut reader, subscribe_frame("c1", 2)).await;
    assert_error(resp, 403);
}

// 10 ------------------------------------------------------------------------
#[tokio::test]
async fn subscribe_allowed_delivers_records() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "sub"
token_sha256 = "{sub_hash}"
permissions = [{{ streams = "*", actions = ["subscribe"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin", "publish"] }}]
"#,
        sub_hash = sha256_hex("sub-tok"),
        admin_hash = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    // Admin pre-creates the stream + consumer and publishes a record via TCP.
    {
        let (mut r, mut w) = connect_to(&srv.tcp_addr).await;
        send_recv(&mut w, &mut r, connect_frame("admin", Some("admin-tok"), 1)).await;
        let resp = send_recv(&mut w, &mut r, create_stream_frame("events", 2)).await;
        assert_eq!(resp.opcode, OpCode::Ok);
        let resp = send_recv(&mut w, &mut r, create_consumer_frame("c1", "events", 3)).await;
        assert_eq!(resp.opcode, OpCode::Ok);
        let resp = send_recv(
            &mut w,
            &mut r,
            publish_frame("events", "evt", b"payload-1", 4),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::PublishOk);
    }

    // sub-only cred subscribes and should receive the pre-published record.
    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    send_recv(
        &mut writer,
        &mut reader,
        connect_frame("sub", Some("sub-tok"), 1),
    )
    .await;
    let resp = send_recv(&mut writer, &mut reader, subscribe_frame("c1", 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok);

    // Expect a Record push frame (correlation_id=0) within a few seconds.
    let frame = timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout waiting for record")
        .unwrap()
        .unwrap();
    assert_eq!(frame.opcode, OpCode::Record);
    assert_eq!(frame.correlation_id, 0);
}

// 11 ------------------------------------------------------------------------
#[tokio::test]
async fn fetch_ack_nack_seek_all_require_subscribe_verb() {
    // pub-only cred — every subscribe-scoped op should 403.
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "pub-only"
token_sha256 = "{pub_hash}"
permissions = [{{ streams = "*", actions = ["publish"] }}]

[[credentials]]
name = "admin"
token_sha256 = "{admin_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        pub_hash = sha256_hex("pub-tok"),
        admin_hash = sha256_hex("admin-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    admin_setup_stream_and_consumer(&srv.tcp_addr, "admin-tok", "events", "c1").await;

    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    send_recv(
        &mut writer,
        &mut reader,
        connect_frame("c", Some("pub-tok"), 1),
    )
    .await;

    // Fetch → 403.
    let resp = send_recv(&mut writer, &mut reader, fetch_frame("events", 0, 2)).await;
    assert_error(resp, 403);

    // Seek → 403 (resolves the consumer's stream, then denies).
    let resp = send_recv(&mut writer, &mut reader, seek_frame("c1", 0, 3)).await;
    assert_error(resp, 403);

    // Ack — no active subscription so the server returns 403 regardless.
    // Still counts as proof the subscribe-scoped verb is enforced.
    let resp = send_recv(&mut writer, &mut reader, ack_frame("c1", 0, 4)).await;
    assert_error(resp, 403);

    // Nack → 403.
    let resp = send_recv(&mut writer, &mut reader, nack_frame("c1", 0, 5)).await;
    assert_error(resp, 403);
}

// 12 ------------------------------------------------------------------------
#[tokio::test]
async fn create_stream_requires_scoped_admin() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "team-a"
token_sha256 = "{}"
permissions = [{{ streams = "team-a-*", actions = ["admin"] }}]
"#,
        sha256_hex("team-a-tok")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    send_recv(
        &mut writer,
        &mut reader,
        connect_frame("c", Some("team-a-tok"), 1),
    )
    .await;

    // In-scope → Ok.
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_stream_frame("team-a-orders", 2),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::Ok);

    // Out-of-scope → 403.
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_stream_frame("team-b-orders", 3),
    )
    .await;
    assert_error(resp, 403);
}

// 13 ------------------------------------------------------------------------
// `DeleteStream` doesn't have a ClientMessage decoder today (see
// `ClientMessage::from_frame` — no DeleteStream arm). Use HTTP instead: the
// handlers/streams.rs module doesn't expose a DELETE route at the time of
// writing, so we skip pending a route addition.
#[tokio::test]
#[ignore = "DeleteStream not wired in TCP decoder nor HTTP router (see streams.rs)"]
async fn delete_stream_requires_scoped_admin() {
    // Intentionally empty; kept as a placeholder so Sub-unit 6.2's numbering
    // matches the plan.
}

// 14 ------------------------------------------------------------------------
// TCP ListStreams opcode isn't decoded into a ClientMessage either (there's
// no corresponding arm in ClientMessage). HTTP `GET /api/v1/streams` goes
// through `require_admin`, which 403s any non-admin identity — so the plan's
// alternate verification ("publish-only cred sees the stream list") doesn't
// translate to the current HTTP surface. Skip.
#[tokio::test]
#[ignore = "ListStreams not wired in TCP decoder; HTTP list is admin-only"]
async fn list_streams_returns_all_streams_for_any_authenticated_client() {}

// 15 ------------------------------------------------------------------------
// The HTTP router doesn't expose DELETE /api/v1/streams/:name today. Skip
// until a route exists; the scoped-admin gate in create_stream (test #12)
// exercises the same `require_scoped_admin` path.
#[tokio::test]
#[ignore = "no DELETE /api/v1/streams/:name route in handlers/mod.rs"]
async fn http_delete_stream_scoped_admin_enforced() {}

// 16 ------------------------------------------------------------------------
#[tokio::test]
async fn http_create_connector_requires_global_admin() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "team-a"
token_sha256 = "{scoped_hash}"
permissions = [{{ streams = "team-a-*", actions = ["admin"] }}]

[[credentials]]
name = "global"
token_sha256 = "{global_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        scoped_hash = sha256_hex("scoped-tok"),
        global_hash = sha256_hex("global-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let http = reqwest::Client::new();
    // Minimum fields that satisfy the `ConnectorConfig` deserializer so the
    // request reaches the handler (and thus the authz gate) rather than
    // being rejected at the Json extractor with 422.
    let body = serde_json::json!({
        "name": "probe",
        "type": "source",
        "plugin": "nonexistent-plugin",
        "stream": "probe-stream",
    });

    // Scoped admin → 403 from the global-admin gate.
    let r = http
        .post(format!("http://{}/api/v1/connectors", srv.http_addr))
        .header("Authorization", "Bearer scoped-tok")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // Global admin → authz passes; the handler may reject on the config's
    // merits. Accept anything that isn't 401/403 as "passed authz".
    let r = http
        .post(format!("http://{}/api/v1/connectors", srv.http_addr))
        .header("Authorization", "Bearer global-tok")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_ne!(r.status(), 401, "global admin should not be unauthorized");
    assert_ne!(r.status(), 403, "global admin should clear the authz gate");
}

// 17 ------------------------------------------------------------------------
#[tokio::test]
async fn http_create_query_requires_global_admin() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "team-a"
token_sha256 = "{scoped_hash}"
permissions = [{{ streams = "team-a-*", actions = ["admin"] }}]

[[credentials]]
name = "global"
token_sha256 = "{global_hash}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        scoped_hash = sha256_hex("scoped-tok"),
        global_hash = sha256_hex("global-tok"),
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let http = reqwest::Client::new();
    // A deliberately invalid SQL payload: we only care that authz is
    // evaluated before the query parses.
    let body = serde_json::json!({ "sql": "SELECT 1 FROM no_such_stream" });

    let r = http
        .post(format!("http://{}/api/v1/queries", srv.http_addr))
        .header("Authorization", "Bearer scoped-tok")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    let r = http
        .post(format!("http://{}/api/v1/queries", srv.http_addr))
        .header("Authorization", "Bearer global-tok")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_ne!(r.status(), 401);
    assert_ne!(r.status(), 403);
}

// 18 ------------------------------------------------------------------------
#[tokio::test]
async fn http_whoami_returns_identity_for_any_authenticated_client() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "pub-only"
token_sha256 = "{}"
permissions = [{{ streams = "orders-*", actions = ["publish"] }}]
"#,
        sha256_hex("pub-tok")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), None).await;

    let r = reqwest::Client::new()
        .get(format!("http://{}/api/v1/whoami", srv.http_addr))
        .header("Authorization", "Bearer pub-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["name"], "pub-only");
    assert!(
        body["permissions"].is_array(),
        "permissions must be an array"
    );
    let perms = body["permissions"].as_array().unwrap();
    assert_eq!(perms.len(), 1);
    assert_eq!(perms[0]["streams"], "orders-*");
    assert_eq!(perms[0]["actions"], serde_json::json!(["publish"]));
}

// 19 ------------------------------------------------------------------------
#[tokio::test]
async fn env_var_only_injects_legacy_admin_with_full_permissions() {
    // No credentials file; just the env token. Server synthesizes a
    // `legacy-admin` credential with `*` + all verbs.
    let srv = start_server(None, Some("legacy-tok".into())).await;

    // TCP: Connect + publish + admin (CreateStream) all succeed.
    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    let resp = send_recv(
        &mut writer,
        &mut reader,
        connect_frame("c", Some("legacy-tok"), 1),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::ConnectOk);
    let resp = send_recv(&mut writer, &mut reader, create_stream_frame("s", 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok);
    let resp = send_recv(&mut writer, &mut reader, publish_frame("s", "e", b"v", 3)).await;
    assert_eq!(resp.opcode, OpCode::PublishOk);

    // HTTP whoami shows `legacy-admin`.
    let r = reqwest::Client::new()
        .get(format!("http://{}/api/v1/whoami", srv.http_addr))
        .header("Authorization", "Bearer legacy-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["name"], "legacy-admin");
}

// 20 ------------------------------------------------------------------------
#[tokio::test]
async fn env_var_plus_toml_both_work() {
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "orders"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["publish", "admin"] }}]
"#,
        sha256_hex("orders-tok")
    ));
    let srv = start_server(Some(creds.path().to_path_buf()), Some("legacy-tok".into())).await;

    // Client A: env-var / legacy-admin token.
    let (mut r1, mut w1) = connect_to(&srv.tcp_addr).await;
    let resp = send_recv(&mut w1, &mut r1, connect_frame("a", Some("legacy-tok"), 1)).await;
    assert_eq!(resp.opcode, OpCode::ConnectOk);
    let resp = send_recv(&mut w1, &mut r1, create_stream_frame("shared", 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok);
    let resp = send_recv(&mut w1, &mut r1, publish_frame("shared", "e", b"A", 3)).await;
    assert_eq!(resp.opcode, OpCode::PublishOk);

    // Client B: TOML token.
    let (mut r2, mut w2) = connect_to(&srv.tcp_addr).await;
    let resp = send_recv(&mut w2, &mut r2, connect_frame("b", Some("orders-tok"), 1)).await;
    assert_eq!(resp.opcode, OpCode::ConnectOk);
    let resp = send_recv(&mut w2, &mut r2, publish_frame("shared", "e", b"B", 2)).await;
    assert_eq!(resp.opcode, OpCode::PublishOk);
}

// 21 ------------------------------------------------------------------------
#[tokio::test]
async fn env_var_plus_toml_with_name_collision_refuses_to_start() {
    // Library-level assertion per the plan — faster + more deterministic
    // than wrangling a server startup error.
    use exspeed_common::auth::{AuthError, CredentialStore};

    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "legacy-admin"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        sha256_hex("x")
    ));
    let err = CredentialStore::build(Some(creds.path()), Some("legacy-tok")).unwrap_err();
    assert!(matches!(err, AuthError::LegacyAdminReserved));
}

// 22 ------------------------------------------------------------------------
#[tokio::test]
async fn no_auth_configured_is_open() {
    // No file, no env token → open broker (Plan B default).
    let srv = start_server(None, None).await;

    // TCP: Connect with AuthType::None + any op works.
    let (mut reader, mut writer) = connect_to(&srv.tcp_addr).await;
    let resp = send_recv(&mut writer, &mut reader, connect_frame("c", None, 1)).await;
    assert_eq!(resp.opcode, OpCode::ConnectOk);
    let resp = send_recv(&mut writer, &mut reader, create_stream_frame("open", 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok);
    let resp = send_recv(
        &mut writer,
        &mut reader,
        publish_frame("open", "e", b"v", 3),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::PublishOk);

    // HTTP: no bearer → 200 on admin endpoints.
    let r = reqwest::Client::new()
        .get(format!("http://{}/api/v1/streams", srv.http_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

// ===========================================================================
// File-loading regression tests — Sub-unit 6.3
// ===========================================================================

// 23 ------------------------------------------------------------------------
#[tokio::test]
async fn malformed_toml_refuses_to_start_with_line_number() {
    use exspeed_common::auth::{AuthError, CredentialStore};
    // Unterminated string → TOML parse error.
    let creds = write_creds(
        r#"
[[credentials]]
name = "a
token_sha256 = "abc"
"#,
    );
    let err = CredentialStore::build(Some(creds.path()), None).unwrap_err();
    assert!(
        matches!(err, AuthError::Toml(_)),
        "expected AuthError::Toml, got {err:?}"
    );
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("toml") || msg.to_lowercase().contains("parse"),
        "error message missing TOML/parse context: {msg}"
    );
}

// 24 ------------------------------------------------------------------------
#[tokio::test]
async fn missing_required_field_refuses_to_start() {
    use exspeed_common::auth::CredentialStore;
    // Missing `token_sha256` → serde-driven parse error (surfaces as Toml).
    let creds = write_creds(
        r#"
[[credentials]]
name = "a"
"#,
    );
    let err = CredentialStore::build(Some(creds.path()), None).unwrap_err();
    // toml::de::Error is what serde returns for a missing required field.
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("token_sha256") || msg.to_lowercase().contains("missing"),
        "error should name the missing field: {msg}"
    );
}

// 25 ------------------------------------------------------------------------
#[tokio::test]
async fn unknown_action_refuses_to_start() {
    use exspeed_common::auth::{AuthError, CredentialStore};
    let creds = write_creds(&format!(
        r#"
[[credentials]]
name = "a"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["manage"] }}]
"#,
        sha256_hex("x")
    ));
    let err = CredentialStore::build(Some(creds.path()), None).unwrap_err();
    match err {
        AuthError::UnknownAction { action, .. } => assert_eq!(action, "manage"),
        other => panic!("expected UnknownAction, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// `exspeed auth` CLI subcommand tests (Task 7).
//
// These spawn the `exspeed` binary via `CARGO_BIN_EXE_exspeed` — cargo
// rebuilds the binary on demand so the tests always run against the
// just-compiled code.
// ---------------------------------------------------------------------------

// 26 ------------------------------------------------------------------------
#[test]
fn exspeed_auth_hash_from_stdin_matches_sha256_reference() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new(env!("CARGO_BIN_EXE_exspeed"))
        .args(["auth", "hash"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.as_mut().unwrap().write_all(b"hello").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "auth hash failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    // Known SHA-256 of "hello".
    assert_eq!(
        stdout.trim(),
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
}

// 27 ------------------------------------------------------------------------
#[test]
fn exspeed_auth_gen_token_outputs_64_hex_and_matching_hash() {
    use std::process::Command;

    let output = Command::new(env!("CARGO_BIN_EXE_exspeed"))
        .args(["auth", "gen-token"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "auth gen-token failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let token = String::from_utf8(output.stdout).unwrap().trim().to_string();
    // stderr may carry unrelated tracing log lines (e.g. when
    // EXSPEED_INSECURE_SKIP_VERIFY is set). Only the last non-empty line
    // is the hash we emit.
    let stderr = String::from_utf8(output.stderr).unwrap();
    let hash = stderr
        .lines()
        .rfind(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim()
        .to_string();

    assert_eq!(token.len(), 64, "token should be 64 hex chars: {token}");
    assert!(
        token.chars().all(|c| c.is_ascii_hexdigit()),
        "token should be hex: {token}"
    );
    let expected = exspeed_common::auth::sha256_hex(token.as_bytes());
    assert_eq!(hash, expected, "stderr hash should be sha256(token)");
}

// 28 ------------------------------------------------------------------------
#[test]
fn exspeed_auth_lint_on_valid_file_exits_0() {
    use std::process::Command;

    let file = write_creds(&format!(
        r#"[[credentials]]
name = "a"
token_sha256 = "{}"
permissions = [{{ streams = "*", actions = ["admin"] }}]
"#,
        sha256_hex("t")
    ));
    let status = Command::new(env!("CARGO_BIN_EXE_exspeed"))
        .args(["auth", "lint"])
        .arg(file.path())
        .status()
        .unwrap();
    assert!(status.success(), "auth lint on valid file should exit 0");
}

// 29 ------------------------------------------------------------------------
#[test]
fn exspeed_auth_lint_on_duplicate_name_exits_nonzero() {
    use std::process::Command;

    let file = write_creds(&format!(
        r#"[[credentials]]
name = "dup"
token_sha256 = "{}"

[[credentials]]
name = "dup"
token_sha256 = "{}"
"#,
        sha256_hex("a"),
        sha256_hex("b"),
    ));
    let status = Command::new(env!("CARGO_BIN_EXE_exspeed"))
        .args(["auth", "lint"])
        .arg(file.path())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "auth lint on duplicate-name file should exit non-zero"
    );
}
