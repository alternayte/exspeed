use std::path::PathBuf;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tempfile::tempdir;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::consumer::{CreateConsumerRequest, StartFrom};
use exspeed_protocol::messages::stream_mgmt::CreateStreamRequest;
use exspeed_protocol::opcodes::OpCode;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_server() -> (String, u16, tempfile::TempDir) {
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let api_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{tcp_port}");
    let api_addr = format!("127.0.0.1:{api_port}");

    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    let args = exspeed::cli::server::ServerArgs {
        bind: tcp_addr.clone(),
        api_bind: api_addr,
        data_dir,
        auth_token: None,
        credentials_file: None,
        tls_cert: None,
        tls_key: None,
    };

    tokio::spawn(async move {
        exspeed::cli::server::run(args).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    (tcp_addr, api_port, dir)
}

type FramedReader = FramedRead<tokio::net::tcp::OwnedReadHalf, ExspeedCodec>;
type FramedWriter = FramedWrite<tokio::net::tcp::OwnedWriteHalf, ExspeedCodec>;

async fn connect_to(addr: &str) -> (FramedReader, FramedWriter) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, writer) = stream.into_split();
    (
        FramedRead::new(reader, ExspeedCodec::new()),
        FramedWrite::new(writer, ExspeedCodec::new()),
    )
}

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

fn connect_frame(corr: u32) -> Frame {
    let req = ConnectRequest {
        client_id: "observability-test".into(),
        auth_type: AuthType::None,
        auth_payload: Bytes::new(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Connect, corr, buf.freeze())
}

fn create_stream_frame(stream: &str, corr: u32) -> Frame {
    let req = CreateStreamRequest {
        stream_name: stream.into(),
        max_age_secs: 0,
        max_bytes: 0,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::CreateStream, corr, buf.freeze())
}

fn create_consumer_frame(
    name: &str,
    stream: &str,
    group: &str,
    subject_filter: &str,
    corr: u32,
) -> Frame {
    let req = CreateConsumerRequest {
        name: name.into(),
        stream: stream.into(),
        group: group.into(),
        subject_filter: subject_filter.into(),
        start_from: StartFrom::Earliest,
        start_offset: 0,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::CreateConsumer, corr, buf.freeze())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn over_long_consumer_name_rejected() {
    let (addr, _api_port, _tmp) = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::ConnectOk, "CONNECT should return ConnectOk");

    let resp = send_recv(&mut writer, &mut reader, create_stream_frame("events", 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "CREATE_STREAM should return Ok");

    let too_long = "x".repeat(256);
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_consumer_frame(&too_long, "events", "", "", 3),
    )
    .await;
    assert_eq!(
        resp.opcode,
        OpCode::Error,
        "over-long consumer name should return Error"
    );
}

#[tokio::test]
async fn publish_latency_histogram_reported_via_metrics() {
    let (_addr, api_port, _tmp) = start_server().await;

    let client = reqwest::Client::new();

    // Create stream via HTTP API.
    let resp = client
        .post(format!("http://127.0.0.1:{api_port}/api/v1/streams"))
        .json(&serde_json::json!({"name": "lat-test", "max_age_secs": 0, "max_bytes": 0}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "create stream: {}", resp.status());

    // Publish a record via the HTTP API.
    let resp = client
        .post(format!(
            "http://127.0.0.1:{api_port}/api/v1/streams/lat-test/publish"
        ))
        .json(&serde_json::json!({"subject": "test", "data": {"msg": "hello"}}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "publish: {}", resp.status());

    // Scrape /metrics.
    let metrics = reqwest::get(format!("http://127.0.0.1:{api_port}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        metrics.contains("publish_latency_seconds"),
        "metrics body did not include publish_latency_seconds; got:\n{metrics}"
    );
    assert!(
        metrics.contains("stream=\"lat-test\""),
        "metrics body did not include stream=lat-test label; got:\n{metrics}"
    );
}

// ---------------------------------------------------------------------------
// Snapshot tests (Task 7)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshot_creates_tar_gz_of_data_dir() {
    let tmp = tempdir().unwrap();
    let data_dir: PathBuf = tmp.path().to_path_buf();

    std::fs::create_dir_all(data_dir.join("streams/example/partitions/0")).unwrap();
    std::fs::write(
        data_dir.join("streams/example/partitions/0/dummy"),
        b"hello",
    )
    .unwrap();

    let out = tmp.path().join("snap.tar.gz");

    exspeed::cli::snapshot::run(exspeed::cli::snapshot::SnapshotArgs {
        data_dir: data_dir.clone(),
        output: out.clone(),
    })
    .await
    .expect("snapshot should succeed against unlocked data_dir");

    let metadata = std::fs::metadata(&out).unwrap();
    assert!(metadata.len() > 0, "snapshot file should not be empty");
    assert!(metadata.is_file());
}

#[tokio::test]
async fn snapshot_refuses_when_server_holds_lock() {
    let tmp = tempdir().unwrap();
    let data_dir: PathBuf = tmp.path().to_path_buf();

    // Take the lock as if we were a running server.
    let _lock = exspeed::cli::server_lock::acquire_data_dir_lock(&data_dir)
        .expect("first lock should succeed");

    let out = tmp.path().join("snap.tar.gz");
    let err = exspeed::cli::snapshot::run(exspeed::cli::snapshot::SnapshotArgs {
        data_dir,
        output: out,
    })
    .await
    .expect_err("snapshot should fail while data_dir is locked");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("already in use") || msg.contains("in use"),
        "unexpected error: {msg}"
    );
}
