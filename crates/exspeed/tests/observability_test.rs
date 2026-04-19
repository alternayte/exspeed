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
    assert_eq!(resp.opcode, OpCode::Ok, "CONNECT should return Ok");

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
