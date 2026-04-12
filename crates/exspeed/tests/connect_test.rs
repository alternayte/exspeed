use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::opcodes::OpCode;

async fn start_server() -> String {
    let port = portpicker::pick_unused_port().unwrap();
    let addr = format!("127.0.0.1:{}", port);
    let bind = addr.clone();
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    tokio::spawn(async move {
        let _tmp = tmp; // keep tempdir alive
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs { bind, data_dir })
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

#[tokio::test]
async fn connect_and_ping_pong() {
    let addr = start_server().await;
    let stream = TcpStream::connect(&addr).await.unwrap();

    let (reader, writer) = stream.into_split();
    let mut framed_read = FramedRead::new(reader, ExspeedCodec::new());
    let mut framed_write = FramedWrite::new(writer, ExspeedCodec::new());

    // Send CONNECT
    let connect_req = ConnectRequest {
        client_id: "test-client".into(),
        auth_type: AuthType::None,
        auth_payload: Bytes::new(),
    };
    let mut payload = BytesMut::new();
    connect_req.encode(&mut payload);
    let connect_frame = Frame::new(OpCode::Connect, 1, payload.freeze());
    framed_write.send(connect_frame).await.unwrap();

    // Expect OK response
    let response = timeout(Duration::from_secs(1), framed_read.next())
        .await
        .expect("timeout waiting for CONNECT response")
        .unwrap()
        .unwrap();

    assert_eq!(response.opcode, OpCode::Ok);
    assert_eq!(response.correlation_id, 1);

    // Send PING
    let ping_frame = Frame::empty(OpCode::Ping, 42);
    framed_write.send(ping_frame).await.unwrap();

    // Expect PONG response
    let pong = timeout(Duration::from_secs(1), framed_read.next())
        .await
        .expect("timeout waiting for PONG")
        .unwrap()
        .unwrap();

    assert_eq!(pong.opcode, OpCode::Pong);
    assert_eq!(pong.correlation_id, 42);
}

#[tokio::test]
async fn unknown_opcode_returns_error() {
    let addr = start_server().await;
    let stream = TcpStream::connect(&addr).await.unwrap();

    let (reader, writer) = stream.into_split();
    let mut framed_read = FramedRead::new(reader, ExspeedCodec::new());
    let mut framed_write = FramedWrite::new(writer, ExspeedCodec::new());

    // Send a PUBLISH frame (not implemented yet)
    let frame = Frame::new(OpCode::Publish, 1, Bytes::from_static(b"garbage"));
    framed_write.send(frame).await.unwrap();

    let response = timeout(Duration::from_secs(1), framed_read.next())
        .await
        .expect("timeout waiting for ERROR response")
        .unwrap()
        .unwrap();

    assert_eq!(response.opcode, OpCode::Error);
    assert_eq!(response.correlation_id, 1);
}
