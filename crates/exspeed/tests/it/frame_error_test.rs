//! Undecodable frames get a v2 `Error 400` (correlation id 0) before the
//! server closes the connection — including the very first (handshake)
//! frame, which used to be dropped silently.

use std::time::Duration;

use bytes::BytesMut;
use exspeed_client::{Request, Response};
use exspeed_protocol::frame::Frame;
use exspeed_protocol::opcodes::OpCode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::common::TestServer;

/// Send `bytes`, read until the server closes, decode every frame.
async fn send_and_collect(addr: &str, chunks: &[&[u8]]) -> Vec<Frame> {
    let mut s = TcpStream::connect(addr).await.unwrap();
    for c in chunks {
        s.write_all(c).await.unwrap();
    }
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut buf))
        .await
        .expect("server must close the connection")
        .unwrap();
    let mut b = BytesMut::from(&buf[..]);
    let mut frames = Vec::new();
    while let Some(f) = Frame::decode(&mut b).expect("server frames decode") {
        frames.push(f);
    }
    frames
}

fn error_of(f: &Frame) -> (u16, String) {
    match Response::from_frame(f).unwrap() {
        Response::Error { code, message, .. } => (code, message),
        other => panic!("expected Error, got {other:?}"),
    }
}

fn connect_bytes() -> Vec<u8> {
    use tokio_util::codec::Encoder;
    let frame = Request::Connect {
        client_id: "raw".into(),
        token: None,
    }
    .into_frame(1);
    let mut out = BytesMut::new();
    exspeed_protocol::codec::ExspeedCodec::new()
        .encode(frame, &mut out)
        .unwrap();
    out.to_vec()
}

#[tokio::test]
async fn old_protocol_version_on_first_frame_gets_an_error() {
    let server = TestServer::start().await;
    // A v1 header: version 1, Connect opcode, correlation 1, empty payload.
    let frames = send_and_collect(&server.addr, &[&[0x01, 0x01, 1, 0, 0, 0, 0, 0, 0, 0]]).await;
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0].opcode, OpCode::Error);
    assert_eq!(frames[0].correlation_id, 0);
    let (code, message) = error_of(&frames[0]);
    assert_eq!(code, 400);
    assert_eq!(
        message,
        "unsupported protocol version 1; this server speaks 2"
    );
}

#[tokio::test]
async fn unknown_opcode_on_first_frame_gets_an_error() {
    let server = TestServer::start().await;
    let frames = send_and_collect(&server.addr, &[&[0x02, 0x7E, 1, 0, 0, 0, 0, 0, 0, 0]]).await;
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0].correlation_id, 0);
    let (code, message) = error_of(&frames[0]);
    assert_eq!(code, 400);
    assert!(message.contains("opcode"), "{message}");
}

#[tokio::test]
async fn unknown_opcode_after_handshake_gets_an_error() {
    let server = TestServer::start().await;
    let frames = send_and_collect(
        &server.addr,
        &[&connect_bytes(), &[0x02, 0x7E, 2, 0, 0, 0, 0, 0, 0, 0]],
    )
    .await;
    assert_eq!(frames[0].opcode, OpCode::ConnectOk, "{frames:?}");
    let last = frames.last().unwrap();
    assert_eq!(last.opcode, OpCode::Error);
    assert_eq!(last.correlation_id, 0);
    assert_eq!(error_of(last).0, 400);
}
