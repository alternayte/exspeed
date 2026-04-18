use std::time::Duration;

use bytes::{Bytes, BytesMut};
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::opcodes::OpCode;
use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

async fn start_server(auth_token: Option<String>) -> (String, TempDir) {
    let port = portpicker::pick_unused_port().unwrap();
    let api_port = portpicker::pick_unused_port().unwrap();
    let bind = format!("127.0.0.1:{port}");
    let api_bind = format!("127.0.0.1:{api_port}");
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let args = exspeed::cli::server::ServerArgs {
        bind: bind.clone(),
        api_bind,
        data_dir,
        auth_token,
        tls_cert: None,
        tls_key: None,
    };

    tokio::spawn(async move {
        exspeed::cli::server::run(args).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    (bind, tmp)
}

fn encode_connect(client_id: &str, auth_type: AuthType, token: &[u8]) -> Frame {
    let mut payload = BytesMut::new();
    ConnectRequest {
        client_id: client_id.to_string(),
        auth_type,
        auth_payload: Bytes::copy_from_slice(token),
    }
    .encode(&mut payload);
    Frame::new(OpCode::Connect, 1, payload.freeze())
}

#[tokio::test]
async fn auth_disabled_accepts_any_connect() {
    let (addr, _tmp) = start_server(None).await;
    let stream = TcpStream::connect(&addr).await.unwrap();
    let (r, w) = stream.into_split();
    let mut fr = FramedRead::new(r, ExspeedCodec::new());
    let mut fw = FramedWrite::new(w, ExspeedCodec::new());

    // AuthType::None is accepted.
    fw.send(encode_connect("c", AuthType::None, b""))
        .await.unwrap();
    let resp = timeout(Duration::from_secs(1), fr.next())
        .await.unwrap().unwrap().unwrap();
    assert_eq!(resp.opcode, OpCode::Ok);
}

#[tokio::test]
async fn auth_enabled_rejects_missing_token() {
    let (addr, _tmp) = start_server(Some("secret123".into())).await;
    let stream = TcpStream::connect(&addr).await.unwrap();
    let (r, w) = stream.into_split();
    let mut fr = FramedRead::new(r, ExspeedCodec::new());
    let mut fw = FramedWrite::new(w, ExspeedCodec::new());

    fw.send(encode_connect("c", AuthType::None, b"")).await.unwrap();
    let resp = timeout(Duration::from_secs(1), fr.next())
        .await.unwrap().unwrap().unwrap();
    assert_eq!(resp.opcode, OpCode::Error);
}

#[tokio::test]
async fn auth_enabled_rejects_wrong_token() {
    let (addr, _tmp) = start_server(Some("secret123".into())).await;
    let stream = TcpStream::connect(&addr).await.unwrap();
    let (r, w) = stream.into_split();
    let mut fr = FramedRead::new(r, ExspeedCodec::new());
    let mut fw = FramedWrite::new(w, ExspeedCodec::new());

    fw.send(encode_connect("c", AuthType::Token, b"wrong")).await.unwrap();
    let resp = timeout(Duration::from_secs(1), fr.next())
        .await.unwrap().unwrap().unwrap();
    assert_eq!(resp.opcode, OpCode::Error);
}

#[tokio::test]
async fn auth_enabled_accepts_correct_token() {
    let (addr, _tmp) = start_server(Some("secret123".into())).await;
    let stream = TcpStream::connect(&addr).await.unwrap();
    let (r, w) = stream.into_split();
    let mut fr = FramedRead::new(r, ExspeedCodec::new());
    let mut fw = FramedWrite::new(w, ExspeedCodec::new());

    fw.send(encode_connect("c", AuthType::Token, b"secret123")).await.unwrap();
    let resp = timeout(Duration::from_secs(1), fr.next())
        .await.unwrap().unwrap().unwrap();
    assert_eq!(resp.opcode, OpCode::Ok);

    // Can Ping now.
    fw.send(Frame::empty(OpCode::Ping, 99)).await.unwrap();
    let pong = timeout(Duration::from_secs(1), fr.next())
        .await.unwrap().unwrap().unwrap();
    assert_eq!(pong.opcode, OpCode::Pong);
}

#[tokio::test]
async fn auth_enabled_blocks_ops_before_connect() {
    let (addr, _tmp) = start_server(Some("secret123".into())).await;
    let stream = TcpStream::connect(&addr).await.unwrap();
    let (r, w) = stream.into_split();
    let mut fr = FramedRead::new(r, ExspeedCodec::new());
    let mut fw = FramedWrite::new(w, ExspeedCodec::new());

    // Try to Ping without authenticating first.
    fw.send(Frame::empty(OpCode::Ping, 7)).await.unwrap();
    let resp = timeout(Duration::from_secs(1), fr.next())
        .await.unwrap().unwrap().unwrap();
    assert_eq!(resp.opcode, OpCode::Error);
}
