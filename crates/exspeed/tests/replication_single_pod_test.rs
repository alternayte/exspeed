#![cfg(test)]
//! Single-pod regression tests for Plan G replication.
//!
//! The whole point of Wave 5's "bind the cluster listener ONLY when
//! `EXSPEED_CONSUMER_STORE` is set" guard is that single-pod deployments
//! keep behaving EXACTLY as they did before Plan G. These tests pin that
//! contract:
//!   * The cluster port (5934 by default) is NOT bound — nothing listens there.
//!   * `GET /api/v1/cluster/followers` returns 503 with the documented
//!     `error`/`hint` body so operator tooling can classify the response.
//!   * Normal publish/fetch over port 5933 still works end-to-end.
//!
//! Every test here uses the file-backed consumer store (the default when
//! `EXSPEED_CONSUMER_STORE` is unset), so they run in the default
//! `cargo test` pass without needing Postgres.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::fetch::FetchRequest;
use exspeed_protocol::messages::publish::PublishRequest;
use exspeed_protocol::messages::stream_mgmt::CreateStreamRequest;
use exspeed_protocol::messages::ServerMessage;
use exspeed_protocol::opcodes::OpCode;

struct SinglePodHarness {
    api_port: u16,
    tcp_port: u16,
    _tmp: TempDir,
}

async fn start_single_pod_server() -> SinglePodHarness {
    // Intentionally do NOT set EXSPEED_CONSUMER_STORE. Guard against a
    // stray leak from another test in the same binary by clearing it.
    std::env::remove_var("EXSPEED_CONSUMER_STORE");
    std::env::remove_var("EXSPEED_OFFSET_STORE");

    let api_port = portpicker::pick_unused_port().unwrap();
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let tmp = tempfile::tempdir().unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{tcp_port}"),
        api_bind: format!("127.0.0.1:{api_port}"),
        data_dir: tmp.path().to_path_buf(),
        auth_token: None,
        credentials_file: None,
        tls_cert: None,
        tls_key: None,
    };

    tokio::spawn(async move {
        let _ = exspeed::cli::server::run(args).await;
    });
    // Give the HTTP server and TCP accept loop time to bind.
    tokio::time::sleep(Duration::from_millis(500)).await;

    SinglePodHarness {
        api_port,
        tcp_port,
        _tmp: tmp,
    }
}

#[tokio::test]
async fn cluster_port_is_not_bound_in_single_pod_mode() {
    let _h = start_single_pod_server().await;

    // The default cluster bind is 0.0.0.0:5934. In single-pod mode the
    // listener must NOT be up. A bounded-time connect attempt either
    // times out (nothing accepting) or fails with ECONNREFUSED. We accept
    // either of those two outcomes as "not bound"; the failure case we're
    // guarding against is a successful connect, which would mean the
    // guard in server.rs regressed.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    let connect = tokio::time::timeout_at(
        deadline,
        TcpStream::connect("127.0.0.1:5934"),
    )
    .await;

    match connect {
        Err(_elapsed) => { /* timed out — nothing accepted */ }
        Ok(Err(_io)) => { /* connection refused — the expected case */ }
        Ok(Ok(_stream)) => {
            panic!(
                "single-pod mode should not bind the cluster listener on 5934, \
                 but a connect succeeded"
            );
        }
    }
}

#[tokio::test]
async fn cluster_followers_endpoint_returns_503_body_in_single_pod_mode() {
    let h = start_single_pod_server().await;

    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/api/v1/cluster/followers",
        h.api_port
    ))
    .await
    .expect("http request");

    assert_eq!(resp.status(), 503);

    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["error"], "not a multi-pod deployment");
    assert!(
        body["hint"]
            .as_str()
            .unwrap_or_default()
            .contains("EXSPEED_CONSUMER_STORE"),
        "hint should mention the env var; got {body:?}"
    );
}

#[tokio::test]
async fn publish_and_fetch_still_works_in_single_pod_mode() {
    let h = start_single_pod_server().await;

    let sock = TcpStream::connect(format!("127.0.0.1:{}", h.tcp_port))
        .await
        .expect("tcp connect");
    let (r, w) = tokio::io::split(sock);
    let mut framed_read = FramedRead::new(r, ExspeedCodec::new());
    let mut framed_write = FramedWrite::new(w, ExspeedCodec::new());

    // ---- Connect ----
    let mut buf = BytesMut::new();
    ConnectRequest {
        client_id: "single-pod-test".into(),
        auth_type: AuthType::None,
        auth_payload: Bytes::new(),
    }
    .encode(&mut buf);
    framed_write
        .send(Frame::new(OpCode::Connect, 1, buf.freeze()))
        .await
        .unwrap();
    let frame = framed_read.next().await.unwrap().unwrap();
    assert!(
        matches!(
            ServerMessage::from_frame(frame),
            Ok(ServerMessage::ConnectOk(_))
        ),
        "expected ConnectOk in single-pod mode with auth off",
    );

    // ---- CreateStream ----
    let mut buf = BytesMut::new();
    CreateStreamRequest {
        stream_name: "sp-test".into(),
        max_age_secs: 0,
        max_bytes: 0,
    }
    .encode(&mut buf);
    framed_write
        .send(Frame::new(OpCode::CreateStream, 2, buf.freeze()))
        .await
        .unwrap();
    // Response is Ok; we don't care which code as long as the server replies.
    let _ = framed_read.next().await.unwrap().unwrap();

    // ---- Publish ----
    let mut buf = BytesMut::new();
    PublishRequest {
        stream: "sp-test".into(),
        subject: "t".into(),
        key: None,
        msg_id: None,
        value: Bytes::from_static(b"hello"),
        headers: vec![],
    }
    .encode(&mut buf);
    framed_write
        .send(Frame::new(OpCode::Publish, 3, buf.freeze()))
        .await
        .unwrap();
    let ack = framed_read.next().await.unwrap().unwrap();
    match ServerMessage::from_frame(ack).expect("publish ack decode") {
        ServerMessage::PublishOk { offset, duplicate } => {
            assert!(!duplicate);
            assert_eq!(offset, 0, "first publish lands at offset 0");
        }
        other => panic!("expected PublishOk, got {other:?}"),
    }

    // ---- Fetch ----
    let mut buf = BytesMut::new();
    FetchRequest {
        stream: "sp-test".into(),
        offset: 0,
        max_records: 10,
        subject_filter: String::new(),
    }
    .encode(&mut buf);
    framed_write
        .send(Frame::new(OpCode::Fetch, 4, buf.freeze()))
        .await
        .unwrap();
    let records_frame = framed_read.next().await.unwrap().unwrap();
    match ServerMessage::from_frame(records_frame).expect("fetch decode") {
        ServerMessage::RecordsBatch(batch) => {
            assert_eq!(
                batch.records.len(),
                1,
                "expected exactly one record after publish"
            );
            assert_eq!(&batch.records[0].value[..], b"hello");
        }
        other => panic!("expected RecordsBatch, got {other:?}"),
    }
}
