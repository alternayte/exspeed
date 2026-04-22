use std::time::Duration;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::consumer::{CreateConsumerRequest, StartFrom};
use exspeed_protocol::opcodes::OpCode;

async fn start_server() -> (String, String) {
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let http_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{}", tcp_port);
    let http_addr = format!("127.0.0.1:{}", http_port);

    let dir = tempfile::TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();
    let tcp_addr_clone = tcp_addr.clone();
    let http_addr_clone = http_addr.clone();

    tokio::spawn(async move {
        let _keep = dir;
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind: tcp_addr_clone,
            api_bind: http_addr_clone,
            data_dir,
            auth_token: None,
            credentials_file: None,
            tls_cert: None,
            tls_key: None,
            storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
            storage_flush_window_us: 500,
            storage_flush_threshold_records: 256,
            storage_flush_threshold_bytes: 1_048_576,
            storage_sync_interval_ms: 10,
            storage_sync_bytes: 4 * 1024 * 1024,
            delivery_buffer: 8192,
        })
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    (tcp_addr, format!("http://127.0.0.1:{}", http_port))
}

#[tokio::test]
async fn delete_nonexistent_returns_404() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!("{}/api/v1/streams/missing", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn delete_with_no_references_succeeds() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "empty-stream"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = client
        .delete(format!("{}/api/v1/streams/empty-stream", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "delete should return 200");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["deleted"], "empty-stream");
    assert_eq!(body["cascaded"]["consumers"].as_array().unwrap().len(), 0);
    assert_eq!(body["cascaded"]["connectors"].as_array().unwrap().len(), 0);
    assert_eq!(body["cascaded"]["queries"].as_array().unwrap().len(), 0);
    assert_eq!(body["cascaded"]["subscriptions_dropped"], 0);

    // GET should now 404.
    let resp = client
        .get(format!("{}/api/v1/streams/empty-stream", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "stream should be gone");
}

async fn create_consumer_via_tcp(tcp_addr: &str, consumer: &str, stream: &str) {
    let sock = TcpStream::connect(tcp_addr).await.unwrap();
    let (reader, writer) = sock.into_split();
    let mut reader = FramedRead::new(reader, ExspeedCodec::new());
    let mut writer = FramedWrite::new(writer, ExspeedCodec::new());

    // Handshake.
    let mut buf = BytesMut::new();
    ConnectRequest {
        client_id: "stream-delete-test".into(),
        auth_type: AuthType::None,
        auth_payload: bytes::Bytes::new(),
    }
    .encode(&mut buf);
    writer
        .send(Frame::new(OpCode::Connect, 1, buf.freeze()))
        .await
        .unwrap();
    let _ = timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout waiting for connect ack")
        .unwrap()
        .unwrap();

    // CreateConsumer.
    let mut buf = BytesMut::new();
    CreateConsumerRequest {
        name: consumer.into(),
        stream: stream.into(),
        group: String::new(),
        subject_filter: String::new(),
        start_from: StartFrom::Earliest,
        start_offset: 0,
    }
    .encode(&mut buf);
    writer
        .send(Frame::new(OpCode::CreateConsumer, 2, buf.freeze()))
        .await
        .unwrap();
    let _ = timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout waiting for create-consumer ack")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn delete_with_consumer_rejects_409() {
    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "held-by-consumer"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    create_consumer_via_tcp(&tcp, "cons-1", "held-by-consumer").await;

    let resp = client
        .delete(format!("{}/api/v1/streams/held-by-consumer", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    let body: Value = resp.json().await.unwrap();
    let consumers = body["blockers"]["consumers"].as_array().unwrap();
    assert!(
        consumers.iter().any(|v| v == "cons-1"),
        "blockers.consumers should include cons-1, got {:?}",
        body
    );
}

#[tokio::test]
async fn delete_with_connector_rejects_409() {
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "held-by-connector"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "hook-x",
            "type": "source",
            "plugin": "http_webhook",
            "stream": "held-by-connector",
            "subject_template": "x",
            "settings": {"path": "/webhooks/hx", "auth_type": "none"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create connector should 201, body: {:?}",
        resp.text().await.unwrap_or_default()
    );

    let resp = client
        .delete(format!("{}/api/v1/streams/held-by-connector", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    let body: Value = resp.json().await.unwrap();
    let connectors = body["blockers"]["connectors"].as_array().unwrap();
    assert!(
        connectors.iter().any(|v| v == "hook-x"),
        "blockers.connectors should include hook-x, got {:?}",
        body
    );
}
