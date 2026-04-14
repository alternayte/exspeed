use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::publish::PublishRequest;
use exspeed_protocol::opcodes::OpCode;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_server() -> (String, String) {
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let http_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{}", tcp_port);
    let http_addr = format!("127.0.0.1:{}", http_port);

    let dir = tempfile::TempDir::new().unwrap();
    let args = exspeed::cli::server::ServerArgs {
        bind: tcp_addr.clone(),
        data_dir: dir.path().to_path_buf(),
        api_bind: http_addr.clone(),
    };

    tokio::spawn(async move {
        let _keep = dir;
        exspeed::cli::server::run(args).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    (tcp_addr, format!("http://{}", http_addr))
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
    writer.send(frame).await.unwrap();
    timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout waiting for response")
        .unwrap()
        .unwrap()
}

fn connect_frame(corr: u32) -> Frame {
    let req = ConnectRequest {
        client_id: "exql-test".into(),
        auth_type: AuthType::None,
        auth_payload: Bytes::new(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Connect, corr, buf.freeze())
}

fn publish_frame(stream: &str, subject: &str, value: &[u8], corr: u32) -> Frame {
    let req = PublishRequest {
        stream: stream.into(),
        subject: subject.into(),
        key: None,
        value: Bytes::copy_from_slice(value),
        headers: vec![],
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Publish, corr, buf.freeze())
}

/// Create a stream via HTTP, then publish `records` via TCP.
///
/// Each record is a `(subject, json_payload)` pair.
async fn setup_stream(
    stream_name: &str,
    records: &[(&str, &str)],
    tcp_addr: &str,
    http_url: &str,
) {
    let client = reqwest::Client::new();

    // Create stream via HTTP
    let resp = client
        .post(format!("{}/api/v1/streams", http_url))
        .json(&serde_json::json!({"name": stream_name}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "failed to create stream '{}'",
        stream_name
    );

    // Publish records via TCP
    let (mut reader, mut writer) = connect_to(tcp_addr).await;
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "CONNECT should return Ok");

    for (i, (subject, payload)) in records.iter().enumerate() {
        let resp = send_recv(
            &mut writer,
            &mut reader,
            publish_frame(stream_name, subject, payload.as_bytes(), 10 + i as u32),
        )
        .await;
        assert_eq!(
            resp.opcode,
            OpCode::Ok,
            "PUBLISH record {} should return Ok",
            i
        );
    }
}

/// Execute a bounded SQL query via the HTTP API and return the JSON response body.
async fn query(http_url: &str, sql: &str) -> Value {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/queries", http_url))
        .json(&serde_json::json!({"sql": sql}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "query failed for SQL: {}",
        sql
    );
    resp.json().await.unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bounded_select_star() {
    let (tcp, http) = start_server().await;

    let records: Vec<(&str, &str)> = vec![
        ("order.created", r#"{"total": 100, "region": "eu"}"#),
        ("order.created", r#"{"total": 200, "region": "us"}"#),
        ("order.created", r#"{"total": 300, "region": "eu"}"#),
        ("order.created", r#"{"total": 400, "region": "us"}"#),
        ("order.created", r#"{"total": 500, "region": "eu"}"#),
    ];
    setup_stream("exql-select-test", &records, &tcp, &http).await;

    let body = query(&http, r#"SELECT * FROM "exql-select-test""#).await;

    assert_eq!(
        body["row_count"], 5,
        "expected 5 rows, got: {}",
        body["row_count"]
    );

    let columns = body["columns"].as_array().expect("columns should be array");
    let col_names: Vec<&str> = columns.iter().map(|c| c.as_str().unwrap()).collect();
    assert!(
        col_names.contains(&"offset"),
        "columns should include 'offset', got: {:?}",
        col_names
    );
    assert!(
        col_names.contains(&"key"),
        "columns should include 'key', got: {:?}",
        col_names
    );
    assert!(
        col_names.contains(&"subject"),
        "columns should include 'subject', got: {:?}",
        col_names
    );
    assert!(
        col_names.contains(&"payload"),
        "columns should include 'payload', got: {:?}",
        col_names
    );
}

#[tokio::test]
async fn bounded_select_with_filter() {
    let (tcp, http) = start_server().await;

    let records: Vec<(&str, &str)> = vec![
        ("order.created", r#"{"total": 100, "region": "eu"}"#),
        ("order.created", r#"{"total": 200, "region": "us"}"#),
        ("order.created", r#"{"total": 300, "region": "eu"}"#),
        ("order.created", r#"{"total": 400, "region": "us"}"#),
        ("order.created", r#"{"total": 500, "region": "eu"}"#),
    ];
    setup_stream("exql-filter-test", &records, &tcp, &http).await;

    let body = query(
        &http,
        r#"SELECT * FROM "exql-filter-test" WHERE payload->>'region' = 'eu'"#,
    )
    .await;

    assert_eq!(
        body["row_count"], 3,
        "expected 3 eu rows, got: {}",
        body["row_count"]
    );
}

#[tokio::test]
async fn bounded_aggregate() {
    let (tcp, http) = start_server().await;

    let records: Vec<(&str, &str)> = vec![
        ("order.created", r#"{"total": 100, "region": "eu"}"#),
        ("order.created", r#"{"total": 200, "region": "us"}"#),
        ("order.created", r#"{"total": 300, "region": "eu"}"#),
        ("order.created", r#"{"total": 400, "region": "us"}"#),
        ("order.created", r#"{"total": 500, "region": "eu"}"#),
    ];
    setup_stream("exql-agg-test", &records, &tcp, &http).await;

    let body = query(
        &http,
        r#"SELECT COUNT(*) AS cnt FROM "exql-agg-test""#,
    )
    .await;

    assert_eq!(
        body["row_count"], 1,
        "aggregate should return 1 row, got: {}",
        body["row_count"]
    );

    let rows = body["rows"].as_array().expect("rows should be array");
    let first_row = rows[0].as_array().expect("row should be array");

    // The count value should be 5
    assert_eq!(
        first_row[0], 5,
        "COUNT(*) should be 5, got: {}",
        first_row[0]
    );
}

#[tokio::test]
async fn bounded_select_with_limit() {
    let (tcp, http) = start_server().await;

    let records: Vec<(&str, &str)> = vec![
        ("order.created", r#"{"total": 100, "region": "eu"}"#),
        ("order.created", r#"{"total": 200, "region": "us"}"#),
        ("order.created", r#"{"total": 300, "region": "eu"}"#),
        ("order.created", r#"{"total": 400, "region": "us"}"#),
        ("order.created", r#"{"total": 500, "region": "eu"}"#),
    ];
    setup_stream("exql-limit-test", &records, &tcp, &http).await;

    let body = query(
        &http,
        r#"SELECT * FROM "exql-limit-test" LIMIT 2"#,
    )
    .await;

    assert_eq!(
        body["row_count"], 2,
        "expected 2 rows with LIMIT 2, got: {}",
        body["row_count"]
    );
}

#[tokio::test]
async fn continuous_query_creates_stream() {
    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    let records: Vec<(&str, &str)> = vec![
        ("order.created", r#"{"total": 100, "region": "eu"}"#),
        ("order.created", r#"{"total": 200, "region": "us"}"#),
        ("order.created", r#"{"total": 300, "region": "eu"}"#),
        ("order.created", r#"{"total": 400, "region": "us"}"#),
        ("order.created", r#"{"total": 500, "region": "eu"}"#),
    ];
    setup_stream("exql-cq-test", &records, &tcp, &http).await;

    // Create continuous query
    let resp = client
        .post(format!("{}/api/v1/queries/continuous", http))
        .json(
            &serde_json::json!({"sql": r#"CREATE VIEW filtered AS SELECT * FROM "exql-cq-test" WHERE payload->>'region' = 'eu'"#}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create continuous query should return 201"
    );

    let cq_body: Value = resp.json().await.unwrap();
    assert!(
        cq_body.get("query_id").is_some(),
        "response should include query_id"
    );
    assert_eq!(cq_body["status"], "running");

    // Wait for the continuous query to process existing records
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Query the derived stream via bounded SQL
    let body = query(&http, r#"SELECT * FROM "filtered""#).await;

    assert_eq!(
        body["row_count"], 3,
        "expected 3 eu rows in derived stream 'filtered', got: {}",
        body["row_count"]
    );
}
