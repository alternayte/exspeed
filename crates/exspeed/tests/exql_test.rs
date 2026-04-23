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
use exspeed_protocol::messages::publish_batch::{PublishBatchRecord, PublishBatchRequest};
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
        msg_id: None,
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
async fn setup_stream(stream_name: &str, records: &[(&str, &str)], tcp_addr: &str, http_url: &str) {
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
    assert_eq!(resp.opcode, OpCode::ConnectOk, "CONNECT should return ConnectOk");

    for (i, (subject, payload)) in records.iter().enumerate() {
        let resp = send_recv(
            &mut writer,
            &mut reader,
            publish_frame(stream_name, subject, payload.as_bytes(), 10 + i as u32),
        )
        .await;
        assert_eq!(
            resp.opcode,
            OpCode::PublishOk,
            "PUBLISH record {} should return PublishOk",
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
    assert_eq!(resp.status(), 200, "query failed for SQL: {}", sql);
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

    let body = query(&http, r#"SELECT COUNT(*) AS cnt FROM "exql-agg-test""#).await;

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

    let body = query(&http, r#"SELECT * FROM "exql-limit-test" LIMIT 2"#).await;

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

/// Create a stream and publish `records` using PublishBatch (chunks of 1 000) for speed.
///
/// Each record is a `(subject, json_payload)` pair.  Much faster than the
/// one-at-a-time `setup_stream` for large datasets because it avoids an
/// individual round-trip per record.
async fn setup_stream_bulk(stream_name: &str, records: &[(&str, String)], tcp_addr: &str, http_url: &str) {
    let client = reqwest::Client::new();

    // Create stream via HTTP
    let resp = client
        .post(format!("{}/api/v1/streams", http_url))
        .json(&serde_json::json!({"name": stream_name}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "failed to create stream '{}'", stream_name);

    // Publish via TCP using PublishBatch in chunks of 1 000
    let (mut reader, mut writer) = connect_to(tcp_addr).await;
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::ConnectOk, "CONNECT should return ConnectOk");

    const CHUNK: usize = 1_000;
    let mut corr: u32 = 10;
    for chunk in records.chunks(CHUNK) {
        let batch = PublishBatchRequest {
            stream: stream_name.into(),
            records: chunk
                .iter()
                .map(|(subject, payload)| PublishBatchRecord {
                    subject: (*subject).into(),
                    key: None,
                    msg_id: None,
                    value: Bytes::copy_from_slice(payload.as_bytes()),
                    headers: vec![],
                })
                .collect(),
        };
        let mut buf = BytesMut::new();
        batch.encode(&mut buf);
        let resp = send_recv(
            &mut writer,
            &mut reader,
            Frame::new(OpCode::PublishBatch, corr, buf.freeze()),
        )
        .await;
        assert_eq!(
            resp.opcode,
            OpCode::PublishBatchOk,
            "PublishBatch (corr={}) should return PublishBatchOk",
            corr
        );
        corr += 1;
    }
}

// ---------------------------------------------------------------------------
// Streaming-scan integration tests (Task 9)
// ---------------------------------------------------------------------------

/// SELECT * LIMIT 5 on a 10 000-row stream must complete in < 500 ms.
///
/// The streaming path stops reading after the 5th row; without it, the engine
/// would read all 10 000 rows before returning — which would take much longer.
#[tokio::test]
async fn streaming_limit_5_is_fast() {
    let (tcp, http) = start_server().await;

    // Build 10 000 records: {"i": N, "kind": "even"|"odd", "nested": {"v": N*10}}
    let payloads: Vec<(&str, String)> = (0u64..10_000)
        .map(|i| {
            let kind = if i % 2 == 0 { "even" } else { "odd" };
            let json = format!(r#"{{"i":{i},"kind":"{kind}","nested":{{"v":{}}}}}"#, i * 10);
            ("s.scan", json)
        })
        .collect();

    setup_stream_bulk("scan_fast", &payloads, &tcp, &http).await;

    let t0 = std::time::Instant::now();
    let body = query(&http, r#"SELECT * FROM "scan_fast" LIMIT 5"#).await;
    let elapsed = t0.elapsed();

    assert_eq!(
        body["row_count"], 5,
        "expected 5 rows, got: {}",
        body["row_count"]
    );
    assert!(
        elapsed.as_millis() < 500,
        "SELECT * LIMIT 5 on 10k rows took {}ms — expected < 500ms (streaming scan broken?)",
        elapsed.as_millis()
    );

    println!("streaming_limit_5_is_fast: {}ms", elapsed.as_millis());
}

/// SELECT offset FROM scan_skip must NOT include a "payload" column.
///
/// When the query never references payload, the streaming scan's ColumnSet
/// excludes it — so the response columns list should contain only "offset".
#[tokio::test]
async fn streaming_skips_payload_when_not_referenced() {
    let (tcp, http) = start_server().await;

    let payloads: Vec<(&str, String)> = (0u64..1_000)
        .map(|i| {
            let kind = if i % 2 == 0 { "even" } else { "odd" };
            let json = format!(r#"{{"i":{i},"kind":"{kind}","nested":{{"v":{}}}}}"#, i * 10);
            ("s.scan", json)
        })
        .collect();

    setup_stream_bulk("scan_skip", &payloads, &tcp, &http).await;

    let body = query(&http, r#"SELECT "offset" FROM "scan_skip" LIMIT 1"#).await;

    assert_eq!(
        body["row_count"], 1,
        "expected 1 row, got: {}",
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
        !col_names.contains(&"payload"),
        "columns should NOT contain 'payload' when payload is unreferenced, got: {:?}",
        col_names
    );
}

/// Chained JSON access (`payload->'nested'->>'v'`) must return correct values.
///
/// Publishes 100 records and selects even rows' nested.v via chained operators.
/// Expected: i=0 → "0", i=2 → "20", i=4 → "40" (LIMIT 3).
#[tokio::test]
async fn streaming_chained_json_access_correct() {
    let (tcp, http) = start_server().await;

    let payloads: Vec<(&str, String)> = (0u64..100)
        .map(|i| {
            let kind = if i % 2 == 0 { "even" } else { "odd" };
            let json = format!(r#"{{"i":{i},"kind":"{kind}","nested":{{"v":{}}}}}"#, i * 10);
            ("s.scan", json)
        })
        .collect();

    setup_stream_bulk("scan_chain", &payloads, &tcp, &http).await;

    let body = query(
        &http,
        r#"SELECT payload->'nested'->>'v' AS v FROM "scan_chain" WHERE payload->>'kind' = 'even' LIMIT 3"#,
    )
    .await;

    assert_eq!(
        body["row_count"], 3,
        "expected 3 rows, got: {}",
        body["row_count"]
    );

    let rows = body["rows"].as_array().expect("rows should be array");

    // i=0 → v = "0", i=2 → v = "20", i=4 → v = "40"
    // ->> returns text so values will be JSON strings
    let expected_v = ["0", "20", "40"];
    for (row_idx, (row, expected)) in rows.iter().zip(expected_v.iter()).enumerate() {
        let row_arr = row.as_array().expect("each row should be array");
        let v_val = &row_arr[0];
        // ->> may return either a JSON string "\"0\"" or a bare number 0; normalise to string
        let v_str = match v_val {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            other => panic!("row {row_idx}: unexpected v value type: {other:?}"),
        };
        assert_eq!(
            v_str, *expected,
            "row {row_idx}: expected v = {expected:?}, got {v_str:?}"
        );
    }

    println!(
        "streaming_chained_json_access_correct: rows = {:?}",
        rows.iter()
            .map(|r| r.as_array().unwrap()[0].clone())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn tcp_query_returns_result() {
    let (tcp, http) = start_server().await;

    let records: Vec<(&str, &str)> = vec![
        ("orders.created", r#"{"total": 100}"#),
        ("orders.created", r#"{"total": 200}"#),
        ("orders.created", r#"{"total": 300}"#),
    ];
    setup_stream("tcp_query_orders", &records, &tcp, &http).await;

    let (mut reader, mut writer) = connect_to(&tcp).await;
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::ConnectOk);

    let sql = r#"SELECT * FROM "tcp_query_orders""#;
    let query_frame = Frame::new(OpCode::Query, 100, Bytes::from(sql.as_bytes().to_vec()));
    let resp = send_recv(&mut writer, &mut reader, query_frame).await;
    assert_eq!(resp.opcode, OpCode::QueryResult, "expected QueryResult, got {:?}", resp.opcode);

    let body: Value = serde_json::from_slice(&resp.payload).unwrap();
    assert_eq!(body["row_count"], 3);
}

#[tokio::test]
async fn tcp_query_returns_error_for_invalid_sql() {
    let (tcp, _http) = start_server().await;

    let (mut reader, mut writer) = connect_to(&tcp).await;
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::ConnectOk);

    let sql = "THIS IS NOT VALID SQL";
    let query_frame = Frame::new(OpCode::Query, 101, Bytes::from(sql.as_bytes().to_vec()));
    let resp = send_recv(&mut writer, &mut reader, query_frame).await;
    assert_eq!(resp.opcode, OpCode::Error, "expected Error for invalid SQL");
}
