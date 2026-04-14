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
// Helpers (same pattern as exql_test.rs)
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
        client_id: "win-test".into(),
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
async fn setup_stream(stream_name: &str, records: &[(&str, &str)], tcp_addr: &str, http_url: &str) {
    let client = reqwest::Client::new();

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
    assert_eq!(resp.status(), 200, "query failed for SQL: {}", sql);
    resp.json().await.unwrap()
}

/// Extract a field from the payload column of an output stream row.
///
/// Output stream records have standard columns (offset, timestamp, key,
/// subject, payload, headers).  The continuous executor serialises the
/// operator output into the payload as a JSON object.
fn payload_field(row: &[Value], columns: &[Value], field: &str) -> Value {
    let idx = columns
        .iter()
        .position(|c| c.as_str() == Some("payload"))
        .expect("output should have 'payload' column");
    let payload = &row[idx];
    // The payload is a JSON object (serde_json::Value::Object) or a string.
    let obj = if let Some(s) = payload.as_str() {
        serde_json::from_str::<Value>(s).unwrap_or(Value::Null)
    } else {
        payload.clone()
    };
    obj.get(field).cloned().unwrap_or(Value::Null)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test 1: Tumbling window via continuous query (EMIT CHANGES).
///
/// Bounded execution does not support WindowedAggregate, so we create a
/// continuous query with a tumbling window that writes to an output stream.
/// All 10 records are published within the same second, so they all fall into
/// a single 1-hour window.  With EMIT CHANGES, every record produces an
/// updated aggregate row in the output stream.  The last row should have
/// cnt = 10.
#[tokio::test]
async fn tumbling_window_continuous_query() {
    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    // 1. Create source stream and publish 10 records.
    let records: Vec<(&str, &str)> = vec![
        ("event.created", r#"{"region": "eu", "val": 1}"#),
        ("event.created", r#"{"region": "us", "val": 1}"#),
        ("event.created", r#"{"region": "eu", "val": 1}"#),
        ("event.created", r#"{"region": "us", "val": 1}"#),
        ("event.created", r#"{"region": "eu", "val": 1}"#),
        ("event.created", r#"{"region": "us", "val": 1}"#),
        ("event.created", r#"{"region": "eu", "val": 1}"#),
        ("event.created", r#"{"region": "us", "val": 1}"#),
        ("event.created", r#"{"region": "eu", "val": 1}"#),
        ("event.created", r#"{"region": "us", "val": 1}"#),
    ];
    setup_stream("windowed-test", &records, &tcp, &http).await;

    // 2. Create continuous query with a tumbling window (no extra group-by
    //    key, so all records go into a single group within each window).
    let resp = client
        .post(format!("{}/api/v1/queries/continuous", http))
        .json(&serde_json::json!({
            "sql": r#"CREATE VIEW windowed_output AS SELECT tumbling(timestamp, '1 hour') AS window_start, COUNT(*) AS cnt FROM "windowed-test" GROUP BY tumbling(timestamp, '1 hour') EMIT CHANGES"#
        }))
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

    // 3. Wait for the continuous query to process existing records.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 4. Query the output stream.
    let body = query(&http, r#"SELECT * FROM "windowed_output""#).await;

    let row_count = body["row_count"].as_u64().unwrap();
    assert!(
        row_count >= 1,
        "expected at least 1 row in windowed output, got: {}",
        row_count
    );

    // With EMIT CHANGES and 10 records in the same window (single group),
    // we get 10 output records with running counts 1..10.
    let rows = body["rows"].as_array().expect("rows should be array");
    let columns = body["columns"].as_array().expect("columns should be array");

    // The output stream stores windowed aggregate results serialised as
    // JSON in the payload column.  Extract cnt from the last row.
    let last_row = rows.last().unwrap().as_array().unwrap();
    let last_cnt = payload_field(last_row, columns, "cnt");
    assert_eq!(
        last_cnt.as_i64().unwrap(),
        10,
        "last running count should be 10, got: {}",
        last_cnt
    );
}

/// Test 2: Create a materialized view (filter/project) and query via
/// bounded SQL.
///
/// MVs in simple mode (no windowed aggregate) work as a per-record
/// filter/project pipeline.  This test creates an MV that filters for
/// EU records and verifies that `SELECT * FROM <mv>` returns only EU rows.
#[tokio::test]
async fn materialized_view_creation_and_query() {
    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    // 1. Create stream and publish records.
    let records: Vec<(&str, &str)> = vec![
        ("order.created", r#"{"region": "eu", "amount": 100}"#),
        ("order.created", r#"{"region": "us", "amount": 200}"#),
        ("order.created", r#"{"region": "eu", "amount": 300}"#),
        ("order.created", r#"{"region": "us", "amount": 400}"#),
        ("order.created", r#"{"region": "eu", "amount": 500}"#),
    ];
    setup_stream("mv-test", &records, &tcp, &http).await;

    // 2. Create materialized view that selects EU records.
    let resp = client
        .post(format!("{}/api/v1/views", http))
        .json(&serde_json::json!({
            "sql": r#"CREATE MATERIALIZED VIEW eu_orders AS SELECT payload->>'region' AS region, payload->>'amount' AS amount FROM "mv-test" WHERE payload->>'region' = 'eu'"#
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create MV should return 201"
    );

    let mv_body: Value = resp.json().await.unwrap();
    assert!(
        mv_body.get("query_id").is_some(),
        "response should include query_id"
    );

    // 3. Wait for the MV to catch up.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 4. Query the MV via bounded SQL.
    let body = query(&http, "SELECT * FROM eu_orders").await;

    let row_count = body["row_count"].as_u64().unwrap();
    assert!(
        row_count >= 1,
        "expected at least 1 row in MV, got: {}. body: {}",
        row_count,
        body
    );

    // Verify all rows have region = "eu".
    let rows = body["rows"].as_array().expect("rows should be array");
    let columns = body["columns"].as_array().expect("columns should be array");
    let region_idx = columns
        .iter()
        .position(|c| c.as_str() == Some("region"))
        .expect("should have 'region' column");

    for (i, row) in rows.iter().enumerate() {
        let row = row.as_array().unwrap();
        let region = row[region_idx].as_str().unwrap_or("?");
        assert_eq!(
            region, "eu",
            "row {} should have region 'eu', got '{}'",
            i, region
        );
    }
}

/// Test 3: Query a materialized view via REST API endpoints.
///
/// Verifies `GET /api/v1/views` lists the view, and
/// `GET /api/v1/views/{name}` returns the expected rows.
#[tokio::test]
async fn materialized_view_via_rest() {
    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    // 1. Setup: create stream, publish records, create MV.
    let records: Vec<(&str, &str)> = vec![
        ("order.created", r#"{"region": "eu", "amount": 50}"#),
        ("order.created", r#"{"region": "us", "amount": 75}"#),
        ("order.created", r#"{"region": "eu", "amount": 60}"#),
        ("order.created", r#"{"region": "us", "amount": 80}"#),
    ];
    setup_stream("mv-rest-test", &records, &tcp, &http).await;

    let resp = client
        .post(format!("{}/api/v1/views", http))
        .json(&serde_json::json!({
            "sql": r#"CREATE MATERIALIZED VIEW rest_eu_orders AS SELECT payload->>'region' AS region, payload->>'amount' AS amount FROM "mv-rest-test" WHERE payload->>'region' = 'eu'"#
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create MV should return 201");

    // 2. Wait for the MV to catch up.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 3. GET /api/v1/views — list should contain our MV.
    let resp = client
        .get(format!("{}/api/v1/views", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let views_body: Value = resp.json().await.unwrap();
    let views = views_body.as_array().expect("views list should be array");
    let found = views
        .iter()
        .any(|v| v["name"].as_str() == Some("rest_eu_orders"));
    assert!(
        found,
        "views list should contain 'rest_eu_orders', got: {:?}",
        views
    );

    // 4. GET /api/v1/views/rest_eu_orders — should have rows.
    let resp = client
        .get(format!("{}/api/v1/views/rest_eu_orders", http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let view_body: Value = resp.json().await.unwrap();

    let row_count = view_body["row_count"].as_u64().unwrap();
    assert!(
        row_count >= 1,
        "expected at least 1 row from REST view endpoint, got: {}",
        row_count
    );

    let columns = view_body["columns"]
        .as_array()
        .expect("should have columns");
    let col_names: Vec<&str> = columns.iter().filter_map(|c| c.as_str()).collect();
    assert!(
        col_names.contains(&"region"),
        "columns should include 'region', got: {:?}",
        col_names
    );
    assert!(
        col_names.contains(&"amount"),
        "columns should include 'amount', got: {:?}",
        col_names
    );

    // Verify all rows have region = "eu".
    let rows = view_body["rows"].as_array().expect("should have rows");
    let region_idx = col_names.iter().position(|&c| c == "region").unwrap();
    for (i, row) in rows.iter().enumerate() {
        let row = row.as_array().unwrap();
        let region = row[region_idx].as_str().unwrap_or("?");
        assert_eq!(
            region, "eu",
            "REST view row {} should have region 'eu', got '{}'",
            i, region
        );
    }
}

/// Test 4: Continuous query with tumbling window and multiple input records.
///
/// Creates a continuous query that counts all records in 1-hour tumbling
/// windows (EMIT CHANGES).  Publishes a second batch of records after the
/// query is running to verify that the continuous executor picks up new
/// data and emits updated counts.
#[tokio::test]
async fn continuous_query_tumbling_picks_up_new_records() {
    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    // 1. Create source stream and publish an initial batch of 3 records.
    let records: Vec<(&str, &str)> = vec![
        ("event.created", r#"{"val": 1}"#),
        ("event.created", r#"{"val": 2}"#),
        ("event.created", r#"{"val": 3}"#),
    ];
    setup_stream("cq-tumble-live", &records, &tcp, &http).await;

    // 2. Create continuous query with tumbling window.
    let resp = client
        .post(format!("{}/api/v1/queries/continuous", http))
        .json(&serde_json::json!({
            "sql": r#"CREATE VIEW tumble_live AS SELECT tumbling(timestamp, '1 hour') AS window_start, COUNT(*) AS cnt FROM "cq-tumble-live" GROUP BY tumbling(timestamp, '1 hour') EMIT CHANGES"#
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // 3. Wait for the initial batch to be processed.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify initial output — should have at least 3 rows (running count 1,2,3).
    let body = query(&http, r#"SELECT * FROM "tumble_live""#).await;
    let initial_count = body["row_count"].as_u64().unwrap();
    assert!(
        initial_count >= 3,
        "expected at least 3 output rows after initial batch, got: {}",
        initial_count
    );

    // 4. Publish 2 more records while the query is already running.
    let (mut reader, mut writer) = connect_to(&tcp).await;
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::Ok);
    for i in 0..2 {
        let resp = send_recv(
            &mut writer,
            &mut reader,
            publish_frame(
                "cq-tumble-live",
                "event.created",
                r#"{"val": 99}"#.as_bytes(),
                100 + i,
            ),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok);
    }

    // 5. Wait for the new records to be processed.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 6. Query again — should have more output rows, and the last row
    //    should have cnt = 5 (total of 3 + 2 records, all in one window).
    let body = query(&http, r#"SELECT * FROM "tumble_live""#).await;
    let final_count = body["row_count"].as_u64().unwrap();
    assert!(
        final_count > initial_count,
        "output row count should increase after new records (was {}, now {})",
        initial_count,
        final_count
    );

    let rows = body["rows"].as_array().unwrap();
    let columns = body["columns"].as_array().unwrap();
    let last_row = rows.last().unwrap().as_array().unwrap();
    let last_cnt = payload_field(last_row, columns, "cnt");
    assert_eq!(
        last_cnt.as_i64().unwrap(),
        5,
        "last running count should be 5 (3 initial + 2 new), got: {}",
        last_cnt
    );
}
