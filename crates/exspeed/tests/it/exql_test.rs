use serde_json::Value;
use tokio::time::Duration;

use exspeed_client::PublishRecord;

use crate::common::TestServer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the server (keep it alive) and its HTTP base URL.
async fn start_server() -> (TestServer, String) {
    let server = TestServer::start().await;
    let http = format!("http://{}", server.api_addr);
    (server, http)
}

async fn http_create_stream(http_url: &str, stream_name: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/streams", http_url))
        .json(&serde_json::json!({"name": stream_name}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "failed to create stream '{stream_name}'"
    );
}

/// Create a stream via HTTP, then publish `(subject, json)` records via TCP.
async fn setup_stream(
    stream_name: &str,
    records: &[(&str, &str)],
    server: &TestServer,
    http_url: &str,
) {
    http_create_stream(http_url, stream_name).await;
    let c = server.client().await;
    for (subject, payload) in records {
        c.publish(
            stream_name,
            PublishRecord::new(*subject, payload.to_string()),
        )
        .await
        .unwrap();
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

/// Like `setup_stream`, publishing in batches of 1000.
async fn setup_stream_bulk(
    stream_name: &str,
    records: &[(&str, String)],
    server: &TestServer,
    http_url: &str,
) {
    http_create_stream(http_url, stream_name).await;
    let c = server.client().await;
    for chunk in records.chunks(1000) {
        c.publish_batch(
            stream_name,
            chunk
                .iter()
                .map(|(s, p)| PublishRecord::new(*s, p.clone()))
                .collect(),
        )
        .await
        .unwrap();
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

    let c = tcp.client().await;
    let body: Value = c
        .query(r#"SELECT * FROM "tcp_query_orders""#)
        .await
        .unwrap();
    assert_eq!(body["row_count"], 3);
}

#[tokio::test]
async fn tcp_query_returns_error_for_invalid_sql() {
    let (tcp, _http) = start_server().await;

    let err = tcp
        .client()
        .await
        .query("THIS IS NOT VALID SQL")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(exspeed_client::code::BAD_REQUEST));
}

#[tokio::test]
async fn predicate_pushdown_filtered_limit_is_fast() {
    let (tcp, http) = start_server().await;

    // Publish 10k records, only 100 have status "active"
    let records: Vec<(&str, String)> = (0..10_000)
        .map(|i| {
            let status = if i % 100 == 0 { "active" } else { "inactive" };
            let json = format!(r#"{{"i": {i}, "status": "{status}"}}"#);
            ("orders.created", json)
        })
        .collect();
    setup_stream_bulk("pushdown_perf", &records, &tcp, &http).await;

    // With pushdown, this should not need to scan all 10k rows
    let start = std::time::Instant::now();
    let result = query(
        &http,
        r#"SELECT * FROM "pushdown_perf" WHERE payload->>'status' = 'active' LIMIT 5"#,
    )
    .await;
    let elapsed = start.elapsed();

    assert_eq!(result["row_count"], 5);
    assert!(
        elapsed < Duration::from_secs(2),
        "filtered LIMIT 5 query took too long: {:?}",
        elapsed
    );
}
