use serde_json::Value;
use tokio::time::Duration;

use exspeed_client::{Client, ConnectOptions, PublishRecord};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_server() -> (String, String) {
    let tcp_port = exspeed_testkit::pick_unused_port().unwrap();
    let http_port = exspeed_testkit::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{}", tcp_port);
    let http_addr = format!("127.0.0.1:{}", http_port);

    let dir = tempfile::TempDir::new().unwrap();
    let args = exspeed::cli::server::ServerArgs {
        bind: tcp_addr.clone(),
        data_dir: dir.path().to_path_buf(),
        api_bind: http_addr.clone(),
        ..Default::default()
    };

    tokio::spawn(async move {
        let _keep = dir;
        exspeed::cli::server::run(args).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    (tcp_addr, format!("http://{}", http_addr))
}

async fn tcp_client(addr: &str) -> Client {
    Client::connect(addr, ConnectOptions::default())
        .await
        .expect("connect")
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
    let c = tcp_client(tcp_addr).await;
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

/// POST a statement; return (status, body).
async fn post_sql(http_url: &str, path: &str, sql: &str) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(format!("{http_url}{path}"))
        .json(&serde_json::json!({"sql": sql}))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

async fn get_json(url: &str) -> (u16, Value) {
    let resp = reqwest::get(url).await.unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

/// Poll a bounded query until `pred` holds (10 s max).
async fn query_until(http_url: &str, sql: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let mut last = Value::Null;
    for _ in 0..100 {
        let (status, body) = post_sql(http_url, "/api/v1/queries", sql).await;
        if status == 200 && pred(&body) {
            return body;
        }
        last = body;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("condition not met for {sql}: last result {last}");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn orders() -> Vec<(&'static str, &'static str)> {
    vec![
        ("order.created", r#"{"total": 100, "region": "eu"}"#),
        ("order.created", r#"{"total": 250, "region": "us"}"#),
        ("order.shipped", r#"{"total": 75.5, "region": "eu"}"#),
        ("order.created", r#"{"total": "1000", "region": "us"}"#),
    ]
}

#[tokio::test]
async fn bounded_queries_return_real_values() {
    let (tcp, http) = start_server().await;
    setup_stream("exql_orders", &orders(), &tcp, &http).await;

    let res = query(&http, "SELECT * FROM exql_orders").await;
    assert_eq!(
        res["columns"],
        serde_json::json!([
            "offset",
            "timestamp",
            "subject",
            "key",
            "payload",
            "headers"
        ])
    );
    assert_eq!(res["row_count"], 4);
    assert_eq!(res["truncated"], false);
    assert_eq!(res["rows"][0][0], 0);
    assert_eq!(res["rows"][0][2], "order.created");
    assert_eq!(
        res["rows"][0][4],
        serde_json::json!({"total": 100, "region": "eu"})
    );
    assert!(res["rows"][0][1].as_str().unwrap().ends_with('Z'));

    // JSON text compares numerically ("1000" > 150, 75.5 < 150).
    let res = query(
        &http,
        "SELECT offset FROM exql_orders WHERE payload->>'total' > 150 ORDER BY offset",
    )
    .await;
    assert_eq!(res["rows"], serde_json::json!([[1], [3]]));

    let res = query(
        &http,
        "SELECT payload->>'region' AS region, COUNT(*) AS n, SUM(payload->>'total') AS total \
         FROM exql_orders GROUP BY payload->>'region' HAVING COUNT(*) > 1 ORDER BY total DESC",
    )
    .await;
    assert_eq!(
        res["rows"],
        serde_json::json!([["us", 2, 1250.0], ["eu", 2, 175.5]])
    );

    let res = query(
        &http,
        "SELECT offset, subject FROM exql_orders ORDER BY offset DESC LIMIT 2",
    )
    .await;
    assert_eq!(
        res["rows"],
        serde_json::json!([[3, "order.created"], [2, "order.shipped"]])
    );

    let res = query(
        &http,
        "SELECT COUNT(*) AS n FROM exql_orders WHERE timestamp > now() - INTERVAL '1 hour'",
    )
    .await;
    assert_eq!(res["rows"], serde_json::json!([[4]]));
}

#[tokio::test]
async fn bounded_errors_are_errors() {
    let (_tcp, http) = start_server().await;
    let (status, body) = post_sql(&http, "/api/v1/queries", "SELECT * FROM no_such_stream").await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], "PLAN_ERROR");
    let (status, body) = post_sql(&http, "/api/v1/queries", "SELEC 1").await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], "PARSE_ERROR");
    let (status, body) = post_sql(
        &http,
        "/api/v1/queries",
        "CREATE INDEX i ON s (payload->>'x')",
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], "UNSUPPORTED");
    assert!(body["hint"].as_str().unwrap().contains("removed"));
    // The index API is gone.
    let (status, _) = get_json(&format!("{http}/api/v1/indexes")).await;
    assert_eq!(status, 404);
    // The inline external form is gone.
    let (status, _) = post_sql(
        &http,
        "/api/v1/queries",
        "SELECT * FROM postgres('postgres://x', 'users')",
    )
    .await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn continuous_stream_and_table_end_to_end() {
    let (tcp, http) = start_server().await;
    setup_stream("cq_orders", &orders(), &tcp, &http).await;

    let (status, created) = post_sql(
        &http,
        "/api/v1/queries",
        "CREATE STREAM cq_big AS SELECT payload->>'region' AS region, payload->>'total' AS total \
         FROM cq_orders WHERE payload->>'total' > 150",
    )
    .await;
    assert_eq!(status, 201, "{created}");
    assert_eq!(created["status"], "running");
    let stream_q = created["query_id"].as_str().unwrap().to_string();

    let (status, created) = post_sql(
        &http,
        "/api/v1/views",
        "CREATE MATERIALIZED VIEW cq_by_region AS SELECT payload->>'region' AS region, \
         COUNT(*) AS n, SUM(payload->>'total') AS total FROM cq_orders GROUP BY payload->>'region'",
    )
    .await;
    assert_eq!(status, 201, "{created}");
    let table_q = created["query_id"].as_str().unwrap().to_string();

    let res = query_until(&http, "SELECT payload FROM cq_big ORDER BY offset", |b| {
        b["row_count"] == 2
    })
    .await;
    assert_eq!(
        res["rows"],
        serde_json::json!([[{"region": "us", "total": "250"}], [{"region": "us", "total": "1000"}]])
    );
    let res = query_until(
        &http,
        "SELECT region, n, total FROM cq_by_region ORDER BY region",
        |b| b["rows"] == serde_json::json!([["eu", 2, 175.5], ["us", 2, 1250.0]]),
    )
    .await;
    assert_eq!(res["row_count"], 2);

    let (status, row) = get_json(&format!("{http}/api/v1/views/cq_by_region?key=eu")).await;
    assert_eq!(status, 200);
    assert_eq!(
        row,
        serde_json::json!({"columns": ["region", "n", "total"], "row": ["eu", 2, 175.5]})
    );
    let (status, _) = get_json(&format!("{http}/api/v1/views/cq_by_region?key=nope")).await;
    assert_eq!(status, 404);
    let (_, views) = get_json(&format!("{http}/api/v1/views")).await;
    assert_eq!(views[0]["name"], "cq_by_region");
    assert_eq!(views[0]["row_count"], 2);

    // Pause, publish, resume: no output while paused, all of it afterwards.
    let client = reqwest::Client::new();
    let r = client
        .post(format!("{http}/api/v1/queries/{stream_q}/pause"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let (_, info) = get_json(&format!("{http}/api/v1/queries/{stream_q}")).await;
    assert_eq!(info["status"], "paused");
    setup_more(
        &tcp,
        "cq_orders",
        &[("order.created", r#"{"total": 500, "region": "eu"}"#)],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        query(&http, "SELECT COUNT(*) FROM cq_big").await["rows"],
        serde_json::json!([[2]])
    );
    let (status, _) = post_sql(
        &http,
        "/api/v1/queries",
        &format!("RESUME QUERY {stream_q}"),
    )
    .await;
    assert_eq!(status, 200);
    query_until(&http, "SELECT COUNT(*) FROM cq_big", |b| {
        b["rows"] == serde_json::json!([[3]])
    })
    .await;
    query_until(
        &http,
        "SELECT n FROM cq_by_region WHERE region = 'eu'",
        |b| b["rows"] == serde_json::json!([[3]]),
    )
    .await;

    let (_, list) = get_json(&format!("{http}/api/v1/queries")).await;
    let ids: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&stream_q.as_str()) && ids.contains(&table_q.as_str()));

    // DROP TABLE / DROP STREAM remove the queries and their streams.
    let (status, body) = post_sql(&http, "/api/v1/queries", "DROP TABLE cq_by_region").await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = post_sql(&http, "/api/v1/queries", "DROP STREAM cq_big").await;
    assert_eq!(status, 200, "{body}");
    let (_, list) = get_json(&format!("{http}/api/v1/queries")).await;
    assert_eq!(list, serde_json::json!([]));
    let (status, _) = post_sql(&http, "/api/v1/queries", "SELECT * FROM cq_big").await;
    assert_eq!(status, 400);
}

/// Publish more records on an existing stream.
async fn setup_more(tcp_addr: &str, stream: &str, records: &[(&str, &str)]) {
    let c = tcp_client(tcp_addr).await;
    for (subject, payload) in records {
        c.publish(stream, PublishRecord::new(*subject, payload.to_string()))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn tcp_query_returns_result() {
    let (tcp, http) = start_server().await;
    setup_stream("tcp_query_orders", &orders(), &tcp, &http).await;
    let sql = "SELECT subject, payload->>'total' AS total FROM tcp_query_orders WHERE payload->>'total' >= 250 ORDER BY offset";
    let body: Value = tcp_client(&tcp).await.query(sql).await.unwrap();
    assert_eq!(body["columns"], serde_json::json!(["subject", "total"]));
    assert_eq!(
        body["rows"],
        serde_json::json!([["order.created", "250"], ["order.created", "1000"]])
    );
    assert_eq!(body["row_count"], 2);
    assert_eq!(body["truncated"], false);
}

#[tokio::test]
async fn tcp_query_returns_error_for_invalid_sql() {
    let (tcp, _http) = start_server().await;
    let err = tcp_client(&tcp)
        .await
        .query("THIS IS NOT VALID SQL")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(400), "expected an error for invalid SQL");
}

/// Create a stream and publish `records` using PublishBatch (chunks of 1 000) for speed.
///
/// Each record is a `(subject, json_payload)` pair.  Much faster than the
/// one-at-a-time `setup_stream` for large datasets because it avoids an
/// individual round-trip per record.
async fn setup_stream_bulk(
    stream_name: &str,
    records: &[(&str, String)],
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

    // Publish via TCP in batches of 1 000
    let c = tcp_client(tcp_addr).await;
    for chunk in records.chunks(1_000) {
        c.publish_batch(
            stream_name,
            chunk
                .iter()
                .map(|(subject, payload)| PublishRecord::new(*subject, payload.clone()))
                .collect(),
        )
        .await
        .unwrap();
    }
}

/// LIMIT and `ORDER BY offset DESC LIMIT n` read only what they need.
#[tokio::test]
async fn limit_and_tail_reads_are_pushed_down() {
    let (tcp, http) = start_server().await;
    let payloads: Vec<(&str, String)> = (0u64..10_000)
        .map(|i| {
            (
                "s.scan",
                format!(
                    r#"{{"i":{i},"kind":"{}"}}"#,
                    if i % 2 == 0 { "even" } else { "odd" }
                ),
            )
        })
        .collect();
    setup_stream_bulk("scan10k", &payloads, &tcp, &http).await;

    let res = query(&http, "SELECT payload->>'i' AS i FROM scan10k LIMIT 5").await;
    assert_eq!(
        res["rows"],
        serde_json::json!([["0"], ["1"], ["2"], ["3"], ["4"]])
    );
    let res = query(
        &http,
        "SELECT offset FROM scan10k WHERE payload->>'kind' = 'odd' ORDER BY offset DESC LIMIT 3",
    )
    .await;
    assert_eq!(res["rows"], serde_json::json!([[9999], [9997], [9995]]));
    let plan = query(
        &http,
        "EXPLAIN SELECT offset FROM scan10k ORDER BY offset DESC LIMIT 3",
    )
    .await;
    let text = plan["rows"].to_string();
    assert!(text.contains("reverse=true"), "{text}");
    let res = query(
        &http,
        "SELECT COUNT(*) FROM scan10k WHERE offset >= 9000 AND offset < 9100",
    )
    .await;
    assert_eq!(res["rows"], serde_json::json!([[100]]));
    let plan = query(
        &http,
        "EXPLAIN SELECT COUNT(*) FROM scan10k WHERE offset >= 9000 AND offset < 9100",
    )
    .await;
    assert!(plan["rows"].to_string().contains("offsets=[9000, 9100)"));
    let res = query(&http, "SELECT COUNT(*), SUM(payload->>'i') FROM scan10k").await;
    assert_eq!(res["rows"], serde_json::json!([[10000, 49995000.0]]));
}
