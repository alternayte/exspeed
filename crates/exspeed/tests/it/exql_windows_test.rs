use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use exspeed_client::{Client, ConnectOptions, PublishRecord};

// ---------------------------------------------------------------------------
// Helpers (same pattern as exql_test.rs)
// ---------------------------------------------------------------------------

async fn start_server() -> (String, String) {
    let tcp_port_l = exspeed_testkit::bind_local();
    let tcp_port = tcp_port_l.local_addr().unwrap().port();
    let http_port_l = exspeed_testkit::bind_local();
    let http_port = http_port_l.local_addr().unwrap().port();
    let tcp_addr = format!("127.0.0.1:{}", tcp_port);
    let http_addr = format!("127.0.0.1:{}", http_port);

    let dir = tempfile::TempDir::new().unwrap();
    let args = exspeed::cli::server::ServerArgs {
        bind: tcp_addr.clone(),
        tcp_listener: Some(std::sync::Arc::new(tcp_port_l)),
        data_dir: dir.path().to_path_buf(),
        api_bind: http_addr.clone(),
        api_listener: Some(std::sync::Arc::new(http_port_l)),
        ..Default::default()
    };

    tokio::spawn(async move {
        let _keep = dir;
        exspeed::cli::server::run(args).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    (tcp_addr, format!("http://{}", http_addr))
}

async fn post_sql(http_url: &str, sql: &str) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(format!("{http_url}/api/v1/queries"))
        .json(&serde_json::json!({"sql": sql}))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

async fn create(http_url: &str, sql: &str) -> String {
    let (status, body) = post_sql(http_url, sql).await;
    assert_eq!(status, 201, "{sql}: {body}");
    body["query_id"].as_str().unwrap().to_string()
}

/// Poll a bounded query until `pred` holds (10 s max).
async fn query_until(http_url: &str, sql: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let mut last = Value::Null;
    for _ in 0..100 {
        let (status, body) = post_sql(http_url, sql).await;
        if status == 200 && pred(&body) {
            return body;
        }
        last = body;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("condition not met for {sql}: last result {last}");
}

/// The `payload` column of every row of `stream`, in offset order.
async fn payloads(http_url: &str, stream: &str) -> Vec<Value> {
    let (status, body) = post_sql(
        http_url,
        &format!("SELECT payload FROM {stream} ORDER BY offset"),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r[0].clone())
        .collect()
}

const T0: i64 = 1_700_000_000_000;

fn ts(d: i64) -> String {
    exspeed_processing::convert::format_ts_millis(T0 + d)
}

async fn publish_json(tcp: &str, stream: &str, payloads: &[Value]) {
    let c = Client::connect(tcp, ConnectOptions::default())
        .await
        .expect("connect");
    for p in payloads {
        c.publish(stream, PublishRecord::new("e", p.to_string()))
            .await
            .unwrap();
    }
}

async fn create_stream(http: &str, name: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{http}/api/v1/streams"))
        .json(&serde_json::json!({"name": name}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Event-time tumbling windows over replayed history: EMIT FINAL emits each
/// closed window exactly once, with every aggregate correct.
#[tokio::test]
async fn tumbling_window_emit_final_on_event_time() {
    let (tcp, http) = start_server().await;
    create_stream(&http, "wclicks").await;
    let events: Vec<Value> = [
        ("a", 1_000, 5),
        ("b", 2_000, 7),
        ("a", 3_000, 1),
        ("a", 12_000, 2),
        ("b", 21_000, 10),
        ("a", 35_000, 1),
    ]
    .iter()
    .map(|(u, d, a)| serde_json::json!({"user": u, "ts": T0 + d, "amount": a}))
    .collect();
    publish_json(&tcp, "wclicks", &events).await;
    create(
        &http,
        "CREATE STREAM wout AS SELECT payload->>'user' AS usr, window_start, COUNT(*) AS n, \
         SUM(payload->>'amount') AS total, AVG(payload->>'amount') AS avg FROM wclicks \
         TIMESTAMP BY payload->>'ts' WINDOW TUMBLING (SIZE 10 SECONDS) GROUP BY payload->>'user' EMIT FINAL",
    )
    .await;
    let expected = vec![
        serde_json::json!({"usr": "a", "window_start": ts(0), "n": 2, "total": 6.0, "avg": 3.0}),
        serde_json::json!({"usr": "b", "window_start": ts(0), "n": 1, "total": 7.0, "avg": 7.0}),
        serde_json::json!({"usr": "a", "window_start": ts(10_000), "n": 1, "total": 2.0, "avg": 2.0}),
        serde_json::json!({"usr": "b", "window_start": ts(20_000), "n": 1, "total": 10.0, "avg": 10.0}),
    ];
    query_until(&http, "SELECT COUNT(*) FROM wout", |b| b["rows"][0][0] == 4).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(payloads(&http, "wout").await, expected);
}

/// Stream-stream LEFT JOIN with WITHIN, through the server.
#[tokio::test]
async fn stream_stream_left_join_within() {
    let (tcp, http) = start_server().await;
    create_stream(&http, "jorders").await;
    create_stream(&http, "jpay").await;
    publish_json(
        &tcp,
        "jorders",
        &[
            serde_json::json!({"id": "o1", "ts": T0 + 1_000}),
            serde_json::json!({"id": "o2", "ts": T0 + 2_000}),
            serde_json::json!({"id": "o3", "ts": T0 + 30_000}),
        ],
    )
    .await;
    publish_json(
        &tcp,
        "jpay",
        &[
            serde_json::json!({"order_id": "o1", "ts": T0 + 3_000, "amt": 10}),
            serde_json::json!({"order_id": "o2", "ts": T0 + 20_000, "amt": 20}),
            serde_json::json!({"order_id": "x", "ts": T0 + 31_000, "amt": 1}),
        ],
    )
    .await;
    create(
        &http,
        "CREATE STREAM jout AS SELECT o.payload->>'id' AS oid, p.payload->>'amt' AS amt \
         FROM jorders o TIMESTAMP BY o.payload->>'ts' LEFT JOIN jpay p TIMESTAMP BY p.payload->>'ts' \
         WITHIN 5 SECONDS ON o.payload->>'id' = p.payload->>'order_id'",
    )
    .await;
    // Watermark = min(30, 31) s: o1 matched, o2's payment is 18 s away so o2
    // is emitted unmatched; o3 stays open.
    query_until(&http, "SELECT COUNT(*) FROM jout", |b| b["rows"][0][0] == 2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut got = payloads(&http, "jout").await;
    got.sort_by_key(|v| v.to_string());
    assert_eq!(
        got,
        vec![
            serde_json::json!({"oid": "o1", "amt": "10"}),
            serde_json::json!({"oid": "o2", "amt": null}),
        ]
    );
}

async fn start_server_at(data_dir: PathBuf) -> (String, String, CancellationToken) {
    let tcp_port_l = exspeed_testkit::bind_local();
    let tcp_port = tcp_port_l.local_addr().unwrap().port();
    let http_port_l = exspeed_testkit::bind_local();
    let http_port = http_port_l.local_addr().unwrap().port();
    let tcp_addr = format!("127.0.0.1:{}", tcp_port);
    let http_addr = format!("127.0.0.1:{}", http_port);
    let cancel = CancellationToken::new();
    let args = exspeed::cli::server::ServerArgs {
        bind: tcp_addr.clone(),
        tcp_listener: Some(std::sync::Arc::new(tcp_port_l)),
        api_bind: http_addr.clone(),
        api_listener: Some(std::sync::Arc::new(http_port_l)),
        data_dir,
        ..Default::default()
    };
    let c = cancel.clone();
    tokio::spawn(async move {
        let shutdown = async move { c.cancelled().await };
        exspeed::cli::server::run_with_shutdown(args, shutdown)
            .await
            .ok();
    });
    let http = format!("http://{http_addr}");
    for _ in 0..100 {
        if let Ok(r) = reqwest::get(format!("{http}/readyz")).await {
            if r.status().is_success() {
                // queries resume once the leader supervisor starts
                tokio::time::sleep(Duration::from_millis(200)).await;
                return (tcp_addr, http, cancel);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become ready");
}

async fn stop_server(cancel: CancellationToken, data_dir: &Path) {
    cancel.cancel();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let _ = std::fs::remove_file(data_dir.join(".exspeed.lock"));
}

/// Tables, windows and paused queries survive a server restart; output has
/// no duplicates.
#[tokio::test]
async fn queries_and_tables_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (tcp, http, cancel) = start_server_at(dir.path().to_path_buf()).await;
    create_stream(&http, "rsrc").await;
    let batch1: Vec<Value> = (0..6)
        .map(|i| serde_json::json!({"k": if i % 2 == 0 { "x" } else { "y" }, "v": i, "ts": T0 + i * 4_000}))
        .collect();
    publish_json(&tcp, "rsrc", &batch1).await;
    create(&http, "CREATE TABLE rtab AS SELECT payload->>'k' AS k, COUNT(*) AS n, SUM(payload->>'v') AS s FROM rsrc GROUP BY payload->>'k'").await;
    create(
        &http,
        "CREATE STREAM rwin AS SELECT payload->>'k' AS k, window_start, COUNT(*) AS n FROM rsrc \
         TIMESTAMP BY payload->>'ts' WINDOW TUMBLING (SIZE 10 SECONDS) GROUP BY payload->>'k' EMIT FINAL",
    )
    .await;
    let paused = create(
        &http,
        "CREATE STREAM rcopy AS SELECT payload->>'v' AS v FROM rsrc",
    )
    .await;
    query_until(&http, "SELECT k, n, s FROM rtab ORDER BY k", |b| {
        b["rows"] == serde_json::json!([["x", 3, 6.0], ["y", 3, 9.0]])
    })
    .await;
    query_until(&http, "SELECT COUNT(*) FROM rcopy", |b| {
        b["rows"][0][0] == 6
    })
    .await;
    // windows [0,10) and [10,20) closed by the record at 20 s
    query_until(&http, "SELECT COUNT(*) FROM rwin", |b| b["rows"][0][0] == 4).await;
    let (status, _) = post_sql(&http, &format!("PAUSE QUERY {paused}")).await;
    assert_eq!(status, 200);
    stop_server(cancel, dir.path()).await;

    let (tcp, http, cancel) = start_server_at(dir.path().to_path_buf()).await;
    let res = post_sql(&http, "SELECT k, n, s FROM rtab ORDER BY k")
        .await
        .1;
    assert_eq!(
        res["rows"],
        serde_json::json!([["x", 3, 6.0], ["y", 3, 9.0]])
    );
    let (_, info) = post_sql(&http, "SELECT 1").await;
    assert_eq!(info["rows"], serde_json::json!([[1]]));
    let q: Value = reqwest::get(format!("{http}/api/v1/queries/{paused}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(q["status"], "paused");

    let batch2: Vec<Value> = (6..10)
        .map(|i| serde_json::json!({"k": if i % 2 == 0 { "x" } else { "y" }, "v": i, "ts": T0 + i * 4_000}))
        .collect();
    publish_json(&tcp, "rsrc", &batch2).await;
    query_until(&http, "SELECT k, n, s FROM rtab ORDER BY k", |b| {
        b["rows"] == serde_json::json!([["x", 5, 20.0], ["y", 5, 25.0]])
    })
    .await;
    // + windows [20,30) x/y closed by the record at 36 s
    query_until(&http, "SELECT COUNT(*) FROM rwin", |b| b["rows"][0][0] == 6).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let wins = payloads(&http, "rwin").await;
    let n: Vec<i64> = wins.iter().map(|w| w["n"].as_i64().unwrap()).collect();
    assert_eq!(n, vec![2, 1, 1, 1, 1, 2]);
    let starts: std::collections::HashSet<String> = wins
        .iter()
        .map(|w| format!("{}{}", w["k"], w["window_start"]))
        .collect();
    assert_eq!(starts.len(), 6, "duplicate window rows: {wins:?}");
    // The paused query stayed paused.
    assert_eq!(
        post_sql(&http, "SELECT COUNT(*) FROM rcopy").await.1["rows"][0][0],
        6
    );
    stop_server(cancel, dir.path()).await;
}
