//! Cluster metadata (query, connection and API-connector definitions) lives
//! in replicated internal streams, not in node-local files.

use std::time::Duration;

use serde_json::{json, Value};

use crate::common::TestServer;

async fn get(server: &TestServer, path: &str) -> Value {
    reqwest::get(server.api_url(path))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn post(server: &TestServer, path: &str, body: Value) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(server.api_url(path))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

async fn delete(server: &TestServer, path: &str) -> u16 {
    reqwest::Client::new()
        .delete(server.api_url(path))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

fn names(list: &Value, field: &str) -> Vec<String> {
    let mut v: Vec<String> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x[field].as_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

/// Poll `GET path` until its `status` field is `status` (10 s max).
async fn wait_status(server: &TestServer, path: &str, status: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let body = get(server, path).await;
        if body["status"] == status {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{path}: expected status {status}, last {body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn query_status(server: &TestServer, id: &str, status: &str) {
    wait_status(server, &format!("/api/v1/queries/{id}"), status).await;
}

async fn connector_status(server: &TestServer, name: &str, status: &str) {
    wait_status(server, &format!("/api/v1/connectors/{name}"), status).await;
}

/// Poll until `SELECT COUNT(*) FROM stream` returns `n` (10 s max).
async fn wait_rows(server: &TestServer, stream: &str, n: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let got = row_count(server, stream).await;
        if got == n {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{stream}: expected {n} rows, got {got}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn row_count(server: &TestServer, stream: &str) -> i64 {
    let (status, body) = post(
        server,
        "/api/v1/queries",
        json!({"sql": format!("SELECT COUNT(*) FROM {stream}")}),
    )
    .await;
    if status != 200 {
        return -1;
    }
    body["rows"][0][0].as_i64().unwrap_or(-1)
}

fn webhook(name: &str, stream: &str) -> Value {
    json!({
        "name": name,
        "type": "source",
        "plugin": "http_webhook",
        "stream": stream,
        "settings": {"path": name, "auth_type": "none"}
    })
}

#[tokio::test]
async fn definitions_survive_restart_from_the_log() {
    let server = TestServer::start().await;
    let (s, b) = post(&server, "/api/v1/streams", json!({"name": "csrc"})).await;
    assert_eq!(s, 201, "{b}");

    let (s, b) = post(
        &server,
        "/api/v1/queries",
        json!({"sql": "CREATE STREAM ccopy AS SELECT payload->>'v' AS v FROM csrc"}),
    )
    .await;
    assert_eq!(s, 201, "{b}");
    let qid = b["query_id"].as_str().unwrap().to_string();
    let (s, b) = post(
        &server,
        "/api/v1/queries",
        json!({"sql": "CREATE STREAM cdrop AS SELECT payload FROM csrc"}),
    )
    .await;
    assert_eq!(s, 201, "{b}");
    let dropped_qid = b["query_id"].as_str().unwrap().to_string();

    for name in ["hook-a", "hook-b"] {
        let (s, b) = post(&server, "/api/v1/connectors", webhook(name, "csrc")).await;
        assert_eq!(s, 201, "{b}");
    }
    for name in ["wh", "gone"] {
        let (s, b) = post(
            &server,
            "/api/v1/connections",
            json!({"name": name, "driver": "postgres", "url": "postgresql://u:${PGPASS}@db/x"}),
        )
        .await;
        assert_eq!(s, 201, "{b}");
    }
    query_status(&server, &qid, "running").await;

    // Nothing is written to node-local definition files.
    let dir = server.data_path().to_path_buf();
    for legacy in ["exql/queries", "connectors", "connections"] {
        assert!(!dir.join(legacy).exists(), "{legacy} should not exist");
    }
    let streams = names(&get(&server, "/api/v1/streams").await, "name");
    for s in ["__exql_queries", "__exql_connections", "__connectors"] {
        assert!(streams.contains(&s.to_string()), "{s} missing: {streams:?}");
    }

    let server = server.restart().await;
    query_status(&server, &qid, "running").await;
    connector_status(&server, "hook-a", "running").await;
    assert_eq!(
        names(&get(&server, "/api/v1/connectors").await, "name"),
        vec!["hook-a", "hook-b"]
    );
    assert_eq!(
        names(&get(&server, "/api/v1/connections").await, "name"),
        vec!["gone", "wh"]
    );
    // The restored query and webhook still work end to end.
    let resp = reqwest::Client::new()
        .post(server.api_url("/webhooks/hook-a"))
        .json(&json!({"v": 7}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "webhook: {}", resp.status());
    wait_rows(&server, "ccopy", 1).await;

    // Deletes are tombstones: they stay deleted across a restart.
    assert_eq!(
        delete(&server, &format!("/api/v1/queries/{dropped_qid}")).await,
        200
    );
    assert_eq!(delete(&server, "/api/v1/connectors/hook-b").await, 200);
    assert_eq!(delete(&server, "/api/v1/connections/gone").await, 200);

    let server = server.restart().await;
    query_status(&server, &qid, "running").await;
    let queries = get(&server, "/api/v1/queries").await;
    assert_eq!(names(&queries, "id"), vec![qid.clone()]);
    assert_eq!(
        names(&get(&server, "/api/v1/connectors").await, "name"),
        vec!["hook-a"]
    );
    assert_eq!(
        names(&get(&server, "/api/v1/connections").await, "name"),
        vec!["wh"]
    );
}

#[tokio::test]
async fn legacy_definition_files_are_migrated() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    {
        let mut server = TestServer::builder().data_dir(&dir).start().await;
        let (s, b) = post(&server, "/api/v1/streams", json!({"name": "lsrc"})).await;
        assert_eq!(s, 201, "{b}");
        server.stop().await;
    }

    // Files as written by older versions.
    std::fs::create_dir_all(dir.join("exql/queries")).unwrap();
    std::fs::write(
        dir.join("exql/queries/lcopy_0000beef.json"),
        serde_json::to_vec_pretty(&json!({
            "id": "lcopy_0000beef",
            "sql": "CREATE STREAM lcopy AS SELECT payload->>'v' AS v FROM lsrc",
            "kind": "stream",
            "name": "lcopy",
            "desired": "running",
            "error": null,
            "created_at": "2026-01-01T00:00:00.000Z"
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::create_dir_all(dir.join("connectors")).unwrap();
    std::fs::write(
        dir.join("connectors/legacy-hook.json"),
        serde_json::to_vec_pretty(&webhook("legacy-hook", "lsrc")).unwrap(),
    )
    .unwrap();
    std::fs::create_dir_all(dir.join("connections")).unwrap();
    std::fs::write(
        dir.join("connections/legacy-db.json"),
        r#"{"name":"legacy-db","driver":"postgres","url":"postgresql://db/x"}"#,
    )
    .unwrap();

    let server = TestServer::builder().data_dir(&dir).start().await;
    query_status(&server, "lcopy_0000beef", "running").await;
    connector_status(&server, "legacy-hook", "running").await;
    assert_eq!(
        names(&get(&server, "/api/v1/connections").await, "name"),
        vec!["legacy-db"]
    );
    for legacy in ["exql/queries", "connectors", "connections"] {
        assert!(!dir.join(legacy).exists(), "{legacy} not renamed");
        assert!(
            dir.join(format!("{legacy}.migrated")).is_dir(),
            "{legacy}.migrated missing"
        );
    }

    // The definitions now come from the log.
    let server = server.restart().await;
    query_status(&server, "lcopy_0000beef", "running").await;
    assert_eq!(
        names(&get(&server, "/api/v1/connectors").await, "name"),
        vec!["legacy-hook"]
    );
    assert_eq!(
        names(&get(&server, "/api/v1/connections").await, "name"),
        vec!["legacy-db"]
    );
    let resp = reqwest::Client::new()
        .post(server.api_url("/webhooks/legacy-hook"))
        .json(&json!({"v": 1}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "webhook: {}", resp.status());
    wait_rows(&server, "lcopy", 1).await;
    drop(server);
    drop(tmp);
}
