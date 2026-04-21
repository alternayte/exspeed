#![cfg(test)]

use std::time::Duration;
use tempfile::TempDir;

async fn start_server(auth_token: Option<String>) -> (u16, TempDir) {
    let api_port = portpicker::pick_unused_port().unwrap();
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let tmp = tempfile::tempdir().unwrap();

    let args = exspeed::cli::server::ServerArgs {
        bind: format!("127.0.0.1:{tcp_port}"),
        api_bind: format!("127.0.0.1:{api_port}"),
        data_dir: tmp.path().to_path_buf(),
        auth_token,
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
        exspeed::cli::server::run(args).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    (api_port, tmp)
}

#[tokio::test]
async fn leases_endpoint_noop_backend_returns_empty_list() {
    let (api_port, _tmp) = start_server(None).await;

    let resp = reqwest::get(format!("http://127.0.0.1:{api_port}/api/v1/leases"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(body.is_empty(), "noop backend should return empty lease list");
}

#[tokio::test]
async fn leases_endpoint_requires_auth_when_configured() {
    let (api_port, _tmp) = start_server(Some("lease-test-token".into())).await;

    // Without bearer → 401.
    let resp = reqwest::get(format!("http://127.0.0.1:{api_port}/api/v1/leases"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // With bearer → 200.
    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{api_port}/api/v1/leases"))
        .header("Authorization", "Bearer lease-test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn metrics_expose_lease_counters() {
    let (api_port, _tmp) = start_server(None).await;

    let body = reqwest::get(format!("http://127.0.0.1:{api_port}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // Counters show up after at least one acquire attempt; with noop backend,
    // every connector always succeeds — but we start the server without any
    // connectors here, so only the metric *descriptor* needs to exist.
    // Presence of the name in the output is enough.
    assert!(
        body.contains("exspeed_lease_held") || body.contains("exspeed_lease_acquire_total"),
        "expected lease metrics in /metrics body; got:\n{body}"
    );
}

fn skip_if_no_postgres() -> bool {
    std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").is_err()
}

async fn ensure_schema(schema: &str) {
    let url = std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").unwrap();
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to postgres");
    tokio::spawn(async move { let _ = connection.await; });
    let sql = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
    client.execute(&sql, &[]).await.expect("create schema");
}

#[tokio::test]
async fn leases_endpoint_postgres_backend_returns_single_cluster_leader_row() {
    if skip_if_no_postgres() {
        return;
    }

    let schema = format!("lease_api_{}", uuid::Uuid::new_v4().simple());
    ensure_schema(&schema).await;

    std::env::set_var("EXSPEED_CONSUMER_STORE", "postgres");
    std::env::set_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA", &schema);
    std::env::set_var("EXSPEED_LEASE_TTL_SECS", "5");
    std::env::set_var("EXSPEED_LEASE_HEARTBEAT_SECS", "1");

    let (api_port, _tmp) = start_server(None).await;

    // Give the leader race a moment to settle.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let resp = reqwest::get(format!("http://127.0.0.1:{api_port}/api/v1/leases"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(
        body.len(),
        1,
        "expected exactly one lease (cluster:leader); got {body:?}"
    );
    assert_eq!(body[0]["name"], "cluster:leader");
    assert!(body[0]["holder"].is_string(), "holder should be a UUID string");
    assert!(body[0]["expires_at"].is_string(), "expires_at should be a timestamp string");
    // `replication_endpoint` is serialized unconditionally. The test
    // server is started without a cluster-bind listener, so this row
    // carries `null`. A real leader with a bound cluster listener would
    // surface its advertised host:port string here instead.
    assert!(
        body[0].get("replication_endpoint").is_some(),
        "replication_endpoint key must always be present (as null when absent)"
    );
    assert!(
        body[0]["replication_endpoint"].is_null(),
        "replication_endpoint should be null when no cluster listener is bound; got {:?}",
        body[0]["replication_endpoint"]
    );

    // Clean up env vars.
    std::env::remove_var("EXSPEED_CONSUMER_STORE");
    std::env::remove_var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA");
    std::env::remove_var("EXSPEED_LEASE_TTL_SECS");
    std::env::remove_var("EXSPEED_LEASE_HEARTBEAT_SECS");
}
