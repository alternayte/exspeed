//! SQLite E2E tests for the JDBC sink. No docker required — uses a
//! tempdir SQLite file; tests always run.

use std::time::Duration;

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

async fn wait_for_rows(pool: &sqlx::sqlite::SqlitePool, table: &str, want: i64, deadline_secs: u64) -> i64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(deadline_secs);
    loop {
        let sql = format!("SELECT COUNT(*) FROM \"{}\"", table);
        let count: (i64,) = sqlx::query_as(&sql).fetch_one(pool).await.unwrap_or((0,));
        if count.0 >= want || std::time::Instant::now() > deadline {
            return count.0;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn sqlite_blob_mode_creates_table_and_writes() {
    let db_dir = tempfile::TempDir::new().unwrap();
    let db_path = db_dir.path().join("test.db");
    // Create empty file so sqlx finds it (SqliteConnectOptions::create_if_missing default false).
    std::fs::File::create(&db_path).unwrap();
    let url = format!("sqlite://{}", db_path.display());

    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "sqlite-blob-stream"}))
        .send().await.unwrap();

    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-sqlite-blob",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "sqlite-blob-stream",
            "settings": {
                "connection": url,
                "table": "events",
                "mode": "upsert",
                "auto_create_table": "true"
            }
        }))
        .send().await.unwrap();
    assert_eq!(resp.status(), 201, "create: {}", resp.text().await.unwrap());

    for n in 0..3 {
        client
            .post(format!("{}/api/v1/streams/sqlite-blob-stream/publish", http))
            .json(&serde_json::json!({"data": {"n": n}}))
            .send().await.unwrap();
    }

    let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    let got = wait_for_rows(&pool, "events", 3, 10).await;
    assert_eq!(got, 3, "expected 3 rows");
    pool.close().await;
}

#[tokio::test]
async fn sqlite_typed_schema_round_trip() {
    let db_dir = tempfile::TempDir::new().unwrap();
    let db_path = db_dir.path().join("typed.db");
    std::fs::File::create(&db_path).unwrap();
    let url = format!("sqlite://{}", db_path.display());

    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "sqlite-typed-stream"}))
        .send().await.unwrap();

    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-sqlite-typed",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "sqlite-typed-stream",
            "settings": {
                "connection": url,
                "table": "orders",
                "mode": "upsert",
                "auto_create_table": "true",
                "upsert_keys": "order_id",
                "schema": "order_id:bigint, total_cents:bigint, name:text"
            }
        }))
        .send().await.unwrap();

    client
        .post(format!("{}/api/v1/streams/sqlite-typed-stream/publish", http))
        .json(&serde_json::json!({"data": {"order_id": 1, "total_cents": 123, "name": "x"}}))
        .send().await.unwrap();
    client
        .post(format!("{}/api/v1/streams/sqlite-typed-stream/publish", http))
        .json(&serde_json::json!({"data": {"order_id": 2, "total_cents": 500, "name": "y"}}))
        .send().await.unwrap();

    let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    wait_for_rows(&pool, "orders", 2, 10).await;

    let (sum,): (i64,) = sqlx::query_as("SELECT SUM(total_cents) FROM \"orders\"")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(sum, 623);
    pool.close().await;
}
