mod common;

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

async fn wait_for_rows(
    pool: &sqlx::mysql::MySqlPool,
    table: &str,
    want: i64,
    deadline_secs: u64,
) -> i64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(deadline_secs);
    loop {
        let sql = format!("SELECT COUNT(*) FROM `{}`", table);
        let count: (i64,) = sqlx::query_as(&sql).fetch_one(pool).await.unwrap_or((0,));
        if count.0 >= want || std::time::Instant::now() > deadline {
            return count.0;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn mysql_blob_mode_writes() {
    let url = crate::require_mysql!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();
    let table = common::db::unique_table("mbl");

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "my-blob"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-my-blob",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "my-blob",
            "settings": {
                "connection": url,
                "table": &table,
                "mode": "upsert",
                "auto_create_table": "true"
            }
        }))
        .send()
        .await
        .unwrap();

    for n in 0..3 {
        client
            .post(format!("{}/api/v1/streams/my-blob/publish", http))
            .json(&serde_json::json!({"data": {"n": n}}))
            .send()
            .await
            .unwrap();
    }

    let pool = sqlx::mysql::MySqlPool::connect(&url).await.unwrap();
    let got = wait_for_rows(&pool, &table, 3, 10).await;
    assert_eq!(got, 3);
    common::db::drop_table_mysql(&url, &table).await;
    pool.close().await;
}

#[tokio::test]
async fn mysql_typed_binds_correct_types() {
    let url = crate::require_mysql!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();
    let table = common::db::unique_table("mty");

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "my-typed"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-my-typed",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "my-typed",
            "settings": {
                "connection": url,
                "table": &table,
                "mode": "upsert",
                "auto_create_table": "true",
                "upsert_keys": "id",
                "schema": "id:bigint, amount:bigint, email:text?"
            }
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/api/v1/streams/my-typed/publish", http))
        .json(&serde_json::json!({"data": {"id": 1, "amount": 100, "email": "a@b"}}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/api/v1/streams/my-typed/publish", http))
        .json(&serde_json::json!({"data": {"id": 2, "amount": 200}}))
        .send()
        .await
        .unwrap();

    let pool = sqlx::mysql::MySqlPool::connect(&url).await.unwrap();
    wait_for_rows(&pool, &table, 2, 10).await;

    let (sum,): (i64,) = sqlx::query_as(&format!("SELECT SUM(amount) FROM `{}`", table))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sum, 300);

    common::db::drop_table_mysql(&url, &table).await;
    pool.close().await;
}
