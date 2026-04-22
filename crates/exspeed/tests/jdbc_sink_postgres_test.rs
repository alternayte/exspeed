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
    pool: &sqlx::postgres::PgPool,
    table: &str,
    want: i64,
    deadline_secs: u64,
) -> i64 {
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
async fn blob_mode_creates_table_and_writes() {
    let pg_url = crate::require_postgres!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();
    let table = common::db::unique_table("blob");

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "blob-stream"}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-blob",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "blob-stream",
            "settings": {
                "connection": pg_url,
                "table": &table,
                "mode": "upsert",
                "auto_create_table": "true"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "create connector: {}",
        resp.text().await.unwrap()
    );

    for n in 0..3 {
        client
            .post(format!("{}/api/v1/streams/blob-stream/publish", http))
            .json(&serde_json::json!({"data": {"n": n, "name": format!("item-{n}")}}))
            .send()
            .await
            .unwrap();
    }

    let pool = sqlx::postgres::PgPool::connect(&pg_url).await.unwrap();
    let got = wait_for_rows(&pool, &table, 3, 10).await;
    assert_eq!(got, 3, "expected 3 rows in {table}");

    let row: (i64, serde_json::Value) = sqlx::query_as(&format!(
        "SELECT \"offset\", \"value\" FROM \"{}\" ORDER BY \"offset\" ASC LIMIT 1",
        table
    ))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, 0);
    assert_eq!(row.1["n"], 0);
    assert_eq!(row.1["name"], "item-0");

    common::db::drop_table_postgres(&pg_url, &table).await;
    pool.close().await;
}

#[tokio::test]
async fn blob_mode_upsert_is_idempotent_on_offset() {
    let pg_url = crate::require_postgres!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();
    let table = common::db::unique_table("idem");

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "idem-stream"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-idem",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "idem-stream",
            "settings": {
                "connection": pg_url,
                "table": &table,
                "mode": "upsert",
                "auto_create_table": "true"
            }
        }))
        .send()
        .await
        .unwrap();

    for n in 0..5 {
        client
            .post(format!("{}/api/v1/streams/idem-stream/publish", http))
            .json(&serde_json::json!({"data": {"n": n}}))
            .send()
            .await
            .unwrap();
    }

    let pool = sqlx::postgres::PgPool::connect(&pg_url).await.unwrap();
    wait_for_rows(&pool, &table, 5, 10).await;

    // Restart connector — replay should be idempotent via offset PK.
    client
        .delete(format!("{}/api/v1/connectors/sink-idem", http))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-idem",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "idem-stream",
            "settings": {
                "connection": pg_url,
                "table": &table,
                "mode": "upsert",
                "auto_create_table": "true"
            }
        }))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let count: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM \"{}\"", table))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count.0, 5,
        "should still be 5 rows after replay (upsert was idempotent)"
    );

    common::db::drop_table_postgres(&pg_url, &table).await;
    pool.close().await;
}
