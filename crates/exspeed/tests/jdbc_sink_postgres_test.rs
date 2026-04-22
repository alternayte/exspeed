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
async fn typed_schema_binds_correct_types() {
    let pg_url = crate::require_postgres!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();
    let table = common::db::unique_table("typed");

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "typed-stream"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-typed",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "typed-stream",
            "settings": {
                "connection": pg_url,
                "table": &table,
                "mode": "upsert",
                "auto_create_table": "true",
                "upsert_keys": "order_id",
                "schema": "order_id:bigint, total_cents:bigint, placed_at:timestamptz, items:jsonb, coupon:text?"
            }
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/api/v1/streams/typed-stream/publish", http))
        .json(&serde_json::json!({"data": {
            "order_id": 1001,
            "total_cents": 12345,
            "placed_at": "2026-04-22T10:00:00Z",
            "items": [{"sku": "x1"}, {"sku": "y2"}],
            "coupon": null
        }}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/api/v1/streams/typed-stream/publish", http))
        .json(&serde_json::json!({"data": {
            "order_id": 1002,
            "total_cents": 50000,
            "placed_at": "2026-04-22T11:00:00Z",
            "items": []
        }}))
        .send()
        .await
        .unwrap();

    let pool = sqlx::postgres::PgPool::connect(&pg_url).await.unwrap();
    wait_for_rows(&pool, &table, 2, 10).await;

    let (sum,): (i64,) = sqlx::query_as(&format!("SELECT SUM(total_cents) FROM \"{}\"", table))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sum, 62345);

    let (ty,): (String,) = sqlx::query_as(&format!(
        "SELECT pg_typeof(total_cents)::text FROM \"{}\" LIMIT 1",
        table
    ))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ty, "bigint");

    let (c1, c2): (Option<String>, Option<String>) = sqlx::query_as(&format!(
        "SELECT \
           (SELECT coupon FROM \"{}\" WHERE order_id = 1001), \
           (SELECT coupon FROM \"{}\" WHERE order_id = 1002)",
        table, table
    ))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(c1.is_none());
    assert!(c2.is_none());

    let (recent,): (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM \"{}\" WHERE placed_at > NOW() - INTERVAL '10 years'",
        table
    ))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(recent, 2);

    common::db::drop_table_postgres(&pg_url, &table).await;
    pool.close().await;
}

#[tokio::test]
async fn typed_schema_rejects_mismatched_json_type() {
    let pg_url = crate::require_postgres!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();
    let table = common::db::unique_table("mismatch");

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "mismatch-stream"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-mismatch",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "mismatch-stream",
            "settings": {
                "connection": pg_url,
                "table": &table,
                "mode": "insert",
                "auto_create_table": "true",
                "schema": "id:bigint, email:text"
            }
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/api/v1/streams/mismatch-stream/publish", http))
        .json(&serde_json::json!({"data": {"id": "not-a-number", "email": "x@y"}}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/api/v1/streams/mismatch-stream/publish", http))
        .json(&serde_json::json!({"data": {"id": 2, "email": "ok@y"}}))
        .send()
        .await
        .unwrap();

    let pool = sqlx::postgres::PgPool::connect(&pg_url).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let (got,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM \"{}\"", table))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(got, 1, "bad record should have been skipped; only valid one should land");

    let (id,): (i64,) = sqlx::query_as(&format!("SELECT id FROM \"{}\"", table))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(id, 2);

    let metrics_body = reqwest::get(format!("{}/metrics", http))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics_body.contains("exspeed_connector_records_skipped_total")
            && metrics_body.contains("type_mismatch"),
        "metrics output should mention records_skipped with reason=type_mismatch, got:\n{metrics_body}"
    );

    common::db::drop_table_postgres(&pg_url, &table).await;
    pool.close().await;
}

#[tokio::test]
async fn table_name_injection_rejected_at_create() {
    let pg_url = crate::require_postgres!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "inj-stream"}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-inj",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "inj-stream",
            "settings": {
                "connection": pg_url,
                "table": "foo; DROP TABLE bar",
                "auto_create_table": "true"
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(
        !resp.status().is_success(),
        "create connector should fail; body={}",
        resp.text().await.unwrap()
    );
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
