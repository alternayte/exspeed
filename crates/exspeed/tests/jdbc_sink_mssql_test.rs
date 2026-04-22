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

async fn mssql_connect(
    url: &str,
) -> tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>> {
    let normalized = if url.to_ascii_lowercase().starts_with("mssql://") {
        format!("sqlserver://{}", &url[8..])
    } else {
        url.to_string()
    };
    let u = url::Url::parse(&normalized).unwrap();
    let mut cfg = tiberius::Config::new();
    cfg.host(u.host_str().unwrap());
    cfg.port(u.port().unwrap_or(1433));
    cfg.authentication(tiberius::AuthMethod::sql_server(
        u.username(),
        u.password().unwrap_or(""),
    ));
    let db = u.path().trim_start_matches('/');
    if !db.is_empty() {
        cfg.database(db);
    }
    for (k, v) in u.query_pairs() {
        if k.eq_ignore_ascii_case("trust_server_certificate") && v.eq_ignore_ascii_case("true") {
            cfg.trust_cert();
        }
    }
    let tcp = tokio::net::TcpStream::connect(cfg.get_addr()).await.unwrap();
    tcp.set_nodelay(true).ok();
    use tokio_util::compat::TokioAsyncWriteCompatExt;
    tiberius::Client::connect(cfg, tcp.compat_write()).await.unwrap()
}

async fn count_rows_mssql(url: &str, table: &str, want: i64, deadline_secs: u64) -> i64 {
    let mut client = mssql_connect(url).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(deadline_secs);
    let mut last_seen: i64 = 0;
    loop {
        let sql = format!("SELECT COUNT(*) FROM [{}]", table);
        // Query may error with "Invalid object name" until the connector's
        // auto_create runs. Treat errors as "table not yet visible" — keep
        // polling until the deadline.
        if let Ok(stream) = client.simple_query(sql).await {
            if let Ok(rows) = stream.into_first_result().await {
                let n: i32 = rows.first().and_then(|r| r.get(0)).unwrap_or(0);
                last_seen = n as i64;
                if last_seen >= want {
                    return last_seen;
                }
            }
        }
        if std::time::Instant::now() > deadline {
            return last_seen;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn blob_mode_creates_table_and_writes() {
    let ms_url = crate::require_mssql!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();
    let table = common::db::unique_table("blob_ms");

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "blob-ms-stream"}))
        .send().await.unwrap();

    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-blob-ms",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "blob-ms-stream",
            "settings": {
                "connection": ms_url,
                "table": &table,
                "mode": "upsert",
                "auto_create_table": "true"
            }
        }))
        .send().await.unwrap();
    assert_eq!(resp.status(), 201, "create connector: {}", resp.text().await.unwrap());

    for n in 0..3 {
        client
            .post(format!("{}/api/v1/streams/blob-ms-stream/publish", http))
            .json(&serde_json::json!({"data": {"n": n, "name": format!("item-{n}")}}))
            .send().await.unwrap();
    }

    let got = count_rows_mssql(&ms_url, &table, 3, 10).await;
    assert_eq!(got, 3, "expected 3 rows in {table}");

    common::db::drop_table_mssql(&ms_url, &table).await;
}

#[tokio::test]
async fn blob_mode_upsert_is_idempotent_on_offset() {
    let ms_url = crate::require_mssql!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();
    let table = common::db::unique_table("idem_ms");

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "idem-ms-stream"}))
        .send().await.unwrap();

    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-idem-ms",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "idem-ms-stream",
            "settings": {
                "connection": ms_url,
                "table": &table,
                "mode": "upsert",
                "auto_create_table": "true"
            }
        }))
        .send().await.unwrap();

    for n in 0..5 {
        client
            .post(format!("{}/api/v1/streams/idem-ms-stream/publish", http))
            .json(&serde_json::json!({"data": {"n": n}}))
            .send().await.unwrap();
    }

    count_rows_mssql(&ms_url, &table, 5, 10).await;

    client
        .delete(format!("{}/api/v1/connectors/sink-idem-ms", http))
        .send().await.unwrap();
    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-idem-ms",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "idem-ms-stream",
            "settings": {
                "connection": ms_url,
                "table": &table,
                "mode": "upsert",
                "auto_create_table": "true"
            }
        }))
        .send().await.unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let got = count_rows_mssql(&ms_url, &table, 5, 5).await;
    assert_eq!(got, 5, "should still be 5 rows after replay (MERGE upsert was idempotent)");

    common::db::drop_table_mssql(&ms_url, &table).await;
}

#[tokio::test]
async fn typed_schema_binds_correct_types() {
    let ms_url = crate::require_mssql!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();
    let table = common::db::unique_table("typed_ms");

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "typed-ms-stream"}))
        .send().await.unwrap();

    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-typed-ms",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "typed-ms-stream",
            "settings": {
                "connection": ms_url,
                "table": &table,
                "mode": "upsert",
                "auto_create_table": "true",
                "upsert_keys": "order_id",
                "schema": "order_id:bigint, total_cents:bigint, placed_at:timestamptz, items:jsonb, coupon:text?"
            }
        }))
        .send().await.unwrap();

    client
        .post(format!("{}/api/v1/streams/typed-ms-stream/publish", http))
        .json(&serde_json::json!({"data": {
            "order_id": 1001,
            "total_cents": 12345,
            "placed_at": "2026-04-22T10:00:00Z",
            "items": [{"sku": "x1"}, {"sku": "y2"}],
            "coupon": null
        }}))
        .send().await.unwrap();

    client
        .post(format!("{}/api/v1/streams/typed-ms-stream/publish", http))
        .json(&serde_json::json!({"data": {
            "order_id": 1002,
            "total_cents": 50000,
            "placed_at": "2026-04-22T11:00:00Z",
            "items": []
        }}))
        .send().await.unwrap();

    count_rows_mssql(&ms_url, &table, 2, 10).await;

    let mut conn = mssql_connect(&ms_url).await;
    let stream = conn
        .simple_query(format!("SELECT SUM(total_cents) FROM [{}]", table))
        .await
        .unwrap();
    let rows = stream.into_first_result().await.unwrap();
    let sum: i64 = rows.first().and_then(|r| r.get(0)).unwrap();
    assert_eq!(sum, 62345);

    let stream = conn
        .simple_query(format!(
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' AND COLUMN_NAME = 'total_cents'",
            table
        ))
        .await
        .unwrap();
    let rows = stream.into_first_result().await.unwrap();
    let ty: &str = rows.first().and_then(|r| r.get(0)).unwrap();
    assert_eq!(ty, "bigint");

    let stream = conn
        .simple_query(format!(
            "SELECT COUNT(*) FROM [{}] WHERE placed_at > DATEADD(year, -10, SYSUTCDATETIME())",
            table
        ))
        .await
        .unwrap();
    let rows = stream.into_first_result().await.unwrap();
    let recent: i32 = rows.first().and_then(|r| r.get(0)).unwrap();
    assert_eq!(recent, 2);

    common::db::drop_table_mssql(&ms_url, &table).await;
}

#[tokio::test]
async fn typed_schema_rejects_mismatched_json_type() {
    let ms_url = crate::require_mssql!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();
    let table = common::db::unique_table("mismatch_ms");

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "mismatch-ms-stream"}))
        .send().await.unwrap();

    client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-mismatch-ms",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "mismatch-ms-stream",
            "settings": {
                "connection": ms_url,
                "table": &table,
                "mode": "insert",
                "auto_create_table": "true",
                "schema": "id:bigint, email:text"
            }
        }))
        .send().await.unwrap();

    client
        .post(format!("{}/api/v1/streams/mismatch-ms-stream/publish", http))
        .json(&serde_json::json!({"data": {"id": "not-a-number", "email": "x@y"}}))
        .send().await.unwrap();
    client
        .post(format!("{}/api/v1/streams/mismatch-ms-stream/publish", http))
        .json(&serde_json::json!({"data": {"id": 2, "email": "ok@y"}}))
        .send().await.unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut conn = mssql_connect(&ms_url).await;
    let stream = conn
        .simple_query(format!("SELECT COUNT(*) FROM [{}]", table))
        .await
        .unwrap();
    let rows = stream.into_first_result().await.unwrap();
    let got: i32 = rows.first().and_then(|r| r.get(0)).unwrap();
    assert_eq!(got, 1, "bad record should have been skipped; only valid one should land");

    let stream = conn
        .simple_query(format!("SELECT id FROM [{}]", table))
        .await
        .unwrap();
    let rows = stream.into_first_result().await.unwrap();
    let id: i64 = rows.first().and_then(|r| r.get(0)).unwrap();
    assert_eq!(id, 2);

    let metrics_body = reqwest::get(format!("{}/metrics", http))
        .await.unwrap()
        .text().await.unwrap();
    assert!(
        metrics_body.contains("exspeed_connector_records_skipped_total")
            && metrics_body.contains("type_mismatch"),
        "metrics should mention records_skipped with reason=type_mismatch, got:\n{metrics_body}"
    );

    common::db::drop_table_mssql(&ms_url, &table).await;
}

#[tokio::test]
async fn table_name_injection_rejected_at_create() {
    let ms_url = crate::require_mssql!();
    let (_tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/api/v1/streams", http))
        .json(&serde_json::json!({"name": "inj-ms-stream"}))
        .send().await.unwrap();

    let resp = client
        .post(format!("{}/api/v1/connectors", http))
        .json(&serde_json::json!({
            "name": "sink-inj-ms",
            "type": "sink",
            "plugin": "jdbc",
            "stream": "inj-ms-stream",
            "settings": {
                "connection": ms_url,
                "table": "foo; DROP TABLE bar",
                "auto_create_table": "true"
            }
        }))
        .send().await.unwrap();
    assert!(
        !resp.status().is_success(),
        "create connector should fail; body={}",
        resp.text().await.unwrap()
    );
}

#[tokio::test]
async fn concurrent_upserts_no_duplicate_keys() {
    let ms_url = crate::require_mssql!();
    let table = common::db::unique_table("concurrent_ms");

    {
        use exspeed_connectors::builtin::jdbc::dialect::{dialect_for, DialectKind};
        let d = dialect_for(DialectKind::Mssql);
        let sql = d.create_table_blob_sql(&table);
        let mut conn = mssql_connect(&ms_url).await;
        conn.simple_query(sql).await.unwrap().into_results().await.unwrap();
    }

    // Four concurrent workers, each upserts 100 records drawn from a shared
    // pool of 100 PKs. Without HOLDLOCK, MSSQL's MERGE can race between the
    // MATCHED check and the INSERT, producing duplicate-key errors (2627).
    let mut joins = Vec::new();
    for worker in 0..4 {
        let ms_url = ms_url.clone();
        let table = table.clone();
        joins.push(tokio::spawn(async move {
            use exspeed_connectors::builtin::jdbc::dialect::{dialect_for, DialectKind};
            let d = dialect_for(DialectKind::Mssql);
            let sql_template =
                d.upsert_sql(&table, &["offset", "subject", "key", "value"], &["offset"]);
            let mut conn = mssql_connect(&ms_url).await;
            for i in 0..100i64 {
                let row_sql = sql_template
                    .clone()
                    .replace("@P1", &i.to_string())
                    .replace("@P2", "NULL")
                    .replace("@P3", "NULL")
                    .replace(
                        "@P4",
                        &format!("'{{\"w\":{worker},\"i\":{i}}}'"),
                    );
                conn.simple_query(row_sql).await.expect("merge ok");
            }
        }));
    }
    for j in joins {
        j.await.unwrap();
    }

    let got = count_rows_mssql(&ms_url, &table, 100, 5).await;
    assert_eq!(
        got, 100,
        "MERGE+HOLDLOCK should produce exactly 100 rows (one per PK)"
    );

    common::db::drop_table_mssql(&ms_url, &table).await;
}
