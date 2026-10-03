//! jdbc_poll E2E — SQLite (always runnable) + MSSQL (DB-gated).

use crate::common;
use std::time::Duration;

use crate::common::TestServer;

/// Returns the server (keep it alive) and its HTTP base URL.
async fn start_server() -> (TestServer, String) {
    let server = TestServer::start().await;
    let http = format!("http://{}", server.api_addr);
    (server, http)
}

async fn fetch_records(server: &TestServer, stream: &str, max: u32) -> Vec<(u64, Vec<u8>)> {
    match server
        .client()
        .await
        .read(stream, 0, max, Duration::ZERO, "")
        .await
    {
        Ok(r) => r
            .records
            .into_iter()
            .map(|rec| (rec.offset, rec.value.to_vec()))
            .collect(),
        Err(_) => Vec::new(),
    }
}

async fn wait_for_records(
    tcp_addr: &TestServer,
    stream: &str,
    want: usize,
    deadline_secs: u64,
) -> Vec<(u64, Vec<u8>)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(deadline_secs);
    loop {
        let recs = fetch_records(tcp_addr, stream, 100).await;
        if recs.len() >= want || std::time::Instant::now() > deadline {
            return recs;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn sqlite_poll_source_emits_new_rows() {
    let db_dir = tempfile::TempDir::new().unwrap();
    let db_path = db_dir.path().join("src.db");
    std::fs::File::create(&db_path).unwrap();
    let url = format!("sqlite://{}", db_path.display());

    // Pre-seed table with 3 rows.
    let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    for (i, n) in [(1i64, "alpha"), (2, "beta"), (3, "gamma")] {
        sqlx::query("INSERT INTO items (id, name) VALUES (?, ?)")
            .bind(i)
            .bind(n)
            .execute(&pool)
            .await
            .unwrap();
    }

    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{http}/api/v1/streams"))
        .json(&serde_json::json!({"name": "items"}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{http}/api/v1/connectors"))
        .json(&serde_json::json!({
            "name": "poll-items",
            "type": "source",
            "plugin": "jdbc_poll",
            "stream": "items",
            "settings": {
                "connection": url,
                "table": "items",
                "tracking_column": "id",
                "schema": "id:bigint, name:text"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create: {}", resp.text().await.unwrap());

    // Allow two polling cycles to pick up the seeded rows.
    let recs = wait_for_records(&tcp, "items", 3, 10).await;
    assert_eq!(recs.len(), 3, "should have emitted 3 rows");

    // Each record is a JSON object with id + name.
    let parsed: Vec<serde_json::Value> = recs
        .iter()
        .map(|(_, v)| serde_json::from_slice(v).unwrap())
        .collect();
    let names: Vec<&str> = parsed.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"alpha"));
    assert!(names.contains(&"beta"));
    assert!(names.contains(&"gamma"));

    // Insert a new row — should get picked up on the next poll.
    sqlx::query("INSERT INTO items (id, name) VALUES (?, ?)")
        .bind(4i64)
        .bind("delta")
        .execute(&pool)
        .await
        .unwrap();

    let recs = wait_for_records(&tcp, "items", 4, 10).await;
    assert_eq!(recs.len(), 4);

    pool.close().await;
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
    let tcp = tokio::net::TcpStream::connect(cfg.get_addr())
        .await
        .unwrap();
    tcp.set_nodelay(true).ok();
    use tokio_util::compat::TokioAsyncWriteCompatExt;
    tiberius::Client::connect(cfg, tcp.compat_write())
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "needs EXSPEED_MSSQL_URL (CI runs it with --include-ignored)"]
async fn mssql_poll_source_emits_new_rows() {
    let ms_url = crate::require_mssql!();
    let table = common::db::unique_table("poll_ms");

    // Create source table via tiberius.
    {
        let mut conn = mssql_connect(&ms_url).await;
        let sql = format!(
            "IF OBJECT_ID(N'[{t}]', N'U') IS NULL \
             CREATE TABLE [{t}] (id BIGINT NOT NULL PRIMARY KEY, name NVARCHAR(MAX) NOT NULL)",
            t = table
        );
        conn.simple_query(sql)
            .await
            .unwrap()
            .into_results()
            .await
            .unwrap();
        for (i, n) in [(1i64, "alpha"), (2, "beta"), (3, "gamma")] {
            let sql = format!(
                "INSERT INTO [{t}] (id, name) VALUES ({i}, '{n}')",
                t = table,
                i = i,
                n = n
            );
            conn.simple_query(sql)
                .await
                .unwrap()
                .into_results()
                .await
                .unwrap();
        }
    }

    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{http}/api/v1/streams"))
        .json(&serde_json::json!({"name": "mssql-poll-stream"}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{http}/api/v1/connectors"))
        .json(&serde_json::json!({
            "name": "poll-mssql",
            "type": "source",
            "plugin": "jdbc_poll",
            "stream": "mssql-poll-stream",
            "settings": {
                "connection": ms_url,
                "table": &table,
                "tracking_column": "id",
                "schema": "id:bigint, name:text"
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create: {}", resp.text().await.unwrap());

    let recs = wait_for_records(&tcp, "mssql-poll-stream", 3, 15).await;
    assert_eq!(recs.len(), 3, "should have emitted 3 rows from MSSQL");

    let parsed: Vec<serde_json::Value> = recs
        .iter()
        .map(|(_, v)| serde_json::from_slice(v).unwrap())
        .collect();
    let names: Vec<&str> = parsed.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"alpha"));
    assert!(names.contains(&"beta"));
    assert!(names.contains(&"gamma"));

    common::db::drop_table_mssql(&ms_url, &table).await;
}
