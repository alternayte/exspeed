//! jdbc_poll E2E — SQLite (always runnable) + MSSQL (DB-gated).

#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::fetch::FetchRequest;
use exspeed_protocol::messages::records_batch::RecordsBatch;
use exspeed_protocol::opcodes::OpCode;

async fn start_server() -> (String, String) {
    let tcp_port = portpicker::pick_unused_port().unwrap();
    let http_port = portpicker::pick_unused_port().unwrap();
    let tcp_addr = format!("127.0.0.1:{tcp_port}");
    let http_addr = format!("127.0.0.1:{http_port}");
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
        .await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    (tcp_addr, format!("http://127.0.0.1:{http_port}"))
}

type FReader = FramedRead<tokio::net::tcp::OwnedReadHalf, ExspeedCodec>;
type FWriter = FramedWrite<tokio::net::tcp::OwnedWriteHalf, ExspeedCodec>;

async fn connect_to(addr: &str) -> (FReader, FWriter) {
    let s = TcpStream::connect(addr).await.unwrap();
    let (r, w) = s.into_split();
    (FramedRead::new(r, ExspeedCodec::new()), FramedWrite::new(w, ExspeedCodec::new()))
}

async fn send_recv(w: &mut FWriter, r: &mut FReader, f: Frame) -> Frame {
    w.send(f).await.unwrap();
    timeout(Duration::from_secs(5), r.next()).await.unwrap().unwrap().unwrap()
}

async fn fetch_records(tcp_addr: &str, stream: &str, max: u32) -> Vec<(u64, Vec<u8>)> {
    let (mut r, mut w) = connect_to(tcp_addr).await;
    let mut buf = BytesMut::new();
    ConnectRequest { client_id: "test".into(), auth_type: AuthType::None, auth_payload: Bytes::new() }.encode(&mut buf);
    let resp = send_recv(&mut w, &mut r, Frame::new(OpCode::Connect, 1, buf.freeze())).await;
    assert_eq!(resp.opcode, OpCode::ConnectOk);

    let mut buf = BytesMut::new();
    FetchRequest { stream: stream.into(), offset: 0, max_records: max, subject_filter: String::new() }.encode(&mut buf);
    let resp = send_recv(&mut w, &mut r, Frame::new(OpCode::Fetch, 2, buf.freeze())).await;
    if resp.opcode != OpCode::RecordsBatch { return Vec::new(); }
    let batch = RecordsBatch::decode(resp.payload.clone()).unwrap();
    batch.records.into_iter().map(|rec| (rec.offset, rec.value.to_vec())).collect()
}

async fn wait_for_records(tcp_addr: &str, stream: &str, want: usize, deadline_secs: u64) -> Vec<(u64, Vec<u8>)> {
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
        .execute(&pool).await.unwrap();
    for (i, n) in [(1i64, "alpha"), (2, "beta"), (3, "gamma")] {
        sqlx::query("INSERT INTO items (id, name) VALUES (?, ?)")
            .bind(i).bind(n).execute(&pool).await.unwrap();
    }

    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client.post(format!("{http}/api/v1/streams"))
        .json(&serde_json::json!({"name": "items"})).send().await.unwrap();

    let resp = client.post(format!("{http}/api/v1/connectors"))
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
        .send().await.unwrap();
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
        .bind(4i64).bind("delta").execute(&pool).await.unwrap();

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
    let tcp = tokio::net::TcpStream::connect(cfg.get_addr()).await.unwrap();
    tcp.set_nodelay(true).ok();
    use tokio_util::compat::TokioAsyncWriteCompatExt;
    tiberius::Client::connect(cfg, tcp.compat_write()).await.unwrap()
}

#[tokio::test]
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
        conn.simple_query(sql).await.unwrap().into_results().await.unwrap();
        for (i, n) in [(1i64, "alpha"), (2, "beta"), (3, "gamma")] {
            let sql = format!(
                "INSERT INTO [{t}] (id, name) VALUES ({i}, '{n}')",
                t = table, i = i, n = n
            );
            conn.simple_query(sql).await.unwrap().into_results().await.unwrap();
        }
    }

    let (tcp, http) = start_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{http}/api/v1/streams"))
        .json(&serde_json::json!({"name": "mssql-poll-stream"}))
        .send().await.unwrap();

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
        .send().await.unwrap();
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
