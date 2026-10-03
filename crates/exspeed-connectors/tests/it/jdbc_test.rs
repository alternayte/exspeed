//! `jdbc` sink, `jdbc_poll` and `mssql_cdc` against real MySQL and SQL
//! Server: crash and resume.
//!
//! - MySQL: `EXSPEED_MYSQL_URL` (e.g. `mysql://exspeed:exspeed@127.0.0.1:3306/exspeed`).
//! - SQL Server: `EXSPEED_MSSQL_URL` (e.g.
//!   `mssql://sa:Exspeed_Test!1@127.0.0.1:1433/exspeed?trust_server_certificate=true`).
//!   The database must exist and must not be a system database (CDC can't be
//!   enabled on `master`); `mssql_cdc` also needs SQL Server Agent running
//!   (`MSSQL_AGENT_ENABLED=true` in the container).
//!
//! Run with `--include-ignored`. Without the variable a test skips, except
//! under `CI=true`, where it fails.

use std::sync::Arc;
use std::time::Duration;

use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use serde_json::{json, Value};

use exspeed_connectors::offset_store::OffsetStore;
use exspeed_connectors::ConnectorType::{Sink, Source};
use exspeed_connectors::Registry;
use exspeed_streams::StoredRecord;

use crate::common::*;

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

enum Db {
    Postgres(sqlx::PgPool),
    MySql(sqlx::MySqlPool),
    Mssql(Pool<ConnectionManager>),
}

fn mssql_config(raw: &str) -> tiberius::Config {
    let normalized = if raw.to_ascii_lowercase().starts_with("mssql://") {
        format!("sqlserver://{}", &raw[8..])
    } else {
        raw.to_string()
    };
    let u = url::Url::parse(&normalized).expect("EXSPEED_MSSQL_URL");
    let mut cfg = tiberius::Config::new();
    cfg.host(u.host_str().unwrap_or("127.0.0.1"));
    cfg.port(u.port().unwrap_or(1433));
    let dec = |s: &str| {
        percent_encoding::percent_decode_str(s)
            .decode_utf8_lossy()
            .into_owned()
    };
    cfg.authentication(tiberius::AuthMethod::sql_server(
        dec(u.username()),
        dec(u.password().unwrap_or("")),
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
    cfg
}

impl Db {
    async fn postgres() -> Option<(Db, String)> {
        let url = service_env("EXSPEED_POSTGRES_URL")?;
        let pool = sqlx::PgPool::connect(&url)
            .await
            .expect("connect to EXSPEED_POSTGRES_URL");
        Some((Db::Postgres(pool), url))
    }

    async fn mysql() -> Option<(Db, String)> {
        let url = service_env("EXSPEED_MYSQL_URL")?;
        let pool = sqlx::MySqlPool::connect(&url)
            .await
            .expect("connect to EXSPEED_MYSQL_URL");
        Some((Db::MySql(pool), url))
    }

    async fn mssql() -> Option<(Db, String)> {
        let url = service_env("EXSPEED_MSSQL_URL")?;
        let mgr = ConnectionManager::build(mssql_config(&url)).unwrap();
        let pool = Pool::builder().max_size(2).build(mgr).await.unwrap();
        Some((Db::Mssql(pool), url))
    }

    async fn exec(&self, sql: &str) {
        match self {
            Db::Postgres(p) => {
                sqlx::raw_sql(sql)
                    .execute(p)
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e}"));
            }
            Db::MySql(p) => {
                sqlx::raw_sql(sql)
                    .execute(p)
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e}"));
            }
            Db::Mssql(p) => {
                let mut c = p.get().await.unwrap();
                c.simple_query(sql)
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    .into_results()
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e}"));
            }
        }
    }

    /// Run `sql` (two integer columns) and return the rows.
    async fn pairs(&self, sql: &str) -> Vec<(i64, i64)> {
        match self {
            Db::Postgres(p) => sqlx::query_as::<_, (i64, i64)>(sql)
                .fetch_all(p)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}")),
            Db::MySql(p) => sqlx::query_as::<_, (i64, i64)>(sql)
                .fetch_all(p)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}")),
            Db::Mssql(p) => {
                let mut c = p.get().await.unwrap();
                c.simple_query(sql)
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    .into_first_result()
                    .await
                    .unwrap()
                    .iter()
                    .map(|r| (r.get::<i64, _>(0).unwrap(), r.get::<i64, _>(1).unwrap()))
                    .collect()
            }
        }
    }

    fn q(&self, ident: &str) -> String {
        match self {
            Db::Postgres(_) => format!("\"{ident}\""),
            Db::MySql(_) => format!("`{ident}`"),
            Db::Mssql(_) => format!("[{ident}]"),
        }
    }

    async fn drop_table(&self, table: &str) {
        let sql = match self {
            Db::Postgres(_) => format!("DROP TABLE IF EXISTS \"{table}\""),
            Db::MySql(_) => format!("DROP TABLE IF EXISTS `{table}`"),
            Db::Mssql(_) => {
                format!("IF OBJECT_ID(N'[{table}]', N'U') IS NOT NULL DROP TABLE [{table}]")
            }
        };
        self.exec(&sql).await;
    }
}

fn json_of(r: &StoredRecord) -> Value {
    serde_json::from_slice(&r.value).unwrap()
}

fn header<'a>(r: &'a StoredRecord, k: &str) -> Option<&'a str> {
    r.headers
        .iter()
        .find(|(n, _)| n == k)
        .map(|(_, v)| v.as_str())
}

// ---------------------------------------------------------------------------
// jdbc sink
// ---------------------------------------------------------------------------

/// Two crashes after rows were written but before the offset commit: the
/// replayed batches are rewritten. In `upsert` mode they overwrite the same
/// rows; in `insert` mode the duplicate-key error on replay counts as
/// written. Either way every record is in the table exactly once and nothing
/// goes to the DLQ.
async fn sink_crash_replay(db: &Db, url: &str, mode: &str) {
    let name = unique(&format!("jdbc_{mode}"));
    let table = name.clone();
    let dlq = format!("{name}_dlq");
    let env = Env::new();
    let vals: Vec<Value> = (0..30)
        .map(|i| json!({"id": i, "amount": i * 10, "note": format!("r{i}")}))
        .collect();
    publish_json(&env, &name, &vals).await;

    let mut cfg = fast_config(&name, Sink, "jdbc", &name);
    cfg.batch_size = 5;
    cfg.dlq_stream = Some(dlq.clone());
    cfg.settings = settings(json!({
        "connection": url,
        "table": table,
        "mode": mode,
        "upsert_keys": ["id"],
        "schema": "id:bigint, amount:bigint, note:text",
        "auto_create_table": true,
    }));
    let mem: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let crashing: Arc<dyn OffsetStore> = Arc::new(CrashingOffsets::new(mem.clone(), 2));
    let (h, state) = env.run(&Registry::builtin(), cfg, crashing);
    wait_committed(&mem, &name, 30).await;
    let restarts = state.snapshot().restart_count;
    h.stop(Duration::from_secs(15)).await;

    let rows = db
        .pairs(&format!(
            "SELECT CAST({id} AS SIGNED_OR_BIGINT), CAST({amount} AS SIGNED_OR_BIGINT) FROM {t} ORDER BY {id}",
            id = db.q("id"),
            amount = db.q("amount"),
            t = db.q(&table),
        )
        .replace(
            "SIGNED_OR_BIGINT",
            if matches!(db, Db::MySql(_)) { "SIGNED" } else { "BIGINT" },
        ))
        .await;
    let dlq_records = env.read_all(&dlq).await;
    db.drop_table(&table).await;

    assert!(restarts >= 2, "both crashes restarted the connector");
    assert_eq!(
        rows,
        (0..30).map(|i| (i, i * 10)).collect::<Vec<_>>(),
        "every record exactly once ({mode} mode)"
    );
    assert!(
        dlq_records.is_empty(),
        "replays are not poison: {dlq_records:?}"
    );
}

// ---------------------------------------------------------------------------
// jdbc_poll
// ---------------------------------------------------------------------------

/// A crash after the first batch is appended but before its checkpoint is
/// saved: the restart re-reads from the previous checkpoint, so that batch
/// is appended twice (at-least-once; `jdbc_poll` sets no idempotency key)
/// and nothing is lost. After a clean stop, rows inserted meanwhile are
/// picked up from the saved cursor without re-reading older rows.
async fn poll_crash_replay(db: &Db, url: &str) {
    let name = unique("jpoll");
    let table = name.clone();
    // An INT identity column: the cursor must handle 32-bit integers.
    let ddl = match db {
        Db::Postgres(_) => {
            format!("CREATE TABLE \"{table}\" (id SERIAL PRIMARY KEY, name VARCHAR(50) NOT NULL)")
        }
        Db::MySql(_) => format!(
            "CREATE TABLE `{table}` (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL)"
        ),
        Db::Mssql(_) => format!(
            "CREATE TABLE [{table}] (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(50) NOT NULL)"
        ),
    };
    db.exec(&ddl).await;
    let insert = |from: u32, to: u32| {
        let values: Vec<String> = (from..=to).map(|i| format!("('r{i}')")).collect();
        format!(
            "INSERT INTO {} (name) VALUES {}",
            db.q(&table),
            values.join(", ")
        )
    };
    db.exec(&insert(1, 12)).await;

    let mut cfg = fast_config(&name, Source, "jdbc_poll", &name);
    cfg.batch_size = 5;
    cfg.settings = settings(json!({
        "connection": url,
        "table": table,
        "tracking_column": "id",
        "schema": "id:bigint, name:text",
    }));
    let env = Env::new();
    let mem: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let crashing: Arc<dyn OffsetStore> = Arc::new(CrashingOffsets::new(mem.clone(), 1));
    let reg = Registry::builtin();
    let (h, state) = env.run(&reg, cfg.clone(), crashing);
    let ids = |recs: &[StoredRecord]| -> Vec<i64> {
        recs.iter()
            .map(|r| json_of(r)["id"].as_i64().unwrap())
            .collect()
    };
    eventually(60, "checkpoint at the last row", || async {
        mem.load_source(&name).await.unwrap().as_deref() == Some("12")
    })
    .await;
    // Nothing else may arrive once caught up.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let first = env.read_all(&name).await;
    let restarts = state.snapshot().restart_count;
    h.stop(Duration::from_secs(15)).await;

    db.exec(&insert(13, 15)).await;
    let (h, _) = env.run(&reg, cfg, mem.clone());
    let all = wait_records(&env, &name, first.len() + 3).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let all_after = env.read_all(&name).await;
    let checkpoint = mem.load_source(&name).await.unwrap();
    h.stop(Duration::from_secs(15)).await;
    db.drop_table(&table).await;

    assert!(restarts >= 1, "the crash restarted the connector");
    // Batch 1 (ids 1-5) was durable before the crash and is replayed.
    let mut want: Vec<i64> = (1..=5).collect();
    want.extend(1..=12);
    assert_eq!(
        ids(&first),
        want,
        "at-least-once: the crashed batch is replayed"
    );
    assert!(json_of(&first[0])["name"]
        .as_str()
        .unwrap()
        .starts_with('r'));
    assert_eq!(first[0].subject, format!("jdbc_poll.{table}"));
    assert_eq!(
        all.len(),
        all_after.len(),
        "nothing re-read after the restart"
    );
    assert_eq!(ids(&all_after[first.len()..]), vec![13, 14, 15]);
    assert_eq!(checkpoint.as_deref(), Some("15"));
}

// ---------------------------------------------------------------------------
// MySQL
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs MySQL (EXSPEED_MYSQL_URL)"]
async fn mysql_sink_upsert_crash_replay_has_no_duplicates() {
    let Some((db, url)) = Db::mysql().await else {
        return;
    };
    sink_crash_replay(&db, &url, "upsert").await;
}

#[tokio::test]
#[ignore = "needs MySQL (EXSPEED_MYSQL_URL)"]
async fn mysql_sink_insert_crash_replay_has_no_duplicates() {
    let Some((db, url)) = Db::mysql().await else {
        return;
    };
    sink_crash_replay(&db, &url, "insert").await;
}

#[tokio::test]
#[ignore = "needs MySQL (EXSPEED_MYSQL_URL)"]
async fn mysql_poll_crash_replays_the_batch() {
    let Some((db, url)) = Db::mysql().await else {
        return;
    };
    poll_crash_replay(&db, &url).await;
}

/// Typical native column types behind each schema type (unsigned or
/// identity ids, small-int or bit booleans, decimals, datetimes, JSON):
/// every row decodes, and each is emitted once.
async fn poll_decodes_common_column_types(db: &Db, url: &str) {
    let table = unique("jptypes");
    let (ddl, insert) = match db {
        Db::Postgres(_) => (
            format!(
                "CREATE TABLE \"{table}\" (id BIGSERIAL PRIMARY KEY, active BOOLEAN NOT NULL, \
                 amount NUMERIC(10,2) NULL, created_at TIMESTAMPTZ NOT NULL, doc JSONB NULL, \
                 note VARCHAR(20) NULL)"
            ),
            format!(
                "INSERT INTO \"{table}\" (active, amount, created_at, doc, note) VALUES \
                 (true, 12.50, '2026-01-02 03:04:05+00', '{{\"a\": [1, 2]}}', 'x'), \
                 (false, NULL, '2026-01-02 03:04:06+00', NULL, NULL)"
            ),
        ),
        Db::MySql(_) => (
            format!(
                "CREATE TABLE `{table}` (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
                 active TINYINT(1) NOT NULL, amount DECIMAL(10,2) NULL, \
                 created_at DATETIME NOT NULL, doc JSON NULL, note VARCHAR(20) NULL)"
            ),
            format!(
                "INSERT INTO `{table}` (active, amount, created_at, doc, note) VALUES \
                 (1, 12.50, '2026-01-02 03:04:05', '{{\"a\": [1, 2]}}', 'x'), \
                 (0, NULL, '2026-01-02 03:04:06', NULL, NULL)"
            ),
        ),
        Db::Mssql(_) => (
            format!(
                "CREATE TABLE [{table}] (id BIGINT IDENTITY(1,1) PRIMARY KEY, active BIT NOT NULL, \
                 amount DECIMAL(10,2) NULL, created_at DATETIME2 NOT NULL, \
                 doc NVARCHAR(MAX) NULL, note NVARCHAR(20) NULL)"
            ),
            format!(
                "INSERT INTO [{table}] (active, amount, created_at, doc, note) VALUES \
                 (1, 12.50, '2026-01-02T03:04:05', N'{{\"a\": [1, 2]}}', N'x'), \
                 (0, NULL, '2026-01-02T03:04:06', NULL, NULL)"
            ),
        ),
    };
    db.exec(&ddl).await;
    db.exec(&insert).await;
    let name = table.clone();
    let mut cfg = fast_config(&name, Source, "jdbc_poll", &name);
    cfg.settings = settings(json!({
        "connection": url,
        "table": table,
        "tracking_column": "id",
        "schema": "id:bigint, active:boolean, amount:double, created_at:timestamptz, \
                   doc:jsonb, note:text",
    }));
    let env = Env::new();
    let (h, state) = env.run(&Registry::builtin(), cfg, Arc::new(MemOffsets::default()));
    eventually(60, "2 records", || async {
        let s = state.snapshot();
        assert_eq!(s.restart_count, 0, "poll failed: {:?}", s.last_error);
        env.read_all(&name).await.len() >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let all = env.read_all(&name).await;
    h.stop(Duration::from_secs(15)).await;
    db.drop_table(&table).await;

    assert_eq!(all.len(), 2, "each row once: {all:?}");
    let (a, b) = (json_of(&all[0]), json_of(&all[1]));
    assert_eq!(a["id"], json!(1));
    assert_eq!(a["active"], json!(true));
    assert_eq!(a["amount"], json!(12.5));
    assert!(
        a["created_at"].as_str().unwrap().starts_with("2026-01-02"),
        "{a}"
    );
    assert_eq!(a["doc"], json!({"a": [1, 2]}));
    assert_eq!(a["note"], json!("x"));
    assert_eq!(b["id"], json!(2));
    assert_eq!(b["active"], json!(false));
    assert_eq!(b["amount"], Value::Null);
    assert_eq!(b["doc"], Value::Null);
    assert_eq!(b["note"], Value::Null);
}

#[tokio::test]
#[ignore = "needs MySQL (EXSPEED_MYSQL_URL)"]
async fn mysql_poll_decodes_common_column_types() {
    let Some((db, url)) = Db::mysql().await else {
        return;
    };
    poll_decodes_common_column_types(&db, &url).await;
}

// ---------------------------------------------------------------------------
// Postgres (`jdbc_poll` and the `jdbc` sink; the Postgres-native plugins are
// in postgres_test)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs Postgres (EXSPEED_POSTGRES_URL)"]
async fn postgres_poll_decodes_common_column_types() {
    let Some((db, url)) = Db::postgres().await else {
        return;
    };
    poll_decodes_common_column_types(&db, &url).await;
}

#[tokio::test]
#[ignore = "needs Postgres (EXSPEED_POSTGRES_URL)"]
async fn postgres_poll_crash_replays_the_batch() {
    let Some((db, url)) = Db::postgres().await else {
        return;
    };
    poll_crash_replay(&db, &url).await;
}

#[tokio::test]
#[ignore = "needs Postgres (EXSPEED_POSTGRES_URL)"]
async fn postgres_sink_upsert_crash_replay_has_no_duplicates() {
    let Some((db, url)) = Db::postgres().await else {
        return;
    };
    sink_crash_replay(&db, &url, "upsert").await;
}

// ---------------------------------------------------------------------------
// SQL Server
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs SQL Server (EXSPEED_MSSQL_URL)"]
async fn mssql_sink_upsert_crash_replay_has_no_duplicates() {
    let Some((db, url)) = Db::mssql().await else {
        return;
    };
    sink_crash_replay(&db, &url, "upsert").await;
}

#[tokio::test]
#[ignore = "needs SQL Server (EXSPEED_MSSQL_URL)"]
async fn mssql_sink_insert_crash_replay_has_no_duplicates() {
    let Some((db, url)) = Db::mssql().await else {
        return;
    };
    sink_crash_replay(&db, &url, "insert").await;
}

#[tokio::test]
#[ignore = "needs SQL Server (EXSPEED_MSSQL_URL)"]
async fn mssql_poll_decodes_common_column_types() {
    let Some((db, url)) = Db::mssql().await else {
        return;
    };
    poll_decodes_common_column_types(&db, &url).await;
}

#[tokio::test]
#[ignore = "needs SQL Server (EXSPEED_MSSQL_URL)"]
async fn mssql_poll_crash_replays_the_batch() {
    let Some((db, url)) = Db::mssql().await else {
        return;
    };
    poll_crash_replay(&db, &url).await;
}

/// `mssql_cdc` with two crashes between the append and the checkpoint save:
/// the replayed changes carry the same `x-idempotency-key`
/// (`mssqlcdc:<ci>:<lsn>:<seqval>`), so the stream holds each change exactly
/// once. Changes made while the connector is stopped (update, delete,
/// insert) are delivered after the restart, once each.
#[tokio::test]
#[ignore = "needs SQL Server with SQL Server Agent (EXSPEED_MSSQL_URL)"]
async fn mssql_cdc_crash_and_resume_is_exactly_once() {
    let Some((db, url)) = Db::mssql().await else {
        return;
    };
    let name = unique("mscdc");
    let table = name.clone();
    let ci = format!("dbo_{table}");
    db.exec(
        "IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 \
         EXEC sys.sp_cdc_enable_db",
    )
    .await;
    db.exec(&format!(
        "CREATE TABLE [dbo].[{table}] (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50) NULL, \
         amount BIGINT NULL)"
    ))
    .await;
    db.exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'{table}', \
         @role_name = NULL, @capture_instance = N'{ci}'"
    ))
    .await;
    for i in 1..=8 {
        db.exec(&format!(
            "INSERT INTO [dbo].[{table}] (id, name, amount) VALUES ({i}, N'r{i}', {})",
            i * 10
        ))
        .await;
    }

    let mut cfg = fast_config(&name, Source, "mssql_cdc", &name);
    cfg.batch_size = 3;
    cfg.settings = settings(json!({
        "connection": url,
        "capture_instance": ci,
        "schema": "id:bigint, name:text, amount:bigint",
        "key_columns": ["id"],
    }));
    let env = Env::new();
    let mem: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let crashing: Arc<dyn OffsetStore> = Arc::new(CrashingOffsets::new(mem.clone(), 2));
    let reg = Registry::builtin();
    let (h, state) = env.run(&reg, cfg.clone(), crashing);
    // The capture job scans the log every few seconds.
    wait_records(&env, &name, 8).await;
    eventually(60, "two crashes", || async {
        state.snapshot().restart_count >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let first = env.read_all(&name).await;
    h.stop(Duration::from_secs(15)).await;

    db.exec(&format!(
        "UPDATE [dbo].[{table}] SET name = N'changed' WHERE id = 1"
    ))
    .await;
    db.exec(&format!("DELETE FROM [dbo].[{table}] WHERE id = 2"))
        .await;
    db.exec(&format!(
        "INSERT INTO [dbo].[{table}] (id, name, amount) VALUES (9, NULL, 90)"
    ))
    .await;
    let (h, _) = env.run(&reg, cfg, mem.clone());
    wait_records(&env, &name, 11).await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let all = env.read_all(&name).await;
    h.stop(Duration::from_secs(15)).await;
    db.exec(&format!(
        "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'{table}', \
         @capture_instance = N'{ci}'"
    ))
    .await;
    db.drop_table(&table).await;

    assert_eq!(first.len(), 8, "no duplicates after the crashes: {first:?}");
    for (i, r) in first.iter().enumerate() {
        let id = i as i64 + 1;
        let v = json_of(r);
        assert_eq!(v["op"], "c");
        assert_eq!(v["before"], Value::Null);
        assert_eq!(
            v["after"],
            json!({"id": id, "name": format!("r{id}"), "amount": id * 10})
        );
        assert_eq!(v["source"]["capture_instance"], json!(ci));
        assert_eq!(r.key.as_deref(), Some(id.to_string().as_bytes()));
        assert_eq!(r.subject, format!("mssql_cdc.{ci}.insert"));
        assert!(header(r, "x-idempotency-key")
            .unwrap()
            .starts_with(&format!("mssqlcdc:{ci}:")));
    }

    assert_eq!(all.len(), 11, "each later change exactly once: {all:?}");
    let (upd, del, ins) = (json_of(&all[8]), json_of(&all[9]), json_of(&all[10]));
    assert_eq!(upd["op"], "u");
    assert_eq!(upd["after"]["name"], json!("changed"));
    assert_eq!(del["op"], "d");
    assert_eq!(del["before"]["id"], json!(2));
    assert_eq!(del["after"], Value::Null);
    assert_eq!(ins["op"], "c");
    assert_eq!(ins["after"], json!({"id": 9, "name": null, "amount": 90}));
}
