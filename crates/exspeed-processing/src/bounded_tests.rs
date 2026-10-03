//! Unit tests for bounded queries against an in-memory store.

use std::sync::Arc;

use bytes::Bytes;
use exspeed_common::StreamName;
use exspeed_storage::memory::MemoryStorage;
use exspeed_streams::{Record, StorageEngine};
use serde_json::json;

use crate::bounded::{execute, QueryResult};
use crate::catalog::Resolver;
use crate::external::{ConnectionRegistry, ExternalConfig, ExternalTables};
use crate::session::{build_state, runtime_env, ExqlConfig};
use crate::tables::TableRegistry;

async fn setup(records: &[(&str, &str)]) -> (Arc<dyn StorageEngine>, Resolver, tempfile::TempDir) {
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let s = StreamName::try_from("orders").unwrap();
    storage.create_stream(&s, 0, 0).await.unwrap();
    for (subject, payload) in records {
        storage
            .append(
                &s,
                &Record {
                    key: Some(Bytes::from(format!("k-{subject}"))),
                    value: Bytes::from(payload.to_string()),
                    subject: subject.to_string(),
                    headers: vec![("h".into(), "v".into())],
                    timestamp_ns: None,
                },
            )
            .await
            .unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let resolver = Resolver {
        storage: storage.clone(),
        tables: Arc::new(TableRegistry::new()),
        external: Arc::new(ExternalTables::new(
            Arc::new(ConnectionRegistry::new(dir.path().to_path_buf())),
            ExternalConfig::default(),
        )),
        allow_external: true,
    };
    (storage, resolver, dir)
}

async fn run(resolver: &Resolver, sql: &str) -> Result<QueryResult, crate::error::ExqlError> {
    let cfg = ExqlConfig::default();
    let state = build_state(&cfg, runtime_env(&cfg).unwrap(), resolver.clone()).unwrap();
    execute(state, sql, &cfg).await
}

fn amounts() -> Vec<(&'static str, &'static str)> {
    vec![
        ("o.eu", r#"{"amount": 100, "region": "eu"}"#),
        ("o.us", r#"{"amount": 300, "region": "us"}"#),
        ("o.eu", r#"{"amount": 25.5, "region": "eu"}"#),
        ("o.us", r#"{"amount": "1000", "region": "us"}"#),
        ("o.eu", r#"{"region": "eu"}"#),
    ]
}

#[tokio::test]
async fn select_star_columns() {
    let (_s, r, _d) = setup(&amounts()).await;
    let res = run(&r, "SELECT * FROM orders").await.unwrap();
    assert_eq!(
        res.columns,
        vec![
            "offset",
            "timestamp",
            "subject",
            "key",
            "payload",
            "headers"
        ]
    );
    assert_eq!(res.row_count, 5);
    assert_eq!(res.rows[0][4], json!({"amount": 100, "region": "eu"}));
    assert_eq!(res.rows[0][5], json!({"h": "v"}));
    assert!(!res.truncated);
}

#[tokio::test]
async fn json_numeric_compare_order_and_sum() {
    let (_s, r, _d) = setup(&amounts()).await;
    let res = run(
        &r,
        "SELECT offset FROM orders WHERE payload->>'amount' > 250 ORDER BY offset",
    )
    .await
    .unwrap();
    assert_eq!(res.rows, vec![vec![json!(1)], vec![json!(3)]]);

    let res = run(
        &r,
        "SELECT payload->>'amount' AS a FROM orders WHERE payload->>'amount' IS NOT NULL ORDER BY payload->>'amount' DESC",
    )
    .await
    .unwrap();
    let got: Vec<_> = res.rows.iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        got,
        vec![json!("1000"), json!("300"), json!("100"), json!("25.5")]
    );

    let res = run(
        &r,
        "SELECT payload->>'region' AS region, SUM(payload->>'amount') AS total, COUNT(*) AS n \
         FROM orders GROUP BY payload->>'region' ORDER BY region",
    )
    .await
    .unwrap();
    assert_eq!(
        res.rows,
        vec![
            vec![json!("eu"), json!(125.5), json!(3)],
            vec![json!("us"), json!(1300.0), json!(2)]
        ]
    );

    // ORDER BY an alias of JSON text sorts numerically too.
    let res = run(
        &r,
        "SELECT payload->>'amount' AS a FROM orders WHERE payload->>'amount' IS NOT NULL ORDER BY a",
    )
    .await
    .unwrap();
    let got: Vec<_> = res.rows.iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        got,
        vec![json!("25.5"), json!("100"), json!("300"), json!("1000")]
    );
}

#[tokio::test]
async fn errors_for_unknown_things() {
    let (_s, r, _d) = setup(&amounts()).await;
    let e = run(&r, "SELECT * FROM nope").await.unwrap_err();
    assert_eq!(e.code(), "PLAN_ERROR", "{e}");
    let e = run(&r, "SELECT nope FROM orders").await.unwrap_err();
    assert_eq!(e.code(), "PLAN_ERROR", "{e}");
    let e = run(&r, "SELECT no_such_fn(1) FROM orders")
        .await
        .unwrap_err();
    assert!(matches!(e.code(), "PLAN_ERROR" | "EXECUTION_ERROR"), "{e}");
    let e = run(&r, "SELEC 1").await.unwrap_err();
    assert_eq!(e.code(), "PARSE_ERROR", "{e}");
    let e = run(&r, "CREATE TABLE x (a INT)").await.unwrap_err();
    assert!(!e.to_string().is_empty());
}

#[tokio::test]
async fn tail_read_and_pushdown() {
    let (_s, r, _d) = setup(&amounts()).await;
    let res = run(&r, "SELECT offset FROM orders ORDER BY offset DESC LIMIT 2")
        .await
        .unwrap();
    assert_eq!(res.rows, vec![vec![json!(4)], vec![json!(3)]]);
    let plan = run(
        &r,
        "EXPLAIN SELECT offset FROM orders ORDER BY offset DESC LIMIT 2",
    )
    .await
    .unwrap();
    let text = serde_json::to_string(&plan.rows).unwrap();
    assert!(text.contains("reverse=true"), "{text}");

    let res = run(
        &r,
        "SELECT offset FROM orders WHERE subject = 'o.eu' ORDER BY offset DESC LIMIT 2",
    )
    .await
    .unwrap();
    assert_eq!(res.rows, vec![vec![json!(4)], vec![json!(2)]]);
    let plan = run(
        &r,
        "EXPLAIN SELECT offset FROM orders WHERE subject = 'o.eu' ORDER BY offset DESC LIMIT 2",
    )
    .await
    .unwrap();
    let text = serde_json::to_string(&plan.rows).unwrap();
    assert!(text.contains("reverse=true"), "{text}");

    let res = run(
        &r,
        "SELECT offset FROM orders WHERE offset >= 2 AND offset < 4",
    )
    .await
    .unwrap();
    assert_eq!(res.rows, vec![vec![json!(2)], vec![json!(3)]]);
    let plan = run(
        &r,
        "EXPLAIN SELECT offset FROM orders WHERE offset >= 2 AND offset < 4",
    )
    .await
    .unwrap();
    let text = serde_json::to_string(&plan.rows).unwrap();
    assert!(text.contains("offsets=[2, 4)"), "{text}");
}

#[tokio::test]
async fn time_filters() {
    let (_s, r, _d) = setup(&amounts()).await;
    let res = run(
        &r,
        "SELECT COUNT(*) AS n FROM orders WHERE timestamp > now() - INTERVAL '1 hour'",
    )
    .await
    .unwrap();
    assert_eq!(res.rows, vec![vec![json!(5)]]);
    let res = run(
        &r,
        "SELECT COUNT(*) AS n FROM orders WHERE timestamp < now() - INTERVAL '1 hour'",
    )
    .await
    .unwrap();
    assert_eq!(res.rows, vec![vec![json!(0)]]);
}

#[tokio::test]
async fn full_sql_features() {
    let (_s, r, _d) = setup(&amounts()).await;
    // HAVING, DISTINCT, window functions, CTE, subquery.
    let res = run(
        &r,
        "WITH t AS (SELECT payload->>'region' AS region, payload->>'amount' AS amount FROM orders) \
         SELECT region, COUNT(*) AS n FROM t GROUP BY region HAVING COUNT(*) > 2",
    )
    .await
    .unwrap();
    assert_eq!(res.rows, vec![vec![json!("eu"), json!(3)]]);
    let res = run(&r, "SELECT DISTINCT subject FROM orders ORDER BY subject")
        .await
        .unwrap();
    assert_eq!(res.rows, vec![vec![json!("o.eu")], vec![json!("o.us")]]);
    let res = run(
        &r,
        "SELECT offset, ROW_NUMBER() OVER (PARTITION BY subject ORDER BY offset) AS rn FROM orders ORDER BY offset",
    )
    .await
    .unwrap();
    let rn: Vec<_> = res.rows.iter().map(|r| r[1].clone()).collect();
    assert_eq!(rn, vec![json!(1), json!(1), json!(2), json!(2), json!(3)]);
    let res = run(
        &r,
        "SELECT offset FROM orders WHERE payload->>'amount' > (SELECT AVG(payload->>'amount') FROM orders) ORDER BY offset",
    )
    .await
    .unwrap();
    assert_eq!(res.rows, vec![vec![json!(3)]]);
    let res = run(
        &r,
        "SELECT a.offset, b.offset FROM orders a JOIN orders b ON a.subject = b.subject AND a.offset < b.offset ORDER BY 1, 2",
    )
    .await
    .unwrap();
    assert_eq!(res.row_count, 4);
    let res = run(
        &r,
        "SELECT subject_part(subject, 2) AS p FROM orders LIMIT 1",
    )
    .await
    .unwrap();
    assert_eq!(res.rows, vec![vec![json!("eu")]]);
}

#[tokio::test]
async fn truncation_is_reported() {
    let (_s, r, _d) = setup(&amounts()).await;
    let cfg = ExqlConfig {
        max_result_rows: 2,
        ..ExqlConfig::default()
    };
    let state = build_state(&cfg, runtime_env(&cfg).unwrap(), r.clone()).unwrap();
    let res = execute(state, "SELECT * FROM orders", &cfg).await.unwrap();
    assert_eq!(res.row_count, 2);
    assert!(res.truncated);
}

/// Joins with a registered Postgres connection. Runs when
/// `EXSPEED_POSTGRES_URL` is set (e.g. `postgres://testuser:testpass@127.0.0.1:5432/testdb`).
#[tokio::test]
async fn external_postgres_join() {
    let Ok(url) = std::env::var("EXSPEED_POSTGRES_URL") else {
        eprintln!("EXSPEED_POSTGRES_URL not set; skipping");
        return;
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query("DROP TABLE IF EXISTS exql_regions")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE exql_regions (code TEXT PRIMARY KEY, name TEXT, vat DOUBLE PRECISION, \"we\"\"ird\" INT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO exql_regions VALUES ('eu', 'Europe', 0.5, 1), ('us', 'United States', 0.0, 2)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let (_s, r, _d) = setup(&amounts()).await;
    r.external
        .registry()
        .add(crate::external::ConnectionConfig {
            name: "wh".into(),
            driver: "postgres".into(),
            url: url.clone(),
        })
        .unwrap();
    let res = run(
        &r,
        "SELECT o.payload->>'region' AS region, w.name, SUM(o.payload->>'amount' * (1 + w.vat)) AS gross \
         FROM orders o JOIN wh.exql_regions w ON o.payload->>'region' = w.code \
         GROUP BY o.payload->>'region', w.name ORDER BY region",
    )
    .await
    .unwrap();
    assert_eq!(
        res.rows,
        vec![
            vec![json!("eu"), json!("Europe"), json!(188.25)],
            vec![json!("us"), json!("United States"), json!(1300.0)]
        ]
    );
    // Quoted identifiers survive; unknown tables are errors.
    let res = run(
        &r,
        "SELECT \"we\"\"ird\" FROM wh.public.exql_regions ORDER BY 1",
    )
    .await
    .unwrap();
    assert_eq!(res.rows, vec![vec![json!(1)], vec![json!(2)]]);
    let e = run(&r, "SELECT * FROM wh.nope").await.unwrap_err();
    assert_eq!(e.code(), "PLAN_ERROR", "{e}");
    let e = run(
        &r,
        "SELECT * FROM wh.\"exql_regions\"\"; DROP TABLE x; --\"",
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), "PLAN_ERROR", "{e}");
    sqlx::query("DROP TABLE exql_regions")
        .execute(&pool)
        .await
        .unwrap();
}

async fn many(n: usize) -> (Arc<dyn StorageEngine>, Resolver, tempfile::TempDir) {
    let recs: Vec<(String, String)> = (0..n)
        .map(|i| {
            (
                format!("s.{}", i % 7),
                format!(r#"{{"i": {i}, "pad": "{}"}}"#, "x".repeat(64)),
            )
        })
        .collect();
    let refs: Vec<(&str, &str)> = recs.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
    setup(&refs).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_and_memory_limit() {
    let (_s, r, _d) = many(1000).await;
    let cfg = ExqlConfig {
        query_timeout: std::time::Duration::from_millis(1),
        ..ExqlConfig::default()
    };
    let state = build_state(&cfg, runtime_env(&cfg).unwrap(), r.clone()).unwrap();
    let e = execute(
        state,
        "SELECT COUNT(*) FROM orders a CROSS JOIN orders b CROSS JOIN orders c",
        &cfg,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), "TIMEOUT", "{e}");

    let cfg = ExqlConfig {
        memory_limit_bytes: 256 * 1024,
        ..ExqlConfig::default()
    };
    let state = build_state(&cfg, runtime_env(&cfg).unwrap(), r.clone()).unwrap();
    let e = execute(
        state,
        "SELECT payload, offset FROM orders ORDER BY payload->>'pad', offset DESC",
        &cfg,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), "RESOURCES_EXHAUSTED", "{e}");
}
