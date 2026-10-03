//! Differential tests: the same queries on ExQL (streams with JSON
//! payloads) and on SQLite (typed tables) over the same generated data.
//!
//! Queries are templates: `{x}` / `{a.x}` expand to `payload->>'x'` /
//! `a.payload->>'x'` for ExQL and to `x` / `a.x` for SQLite. ExQL's JSON
//! text must behave numerically where SQLite's REAL column does.

use std::sync::Arc;

use bytes::Bytes;
use exspeed_common::StreamName;
use exspeed_storage::memory::MemoryStorage;
use exspeed_streams::{Record, StorageEngine};
use serde_json::{json, Value as Json};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::{Column, Row, TypeInfo, ValueRef};

use crate::bounded::execute;
use crate::catalog::Resolver;
use crate::external::{ConnectionRegistry, ExternalConfig, ExternalTables};
use crate::session::{build_state, runtime_env, ExqlConfig};
use crate::tables::TableRegistry;

/// Deterministic pseudo-random numbers.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct T1 {
    id: i64,
    grp: Option<&'static str>,
    /// `None` = missing; `(value, true)` = encoded as a JSON string.
    amount: Option<(f64, bool)>,
    name: Option<String>,
    key: String,
}

struct T2 {
    id: i64,
    grp: &'static str,
    val: i64,
    key: String,
}

fn gen() -> (Vec<T1>, Vec<T2>) {
    let mut r = Lcg(42);
    let grps = ["eu", "us", "apac"];
    let names = ["alice", "bob", "carol", "dave", "erin", "frank"];
    let t1 = (0..60)
        .map(|id| T1 {
            id,
            grp: if r.below(10) == 0 { None } else { Some(grps[r.below(3) as usize]) },
            amount: match r.below(12) {
                0 => None,
                1 => Some((r.below(500) as f64, true)),
                2 => Some(((r.below(1000) as f64) / 4.0, false)),
                _ => Some((r.below(500) as f64, false)),
            },
            name: if r.below(8) == 0 { None } else { Some(names[r.below(6) as usize].to_string()) },
            key: format!("k{}", r.below(5)),
        })
        .collect();
    let t2 = (0..40)
        .map(|_| T2 {
            id: r.below(70) as i64,
            grp: grps[r.below(3) as usize],
            val: r.below(100) as i64,
            key: format!("k{}", r.below(5)),
        })
        .collect();
    (t1, t2)
}

async fn setup() -> (Resolver, SqlitePool, tempfile::TempDir) {
    let (t1, t2) = gen();
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query("CREATE TABLE t1 (id INTEGER, grp TEXT, amount REAL, name TEXT, k TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE t2 (id INTEGER, grp TEXT, val INTEGER, k TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    let s1 = StreamName::try_from("t1").unwrap();
    let s2 = StreamName::try_from("t2").unwrap();
    storage.create_stream(&s1, 0, 0).await.unwrap();
    storage.create_stream(&s2, 0, 0).await.unwrap();
    for r in &t1 {
        let mut p = serde_json::Map::new();
        p.insert("id".into(), json!(r.id));
        if let Some(g) = r.grp {
            p.insert("grp".into(), json!(g));
        }
        match r.amount {
            None => {}
            Some((a, true)) => {
                p.insert("amount".into(), json!(a.to_string()));
            }
            Some((a, false)) if a.fract() == 0.0 => {
                p.insert("amount".into(), json!(a as i64));
            }
            Some((a, false)) => {
                p.insert("amount".into(), json!(a));
            }
        }
        p.insert("name".into(), r.name.clone().map(Json::String).unwrap_or(Json::Null));
        storage
            .append(
                &s1,
                &Record {
                    key: Some(Bytes::from(r.key.clone())),
                    value: Bytes::from(Json::Object(p).to_string()),
                    subject: "t1".into(),
                    headers: vec![],
                    timestamp_ns: None,
                },
            )
            .await
            .unwrap();
        sqlx::query("INSERT INTO t1 VALUES (?, ?, ?, ?, ?)")
            .bind(r.id)
            .bind(r.grp)
            .bind(r.amount.map(|(a, _)| a))
            .bind(r.name.clone())
            .bind(r.key.clone())
            .execute(&pool)
            .await
            .unwrap();
    }
    for r in &t2 {
        storage
            .append(
                &s2,
                &Record {
                    key: Some(Bytes::from(r.key.clone())),
                    value: Bytes::from(json!({"id": r.id, "grp": r.grp, "val": r.val}).to_string()),
                    subject: "t2".into(),
                    headers: vec![],
                    timestamp_ns: None,
                },
            )
            .await
            .unwrap();
        sqlx::query("INSERT INTO t2 VALUES (?, ?, ?, ?)")
            .bind(r.id)
            .bind(r.grp)
            .bind(r.val)
            .bind(r.key.clone())
            .execute(&pool)
            .await
            .unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let resolver = Resolver {
        storage,
        tables: Arc::new(TableRegistry::new()),
        external: Arc::new(ExternalTables::new(
            Arc::new(ConnectionRegistry::new(dir.path().to_path_buf())),
            ExternalConfig::default(),
        )),
        allow_external: false,
    };
    (resolver, pool, dir)
}

/// Expand `{x}` / `{a.x}` placeholders.
fn expand(template: &str, exql: bool) -> String {
    let mut out = String::new();
    let mut rest = template;
    while let Some(i) = rest.find('{') {
        out.push_str(&rest[..i]);
        let j = rest[i..].find('}').unwrap() + i;
        let inner = &rest[i + 1..j];
        if exql {
            match inner.split_once('.') {
                Some((a, c)) if c == "k" => out.push_str(&format!("{a}.key")),
                Some((a, c)) => out.push_str(&format!("{a}.payload->>'{c}'")),
                None if inner == "k" => out.push_str("key"),
                None => out.push_str(&format!("payload->>'{inner}'")),
            }
        } else {
            out.push_str(inner);
        }
        rest = &rest[j + 1..];
    }
    out.push_str(rest);
    out
}

/// Normalize a cell: numbers (and numeric text) to a rounded decimal
/// string, booleans to 0/1.
fn norm(v: &Json) -> Json {
    match v {
        Json::Null => Json::Null,
        Json::Bool(b) => json!(format!("{:.6}", *b as i32 as f64)),
        Json::Number(n) => json!(format!("{:.6}", n.as_f64().unwrap())),
        Json::String(s) => match s.parse::<f64>() {
            Ok(f) => json!(format!("{f:.6}")),
            Err(_) => json!(s),
        },
        other => json!(other.to_string()),
    }
}

fn sqlite_rows(rows: &[SqliteRow]) -> Vec<Vec<Json>> {
    rows.iter()
        .map(|row| {
            (0..row.columns().len())
                .map(|i| {
                    let raw = row.try_get_raw(i).unwrap();
                    if raw.is_null() {
                        return Json::Null;
                    }
                    match raw.type_info().name() {
                        "INTEGER" | "BOOLEAN" => json!(row.get::<i64, _>(i)),
                        "REAL" | "NUMERIC" => json!(row.get::<f64, _>(i)),
                        _ => json!(row.get::<String, _>(i)),
                    }
                })
                .collect()
        })
        .collect()
}

async fn check(r: &Resolver, pool: &SqlitePool, template: &str, ordered: bool) {
    let exql_sql = expand(template, true);
    let lite_sql = expand(template, false);
    let cfg = ExqlConfig::default();
    let state = build_state(&cfg, runtime_env(&cfg).unwrap(), r.clone()).unwrap();
    let got = execute(state, &exql_sql, &cfg)
        .await
        .unwrap_or_else(|e| panic!("ExQL failed: {exql_sql}\n{e}"));
    let want = sqlx::query(&lite_sql)
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("SQLite failed: {lite_sql}\n{e}"));
    let mut a: Vec<Vec<Json>> = got.rows.iter().map(|r| r.iter().map(norm).collect()).collect();
    let mut b: Vec<Vec<Json>> = sqlite_rows(&want).iter().map(|r| r.iter().map(norm).collect()).collect();
    if !ordered {
        a.sort_by_key(|r| serde_json::to_string(r).unwrap());
        b.sort_by_key(|r| serde_json::to_string(r).unwrap());
    }
    assert!(!b.is_empty() || template.contains("empty"), "SQLite returned no rows: {lite_sql}");
    assert_eq!(a, b, "\nExQL:   {exql_sql}\nSQLite: {lite_sql}");
}

const ORDERED: &[&str] = &[
    // filters + JSON numeric comparison
    "SELECT {id} FROM t1 WHERE {amount} > 250 ORDER BY {id}",
    "SELECT {id}, {amount} FROM t1 WHERE {amount} BETWEEN 100 AND 200 AND {grp} IN ('eu', 'us') ORDER BY {amount} DESC, {id}",
    "SELECT {id} FROM t1 WHERE NOT ({amount} < 300) OR {name} LIKE 'a%' ORDER BY {id}",
    "SELECT {id} FROM t1 WHERE {amount} IS NULL ORDER BY {id}",
    "SELECT {id} FROM t1 WHERE {grp} IS NULL OR {name} IS NULL ORDER BY {id}",
    "SELECT {id}, {amount} * 2 + 1 FROM t1 WHERE {id} % 3 = 0 ORDER BY {id}",
    // ORDER BY + LIMIT, ORDER BY a non-projected column, NULL ordering
    "SELECT {id} FROM t1 ORDER BY {amount} DESC NULLS LAST, {id} LIMIT 7",
    "SELECT {name} FROM t1 WHERE {name} IS NOT NULL ORDER BY {amount} ASC NULLS FIRST, {id} LIMIT 10",
    "SELECT {id}, {amount} FROM t1 ORDER BY {amount} NULLS FIRST, {id} LIMIT 12",
    // GROUP BY with expressions, HAVING, aggregates over expressions
    "SELECT {grp}, COUNT(*), COUNT({amount}), SUM({amount}), MIN({amount}), MAX({amount}) FROM t1 GROUP BY {grp} ORDER BY {grp} NULLS FIRST",
    "SELECT {amount} > 200 AS big, COUNT(*) FROM t1 WHERE {amount} IS NOT NULL GROUP BY {amount} > 200 ORDER BY big",
    "SELECT substr({name}, 1, 1) AS initial, COUNT(*), AVG({amount}) FROM t1 WHERE {name} IS NOT NULL GROUP BY substr({name}, 1, 1) ORDER BY initial",
    "SELECT {grp}, SUM({amount} * 2) AS s2, AVG({amount} + 1) AS a1, MAX(length({name})) AS ml FROM t1 WHERE {grp} IS NOT NULL GROUP BY {grp} HAVING COUNT(*) > 5 ORDER BY {grp}",
    "SELECT {name}, COUNT(*) AS n FROM t1 GROUP BY {name} HAVING SUM({amount}) > 1000 ORDER BY n DESC, {name}",
    "SELECT COUNT(DISTINCT {grp}), COUNT(DISTINCT {name}), COUNT(*) FROM t1",
    "SELECT SUM({amount}), AVG({amount}), COUNT({amount}) FROM t1 WHERE {amount} > 100000",
    // DISTINCT
    "SELECT DISTINCT {grp} FROM t1 WHERE {grp} IS NOT NULL ORDER BY {grp}",
    "SELECT DISTINCT {grp}, {k} FROM t1 WHERE {grp} IS NOT NULL ORDER BY {grp}, {k}",
    // NULL semantics
    "SELECT COUNT(*) FROM t1 WHERE {amount} NOT IN (100, 200)",
    "SELECT COUNT(*) FROM t1 WHERE {amount} = NULL",
    "SELECT COUNT(*) FROM t1 WHERE {amount} <> 1 OR {amount} = 1",
    "SELECT {id}, COALESCE({name}, 'none') FROM t1 ORDER BY {id} LIMIT 15",
    "SELECT {id}, CASE WHEN {amount} > 250 THEN 'hi' WHEN {amount} IS NULL THEN 'none' ELSE 'lo' END FROM t1 ORDER BY {id}",
    // subqueries and CTEs
    "SELECT {id} FROM t1 WHERE {amount} > (SELECT AVG({amount}) FROM t1) ORDER BY {id}",
    "WITH g AS (SELECT {grp} AS grp, SUM({amount}) AS s FROM t1 GROUP BY {grp}) SELECT grp, s FROM g WHERE s > 1000 ORDER BY grp",
    "SELECT {id} FROM t1 WHERE {grp} IN (SELECT {grp} FROM t2 WHERE {val} > 90) ORDER BY {id}",
    // joins
    "SELECT {a.id}, {b.val} FROM t1 a JOIN t2 b ON {a.id} = {b.id} ORDER BY {a.id}, {b.val}",
    "SELECT {a.id}, {b.val} FROM t1 a JOIN t2 b ON {b.id} = {a.id} ORDER BY {a.id}, {b.val}",
    "SELECT {a.id}, {b.val} FROM t1 a JOIN t2 b ON {a.grp} = {b.grp} AND {a.k} = {b.k} AND {b.val} > {a.amount} / 10 ORDER BY {a.id}, {b.val}",
    "SELECT {a.id}, {b.val} FROM t1 a LEFT JOIN t2 b ON {a.id} = {b.id} ORDER BY {a.id}, {b.val} NULLS FIRST",
    "SELECT {b.id}, {a.name} FROM t2 b LEFT JOIN t1 a ON {b.id} = {a.id} AND {a.amount} > 100 ORDER BY {b.id}, {a.name} NULLS FIRST",
    "SELECT {a.grp}, COUNT(*), SUM({b.val}) FROM t1 a JOIN t2 b ON {a.k} = {b.k} AND {a.grp} = {b.grp} GROUP BY {a.grp} ORDER BY {a.grp}",
    "SELECT {a.id} FROM t1 a WHERE NOT EXISTS (SELECT 1 FROM t2 b WHERE {b.id} = {a.id}) ORDER BY {a.id}",
];

const UNORDERED: &[&str] = &[
    "SELECT {grp}, {k}, COUNT(*) FROM t1 GROUP BY {grp}, {k}",
    "SELECT {a.id}, {b.id} FROM t1 a JOIN t2 b ON {a.k} = {b.k} AND {a.grp} = {b.grp} AND {a.id} < {b.id}",
    "SELECT {a.id}, {b.val} FROM t1 a LEFT JOIN t2 b ON {a.id} = {b.id} AND {b.val} > 50",
];

#[tokio::test]
async fn exql_matches_sqlite() {
    let (r, pool, _dir) = setup().await;
    for q in ORDERED {
        check(&r, &pool, q, true).await;
    }
    for q in UNORDERED {
        check(&r, &pool, q, false).await;
    }
}

#[tokio::test]
async fn json_text_compares_and_sums_numerically() {
    // "100" > "25" as text is false; numerically it is true.
    let (r, pool, _dir) = setup().await;
    check(&r, &pool, "SELECT COUNT(*) FROM t1 WHERE {amount} > 25", true).await;
    check(&r, &pool, "SELECT MAX({amount}), MIN({amount}) FROM t1", true).await;
    check(&r, &pool, "SELECT {id} FROM t1 WHERE {amount} IS NOT NULL ORDER BY {amount}, {id} LIMIT 20", true).await;
}

