//! Continuous-query tests: results, event time, joins, tables, recovery
//! and lifecycle, all asserting concrete output values.

use std::collections::HashSet;
use std::time::Duration;

use serde_json::{json, Value as Json};

use crate::continuous::runner::H_QUERY;
use crate::test_util::{eventually, test_config, Node, World};

const T0: i64 = 1_700_000_000_000; // a multiple of 10 s

fn sorted(mut v: Vec<Json>) -> Vec<Json> {
    v.sort_by_key(|a| a.to_string());
    v
}

fn qid(created: &Json) -> String {
    created["query_id"].as_str().unwrap().to_string()
}

async fn clicks(w: &World) {
    w.stream("clicks").await;
    for (u, dt, amount) in [
        ("a", 1_000, 5),
        ("b", 2_000, 7),
        ("a", 3_000, 1),
        ("a", 12_000, 2),
        ("b", 15_000, 4),
        ("b", 21_000, 10),
        ("a", 35_000, 1),
    ] {
        w.publish(
            "clicks",
            None,
            "c",
            json!({"user": u, "ts": T0 + dt, "amount": amount}),
        )
        .await;
    }
}

#[tokio::test]
async fn tumbling_emit_final_once_per_window_on_replay() {
    let w = World::new().await;
    clicks(&w).await;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(&w, dir.path(), test_config()).await;
    let q = node
        .sql(
            "CREATE STREAM w AS SELECT payload->>'user' AS usr, window_start, window_end, \
             COUNT(*) AS n, SUM(payload->>'amount') AS total, MAX(payload->>'amount') AS mx \
             FROM clicks TIMESTAMP BY payload->>'ts' \
             WINDOW TUMBLING (SIZE 10 SECONDS) GROUP BY payload->>'user' EMIT FINAL",
        )
        .await;
    let id = qid(&q);
    node.wait_input(&id, 7).await;
    let ts = |d: i64| crate::convert::format_ts_millis(T0 + d);
    let row = |u: &str, s: i64, n: i64, total: f64, mx: f64| json!({"usr": u, "window_start": ts(s), "window_end": ts(s + 10_000), "n": n, "total": total, "mx": mx});
    let expected = vec![
        row("a", 0, 2, 6.0, 5.0),
        row("b", 0, 1, 7.0, 7.0),
        row("a", 10_000, 1, 2.0, 2.0),
        row("b", 10_000, 1, 4.0, 4.0),
        row("b", 20_000, 1, 10.0, 10.0),
    ];
    assert_eq!(w.payloads("w").await, expected);

    // A record for a closed window is late: dropped and counted.
    w.publish(
        "clicks",
        None,
        "c",
        json!({"user": "a", "ts": T0 + 5_000, "amount": 100}),
    )
    .await;
    node.wait_input(&id, 8).await;
    assert_eq!(w.payloads("w").await, expected);
    let info = node.engine.get_query(&id).unwrap();
    assert_eq!(info.stats["late_records_dropped"], 1);

    // Output records carry the group key and query id.
    let recs = w.read_all("w").await;
    assert_eq!(recs[0].key.as_deref(), Some(&b"a"[..]));
    assert!(recs[0]
        .headers
        .iter()
        .any(|(k, v)| k == H_QUERY && v == &id));
    node.stop().await;
}

#[tokio::test]
async fn hopping_window_table() {
    let w = World::new().await;
    clicks(&w).await;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(&w, dir.path(), test_config()).await;
    let q = node
        .sql(
            "CREATE TABLE h AS SELECT window_start, COUNT(*) AS n FROM clicks \
             TIMESTAMP BY payload->>'ts' WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 5 SECONDS)",
        )
        .await;
    node.wait_input(&qid(&q), 7).await;
    let res = node.sql("SELECT n FROM h ORDER BY window_start").await;
    let ns: Vec<i64> = res["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r[0].as_i64().unwrap())
        .collect();
    // windows starting -5, 0, 5, 10, 15, 20, 30, 35 (s)
    assert_eq!(ns, vec![3, 3, 1, 2, 2, 1, 1, 1]);
    node.stop().await;
}

#[tokio::test]
async fn tables_group_by_global_and_having() {
    let w = World::new().await;
    w.stream("orders").await;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(&w, dir.path(), test_config()).await;
    let by_region = qid(&node
        .sql(
            "CREATE TABLE by_region AS SELECT payload->>'region' AS region, COUNT(*) AS n, \
             SUM(payload->>'amount') AS total FROM orders GROUP BY payload->>'region'",
        )
        .await);
    let totals = qid(&node
        .sql("CREATE MATERIALIZED VIEW totals AS SELECT COUNT(*) AS n, AVG(payload->>'amount') AS avg FROM orders")
        .await);
    let big = qid(&node
        .sql(
            "CREATE TABLE big AS SELECT payload->>'region' AS region, COUNT(*) AS n FROM orders \
             GROUP BY payload->>'region' HAVING COUNT(*) >= 2",
        )
        .await);
    let changes = qid(&node
        .sql(
            "CREATE STREAM region_changes AS SELECT payload->>'region' AS region, COUNT(*) AS n \
             FROM orders GROUP BY payload->>'region' EMIT CHANGES",
        )
        .await);
    // A global aggregate has its one row before any input.
    eventually("global row", || async {
        node.engine
            .table_rows("totals")
            .map(|v| v["row_count"] == 1)
            .unwrap_or(false)
    })
    .await;
    assert_eq!(
        node.engine.table_rows("totals").unwrap()["rows"],
        json!([[0, null]])
    );

    for (r, a) in [("eu", 100), ("us", 300), ("eu", 50)] {
        w.publish("orders", None, "o", json!({"region": r, "amount": a}))
            .await;
    }
    for id in [&by_region, &totals, &big, &changes] {
        node.wait_input(id, 3).await;
    }
    let res = node
        .sql("SELECT region, n, total FROM by_region ORDER BY region")
        .await;
    assert_eq!(res["rows"], json!([["eu", 2, 150.0], ["us", 1, 300.0]]));
    assert_eq!(
        node.engine.table_rows("totals").unwrap()["rows"],
        json!([[3, 150.0]])
    );
    assert_eq!(
        node.engine.table_rows("big").unwrap()["rows"],
        json!([["eu", 2]])
    );
    assert_eq!(
        node.engine.table_row("by_region", "us").unwrap(),
        json!({"columns": ["region", "n", "total"], "row": ["us", 1, 300.0]})
    );
    assert!(node.engine.table_row("by_region", "xx").is_err());
    // One micro-batch (3 records) → one update per changed key.
    assert_eq!(
        sorted(w.payloads("region_changes").await),
        sorted(vec![
            json!({"region": "eu", "n": 2}),
            json!({"region": "us", "n": 1})
        ])
    );
    let keys: Vec<_> = w
        .read_all("region_changes")
        .await
        .iter()
        .map(|r| String::from_utf8(r.key.clone().unwrap().to_vec()).unwrap())
        .collect();
    assert_eq!(
        sorted(keys.iter().map(|k| json!(k)).collect()),
        vec![json!("eu"), json!("us")]
    );
    // The changelog stream backs the table.
    assert_eq!(w.read_all("by_region").await.len(), 2);
    node.stop().await;

    // Tables survive a restart (restored from checkpoint + changelog).
    let node = Node::start(&w, dir.path(), test_config()).await;
    let res = node
        .sql("SELECT region, n, total FROM by_region ORDER BY region")
        .await;
    assert_eq!(res["rows"], json!([["eu", 2, 150.0], ["us", 1, 300.0]]));
    w.publish("orders", None, "o", json!({"region": "us", "amount": 1}))
        .await;
    node.wait_input(&by_region, 4).await;
    let res = node
        .sql("SELECT region, n FROM by_region ORDER BY region")
        .await;
    assert_eq!(res["rows"], json!([["eu", 2], ["us", 2]]));
    node.wait_input(&big, 4).await;
    assert_eq!(
        node.engine.table_rows("big").unwrap()["rows"],
        json!([["eu", 2], ["us", 2]])
    );
    node.stop().await;
}

async fn join_data(w: &World) {
    w.stream("orders").await;
    w.stream("payments").await;
    for (id, dt) in [("o1", 1_000), ("o2", 2_000), ("o3", 3_000)] {
        w.publish("orders", Some("k"), "o", json!({"id": id, "ts": T0 + dt}))
            .await;
    }
    // Out of order: p(o1) is older than p(o2) but arrives later.
    for (oid, dt, amt) in [("o2", 4_000, 20), ("o1", 0, 10), ("o3", 20_000, 30)] {
        w.publish(
            "payments",
            Some("k"),
            "p",
            json!({"order_id": oid, "ts": T0 + dt, "amt": amt}),
        )
        .await;
    }
    w.publish(
        "orders",
        Some("k"),
        "o",
        json!({"id": "o4", "ts": T0 + 30_000}),
    )
    .await;
    w.publish(
        "payments",
        Some("k"),
        "p",
        json!({"order_id": "x", "ts": T0 + 31_000, "amt": 1}),
    )
    .await;
}

#[tokio::test]
async fn stream_stream_inner_and_left_joins() {
    let w = World::new().await;
    join_data(&w).await;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(&w, dir.path(), test_config()).await;
    let inner = qid(&node
        .sql(
            "CREATE STREAM paid AS SELECT o.payload->>'id' AS oid, p.payload->>'amt' AS amt \
             FROM orders o TIMESTAMP BY o.payload->>'ts' \
             JOIN payments p TIMESTAMP BY p.payload->>'ts' WITHIN 5 SECONDS \
             ON o.payload->>'id' = p.payload->>'order_id' AND o.key = p.key AND p.payload->>'amt' > 0 \
             GRACE PERIOD 10 SECONDS",
        )
        .await);
    let left = qid(&node
        .sql(
            "CREATE STREAM unpaid AS SELECT o.payload->>'id' AS oid, p.payload->>'amt' AS amt \
             FROM orders o TIMESTAMP BY o.payload->>'ts' \
             LEFT JOIN payments p TIMESTAMP BY p.payload->>'ts' WITHIN 5 SECONDS \
             ON p.payload->>'order_id' = o.payload->>'id' \
             GRACE PERIOD 10 SECONDS",
        )
        .await);
    node.wait_input(&inner, 8).await;
    node.wait_input(&left, 8).await;
    assert_eq!(
        sorted(w.payloads("paid").await),
        vec![
            json!({"oid": "o1", "amt": "10"}),
            json!({"oid": "o2", "amt": "20"})
        ]
    );
    // o3's payment is 17 s away (> WITHIN); it is emitted unmatched once the
    // watermark (min(30, 31) - 10 = 20 s) passes 3 + 5 s. o4 is still open.
    assert_eq!(
        sorted(w.payloads("unpaid").await),
        vec![
            json!({"oid": "o1", "amt": "10"}),
            json!({"oid": "o2", "amt": "20"}),
            json!({"oid": "o3", "amt": null}),
        ]
    );
    // A payment older than the watermark is late.
    w.publish(
        "payments",
        Some("k"),
        "p",
        json!({"order_id": "o4", "ts": T0 + 2_000, "amt": 5}),
    )
    .await;
    node.wait_input(&inner, 9).await;
    assert_eq!(w.payloads("paid").await.len(), 2);
    assert_eq!(
        node.engine.get_query(&inner).unwrap().stats["late_records_dropped"],
        1
    );
    // Advancing both sides past o4 + WITHIN emits o4 unmatched.
    w.publish(
        "orders",
        Some("k"),
        "o",
        json!({"id": "o5", "ts": T0 + 60_000}),
    )
    .await;
    w.publish(
        "payments",
        Some("k"),
        "p",
        json!({"order_id": "y", "ts": T0 + 60_000, "amt": 1}),
    )
    .await;
    node.wait_input(&left, 11).await;
    let un = w.payloads("unpaid").await;
    assert_eq!(un.last().unwrap(), &json!({"oid": "o4", "amt": null}));
    assert_eq!(un.len(), 4);
    node.stop().await;
}

#[tokio::test]
async fn stream_table_join_sees_live_updates() {
    let w = World::new().await;
    w.stream("regions").await;
    w.stream("orders").await;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(&w, dir.path(), test_config()).await;
    let rn = qid(&node
        .sql(
            "CREATE TABLE rn AS SELECT payload->>'id' AS id, LAST_VALUE(payload->>'name') AS name \
             FROM regions GROUP BY payload->>'id'",
        )
        .await);
    let en = qid(&node
        .sql(
            "CREATE STREAM enriched AS SELECT o.payload->>'oid' AS oid, r.name AS region_name \
             FROM orders o LEFT JOIN rn r ON o.payload->>'region' = r.id",
        )
        .await);
    w.publish("regions", None, "r", json!({"id": "eu", "name": "Europe"}))
        .await;
    node.wait_input(&rn, 1).await;
    w.publish("orders", None, "o", json!({"oid": "o1", "region": "eu"}))
        .await;
    node.wait_input(&en, 1).await;
    w.publish("regions", None, "r", json!({"id": "eu", "name": "EU"}))
        .await;
    node.wait_input(&rn, 2).await;
    w.publish("orders", None, "o", json!({"oid": "o2", "region": "eu"}))
        .await;
    w.publish("orders", None, "o", json!({"oid": "o3", "region": "xx"}))
        .await;
    node.wait_input(&en, 3).await;
    assert_eq!(
        w.payloads("enriched").await,
        vec![
            json!({"oid": "o1", "region_name": "Europe"}),
            json!({"oid": "o2", "region_name": "EU"}),
            json!({"oid": "o3", "region_name": null}),
        ]
    );
    node.stop().await;
}

const RECOVERY_QUERIES: &[&str] = &[
    "CREATE STREAM c AS SELECT payload->>'k' AS k, window_start, COUNT(*) AS n, SUM(payload->>'v') AS s \
     FROM src TIMESTAMP BY payload->>'ts' WINDOW TUMBLING (SIZE 10 SECONDS) GROUP BY payload->>'k'",
    "CREATE STREAM f AS SELECT payload->>'k' AS k, window_start, COUNT(*) AS n \
     FROM src TIMESTAMP BY payload->>'ts' WINDOW TUMBLING (SIZE 10 SECONDS) GROUP BY payload->>'k' EMIT FINAL",
    "CREATE TABLE t AS SELECT payload->>'k' AS k, COUNT(*) AS n, SUM(payload->>'v') AS s FROM src GROUP BY payload->>'k'",
    "CREATE STREAM j AS SELECT a.payload->>'v' AS av, b.payload->>'v' AS bv FROM src a TIMESTAMP BY a.payload->>'ts' \
     JOIN src b TIMESTAMP BY b.payload->>'ts' WITHIN 6 SECONDS ON a.payload->>'k' = b.payload->>'k' AND a.offset < b.offset",
    "CREATE STREAM lj AS SELECT a.payload->>'v' AS av, b.payload->>'v' AS bv FROM src a TIMESTAMP BY a.payload->>'ts' \
     LEFT JOIN src b TIMESTAMP BY b.payload->>'ts' WITHIN 1 SECONDS ON a.payload->>'k' = b.payload->>'k' AND a.offset < b.offset",
];
const OUTPUTS: &[&str] = &["c", "f", "t", "j", "lj"];

async fn publish_src(w: &World, from: i64, to: i64) {
    for i in from..to {
        let k = ["x", "y", "z"][(i % 3) as usize];
        // Mostly increasing event time with some disorder.
        let dt = i * 1_700 - if i % 4 == 0 { 900 } else { 0 };
        w.publish("src", None, "s", json!({"k": k, "v": i, "ts": T0 + dt}))
            .await;
    }
}

/// Run the recovery queries over 20 records, stop the node (crash or
/// graceful), publish 10 more and restart with a larger micro-batch.
/// The crashed run must replay its last micro-batch with the original
/// boundaries to match the graceful one.
async fn recovery_run(crash: bool) -> (Vec<Vec<Json>>, Json, Vec<Vec<String>>) {
    let w = World::new().await;
    w.stream("src").await;
    publish_src(&w, 0, 20).await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = |batch: usize| crate::session::ExqlConfig {
        micro_batch_records: batch,
        checkpoint_every_batches: 3,
        ..test_config()
    };
    let node = Node::start(&w, dir.path(), cfg(2)).await;
    let mut ids = vec![];
    for q in RECOVERY_QUERIES {
        ids.push(qid(&node.sql(q).await));
    }
    for id in &ids {
        node.wait_input(id, 20).await;
    }
    if crash {
        node.crash().await;
    } else {
        node.stop().await;
    }
    publish_src(&w, 20, 30).await;
    let node = Node::start(&w, dir.path(), cfg(10)).await;
    for id in &ids {
        node.wait_input(id, 30).await;
    }
    let mut outs = vec![];
    let mut keys = vec![];
    for o in OUTPUTS {
        outs.push(w.payloads(o).await);
        keys.push(
            w.read_all(o)
                .await
                .iter()
                .flat_map(|r| {
                    r.headers
                        .iter()
                        .filter(|(k, _)| k == "x-idempotency-key")
                        .map(|(_, v)| v.clone())
                })
                .collect(),
        );
    }
    let table = node.sql("SELECT k, n, s FROM t ORDER BY k").await["rows"].clone();
    node.stop().await;
    (outs, table, keys)
}

#[tokio::test]
async fn crash_recovery_gives_identical_output_without_duplicates() {
    let (reference, ref_table, _) = recovery_run(false).await;
    let (recovered, table, keys) = recovery_run(true).await;
    for (i, name) in OUTPUTS.iter().enumerate() {
        assert!(!reference[i].is_empty(), "{name} produced no output");
        assert_eq!(
            recovered[i], reference[i],
            "output '{name}' differs after a crash"
        );
        let unique: HashSet<&String> = keys[i].iter().collect();
        assert_eq!(unique.len(), keys[i].len(), "duplicate records in '{name}'");
    }
    assert_eq!(table, ref_table);
    assert_eq!(
        table,
        json!([["x", 10, 135.0], ["y", 10, 145.0], ["z", 10, 155.0]])
    );
}

#[tokio::test]
async fn pause_resume_and_stopped_queries_stay_stopped() {
    let w = World::new().await;
    w.stream("ev").await;
    w.stream("tmp").await;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(&w, dir.path(), test_config()).await;
    let id = qid(&node
        .sql("CREATE STREAM ev2 AS SELECT payload->>'n' AS n FROM ev WHERE payload->>'n' >= 0")
        .await);
    for n in 0..3 {
        w.publish("ev", Some("k"), "e", json!({"n": n})).await;
    }
    node.wait_input(&id, 3).await;
    let p = node.sql(&format!("PAUSE QUERY {id}")).await;
    assert_eq!(p["status"], "paused");
    for n in 3..6 {
        w.publish("ev", Some("k"), "e", json!({"n": n})).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(w.payloads("ev2").await.len(), 3);

    // A query that fails (its source disappears) is stopped with an error.
    let bad = qid(&node.sql("CREATE STREAM tmp2 AS SELECT * FROM tmp").await);
    w.log
        .delete_stream(&exspeed_common::StreamName::try_from("tmp").unwrap())
        .await
        .unwrap();
    eventually("query failure", || async {
        node.engine.get_query(&bad).unwrap().status == "failed"
    })
    .await;
    node.stop().await;

    // After a restart neither the paused nor the failed query runs.
    let node = Node::start(&w, dir.path(), test_config()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let q = node.engine.get_query(&id).unwrap();
    assert_eq!(
        (q.status.as_str(), q.desired_state),
        ("paused", crate::DesiredState::Paused)
    );
    let b = node.engine.get_query(&bad).unwrap();
    assert_eq!(b.status, "failed");
    assert_eq!(b.desired_state, crate::DesiredState::Stopped);
    assert!(b.error.unwrap().contains("not found"));
    assert_eq!(w.payloads("ev2").await.len(), 3);

    // Resume continues from the checkpoint, without duplicates.
    let r = node.sql(&format!("RESUME QUERY {id}")).await;
    assert_eq!(r["status"], "running");
    node.wait_input(&id, 6).await;
    let got: Vec<Json> = w.payloads("ev2").await;
    assert_eq!(
        got,
        (0..6)
            .map(|n| json!({"n": n.to_string()}))
            .collect::<Vec<_>>()
    );
    // Output records keep the source key and subject.
    let recs = w.read_all("ev2").await;
    assert_eq!(recs[0].subject, "e");
    assert_eq!(recs[0].key.as_deref(), Some(&b"k"[..]));

    // DROP STREAM removes the query and the stream.
    node.sql("DROP STREAM ev2").await;
    assert!(node.engine.get_query(&id).is_none());
    assert!(w.read_all("ev2").await.is_empty());
    node.stop().await;
}

#[tokio::test]
async fn invalid_queries_are_rejected_before_anything_is_persisted() {
    let w = World::new().await;
    w.stream("s").await;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(&w, dir.path(), test_config()).await;
    for (sql, code) in [
        ("CREATE STREAM o AS SELECT * FROM nope", "PLAN_ERROR"),
        ("CREATE STREAM o AS SELECT nope FROM s", "PLAN_ERROR"),
        (
            "CREATE STREAM o AS SELECT * FROM s ORDER BY offset",
            "UNSUPPORTED",
        ),
        (
            "CREATE STREAM o AS SELECT * FROM s a JOIN s b ON a.key = b.key",
            "PLAN_ERROR",
        ),
        ("CREATE TABLE o AS SELECT * FROM s", "PLAN_ERROR"),
        (
            "CREATE STREAM o AS SELECT COUNT(*) FROM s EMIT FINAL",
            "PLAN_ERROR",
        ),
        ("CREATE STREAM s AS SELECT * FROM s", "PLAN_ERROR"),
        ("CREATE STREAM ../x AS SELECT * FROM s", "PARSE_ERROR"),
        ("CREATE STREAM __x AS SELECT * FROM s", "PLAN_ERROR"),
        ("CREATE INDEX i ON s (key)", "UNSUPPORTED"),
    ] {
        let e = node.engine.execute(sql).await.unwrap_err();
        assert_eq!(e.code(), code, "{sql}: {e}");
    }
    assert!(node.engine.list_queries().is_empty());
    assert!(!dir.path().join("exql").exists());
    let catalog = exspeed_broker::catalog::CatalogStore::new(
        w.log.clone(),
        crate::engine::QUERIES_STREAM,
        "exql.query",
    );
    assert!(catalog.load().await.unwrap().is_empty());
    node.stop().await;
}

/// The query catalog lives in `__exql_queries`: another node on the same
/// log loads it, `load()` can be repeated (each leader tenure) and follows
/// creates, pauses and drops.
#[tokio::test]
async fn query_catalog_is_in_the_log_and_reloadable() {
    let w = World::new().await;
    clicks(&w).await;
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let a = Node::start(&w, dir_a.path(), test_config()).await;
    let keep = qid(&a
        .sql("CREATE STREAM kept AS SELECT payload->>'user' AS usr FROM clicks")
        .await);
    let gone = qid(&a
        .sql("CREATE STREAM gone AS SELECT payload->>'user' AS usr FROM clicks")
        .await);
    a.sql(
        "CREATE TABLE per_user AS SELECT payload->>'user' AS usr, COUNT(*) AS n \
         FROM clicks GROUP BY payload->>'user'",
    )
    .await;
    a.wait_input(&keep, 7).await;
    assert!(!dir_a.path().join("exql").exists(), "no node-local files");

    // A second engine on the same log (a promoted follower) sees the
    // catalog without any files of its own.
    let b = crate::engine::ExqlEngine::new(
        w.log.clone(),
        dir_b.path().to_path_buf(),
        w.leadership.clone(),
        w.metrics.clone(),
        test_config(),
    )
    .unwrap();
    b.load().await.unwrap();
    let ids: HashSet<String> = b.list_queries().into_iter().map(|q| q.id).collect();
    assert_eq!(ids.len(), 3);
    assert!(ids.contains(&keep) && ids.contains(&gone));
    assert_eq!(b.list_tables().len(), 1);

    a.engine.drop_query(&gone).await.unwrap();
    a.engine.pause_query(&keep).await.unwrap();
    b.load().await.unwrap();
    let qs = b.list_queries();
    assert_eq!(qs.len(), 2);
    assert!(qs.iter().all(|q| q.id != gone));
    assert_eq!(b.info(&keep).unwrap().status, "paused");
    // Repeating the load is idempotent.
    b.load().await.unwrap();
    assert_eq!(b.list_queries().len(), 2);
    assert_eq!(b.list_tables().len(), 1);
    a.stop().await;
}

#[tokio::test]
async fn legacy_query_files_are_imported_once() {
    let w = World::new().await;
    clicks(&w).await;
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("exql").join("queries");
    std::fs::create_dir_all(&legacy).unwrap();
    let def = json!({
        "id": "old_0000cafe",
        "sql": "CREATE STREAM old AS SELECT payload->>'user' AS usr FROM clicks",
        "kind": "stream",
        "name": "old",
        "desired": "running",
        "error": null,
        "created_at": "2026-01-01T00:00:00.000Z"
    });
    std::fs::write(legacy.join("old_0000cafe.json"), def.to_string()).unwrap();
    std::fs::write(legacy.join("junk.json"), "not json").unwrap();

    let node = Node::start(&w, dir.path(), test_config()).await;
    node.wait_input("old_0000cafe", 7).await;
    assert!(!legacy.exists());
    assert!(dir
        .path()
        .join("exql/queries.migrated/old_0000cafe.json")
        .exists());
    node.stop().await;

    // From the log on: a fresh engine with no files still has it.
    let other = tempfile::tempdir().unwrap();
    let node = Node::start(&w, other.path(), test_config()).await;
    let ids: Vec<String> = node
        .engine
        .list_queries()
        .into_iter()
        .map(|q| q.id)
        .collect();
    assert_eq!(ids, vec!["old_0000cafe".to_string()]);
    node.stop().await;
}

#[tokio::test]
async fn filter_coalesce_and_counts_in_continuous_aggregates() {
    let w = World::new().await;
    w.stream("ev").await;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(&w, dir.path(), test_config()).await;
    let win = qid(&node
        .sql(
            "CREATE STREAM wagg AS SELECT window_start, COUNT(*) AS n, COUNT(payload->>'v') AS nv, \
             COUNT(DISTINCT payload->>'u') AS du, COUNT(*) FILTER (WHERE payload->>'w' > 150) AS big \
             FROM ev TIMESTAMP BY payload->>'ts' WINDOW TUMBLING (SIZE 10 SECONDS) EMIT FINAL",
        )
        .await);
    let tbl = qid(&node
        .sql(
            "CREATE TABLE byu AS SELECT payload->>'u' AS u, COALESCE(SUM(payload->>'missing'), 0) AS z, \
             COUNT(*) FILTER (WHERE payload->>'w' > 150) AS big FROM ev GROUP BY payload->>'u'",
        )
        .await);
    let co = qid(&node
        .sql("CREATE STREAM c1 AS SELECT COALESCE(payload->>'v', 'none') AS v, NVL(payload->>'u', 'x') AS u FROM ev")
        .await);
    // Several micro-batches (one record each) exercise the FILTER path both
    // with and without nulls in the mask.
    for (u, v, wv, dt) in [
        ("a", Some(1), 100, 1_000),
        ("a", None, 200, 2_000),
        ("b", Some(5), 300, 3_000),
        ("b", Some(3), 400, 4_000),
        ("c", Some(9), 1, 25_000),
    ] {
        let mut p = json!({"u": u, "w": wv, "ts": T0 + dt});
        if let Some(v) = v {
            p["v"] = json!(v);
        }
        w.publish("ev", None, "e", p).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    for id in [&win, &tbl, &co] {
        node.wait_input(id, 5).await;
        let q = node.engine.get_query(id).unwrap();
        assert!(q.error.is_none(), "{id}: {:?}", q.error);
    }
    let out = w.payloads("wagg").await;
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["n"], json!(4));
    assert_eq!(out[0]["nv"], json!(3));
    assert_eq!(out[0]["du"], json!(2));
    assert_eq!(out[0]["big"], json!(3));
    let crate::engine::StatementResult::Rows(res) = node
        .engine
        .execute("SELECT u, z, big FROM byu ORDER BY u")
        .await
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(
        res.rows,
        vec![
            vec![json!("a"), json!(0.0), json!(1)],
            vec![json!("b"), json!(0.0), json!(2)],
            vec![json!("c"), json!(0.0), json!(0)],
        ]
    );
    let vs: Vec<Json> = w
        .payloads("c1")
        .await
        .iter()
        .map(|p| p["v"].clone())
        .collect();
    assert_eq!(
        vs,
        vec![
            json!("1"),
            json!("none"),
            json!("5"),
            json!("3"),
            json!("9")
        ]
    );
    node.stop().await;
}

#[tokio::test]
async fn extreme_event_times_neither_fail_nor_poison_the_watermark() {
    let w = World::new().await;
    w.stream("ev").await;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(&w, dir.path(), test_config()).await;
    let id = qid(&node
        .sql(
            "CREATE STREAM wx AS SELECT window_start, COUNT(*) AS n FROM ev TIMESTAMP BY payload->>'ts' \
             WINDOW TUMBLING (SIZE 10 SECONDS) EMIT FINAL",
        )
        .await);
    // Out-of-range event times fall back to the record timestamp (about
    // now); the valid ones are in the near future, inside the allowed skew,
    // so nothing is late.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let base = now - now.rem_euclid(10_000) + 20_000;
    w.publish("ev", None, "e", json!({"ts": "9223372036854775807"}))
        .await;
    w.publish("ev", None, "e", json!({"ts": 4_102_444_800_000i64}))
        .await; // year 2100
    w.publish("ev", None, "e", json!({"ts": -5})).await;
    w.publish("ev", None, "e", json!({"ts": base})).await;
    w.publish("ev", None, "e", json!({"ts": base + 1_000}))
        .await;
    w.publish("ev", None, "e", json!({"ts": base + 20_000}))
        .await;
    node.wait_input(&id, 6).await;
    let q = node.engine.get_query(&id).unwrap();
    assert!(q.error.is_none(), "{:?}", q.error);
    assert_eq!(q.stats["invalid_event_times"], json!(3), "{}", q.stats);
    assert_eq!(q.stats["late_records_dropped"], json!(0), "{}", q.stats);
    // The window at `base` closed with both of its records; the clamped
    // ones landed in the window around now.
    eventually("windows emitted", || async {
        let out = w.payloads("wx").await;
        out.iter().any(|p| p["n"] == json!(2)) && out.iter().any(|p| p["n"] == json!(3))
    })
    .await;
    node.stop().await;
}

#[tokio::test]
async fn continuous_queries_cannot_read_external_tables() {
    let w = World::new().await;
    w.stream("ev").await;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(&w, dir.path(), test_config()).await;
    // Never contacted: the plan is rejected before any fetch.
    node.engine
        .add_connection(crate::external::ConnectionConfig {
            name: "wh".into(),
            driver: "postgres".into(),
            url: "postgresql://nobody@127.0.0.1:1/none".into(),
        })
        .await
        .unwrap();
    let e = node
        .engine
        .execute(
            "CREATE STREAM joined AS SELECT e.key, c.name FROM ev e JOIN wh.customers c ON e.key = c.id",
        )
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("only available in bounded queries"),
        "{e}"
    );
    assert!(node.engine.list_queries().is_empty());
    node.stop().await;
}
