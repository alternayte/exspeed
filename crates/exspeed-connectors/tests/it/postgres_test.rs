//! `postgres_cdc`, `postgres_outbox` and `postgres_poll` against a real
//! Postgres with `wal_level = logical`.
//!
//! Set `EXSPEED_POSTGRES_URL` (the user needs REPLICATION and CREATE) and
//! run with `--include-ignored`. Without the variable the tests skip, except
//! under `CI=true`, where they fail so a misconfigured service job can't
//! pass silently.

use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use exspeed_connectors::config::ConnectorConfig;
use exspeed_connectors::offset_store::OffsetStore;
use exspeed_connectors::ConnectorType::Source;
use exspeed_connectors::Registry;
use exspeed_streams::StoredRecord;

use crate::common::*;

fn pg_url() -> Option<String> {
    service_env("EXSPEED_POSTGRES_URL")
}

macro_rules! require_pg {
    () => {
        match pg_url() {
            Some(u) => u,
            None => return,
        }
    };
}

async fn client(url: &str) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("connect to EXSPEED_POSTGRES_URL");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

/// Drop the slot (retrying while the walsender shuts down), the publication
/// and the tables.
async fn cleanup(url: &str, slot: Option<&str>, publication: Option<&str>, tables: &[&str]) {
    let c = client(url).await;
    if let Some(slot) = slot {
        for _ in 0..50 {
            let r = c
                .execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                     WHERE slot_name = $1",
                    &[&slot],
                )
                .await;
            if r.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    if let Some(p) = publication {
        let _ = c
            .batch_execute(&format!("DROP PUBLICATION IF EXISTS {p}"))
            .await;
    }
    for t in tables {
        let _ = c.batch_execute(&format!("DROP TABLE IF EXISTS {t}")).await;
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

fn cdc_config(
    name: &str,
    url: &str,
    tables: &[String],
    slot: &str,
    publication: &str,
) -> ConnectorConfig {
    let mut c = fast_config(name, Source, "postgres_cdc", name);
    c.settings = json!({
        "connection": url,
        "tables": tables,
        "slot_name": slot,
        "publication_name": publication,
    })
    .as_object()
    .unwrap()
    .clone();
    c
}

// ---------------------------------------------------------------------------
// postgres_cdc
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs Postgres with wal_level=logical (EXSPEED_POSTGRES_URL)"]
async fn cdc_insert_update_delete_envelope_and_keys() {
    let url = require_pg!();
    let name = unique("cdc");
    let t = format!("{name}_t");
    let t2 = format!("{name}_k2");
    let c = client(&url).await;
    c.batch_execute(&format!(
        "CREATE TABLE {t} (id int PRIMARY KEY, name text, amount numeric(10,2), \
         active boolean, doc jsonb, big text);
         ALTER TABLE {t} ALTER COLUMN big SET STORAGE EXTERNAL;
         CREATE TABLE {t2} (a int, b text, v int, PRIMARY KEY (a, b));"
    ))
    .await
    .unwrap();

    let env = Env::new();
    let slot = format!("{name}_slot");
    let publication = format!("{name}_pub");
    let cfg = cdc_config(
        &name,
        &url,
        &[format!("public.{t}"), format!("public.{t2}")],
        &slot,
        &publication,
    );
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let (h, state) = env.run(&Registry::builtin(), cfg, offsets);
    wait_running(&state).await;

    c.batch_execute(&format!(
        "INSERT INTO {t} VALUES (1, 'a', 12.50, true, '{{\"x\": [1, 2]}}', repeat('z', 10000));
         UPDATE {t} SET name = 'b' WHERE id = 1;
         DELETE FROM {t} WHERE id = 1;
         INSERT INTO {t2} VALUES (7, 'x', 1);"
    ))
    .await
    .unwrap();

    let recs = wait_records(&env, &name, 4).await;
    h.stop(Duration::from_secs(15)).await;
    cleanup(&url, Some(&slot), Some(&publication), &[&t, &t2]).await;

    assert_eq!(recs.len(), 4, "{recs:?}");
    let (ins, upd, del, comp) = (
        json_of(&recs[0]),
        json_of(&recs[1]),
        json_of(&recs[2]),
        json_of(&recs[3]),
    );

    // Insert: typed values, Debezium-like envelope, key = primary key.
    assert_eq!(recs[0].subject, format!("public.{t}.insert"));
    assert_eq!(recs[0].key.as_deref(), Some(&b"1"[..]));
    assert_eq!(ins["op"], "c");
    assert_eq!(ins["before"], Value::Null);
    assert_eq!(ins["after"]["id"], json!(1));
    assert_eq!(ins["after"]["name"], json!("a"));
    assert_eq!(ins["after"]["amount"], json!(12.5));
    assert_eq!(ins["after"]["active"], json!(true));
    assert_eq!(ins["after"]["doc"], json!({"x": [1, 2]}));
    assert_eq!(ins["source"]["table"], json!(t));
    assert_eq!(ins["source"]["schema"], json!("public"));
    assert!(ins["source"]["lsn"].as_str().unwrap().contains('/'));
    assert!(ins["source"]["txid"].as_u64().unwrap() > 0);
    assert!(header(&recs[0], "x-idempotency-key")
        .unwrap()
        .starts_with(&format!("pgcdc:{slot}:")));

    // Update: the TOASTed column is unchanged → omitted and listed.
    assert_eq!(upd["op"], "u");
    assert_eq!(upd["after"]["name"], json!("b"));
    assert!(upd["after"].get("big").is_none(), "{upd}");
    assert_eq!(upd["__unchanged"], json!(["big"]));
    assert_eq!(recs[1].key.as_deref(), Some(&b"1"[..]));

    // Delete: before = the key columns.
    assert_eq!(del["op"], "d");
    assert_eq!(del["before"], json!({"id": 1}));
    assert_eq!(del["after"], Value::Null);
    assert_eq!(recs[2].key.as_deref(), Some(&b"1"[..]));
    assert_eq!(recs[2].subject, format!("public.{t}.delete"));

    // Composite key: JSON array in column order.
    assert_eq!(comp["after"], json!({"a": 7, "b": "x", "v": 1}));
    assert_eq!(recs[3].key.as_deref(), Some(&br#"[7,"x"]"#[..]));
}

#[tokio::test]
#[ignore = "needs Postgres with wal_level=logical (EXSPEED_POSTGRES_URL)"]
async fn cdc_restart_resumes_without_loss() {
    let url = require_pg!();
    let name = unique("cdcr");
    let t = format!("{name}_t");
    let c = client(&url).await;
    c.batch_execute(&format!("CREATE TABLE {t} (id int PRIMARY KEY, v text)"))
        .await
        .unwrap();
    let slot = format!("{name}_slot");
    let publication = format!("{name}_pub");
    let cfg = cdc_config(&name, &url, std::slice::from_ref(&t), &slot, &publication);

    let env = Env::new();
    let mem: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    // The first checkpoint save "crashes" after the append: that batch is
    // replayed from the slot and deduplicated by its idempotency keys.
    let offsets = Arc::new(CrashingOffsets::new(mem.clone(), 1));
    let reg = Registry::builtin();

    let (h, state) = env.run(&reg, cfg.clone(), offsets.clone());
    wait_running(&state).await;
    for i in 1..=5 {
        c.execute(&format!("INSERT INTO {t} VALUES ($1, 'x')"), &[&i])
            .await
            .unwrap();
    }
    wait_records(&env, &name, 5).await;
    eventually(30, "checkpoint saved", || async {
        mem.load_source(&name).await.unwrap().is_some()
    })
    .await;
    h.stop(Duration::from_secs(15)).await;
    assert!(
        state.snapshot().restart_count >= 1,
        "the crash restarted the connector"
    );

    // Changes while the connector is down are retained by the slot.
    for i in 6..=10 {
        c.execute(&format!("INSERT INTO {t} VALUES ($1, 'y')"), &[&i])
            .await
            .unwrap();
    }
    let (h, state) = env.run(&reg, cfg, mem.clone());
    wait_running(&state).await;
    let recs = wait_records(&env, &name, 10).await;
    // Let a few keepalives/acks go by, then check nothing else arrives.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let recs_after = env.read_all(&name).await;
    let checkpoint = mem.load_source(&name).await.unwrap().unwrap();
    h.stop(Duration::from_secs(15)).await;

    let confirmed: Option<String> = c
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .get(0);
    cleanup(&url, Some(&slot), Some(&publication), &[&t]).await;

    assert_eq!(recs.len(), recs_after.len());
    let ids: Vec<i64> = recs_after
        .iter()
        .map(|r| json_of(r)["after"]["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, (1..=10).collect::<Vec<_>>(), "no loss, no duplicates");
    // The slot was confirmed only up to what is durable.
    let parse = |s: &str| {
        let (a, b) = s.split_once('/').unwrap();
        (u64::from_str_radix(a, 16).unwrap() << 32) | u64::from_str_radix(b, 16).unwrap()
    };
    assert!(parse(&confirmed.unwrap()) <= parse(&checkpoint));
}

#[tokio::test]
#[ignore = "needs Postgres with wal_level=logical (EXSPEED_POSTGRES_URL)"]
async fn cdc_dry_run_creates_nothing() {
    let url = require_pg!();
    let name = unique("cdcd");
    let t = format!("{name}_t");
    let c = client(&url).await;
    c.batch_execute(&format!("CREATE TABLE {t} (id int PRIMARY KEY)"))
        .await
        .unwrap();
    let slot = format!("{name}_slot");
    let publication = format!("{name}_pub");
    let cfg = cdc_config(&name, &url, std::slice::from_ref(&t), &slot, &publication);
    let env = Env::new();
    let reg = Registry::builtin();
    let init = reg.init(&cfg, false, env.metrics.clone()).unwrap();
    let mut src = reg.create_source(&init).unwrap();
    let sample = src.dry_run(5).await.unwrap();
    assert!(sample.is_empty());
    let slots: i64 = c
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .get(0);
    let pubs: i64 = c
        .query_one(
            "SELECT count(*) FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .unwrap()
        .get(0);
    cleanup(&url, None, None, &[&t]).await;
    assert_eq!(
        (slots, pubs),
        (0, 0),
        "dry run must not create a slot or publication"
    );
}

// ---------------------------------------------------------------------------
// postgres_outbox
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs Postgres (EXSPEED_POSTGRES_URL)"]
async fn outbox_poll_publishes_then_deletes() {
    let url = require_pg!();
    let name = unique("obp");
    let t = format!("{name}_outbox");
    let c = client(&url).await;
    c.batch_execute(&format!(
        "CREATE TABLE {t} (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), \
         seq bigserial, aggregate_type text, aggregate_id text, event_type text, payload jsonb);
         INSERT INTO {t} (aggregate_type, aggregate_id, event_type, payload) VALUES
           ('order', 'o-1', 'created', '{{\"total\": 5}}'),
           ('order', 'o-1', 'paid', '{{\"total\": 5}}'),
           ('order', 'o-2', 'created', '{{\"total\": 7.5}}');"
    ))
    .await
    .unwrap();
    let ids: Vec<String> = c
        .query(&format!("SELECT id::text FROM {t} ORDER BY seq"), &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();

    let mut cfg = fast_config(&name, Source, "postgres_outbox", &name);
    cfg.batch_size = 2;
    cfg.settings = json!({"connection": url, "table": t, "order_column": "seq"})
        .as_object()
        .unwrap()
        .clone();
    let env = Env::new();
    let (h, _) = env.run(&Registry::builtin(), cfg, Arc::new(MemOffsets::default()));
    let recs = wait_records(&env, &name, 3).await;
    eventually(30, "published rows deleted", || async {
        let n: i64 = c
            .query_one(&format!("SELECT count(*) FROM {t}"), &[])
            .await
            .unwrap()
            .get(0);
        n == 0
    })
    .await;
    h.stop(Duration::from_secs(15)).await;
    cleanup(&url, None, None, &[&t]).await;

    assert_eq!(recs.len(), 3);
    let subjects: Vec<&str> = recs.iter().map(|r| r.subject.as_str()).collect();
    assert_eq!(
        subjects,
        vec!["order.created", "order.paid", "order.created"]
    );
    let got_ids: Vec<&str> = recs
        .iter()
        .map(|r| header(r, "x-idempotency-key").unwrap())
        .collect();
    assert_eq!(got_ids, ids.iter().map(String::as_str).collect::<Vec<_>>());
    assert_eq!(recs[0].key.as_deref(), Some(&b"o-1"[..]));
    assert_eq!(json_of(&recs[2]), json!({"total": 7.5}));
}

#[tokio::test]
#[ignore = "needs Postgres with wal_level=logical (EXSPEED_POSTGRES_URL)"]
async fn outbox_cdc_streams_inserts() {
    let url = require_pg!();
    let name = unique("obc");
    let t = format!("{name}_outbox");
    let c = client(&url).await;
    c.batch_execute(&format!(
        "CREATE TABLE {t} (id bigserial PRIMARY KEY, aggregate_type text, aggregate_id text, \
         event_type text, payload jsonb)"
    ))
    .await
    .unwrap();
    let slot = format!("{name}_slot");
    let publication = format!("{name}_pub");
    let mut cfg = fast_config(&name, Source, "postgres_outbox", &name);
    cfg.settings = json!({
        "connection": url, "table": t, "mode": "cdc", "cleanup": "none",
        "slot_name": slot, "publication_name": publication,
    })
    .as_object()
    .unwrap()
    .clone();
    let env = Env::new();
    let (h, state) = env.run(&Registry::builtin(), cfg, Arc::new(MemOffsets::default()));
    wait_running(&state).await;
    c.batch_execute(&format!(
        "INSERT INTO {t} (aggregate_type, aggregate_id, event_type, payload) VALUES
           ('user', 'u-1', 'signed_up', '{{\"plan\": \"pro\"}}'),
           ('user', 'u-2', 'signed_up', '{{\"plan\": \"free\"}}');
         UPDATE {t} SET event_type = 'ignored';"
    ))
    .await
    .unwrap();
    let recs = wait_records(&env, &name, 2).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let all = env.read_all(&name).await;
    h.stop(Duration::from_secs(15)).await;
    cleanup(&url, Some(&slot), Some(&publication), &[&t]).await;

    assert_eq!(all.len(), 2, "only inserts are published: {all:?}");
    assert_eq!(recs[0].subject, "user.signed_up");
    assert_eq!(header(&recs[0], "x-idempotency-key"), Some("1"));
    assert_eq!(header(&recs[1], "x-idempotency-key"), Some("2"));
    assert_eq!(recs[1].key.as_deref(), Some(&b"u-2"[..]));
    assert_eq!(json_of(&recs[1]), json!({"plan": "free"}));
}

/// Two crashes after the append but before the outbox rows are deleted: the
/// rows are polled again, and their ids (the idempotency keys) make the
/// broker drop the replays. Each event is in the stream exactly once and the
/// outbox ends up empty.
#[tokio::test]
#[ignore = "needs Postgres (EXSPEED_POSTGRES_URL)"]
async fn outbox_crash_before_delete_is_exactly_once() {
    let url = require_pg!();
    let name = unique("obx");
    let t = format!("{name}_outbox");
    let c = client(&url).await;
    c.batch_execute(&format!(
        "CREATE TABLE {t} (id bigserial PRIMARY KEY, aggregate_type text, aggregate_id text, \
         event_type text, payload jsonb);
         INSERT INTO {t} (aggregate_type, aggregate_id, event_type, payload)
         SELECT 'order', 'o-' || i, 'created', json_build_object('i', i)
         FROM generate_series(1, 7) AS i;"
    ))
    .await
    .unwrap();

    let mut cfg = fast_config(&name, Source, "postgres_outbox", &name);
    cfg.batch_size = 3;
    cfg.settings = json!({"connection": url, "table": t})
        .as_object()
        .unwrap()
        .clone();
    let env = Env::new();
    let reg = crash_before_ack_registry("postgres_outbox", Arc::new(AtomicU32::new(2)));
    let (h, state) = env.run(&reg, cfg, Arc::new(MemOffsets::default()));
    eventually(30, "outbox drained", || async {
        let n: i64 = c
            .query_one(&format!("SELECT count(*) FROM {t}"), &[])
            .await
            .unwrap()
            .get(0);
        n == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let recs = env.read_all(&name).await;
    let restarts = state.snapshot().restart_count;
    h.stop(Duration::from_secs(15)).await;
    cleanup(&url, None, None, &[&t]).await;

    assert!(restarts >= 2, "both crashes restarted the connector");
    let ids: Vec<&str> = recs
        .iter()
        .map(|r| header(r, "x-idempotency-key").unwrap())
        .collect();
    assert_eq!(
        ids,
        vec!["1", "2", "3", "4", "5", "6", "7"],
        "exactly once, in order"
    );
}

// ---------------------------------------------------------------------------
// postgres_poll
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs Postgres (EXSPEED_POSTGRES_URL)"]
async fn poll_mode_types_ties_and_resume() {
    let url = require_pg!();
    let name = unique("poll");
    let t = format!("{name}_t");
    let c = client(&url).await;
    // Five rows share one timestamp: a plain `updated_at > $1` cursor with
    // batch_size 2 would lose three of them.
    c.batch_execute(&format!(
        "CREATE TABLE {t} (id int PRIMARY KEY, updated_at timestamptz NOT NULL, \
         amount numeric(12,3), uid uuid, doc json);
         INSERT INTO {t}
         SELECT i, '2026-01-01T00:00:00Z', i * 1.5, \
                ('00000000-0000-0000-0000-00000000000' || i)::uuid, \
                json_build_object('i', i)
         FROM generate_series(1, 5) AS i;"
    ))
    .await
    .unwrap();

    let mut cfg = fast_config(&name, Source, "postgres_poll", &name);
    cfg.batch_size = 2;
    cfg.settings = json!({"connection": url, "tables": [t]})
        .as_object()
        .unwrap()
        .clone();
    let env = Env::new();
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let reg = Registry::builtin();

    let (h, _) = env.run(&reg, cfg.clone(), offsets.clone());
    let recs = wait_records(&env, &name, 5).await;
    h.stop(Duration::from_secs(15)).await;
    assert_eq!(recs.len(), 5);
    let ids: Vec<i64> = recs
        .iter()
        .map(|r| json_of(r)["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    let first = json_of(&recs[0]);
    assert_eq!(first["amount"], json!(1.5));
    assert_eq!(first["uid"], json!("00000000-0000-0000-0000-000000000001"));
    assert_eq!(first["doc"], json!({"i": 1}));
    assert!(first["updated_at"]
        .as_str()
        .unwrap()
        .starts_with("2026-01-01T00:00:00"));
    assert_eq!(recs[0].subject, format!("public.{t}"));

    // While stopped: one more row tying on the timestamp (higher id), one
    // later row. The restart resumes from the stored cursor.
    c.batch_execute(&format!(
        "INSERT INTO {t} VALUES (6, '2026-01-01T00:00:00Z', 0, NULL, NULL);
         INSERT INTO {t} VALUES (0, '2026-01-02T00:00:00Z', 0, NULL, NULL);"
    ))
    .await
    .unwrap();
    let (h, _) = env.run(&reg, cfg, offsets.clone());
    let recs = wait_records(&env, &name, 7).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let all = env.read_all(&name).await;
    h.stop(Duration::from_secs(15)).await;
    cleanup(&url, None, None, &[&t]).await;

    assert_eq!(recs.len(), all.len(), "nothing re-delivered");
    let ids: Vec<i64> = all
        .iter()
        .map(|r| json_of(r)["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 0]);
    let cp: Value =
        serde_json::from_str(&offsets.load_source(&name).await.unwrap().unwrap()).unwrap();
    assert!(
        cp.as_object().unwrap().keys().any(|k| k.contains(&t)),
        "per-table cursor: {cp}"
    );
}
