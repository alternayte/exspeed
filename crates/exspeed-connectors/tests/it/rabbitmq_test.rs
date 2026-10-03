//! `rabbitmq` source and sink against a real RabbitMQ: crash and resume.
//!
//! Set `EXSPEED_RABBITMQ_URL` (e.g. `amqp://guest:guest@127.0.0.1:5672/%2f`)
//! and run with `--include-ignored`. Without the variable the tests skip,
//! except under `CI=true`, where they fail.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use lapin::options::{
    BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use serde_json::json;

use exspeed_connectors::offset_store::OffsetStore;
use exspeed_connectors::ConnectorType::{Sink, Source};
use exspeed_connectors::Registry;
use exspeed_streams::StoredRecord;

use crate::common::*;

fn rabbitmq_url() -> Option<String> {
    service_env("EXSPEED_RABBITMQ_URL")
}

async fn amqp(url: &str) -> (Connection, Channel) {
    let conn = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("connect to EXSPEED_RABBITMQ_URL");
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    (conn, ch)
}

async fn declare_queue(ch: &Channel, queue: &str) {
    ch.queue_declare(
        queue,
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
}

/// Ready (not unacked) messages in `queue`.
async fn ready_count(ch: &Channel, queue: &str) -> u32 {
    ch.queue_declare(
        queue,
        QueueDeclareOptions {
            passive: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap()
    .message_count()
}

/// Publish `n:<i>` bodies to `queue` through the default exchange, waiting
/// for each publisher confirm. With `ids`, each carries `message_id = m-<i>`.
async fn publish(ch: &Channel, queue: &str, range: std::ops::RangeInclusive<u32>, ids: bool) {
    for i in range {
        let mut props = BasicProperties::default().with_delivery_mode(2);
        if ids {
            props = props.with_message_id(format!("m-{i}").into());
        }
        ch.basic_publish(
            "",
            queue,
            BasicPublishOptions::default(),
            format!("n:{i}").as_bytes(),
            props,
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
}

struct Msg {
    message_id: Option<String>,
    body: String,
    delivery_mode: Option<u8>,
}

/// Take every ready message off `queue` (auto-ack).
async fn drain(ch: &Channel, queue: &str) -> Vec<Msg> {
    let mut out = Vec::new();
    while let Some(m) = ch
        .basic_get(queue, BasicGetOptions { no_ack: true })
        .await
        .unwrap()
    {
        let d = m.delivery;
        out.push(Msg {
            message_id: d.properties.message_id().as_ref().map(|s| s.to_string()),
            body: String::from_utf8_lossy(&d.data).into_owned(),
            delivery_mode: *d.properties.delivery_mode(),
        });
    }
    out
}

async fn delete_queue(ch: &Channel, queue: &str) {
    let _ = ch.queue_delete(queue, QueueDeleteOptions::default()).await;
}

fn header<'a>(r: &'a StoredRecord, k: &str) -> Option<&'a str> {
    r.headers
        .iter()
        .find(|(n, _)| n == k)
        .map(|(_, v)| v.as_str())
}

fn bodies(recs: &[StoredRecord]) -> Vec<String> {
    values(recs)
}

fn expected(range: std::ops::RangeInclusive<u32>) -> BTreeSet<String> {
    range.map(|i| format!("n:{i}")).collect()
}

fn source_config(
    name: &str,
    url: &str,
    queue: &str,
    dedup: bool,
) -> exspeed_connectors::config::ConnectorConfig {
    let mut cfg = fast_config(name, Source, "rabbitmq", name);
    cfg.batch_size = 5;
    cfg.settings = settings(json!({
        "url": url,
        "queue": queue,
        "poll_wait_ms": 100,
        "dedup_on_message_id": dedup,
    }));
    cfg
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// A crash after the append but before the AMQP ack: RabbitMQ redelivers
/// the batch, so every message lands at least once (the replayed ones twice,
/// marked `x-amqp-redelivered`), and nothing is left in the queue.
#[tokio::test]
#[ignore = "needs RabbitMQ (EXSPEED_RABBITMQ_URL)"]
async fn source_crash_before_ack_is_at_least_once() {
    let Some(url) = rabbitmq_url() else { return };
    let name = unique("rmqsrc");
    let queue = format!("{name}_q");
    let (_conn, ch) = amqp(&url).await;
    declare_queue(&ch, &queue).await;
    publish(&ch, &queue, 1..=20, false).await;

    let env = Env::new();
    let reg = crash_before_ack_registry("rabbitmq", Arc::new(AtomicU32::new(1)));
    let cfg = source_config(&name, &url, &queue, false);
    let (h, state) = env.run(&reg, cfg, Arc::new(MemOffsets::default()));

    eventually(60, "all 20 messages in the stream", || async {
        let got: BTreeSet<String> = bodies(&env.read_all(&name).await).into_iter().collect();
        got == expected(1..=20)
    })
    .await;
    // The last batch's ack follows its append immediately.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let recs = env.read_all(&name).await;
    let restarts = state.snapshot().restart_count;
    h.stop(Duration::from_secs(15)).await;
    let left = ready_count(&ch, &queue).await;
    delete_queue(&ch, &queue).await;

    assert!(restarts >= 1, "the crash restarted the connector");
    assert!(
        recs.len() > 20,
        "the crashed batch was durable and is redelivered: {} records",
        recs.len()
    );
    let redelivered: Vec<&StoredRecord> = recs
        .iter()
        .filter(|r| header(r, "x-amqp-redelivered") == Some("true"))
        .collect();
    assert!(!redelivered.is_empty(), "redeliveries are marked");
    // Every duplicate is one of the redelivered messages.
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for b in bodies(&recs) {
        *counts.entry(b).or_default() += 1;
    }
    let redelivered_bodies: BTreeSet<String> =
        bodies(&redelivered.into_iter().cloned().collect::<Vec<_>>())
            .into_iter()
            .collect();
    for (b, n) in &counts {
        assert!(
            *n == 1 || redelivered_bodies.contains(b),
            "{b} duplicated without a redelivery"
        );
    }
    assert_eq!(left, 0, "every delivery was acknowledged");
}

/// With `dedup_on_message_id`, the redelivered batch is dropped by the
/// broker's dedup: exactly once across two crashes and a stop/restart with
/// messages published while the connector was down.
#[tokio::test]
#[ignore = "needs RabbitMQ (EXSPEED_RABBITMQ_URL)"]
async fn source_dedup_on_message_id_is_exactly_once_across_crashes() {
    let Some(url) = rabbitmq_url() else { return };
    let name = unique("rmqdd");
    let queue = format!("{name}_q");
    let (_conn, ch) = amqp(&url).await;
    declare_queue(&ch, &queue).await;
    publish(&ch, &queue, 1..=20, true).await;

    let env = Env::new();
    let offsets: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let cfg = source_config(&name, &url, &queue, true);
    let reg = crash_before_ack_registry("rabbitmq", Arc::new(AtomicU32::new(2)));
    let (h, state) = env.run(&reg, cfg.clone(), offsets.clone());
    wait_records(&env, &name, 20).await;
    eventually(60, "two crashes", || async {
        state.snapshot().restart_count >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let first = env.read_all(&name).await;
    h.stop(Duration::from_secs(15)).await;

    // Published while the connector is down: delivered after the restart.
    publish(&ch, &queue, 21..=30, true).await;
    let (h, _) = env.run(&Registry::builtin(), cfg, offsets);
    wait_records(&env, &name, 30).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let all = env.read_all(&name).await;
    h.stop(Duration::from_secs(15)).await;
    let left = ready_count(&ch, &queue).await;
    delete_queue(&ch, &queue).await;

    assert_eq!(first.len(), 20, "no duplicates after the crashes");
    assert_eq!(all.len(), 30, "no duplicates after the restart");
    let got: BTreeSet<String> = bodies(&all).into_iter().collect();
    assert_eq!(got, expected(1..=30), "no loss");
    for r in &all {
        let body = String::from_utf8_lossy(&r.value);
        let id = format!("m-{}", body.trim_start_matches("n:"));
        assert_eq!(header(r, "x-idempotency-key"), Some(id.as_str()));
        assert_eq!(header(r, "x-message-id"), Some(id.as_str()));
    }
    assert_eq!(left, 0, "every delivery was acknowledged");
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// A crash after the publisher confirms but before the offset commit: the
/// batch is published again, with the same `message_id`s, so every record
/// reaches the queue at least once and a consumer deduplicating on
/// `message_id` sees each exactly once. After a clean restart only new
/// records are published.
#[tokio::test]
#[ignore = "needs RabbitMQ (EXSPEED_RABBITMQ_URL)"]
async fn sink_crash_before_commit_republishes_with_stable_message_ids() {
    let Some(url) = rabbitmq_url() else { return };
    let name = unique("rmqsink");
    let exchange = format!("{name}_ex");
    let queue = format!("{name}_q");
    let (_conn, ch) = amqp(&url).await;
    ch.exchange_declare(
        &exchange,
        ExchangeKind::Topic,
        ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    declare_queue(&ch, &queue).await;
    ch.queue_bind(
        &queue,
        &exchange,
        "#",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    let env = Env::new();
    let vals: Vec<serde_json::Value> = (0..20).map(|i| json!({"n": i})).collect();
    publish_json(&env, &name, &vals).await;

    let mut cfg = fast_config(&name, Sink, "rabbitmq", &name);
    cfg.batch_size = 5;
    cfg.settings = settings(json!({"url": url, "exchange": exchange}));
    let mem: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    // The first commit "crashes" after RabbitMQ confirmed the batch.
    let crashing: Arc<dyn OffsetStore> = Arc::new(CrashingOffsets::new(mem.clone(), 1));
    let reg = Registry::builtin();
    let (h, state) = env.run(&reg, cfg.clone(), crashing);
    wait_committed(&mem, &name, 20).await;
    let restarts = state.snapshot().restart_count;
    h.stop(Duration::from_secs(15)).await;
    let first = drain(&ch, &queue).await;

    // A clean restart resumes after the committed offset.
    let more: Vec<serde_json::Value> = (20..25).map(|i| json!({"n": i})).collect();
    publish_json(&env, &name, &more).await;
    let (h, _) = env.run(&reg, cfg, mem.clone());
    wait_committed(&mem, &name, 25).await;
    h.stop(Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let second = drain(&ch, &queue).await;
    delete_queue(&ch, &queue).await;
    let _ = ch.exchange_delete(&exchange, Default::default()).await;

    assert!(restarts >= 1, "the crash restarted the connector");
    assert!(
        first.len() > 20,
        "the confirmed but uncommitted batch is published again: {}",
        first.len()
    );
    let mut by_id: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for m in &first {
        assert_eq!(m.delivery_mode, Some(2), "persistent");
        by_id
            .entry(m.message_id.clone().expect("message_id"))
            .or_default()
            .insert(m.body.clone());
    }
    let want: BTreeSet<String> = (0..20).map(|i| format!("{name}:{i}")).collect();
    assert_eq!(
        by_id.keys().cloned().collect::<BTreeSet<_>>(),
        want,
        "every record at least once, identified by <stream>:<offset>"
    );
    for (id, bodies) in &by_id {
        assert_eq!(bodies.len(), 1, "{id}: a replay carries the same body");
    }

    let ids: Vec<String> = second
        .iter()
        .map(|m| m.message_id.clone().unwrap())
        .collect();
    let want: Vec<String> = (20..25).map(|i| format!("{name}:{i}")).collect();
    assert_eq!(ids, want, "only the new records after a clean restart");
}

/// With `mandatory`, a message no queue is bound for is returned and the
/// record goes to the DLQ. The routable records around it, in the same
/// batch, are published exactly once.
#[tokio::test]
#[ignore = "needs RabbitMQ (EXSPEED_RABBITMQ_URL)"]
async fn sink_unroutable_records_go_to_the_dlq_without_duplicating_the_batch() {
    let Some(url) = rabbitmq_url() else { return };
    let name = unique("rmqret");
    let exchange = format!("{name}_ex");
    let queue = format!("{name}_q");
    let dlq = format!("{name}_dlq");
    let (_conn, ch) = amqp(&url).await;
    ch.exchange_declare(
        &exchange,
        ExchangeKind::Topic,
        ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    declare_queue(&ch, &queue).await;
    ch.queue_bind(
        &queue,
        &exchange,
        "ok.#",
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();

    // Offsets 1, 4, 7 have no binding.
    let env = Env::new();
    let recs: Vec<(String, String)> = (0..9)
        .map(|i| {
            let subject = if i % 3 == 1 { "bad.x" } else { "ok.x" };
            (subject.to_string(), json!({"n": i}).to_string())
        })
        .collect();
    let refs: Vec<(&str, &str)> = recs.iter().map(|(s, v)| (s.as_str(), v.as_str())).collect();
    env.publish_with_subjects(&name, &refs).await;

    let mut cfg = fast_config(&name, Sink, "rabbitmq", &name);
    cfg.batch_size = 9;
    cfg.dlq_stream = Some(dlq.clone());
    cfg.settings = settings(json!({"url": url, "exchange": exchange}));
    let mem: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    let (h, _) = env.run(&Registry::builtin(), cfg, mem.clone());
    wait_committed(&mem, &name, 9).await;
    h.stop(Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let got = drain(&ch, &queue).await;
    let dead = env.read_all(&dlq).await;
    delete_queue(&ch, &queue).await;
    let _ = ch.exchange_delete(&exchange, Default::default()).await;

    let ids: Vec<String> = got.iter().map(|m| m.message_id.clone().unwrap()).collect();
    let want: Vec<String> = [0, 2, 3, 5, 6, 8]
        .iter()
        .map(|i| format!("{name}:{i}"))
        .collect();
    assert_eq!(ids, want, "each routable record exactly once, in order");
    let dead_offsets: Vec<&str> = dead
        .iter()
        .map(|r| header(r, "exspeed-dlq-original-offset").unwrap())
        .collect();
    assert_eq!(dead_offsets, vec!["1", "4", "7"]);
}
