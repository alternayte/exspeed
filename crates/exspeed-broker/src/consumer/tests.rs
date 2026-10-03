use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use exspeed_common::{Metrics, Offset, StreamName};
use exspeed_protocol::client::{AckPolicy, ConsumerSpec, DeliverPolicy, SeekTo, WireRecord};
use exspeed_storage::memory::MemoryStorage;
use exspeed_streams::{ReadLimits, Record, StorageEngine, StreamConfig};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::{ConsumerError, ConsumerManager, SubEvent};
use crate::broker_append::BrokerAppend;
use crate::log::Log;

struct Env {
    log: Arc<Log>,
    metrics: Arc<Metrics>,
}

fn env() -> Env {
    env_on(Arc::new(MemoryStorage::new()))
}

fn env_on(storage: Arc<dyn StorageEngine>) -> Env {
    let dedup = Arc::new(BrokerAppend::new(storage.clone(), 300));
    let metrics = Arc::new(Metrics::new().0);
    let log = Arc::new(Log::new(
        storage,
        dedup,
        metrics.clone(),
        Arc::new(AtomicBool::new(true)),
    ));
    Env { log, metrics }
}

impl Env {
    async fn manager(&self) -> (Arc<ConsumerManager>, CancellationToken) {
        let m = ConsumerManager::new(self.log.clone(), self.metrics.clone());
        let token = CancellationToken::new();
        m.start(token.clone()).await.unwrap();
        (m, token)
    }

    async fn stream(&self, name: &str) {
        self.log
            .create_stream(&sn(name), &StreamConfig::default())
            .await
            .unwrap();
    }

    async fn publish(&self, stream: &str, subject: &str, n: usize) {
        for i in 0..n {
            self.log
                .append(
                    &sn(stream),
                    Record {
                        key: Some(Bytes::from(format!("k{i}"))),
                        value: Bytes::from(format!("v{i}")),
                        subject: subject.into(),
                        headers: vec![],
                        timestamp_ns: None,
                    },
                )
                .await
                .unwrap();
        }
    }
}

fn sn(s: &str) -> StreamName {
    StreamName::try_from(s).unwrap()
}

fn spec(name: &str, stream: &str) -> ConsumerSpec {
    ConsumerSpec::new(name, stream)
}

async fn next_batch(sub: &mut super::Subscription) -> Vec<WireRecord> {
    match timeout(Duration::from_secs(5), sub.events.recv()).await {
        Ok(Some(SubEvent::Deliver(r))) => r,
        other => panic!("expected a delivery, got {other:?}"),
    }
}

/// Collect deliveries until `n` records arrived.
async fn collect(sub: &mut super::Subscription, n: usize) -> Vec<WireRecord> {
    let mut out = Vec::new();
    while out.len() < n {
        out.extend(next_batch(sub).await);
    }
    out
}

async fn assert_quiet(sub: &mut super::Subscription, for_ms: u64) {
    if let Ok(Some(ev)) = timeout(Duration::from_millis(for_ms), sub.events.recv()).await {
        panic!("expected no delivery, got {ev:?}");
    }
}

#[tokio::test]
async fn not_leader_until_started() {
    let e = env();
    let m = ConsumerManager::new(e.log.clone(), e.metrics.clone());
    assert_eq!(
        m.create(spec("c", "s")).await.unwrap_err(),
        ConsumerError::NotLeader
    );
}

#[tokio::test]
async fn push_respects_credits_and_acks_move_the_floor() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "a", 10).await;
    let (m, _t) = e.manager().await;
    m.create(spec("c", "s")).await.unwrap();

    let mut sub = m.subscribe("c", 4).await.unwrap();
    let first = collect(&mut sub, 4).await;
    assert_eq!(
        first.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert!(first.iter().all(|r| r.delivery_count == 1));
    assert_quiet(&mut sub, 150).await;

    m.credit("c", sub.sub_id, 100).await.unwrap();
    let rest = collect(&mut sub, 6).await;
    assert_eq!(rest.first().unwrap().offset, 4);
    assert_eq!(rest.last().unwrap().offset, 9);

    m.ack("c", (0..10).collect()).await.unwrap();
    let info = m.info("c").await.unwrap();
    assert_eq!(info.ack_floor, 10);
    assert_eq!(info.num_unacked, 0);
    assert_eq!(info.stats.acked, 10);
}

#[tokio::test]
async fn subscribers_on_one_consumer_share_the_work() {
    let e = env();
    e.stream("s").await;
    let (m, _t) = e.manager().await;
    m.create(spec("c", "s")).await.unwrap();
    let mut a = m.subscribe("c", 1000).await.unwrap();
    let mut b = m.subscribe("c", 1000).await.unwrap();
    e.publish("s", "x", 100).await;

    let mut seen = Vec::new();
    let mut from_a = 0;
    let mut from_b = 0;
    while seen.len() < 100 {
        tokio::select! {
            Some(SubEvent::Deliver(r)) = a.events.recv() => { from_a += r.len(); seen.extend(r); }
            Some(SubEvent::Deliver(r)) = b.events.recv() => { from_b += r.len(); seen.extend(r); }
            _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("timed out at {}", seen.len()),
        }
    }
    let mut offsets: Vec<u64> = seen.iter().map(|r| r.offset).collect();
    offsets.sort_unstable();
    assert_eq!(offsets, (0..100).collect::<Vec<_>>(), "each record exactly once");
    assert!(from_a > 0 && from_b > 0, "both subscribers got work: {from_a}/{from_b}");
}

#[tokio::test]
async fn unacked_records_are_redelivered_after_ack_wait() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "x", 1).await;
    let (m, _t) = e.manager().await;
    let mut s = spec("c", "s");
    s.ack_wait_ms = 100;
    m.create(s).await.unwrap();
    let mut sub = m.subscribe("c", 10).await.unwrap();

    let r1 = next_batch(&mut sub).await;
    assert_eq!((r1[0].offset, r1[0].delivery_count), (0, 1));
    let r2 = next_batch(&mut sub).await;
    assert_eq!((r2[0].offset, r2[0].delivery_count), (0, 2));
    m.ack("c", vec![0]).await.unwrap();
    assert_quiet(&mut sub, 300).await;
    assert_eq!(m.info("c").await.unwrap().stats.redelivered, 1);
}

#[tokio::test]
async fn max_deliver_sends_to_dlq_with_headers() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "orders.created", 1).await;
    let (m, _t) = e.manager().await;
    let mut s = spec("c", "s");
    s.ack_wait_ms = 50;
    s.max_deliver = 2;
    s.dlq_stream = Some("s-dlq".into());
    m.create(s).await.unwrap();
    let mut sub = m.subscribe("c", 10).await.unwrap();
    next_batch(&mut sub).await;
    next_batch(&mut sub).await;

    // After the second timeout the record goes to the DLQ instead.
    let storage = e.log.storage().clone();
    let dlq = sn("s-dlq");
    let rec = timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(b) = storage
                .read_batch(&dlq, Offset(0), ReadLimits::default())
                .await
            {
                if let Some(r) = b.records.into_iter().next() {
                    return r;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("record reached the DLQ");
    assert_eq!(rec.value, Bytes::from("v0"));
    assert_eq!(rec.subject, "orders.created");
    let h = |k: &str| {
        rec.headers
            .iter()
            .find(|(hk, _)| hk == k)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(h("exspeed-dlq-origin").as_deref(), Some("c"));
    assert_eq!(h("exspeed-dlq-original-offset").as_deref(), Some("0"));
    assert_eq!(h("exspeed-dlq-deliveries").as_deref(), Some("2"));
    assert_quiet(&mut sub, 200).await;
    let info = m.info("c").await.unwrap();
    assert_eq!(info.stats.dead_lettered, 1);
    assert_eq!(info.ack_floor, 1);
}

#[tokio::test]
async fn term_dead_letters_immediately() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "x", 2).await;
    let (m, _t) = e.manager().await;
    let mut s = spec("c", "s");
    s.dlq_stream = Some("dead".into());
    m.create(s).await.unwrap();
    let mut sub = m.subscribe("c", 10).await.unwrap();
    collect(&mut sub, 2).await;
    m.term("c", 0, "unparseable".into()).await.unwrap();
    m.ack("c", vec![1]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let b = e
        .log
        .storage()
        .read_batch(&sn("dead"), Offset(0), ReadLimits::default())
        .await
        .unwrap();
    assert_eq!(b.records.len(), 1);
    assert!(b
        .records[0]
        .headers
        .iter()
        .any(|(k, v)| k == "exspeed-dlq-reason" && v == "unparseable"));
    assert_eq!(m.info("c").await.unwrap().ack_floor, 2);
}

#[tokio::test]
async fn nack_with_delay_redelivers_later() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "x", 1).await;
    let (m, _t) = e.manager().await;
    m.create(spec("c", "s")).await.unwrap();
    let mut sub = m.subscribe("c", 10).await.unwrap();
    next_batch(&mut sub).await;
    m.nack("c", 0, 300).await.unwrap();
    assert_quiet(&mut sub, 200).await;
    let r = next_batch(&mut sub).await;
    assert_eq!((r[0].offset, r[0].delivery_count), (0, 2));
}

#[tokio::test]
async fn pull_long_polls_until_data_arrives() {
    let e = env();
    e.stream("s").await;
    let (m, _t) = e.manager().await;
    m.create(spec("c", "s")).await.unwrap();

    // Nothing there: returns empty after the timeout.
    let t0 = std::time::Instant::now();
    let empty = m.pull("c", 10, 0, Duration::from_millis(150)).await.unwrap();
    assert!(empty.is_empty());
    assert!(t0.elapsed() >= Duration::from_millis(140));

    // A publish while waiting completes the pull early.
    let m2 = m.clone();
    let pull = tokio::spawn(async move { m2.pull("c", 10, 0, Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    e.publish("s", "x", 3).await;
    let got = timeout(Duration::from_secs(2), pull)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!got.is_empty());
    assert_eq!(got[0].offset, 0);
}

#[tokio::test]
async fn subject_filters_skip_other_records() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "orders.eu", 3).await;
    e.publish("s", "payments.eu", 3).await;
    e.publish("s", "orders.us", 3).await;
    let (m, _t) = e.manager().await;
    let mut s = spec("c", "s");
    s.filter_subjects = vec!["orders.*".into()];
    m.create(s).await.unwrap();
    let got = m.pull("c", 100, 0, Duration::from_millis(200)).await.unwrap();
    assert_eq!(got.len(), 6);
    assert!(got.iter().all(|r| r.subject.starts_with("orders.")));
    m.ack("c", got.iter().map(|r| r.offset).collect())
        .await
        .unwrap();
    assert_eq!(m.info("c").await.unwrap().ack_floor, 9);
}

#[tokio::test]
async fn max_ack_pending_pauses_delivery() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "x", 10).await;
    let (m, _t) = e.manager().await;
    let mut s = spec("c", "s");
    s.max_ack_pending = 3;
    m.create(s).await.unwrap();
    let mut sub = m.subscribe("c", 100).await.unwrap();
    let got = collect(&mut sub, 3).await;
    assert_eq!(got.len(), 3);
    assert_quiet(&mut sub, 150).await;
    m.ack("c", vec![0]).await.unwrap();
    let more = next_batch(&mut sub).await;
    assert_eq!(more[0].offset, 3);
}

#[tokio::test]
async fn deliver_policies() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "x", 5).await;
    let (m, _t) = e.manager().await;
    let mut s = spec("new", "s");
    s.deliver = DeliverPolicy::New;
    assert_eq!(m.create(s).await.unwrap().next_offset, 5);
    let mut s = spec("from3", "s");
    s.deliver = DeliverPolicy::FromOffset(3);
    assert_eq!(m.create(s).await.unwrap().next_offset, 3);
}

#[tokio::test]
async fn create_is_idempotent_but_rejects_a_different_spec() {
    let e = env();
    e.stream("s").await;
    let (m, _t) = e.manager().await;
    m.create(spec("c", "s")).await.unwrap();
    m.create(spec("c", "s")).await.unwrap();
    let mut other = spec("c", "s");
    other.ack_wait_ms = 1;
    assert!(matches!(
        m.create(other).await.unwrap_err(),
        ConsumerError::Conflict(_)
    ));
    assert!(matches!(
        m.create(spec("x", "missing")).await.unwrap_err(),
        ConsumerError::NotFound(_)
    ));
    let mut bad = spec("y", "s");
    bad.filter_subjects = vec!["a.>.b".into()];
    assert!(matches!(
        m.create(bad).await.unwrap_err(),
        ConsumerError::Invalid(_)
    ));
}

#[tokio::test]
async fn state_survives_restart_and_unacked_records_come_back() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "x", 5).await;
    {
        let (m, token) = e.manager().await;
        m.create(spec("c", "s")).await.unwrap();
        let got = m.pull("c", 5, 0, Duration::from_millis(200)).await.unwrap();
        assert_eq!(got.len(), 5);
        m.ack("c", vec![0, 1, 3]).await.unwrap();
        // Let the debounced persist run, then "crash".
        tokio::time::sleep(Duration::from_millis(250)).await;
        token.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (m, _t) = e.manager().await;
    let info = m.info("c").await.unwrap();
    assert_eq!(info.ack_floor, 2);
    assert_eq!(info.num_unacked, 2);
    let again = m.pull("c", 10, 0, Duration::from_millis(200)).await.unwrap();
    let offsets: Vec<u64> = again.iter().map(|r| r.offset).collect();
    assert_eq!(offsets, vec![2, 4], "only the unacked records are redelivered");
    assert!(again.iter().all(|r| r.delivery_count == 2));
}

#[tokio::test]
async fn deleted_consumers_stay_deleted_and_end_subscriptions() {
    let e = env();
    e.stream("s").await;
    let (m, token) = e.manager().await;
    m.create(spec("c", "s")).await.unwrap();
    let mut sub = m.subscribe("c", 10).await.unwrap();
    m.delete("c").await.unwrap();
    match timeout(Duration::from_secs(2), sub.events.recv()).await {
        Ok(Some(SubEvent::Ended { code, .. })) => assert_eq!(code, 404),
        other => panic!("expected Ended, got {other:?}"),
    }
    token.cancel();
    let (m, _t) = e.manager().await;
    assert!(matches!(
        m.info("c").await.unwrap_err(),
        ConsumerError::NotFound(_)
    ));
}

#[tokio::test]
async fn seek_repositions() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "x", 10).await;
    let (m, _t) = e.manager().await;
    m.create(spec("c", "s")).await.unwrap();
    m.seek("c", SeekTo::Offset(7)).await.unwrap();
    let got = m.pull("c", 10, 0, Duration::from_millis(200)).await.unwrap();
    assert_eq!(got.first().unwrap().offset, 7);
    m.seek("c", SeekTo::Latest).await.unwrap();
    assert_eq!(m.info("c").await.unwrap().next_offset, 10);
}

#[tokio::test]
async fn ack_policy_none_never_redelivers() {
    let e = env();
    e.stream("s").await;
    e.publish("s", "x", 3).await;
    let (m, _t) = e.manager().await;
    let mut s = spec("c", "s");
    s.ack = AckPolicy::None;
    s.ack_wait_ms = 50;
    m.create(s).await.unwrap();
    let mut sub = m.subscribe("c", 10).await.unwrap();
    collect(&mut sub, 3).await;
    assert_quiet(&mut sub, 200).await;
    assert_eq!(m.info("c").await.unwrap().ack_floor, 3);
}

#[tokio::test]
async fn demotion_ends_subscriptions_with_503() {
    let e = env();
    e.stream("s").await;
    let (m, token) = e.manager().await;
    m.create(spec("c", "s")).await.unwrap();
    let mut sub = m.subscribe("c", 10).await.unwrap();
    token.cancel();
    match timeout(Duration::from_secs(2), sub.events.recv()).await {
        Ok(Some(SubEvent::Ended { code, .. })) => assert_eq!(code, 503),
        other => panic!("expected Ended, got {other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(m.info("c").await.unwrap_err(), ConsumerError::NotLeader);
}
