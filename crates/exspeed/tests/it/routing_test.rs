//! RabbitMQ-style delivery features: header filters, single active
//! consumers, priority within a lookahead window, and dead-letter causes.

use std::time::Duration;

use exspeed_client::{code, ConsumerSpec, HeaderMatch, PublishRecord};

use crate::common::{create_stream, eventually, TestServer};

const WAIT: Duration = Duration::from_secs(5);

fn texts(records: &[exspeed_client::WireRecord]) -> Vec<String> {
    records
        .iter()
        .map(|r| String::from_utf8_lossy(&r.value).into_owned())
        .collect()
}

#[tokio::test]
async fn header_filters_select_records() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "events").await;
    for (region, tier, v) in [
        ("eu", "gold", "a"),
        ("us", "gold", "b"),
        ("eu", "free", "c"),
        ("asia", "free", "d"),
    ] {
        c.publish(
            "events",
            PublishRecord::new("e", v)
                .header("region", region)
                .header("tier", tier),
        )
        .await
        .unwrap();
    }
    let mut all = ConsumerSpec::new("eu-gold", "events");
    all.filter_headers.insert("region".into(), "eu".into());
    all.filter_headers.insert("tier".into(), "gold".into());
    c.create_consumer(all).await.unwrap();
    let got = c
        .pull("eu-gold", 10, Duration::from_millis(300))
        .await
        .unwrap();
    assert_eq!(texts(&got), ["a"]);

    let mut any = ConsumerSpec::new("eu-or-gold", "events");
    any.filter_headers.insert("region".into(), "eu".into());
    any.filter_headers.insert("tier".into(), "gold".into());
    any.header_match = HeaderMatch::Any;
    c.create_consumer(any).await.unwrap();
    let got = c
        .pull("eu-or-gold", 10, Duration::from_millis(300))
        .await
        .unwrap();
    assert_eq!(texts(&got), ["a", "b", "c"]);
}

#[tokio::test]
async fn single_active_consumer_fails_over_to_the_next_subscriber() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "orders").await;
    let mut spec = ConsumerSpec::new("ordered", "orders");
    spec.single_active = true;
    c.create_consumer(spec).await.unwrap();

    let (c1, c2) = (server.client().await, server.client().await);
    let mut first = c1.subscribe("ordered", 100).await.unwrap();
    let mut second = c2.subscribe("ordered", 100).await.unwrap();
    for i in 0..5 {
        c.publish("orders", PublishRecord::new("o", i.to_string()))
            .await
            .unwrap();
    }
    for _ in 0..5 {
        first.next_timeout(WAIT).await.unwrap().ack().await.unwrap();
    }
    assert!(
        second
            .next_timeout(Duration::from_millis(300))
            .await
            .is_none(),
        "the standby gets nothing while the active one is connected"
    );

    // The active subscriber goes away holding an unacked record: the
    // standby takes over, starting with that record.
    c.publish("orders", PublishRecord::new("o", "5"))
        .await
        .unwrap();
    let held = first.next_timeout(WAIT).await.unwrap();
    assert_eq!(&held.record.value[..], b"5");
    drop(first);
    c1.close();
    c.publish("orders", PublishRecord::new("o", "6"))
        .await
        .unwrap();
    let next = second.next_timeout(WAIT).await.unwrap();
    assert_eq!(&next.record.value[..], b"5", "redelivered first");
    next.ack().await.unwrap();
    let next = second.next_timeout(WAIT).await.unwrap();
    assert_eq!(&next.record.value[..], b"6");

    let err = c
        .pull("ordered", 1, Duration::from_millis(10))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::BAD_REQUEST));
}

#[tokio::test]
async fn higher_priority_first_within_the_window() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "tasks").await;
    for (v, p) in [
        ("low1", 0),
        ("high1", 9),
        ("mid", 5),
        ("low2", 0),
        ("high2", 9),
    ] {
        c.publish("tasks", PublishRecord::new("t", v).priority(p))
            .await
            .unwrap();
    }
    let mut spec = ConsumerSpec::new("w", "tasks");
    spec.priority_window = 100;
    c.create_consumer(spec).await.unwrap();
    let mut order = Vec::new();
    while order.len() < 5 {
        let got = c.pull("w", 10, Duration::from_millis(500)).await.unwrap();
        c.ack("w", got.iter().map(|r| r.offset).collect())
            .await
            .unwrap();
        order.extend(texts(&got));
    }
    assert_eq!(order, ["high1", "high2", "mid", "low1", "low2"]);
    let err = c
        .create_consumer({
            let mut s = ConsumerSpec::new("too-wide", "tasks");
            s.priority_window = 1_000_000;
            s
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::BAD_REQUEST));
}

#[tokio::test]
async fn dead_letters_carry_a_cause() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "jobs").await;
    let mut spec = ConsumerSpec::new("w", "jobs");
    spec.dlq_stream = Some("jobs-dlq".into());
    spec.max_deliver = 1;
    spec.ack_wait_ms = 100;
    c.create_consumer(spec).await.unwrap();
    c.publish("jobs", PublishRecord::new("j", "rejected"))
        .await
        .unwrap();
    c.publish("jobs", PublishRecord::new("j", "timed out"))
        .await
        .unwrap();

    let got = c.pull("w", 2, Duration::from_secs(1)).await.unwrap();
    c.term("w", got[0].offset, "bad input").await.unwrap();
    // The second is never acked: max_deliver = 1 dead-letters it.
    let dead = eventually(WAIT, || async {
        let r = c.read("jobs-dlq", 0, 10, Duration::ZERO, "").await.ok()?;
        (r.records.len() == 2).then_some(r.records)
    })
    .await;
    let header = |r: &exspeed_client::WireRecord, k: &str| {
        r.headers
            .iter()
            .find(|(hk, _)| hk == k)
            .map(|(_, v)| v.clone())
    };
    let by_value = |v: &str| dead.iter().find(|r| &r.value[..] == v.as_bytes()).unwrap();
    let rejected = by_value("rejected");
    assert_eq!(
        header(rejected, "exspeed-dlq-cause").as_deref(),
        Some("rejected")
    );
    assert_eq!(
        header(rejected, "exspeed-dlq-reason").as_deref(),
        Some("bad input")
    );
    let timed_out = by_value("timed out");
    assert_eq!(
        header(timed_out, "exspeed-dlq-cause").as_deref(),
        Some("max_deliver")
    );
    assert!(header(timed_out, "exspeed-dlq-time").is_some());
}
