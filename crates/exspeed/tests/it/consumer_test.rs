//! Consumers over the client protocol: push and pull delivery, acks,
//! redelivery, dead-lettering, work sharing between app instances, seek,
//! ephemeral consumers, and state surviving a restart.

use std::collections::HashSet;
use std::time::Duration;

use exspeed_client::{code, ConsumerSpec, DeliverPolicy, PublishRecord, SeekTo};

use crate::common::{create_stream, eventually, publish_n, TestServer};

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn subscribe_receive_and_ack() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "orders").await;
    publish_n(&c, "orders", "orders.placed", 5).await;

    let info = c
        .create_consumer(ConsumerSpec::new("billing", "orders"))
        .await
        .unwrap();
    assert_eq!(info["spec"]["name"], "billing");

    let mut sub = c.subscribe("billing", 16).await.unwrap();
    for i in 0..5u64 {
        let msg = sub.next_timeout(WAIT).await.expect("message");
        assert_eq!(msg.record.offset, i);
        assert_eq!(msg.record.delivery_count, 1);
        let v: serde_json::Value = msg.json().unwrap();
        assert_eq!(v["i"], i);
        msg.ack().await.unwrap();
    }
    // New records arrive on the live subscription.
    publish_n(&c, "orders", "orders.placed", 1).await;
    let msg = sub.next_timeout(WAIT).await.expect("live message");
    assert_eq!(msg.record.offset, 5);
    msg.ack().await.unwrap();

    let info = eventually(WAIT, || async {
        let i = c.consumer_info("billing").await.unwrap();
        (i["ack_floor"] == 6).then_some(i)
    })
    .await;
    assert_eq!(info["num_unacked"], 0);
    assert_eq!(info["lag"], 0);
}

#[tokio::test]
async fn resume_after_disconnect() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "s").await;
    publish_n(&c, "s", "x", 6).await;
    c.create_consumer(ConsumerSpec::new("durable", "s"))
        .await
        .unwrap();

    {
        let c1 = server.client().await;
        let mut sub = c1.subscribe("durable", 3).await.unwrap();
        for _ in 0..3 {
            sub.next_timeout(WAIT).await.unwrap().ack().await.unwrap();
        }
        c1.close();
    }

    let c2 = server.client().await;
    let mut sub = c2.subscribe("durable", 16).await.unwrap();
    let msg = sub.next_timeout(WAIT).await.unwrap();
    assert_eq!(msg.record.offset, 3, "resumes after the acked records");
}

#[tokio::test]
async fn unacked_records_are_redelivered_after_disconnect() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "s").await;
    publish_n(&c, "s", "x", 2).await;
    c.create_consumer(ConsumerSpec {
        ack_wait_ms: 300,
        ..ConsumerSpec::new("work", "s")
    })
    .await
    .unwrap();

    {
        let c1 = server.client().await;
        let mut sub = c1.subscribe("work", 16).await.unwrap();
        let m = sub.next_timeout(WAIT).await.unwrap();
        assert_eq!(m.record.offset, 0);
        // Crash before acking.
        c1.close();
    }

    let c2 = server.client().await;
    let mut sub = c2.subscribe("work", 16).await.unwrap();
    let mut seen = Vec::new();
    while seen.len() < 2 {
        let m = sub.next_timeout(WAIT).await.expect("redelivery");
        seen.push((m.record.offset, m.record.delivery_count));
        m.ack().await.unwrap();
    }
    seen.sort();
    assert_eq!(seen[0].0, 0);
    assert!(seen[0].1 >= 2, "offset 0 was redelivered: {seen:?}");
    assert_eq!(seen[1].0, 1);
}

#[tokio::test]
async fn subject_filters() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "orders").await;
    for s in [
        "orders.placed",
        "orders.shipped",
        "orders.eu.placed",
        "orders.placed",
    ] {
        c.publish("orders", PublishRecord::new(s, "{}")).await.unwrap();
    }
    c.create_consumer(ConsumerSpec {
        filter_subjects: vec!["orders.placed".into(), "orders.eu.>".into()],
        ..ConsumerSpec::new("placed", "orders")
    })
    .await
    .unwrap();
    let mut sub = c.subscribe("placed", 16).await.unwrap();
    let mut offsets = Vec::new();
    for _ in 0..3 {
        let m = sub.next_timeout(WAIT).await.unwrap();
        offsets.push(m.record.offset);
        m.ack().await.unwrap();
    }
    assert_eq!(offsets, vec![0, 2, 3]);
    assert!(sub.next_timeout(Duration::from_millis(300)).await.is_none());

    let err = c
        .create_consumer(ConsumerSpec {
            filter_subjects: vec!["orders.>.bad".into()],
            ..ConsumerSpec::new("bad", "orders")
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::BAD_REQUEST));
}

#[tokio::test]
async fn nack_redelivers_and_dead_letters_after_max_deliver() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "jobs").await;
    create_stream(&c, "jobs-dlq").await;
    c.publish("jobs", PublishRecord::new("job", "poison"))
        .await
        .unwrap();
    c.create_consumer(ConsumerSpec {
        max_deliver: 3,
        dlq_stream: Some("jobs-dlq".into()),
        ..ConsumerSpec::new("worker", "jobs")
    })
    .await
    .unwrap();

    let mut sub = c.subscribe("worker", 16).await.unwrap();
    for attempt in 1..=3u16 {
        let m = sub.next_timeout(WAIT).await.expect("delivery");
        assert_eq!(m.record.offset, 0);
        assert_eq!(m.record.delivery_count, attempt);
        m.nack(Duration::from_millis(10)).await.unwrap();
    }
    assert!(sub.next_timeout(Duration::from_millis(500)).await.is_none());

    let dlq = eventually(WAIT, || async {
        let r = c.read("jobs-dlq", 0, 10, Duration::ZERO, "").await.unwrap();
        (!r.records.is_empty()).then_some(r)
    })
    .await;
    let rec = &dlq.records[0];
    assert_eq!(rec.value.as_ref(), b"poison");
    let header = |k: &str| {
        rec.headers
            .iter()
            .find(|(hk, _)| hk == k)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(header("exspeed-dlq-origin").as_deref(), Some("worker"));
    assert_eq!(header("exspeed-dlq-original-offset").as_deref(), Some("0"));
    assert_eq!(header("exspeed-dlq-deliveries").as_deref(), Some("3"));
}

#[tokio::test]
async fn term_dead_letters_immediately() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "jobs").await;
    create_stream(&c, "dead").await;
    publish_n(&c, "jobs", "job", 2).await;
    c.create_consumer(ConsumerSpec {
        dlq_stream: Some("dead".into()),
        ..ConsumerSpec::new("w", "jobs")
    })
    .await
    .unwrap();
    let mut sub = c.subscribe("w", 16).await.unwrap();
    let m = sub.next_timeout(WAIT).await.unwrap();
    m.term("cannot parse").await.unwrap();
    let m2 = sub.next_timeout(WAIT).await.unwrap();
    assert_eq!(m2.record.offset, 1);
    m2.ack().await.unwrap();

    let dlq = eventually(WAIT, || async {
        let r = c.read("dead", 0, 10, Duration::ZERO, "").await.unwrap();
        (!r.records.is_empty()).then_some(r)
    })
    .await;
    assert!(dlq.records[0]
        .headers
        .contains(&("exspeed-dlq-reason".to_string(), "cannot parse".to_string())));
}

#[tokio::test]
async fn ack_wait_expiry_redelivers() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "s").await;
    publish_n(&c, "s", "x", 1).await;
    c.create_consumer(ConsumerSpec {
        ack_wait_ms: 200,
        ..ConsumerSpec::new("slow", "s")
    })
    .await
    .unwrap();
    let mut sub = c.subscribe("slow", 16).await.unwrap();
    let first = sub.next_timeout(WAIT).await.unwrap();
    assert_eq!(first.record.delivery_count, 1);
    // Don't ack: it comes back.
    let again = sub.next_timeout(WAIT).await.unwrap();
    assert_eq!(again.record.offset, 0);
    assert_eq!(again.record.delivery_count, 2);
    again.ack().await.unwrap();
}

/// Several app instances (separate connections) subscribed to one consumer
/// split the records between them, each record going to exactly one.
#[tokio::test]
async fn work_is_shared_across_instances() {
    let server = TestServer::start().await;
    let admin = server.client().await;
    create_stream(&admin, "tasks").await;
    admin
        .create_consumer(ConsumerSpec::new("pool", "tasks"))
        .await
        .unwrap();

    let total = 200usize;
    let mut handles = Vec::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(usize, u64)>();
    for instance in 0..3 {
        let client = server.client().await;
        let tx = tx.clone();
        let mut sub = client.subscribe("pool", 8).await.unwrap();
        handles.push(tokio::spawn(async move {
            while let Some(m) = sub.next_timeout(Duration::from_secs(2)).await {
                tx.send((instance, m.record.offset)).unwrap();
                m.ack().await.unwrap();
            }
            drop(client);
        }));
    }
    drop(tx);
    publish_n(&admin, "tasks", "t", total).await;

    let mut seen = HashSet::new();
    let mut per_instance = [0usize; 3];
    while seen.len() < total {
        let (inst, off) = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("all records delivered")
            .expect("channel open");
        assert!(seen.insert(off), "offset {off} delivered twice");
        per_instance[inst] += 1;
    }
    assert!(
        per_instance.iter().filter(|&&n| n > 0).count() >= 2,
        "work should spread across instances: {per_instance:?}"
    );
    for h in handles {
        h.abort();
    }
}

#[tokio::test]
async fn pull_consumer() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "s").await;
    c.create_consumer(ConsumerSpec::new("puller", "s"))
        .await
        .unwrap();

    // Long-poll: waits for data.
    let puller = c.clone();
    let wait = tokio::spawn(async move {
        puller
            .pull("puller", 10, Duration::from_secs(10))
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    publish_n(&c, "s", "x", 3).await;
    let batch = tokio::time::timeout(WAIT, wait).await.unwrap().unwrap();
    assert!(!batch.is_empty());
    let mut got: Vec<u64> = batch.iter().map(|r| r.offset).collect();
    while got.len() < 3 {
        let more = c.pull("puller", 10, Duration::from_secs(2)).await.unwrap();
        got.extend(more.iter().map(|r| r.offset));
    }
    assert_eq!(got, vec![0, 1, 2]);
    c.ack("puller", got).await.unwrap();

    // Expires empty.
    let empty = c
        .pull("puller", 10, Duration::from_millis(200))
        .await
        .unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn deliver_policies_and_seek() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "s").await;
    publish_n(&c, "s", "x", 10).await;

    c.create_consumer(ConsumerSpec {
        deliver: DeliverPolicy::New,
        ..ConsumerSpec::new("new-only", "s")
    })
    .await
    .unwrap();
    c.create_consumer(ConsumerSpec {
        deliver: DeliverPolicy::FromOffset(7),
        ..ConsumerSpec::new("from7", "s")
    })
    .await
    .unwrap();

    let r = c.pull("from7", 10, Duration::from_millis(500)).await.unwrap();
    assert_eq!(r.first().map(|r| r.offset), Some(7));

    let r = c
        .pull("new-only", 10, Duration::from_millis(200))
        .await
        .unwrap();
    assert!(r.is_empty());
    publish_n(&c, "s", "x", 1).await;
    let r = c.pull("new-only", 10, Duration::from_secs(2)).await.unwrap();
    assert_eq!(r.first().map(|r| r.offset), Some(10));

    c.seek("new-only", SeekTo::Offset(4)).await.unwrap();
    let r = c.pull("new-only", 1, Duration::from_secs(2)).await.unwrap();
    assert_eq!(r[0].offset, 4);

    c.seek("new-only", SeekTo::Earliest).await.unwrap();
    let r = c.pull("new-only", 1, Duration::from_secs(2)).await.unwrap();
    assert_eq!(r[0].offset, 0);

    c.seek("new-only", SeekTo::Latest).await.unwrap();
    let r = c
        .pull("new-only", 1, Duration::from_millis(200))
        .await
        .unwrap();
    assert!(r.is_empty());

    let err = c.seek("missing", SeekTo::Earliest).await.unwrap_err();
    assert_eq!(err.code(), Some(code::NOT_FOUND));
}

#[tokio::test]
async fn create_consumer_is_idempotent_and_validated() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "s").await;
    let spec = ConsumerSpec::new("c1", "s");
    c.create_consumer(spec.clone()).await.unwrap();
    c.create_consumer(spec.clone()).await.unwrap();
    let err = c
        .create_consumer(ConsumerSpec {
            max_deliver: 9,
            ..spec
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::CONFLICT));

    let err = c
        .create_consumer(ConsumerSpec::new("c2", "missing"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::NOT_FOUND));

    let err = c
        .create_consumer(ConsumerSpec::new("x".repeat(300), "s"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::BAD_REQUEST));

    let list = c.list_consumers(Some("s")).await.unwrap();
    assert_eq!(list.len(), 1);
    c.delete_consumer("c1").await.unwrap();
    assert!(c.list_consumers(None).await.unwrap().is_empty());
    let err = c.delete_consumer("c1").await.unwrap_err();
    assert_eq!(err.code(), Some(code::NOT_FOUND));
}

#[tokio::test]
async fn deleting_consumer_ends_subscriptions() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "s").await;
    c.create_consumer(ConsumerSpec::new("doomed", "s"))
        .await
        .unwrap();
    let watcher = server.client().await;
    let mut sub = watcher.subscribe("doomed", 8).await.unwrap();
    c.delete_consumer("doomed").await.unwrap();
    assert!(sub.next_timeout(WAIT).await.is_none());
    let (code_, _) = sub.end_reason().expect("ended");
    assert_eq!(code_, code::NOT_FOUND);
}

#[tokio::test]
async fn ephemeral_consumer_is_removed_with_its_connection() {
    let server = TestServer::start().await;
    let admin = server.client().await;
    create_stream(&admin, "s").await;
    {
        let c = server.client().await;
        c.create_consumer(ConsumerSpec {
            ephemeral: true,
            ..ConsumerSpec::new("temp", "s")
        })
        .await
        .unwrap();
        assert_eq!(admin.list_consumers(None).await.unwrap().len(), 1);
        c.close();
    }
    eventually(WAIT, || async {
        admin
            .list_consumers(None)
            .await
            .unwrap()
            .is_empty()
            .then_some(())
    })
    .await;
}

#[tokio::test]
async fn consumer_state_survives_restart() {
    let server = TestServer::start().await;
    {
        let c = server.client().await;
        create_stream(&c, "s").await;
        publish_n(&c, "s", "x", 5).await;
        c.create_consumer(ConsumerSpec::new("keeper", "s"))
            .await
            .unwrap();
        let mut sub = c.subscribe("keeper", 16).await.unwrap();
        for _ in 0..3 {
            let m = sub.next_timeout(WAIT).await.unwrap();
            m.ack().await.unwrap();
        }
        // Give the actor a moment to persist the ack floor.
        eventually(WAIT, || async {
            let i = c.consumer_info("keeper").await.unwrap();
            (i["ack_floor"] == 3).then_some(())
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    let server = server.restart().await;
    let c = server.client().await;
    let info = c.consumer_info("keeper").await.unwrap();
    assert_eq!(info["ack_floor"], 3);
    let mut sub = c.subscribe("keeper", 16).await.unwrap();
    let m = sub.next_timeout(WAIT).await.unwrap();
    assert_eq!(m.record.offset, 3);
}

#[tokio::test]
async fn retention_by_age() {
    let server = TestServer::start().await;
    let c = server.client().await;
    c.create_stream(exspeed_client::StreamSpec {
        name: "short".into(),
        max_age_secs: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    publish_n(&c, "short", "tick", 3).await;
    let r = c.read("short", 0, 10, Duration::ZERO, "").await.unwrap();
    assert_eq!(r.records.len(), 3);
    let info = c.stream_info("short").await.unwrap();
    assert_eq!(info["config"]["max_age_secs"], 1);
}
