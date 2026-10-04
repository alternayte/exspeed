//! Retention by acknowledgement: `work_queue` streams remove a record once
//! its consumer acked it; `interest` streams once every consumer did (and
//! keep nothing while nobody is interested).

use std::time::Duration;

use exspeed_client::{
    code, Client, ConsumerSpec, DeliverPolicy, DiscardPolicy, PublishRecord, RetentionPolicy,
    StreamLimits, StreamSpec,
};

use crate::common::{eventually, publish_n, TestServer};

const WAIT: Duration = Duration::from_secs(10);

async fn create(c: &Client, name: &str, retention: RetentionPolicy) {
    c.create_stream(StreamSpec {
        limits: StreamLimits {
            retention,
            ..Default::default()
        },
        ..StreamSpec::named(name)
    })
    .await
    .unwrap();
}

async fn earliest(c: &Client, stream: &str) -> u64 {
    c.stream_info(stream).await.unwrap()["earliest_offset"]
        .as_u64()
        .unwrap()
}

async fn wait_earliest(c: &Client, stream: &str, want: u64) {
    eventually(WAIT, || async {
        (earliest(c, stream).await == want).then_some(())
    })
    .await;
}

#[tokio::test]
async fn work_queue_removes_acked_records() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create(&c, "jobs", RetentionPolicy::WorkQueue).await;
    publish_n(&c, "jobs", "job", 5).await;
    // No consumer yet: a queue keeps its records.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(earliest(&c, "jobs").await, 0);

    c.create_consumer(ConsumerSpec::new("workers", "jobs"))
        .await
        .unwrap();
    let got = c.pull("workers", 3, Duration::from_secs(1)).await.unwrap();
    assert_eq!(got.len(), 3);
    c.ack("workers", got.iter().map(|r| r.offset).collect())
        .await
        .unwrap();
    wait_earliest(&c, "jobs", 3).await;
    let info = c.stream_info("jobs").await.unwrap();
    assert_eq!(info["records"], 2, "two jobs left in the queue");

    // An out-of-order ack doesn't remove anything below an unacked record.
    let rest = c.pull("workers", 2, Duration::from_secs(1)).await.unwrap();
    c.ack("workers", vec![rest[1].offset]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(earliest(&c, "jobs").await, 3);
    c.ack("workers", vec![rest[0].offset]).await.unwrap();
    wait_earliest(&c, "jobs", 5).await;
}

#[tokio::test]
async fn work_queue_consumers_must_not_overlap() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create(&c, "jobs", RetentionPolicy::WorkQueue).await;
    let mut eu = ConsumerSpec::new("eu", "jobs");
    eu.filter_subjects = vec!["jobs.eu.>".into()];
    c.create_consumer(eu).await.unwrap();

    let mut us = ConsumerSpec::new("us", "jobs");
    us.filter_subjects = vec!["jobs.us.>".into()];
    c.create_consumer(us).await.unwrap();

    let err = c
        .create_consumer(ConsumerSpec::new("all", "jobs"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::CONFLICT), "{err}");

    let mut late = ConsumerSpec::new("late", "jobs");
    late.filter_subjects = vec!["jobs.asia.>".into()];
    late.deliver = DeliverPolicy::New;
    let err = c.create_consumer(late).await.unwrap_err();
    assert_eq!(err.code(), Some(code::BAD_REQUEST), "{err}");
}

#[tokio::test]
async fn interest_keeps_records_until_every_consumer_acked() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create(&c, "events", RetentionPolicy::Interest).await;
    // Nobody is interested: records don't stay.
    publish_n(&c, "events", "e", 3).await;
    wait_earliest(&c, "events", 3).await;

    for name in ["a", "b"] {
        let mut spec = ConsumerSpec::new(name, "events");
        spec.deliver = DeliverPolicy::New;
        c.create_consumer(spec).await.unwrap();
    }
    publish_n(&c, "events", "e", 2).await;
    let a = c.pull("a", 10, Duration::from_secs(1)).await.unwrap();
    c.ack("a", a.iter().map(|r| r.offset).collect())
        .await
        .unwrap();
    let b = c.pull("b", 10, Duration::from_secs(1)).await.unwrap();
    c.ack("b", vec![b[0].offset]).await.unwrap();
    wait_earliest(&c, "events", 4).await;

    // Without its last interested consumer the record goes.
    c.delete_consumer("b").await.unwrap();
    wait_earliest(&c, "events", 5).await;
}

#[tokio::test]
async fn bounded_work_queue_rejects_when_full_and_drains() {
    let server = TestServer::start().await;
    let c = server.client().await;
    c.create_stream(StreamSpec {
        limits: StreamLimits {
            retention: RetentionPolicy::WorkQueue,
            max_msgs: 2,
            discard: DiscardPolicy::New,
            ..Default::default()
        },
        ..StreamSpec::named("q")
    })
    .await
    .unwrap();
    c.create_consumer(ConsumerSpec::new("w", "q"))
        .await
        .unwrap();
    publish_n(&c, "q", "x", 2).await;
    let err = c
        .publish("q", PublishRecord::new("x", "full"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::TOO_MANY_REQUESTS));

    let got = c.pull("w", 2, Duration::from_secs(1)).await.unwrap();
    c.ack("w", got.iter().map(|r| r.offset).collect())
        .await
        .unwrap();
    wait_earliest(&c, "q", 2).await;
    c.publish("q", PublishRecord::new("x", "room again"))
        .await
        .unwrap();
}

#[tokio::test]
async fn work_queue_progress_survives_a_restart() {
    let server = TestServer::start().await;
    {
        let c = server.client().await;
        create(&c, "jobs", RetentionPolicy::WorkQueue).await;
        c.create_consumer(ConsumerSpec::new("w", "jobs"))
            .await
            .unwrap();
        publish_n(&c, "jobs", "j", 4).await;
        let got = c.pull("w", 2, Duration::from_secs(1)).await.unwrap();
        c.ack("w", got.iter().map(|r| r.offset).collect())
            .await
            .unwrap();
        wait_earliest(&c, "jobs", 2).await;
        // Let the log start offset reach disk.
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
    let server = server.restart().await;
    let c = server.client().await;
    assert_eq!(earliest(&c, "jobs").await, 2);
    let got = c.pull("w", 10, Duration::from_secs(1)).await.unwrap();
    assert_eq!(
        got.iter().map(|r| r.offset).collect::<Vec<_>>(),
        [2, 3],
        "acked jobs are gone, the rest are still queued"
    );
}

#[tokio::test]
async fn retention_policy_and_compaction_do_not_mix() {
    let server = TestServer::start().await;
    let c = server.client().await;
    let err = c
        .create_stream(StreamSpec {
            compaction: true,
            limits: StreamLimits {
                retention: RetentionPolicy::WorkQueue,
                ..Default::default()
            },
            ..StreamSpec::named("x")
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::BAD_REQUEST));
}
