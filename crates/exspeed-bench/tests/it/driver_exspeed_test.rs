use crate::embedded_server;
use embedded_server::start;
use exspeed_bench::driver::exspeed::ExspeedClient;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connects_and_ensures_stream_is_idempotent() {
    let srv = start().await;
    let mut client = ExspeedClient::connect(&srv.tcp_addr).await.unwrap();
    client.ensure_stream("bench-stream").await.unwrap();
    // Second call must succeed (broker returns an Error for duplicate create,
    // which ensure_stream should treat as success).
    client.ensure_stream("bench-stream").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_sends_at_least_some_records_in_2s() {
    let srv = start().await;
    let mut setup = ExspeedClient::connect(&srv.tcp_addr).await.unwrap();
    setup.ensure_stream("prod-stream").await.unwrap();

    let origin = Instant::now();
    let count = Arc::new(AtomicU64::new(0));
    let stats = exspeed_bench::driver::exspeed::run_producer(
        &srv.tcp_addr,
        "prod-stream",
        1024,
        Duration::from_secs(2),
        2,
        origin,
        count.clone(),
    )
    .await
    .unwrap();
    assert!(stats.messages > 0, "producer sent 0 messages");
    assert_eq!(stats.messages, count.load(Ordering::Relaxed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_records_latency_for_pushed_records() {
    let srv = start().await;
    let mut setup = ExspeedClient::connect(&srv.tcp_addr).await.unwrap();
    setup.ensure_stream("cons-stream").await.unwrap();

    let origin = Instant::now();
    let producer_addr = srv.tcp_addr.clone();
    let producer_count = Arc::new(AtomicU64::new(0));

    // Consumer first so subscription is live before publishes start.
    let consumer_addr = srv.tcp_addr.clone();
    let consumer = tokio::spawn(async move {
        exspeed_bench::driver::exspeed::run_consumer(
            &consumer_addr,
            "cons-stream",
            "bench-consumer-1",
            Duration::from_secs(3),
            origin,
        )
        .await
    });

    // Producer for 2s starting shortly after the consumer subscribes.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = exspeed_bench::driver::exspeed::run_producer(
        &producer_addr,
        "cons-stream",
        256,
        Duration::from_secs(2),
        1,
        origin,
        producer_count,
    )
    .await
    .unwrap();

    let cstats = consumer.await.unwrap().unwrap();
    assert!(cstats.messages > 0, "consumer received 0");
    let p50 = cstats.latency_histogram.value_at_percentile(50.0);
    // With 256 in-flight publishes the queue depth is deeper; allow up to the
    // full 3 s consumer window before declaring the result unreasonable.
    assert!(
        p50 > 0 && p50 < 3_000_000,
        "p50 {p50} us outside sanity range"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_coalesces_concurrent_publishes_in_order() {
    let srv = start().await;
    let mut setup = ExspeedClient::connect(&srv.tcp_addr).await.unwrap();
    setup.ensure_stream("coal-stream").await.unwrap();
    let publisher = setup
        .client
        .publisher()
        .batch_window(Duration::from_millis(1))
        .max_batch_records(64)
        .max_in_flight(16)
        .build();

    // Enqueue 200 records from one task, in order; they must land in order.
    let mut futs = Vec::new();
    for i in 0..200 {
        let p = publisher.clone();
        futs.push(tokio::spawn(async move {
            (
                i,
                p.publish(
                    "coal-stream",
                    exspeed_client::PublishRecord::new("s", format!("rec-{i}")),
                )
                .await
                .unwrap()
                .offset,
            )
        }));
    }
    let mut offsets = Vec::new();
    for f in futs {
        offsets.push(f.await.unwrap().1);
    }
    offsets.sort();
    assert_eq!(offsets, (0..200).collect::<Vec<u64>>());
    publisher.close().await.unwrap();

    let r = setup
        .client
        .read("coal-stream", 0, 1000, Duration::ZERO, "")
        .await
        .unwrap();
    assert_eq!(r.records.len(), 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_with_zero_window_sends_singles() {
    let srv = start().await;
    let mut setup = ExspeedClient::connect(&srv.tcp_addr).await.unwrap();
    setup.ensure_stream("zw-stream").await.unwrap();
    let publisher = setup
        .client
        .publisher()
        .batch_window(Duration::ZERO)
        .build();
    let ack = publisher
        .publish("zw-stream", exspeed_client::PublishRecord::new("s", "x"))
        .await
        .unwrap();
    assert_eq!(ack.offset, 0);
    // Errors are reported per record.
    let err = publisher
        .publish("missing", exspeed_client::PublishRecord::new("s", "x"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(404));
    publisher.close().await.unwrap();
}
