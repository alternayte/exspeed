mod embedded_server;
use embedded_server::start;
use exspeed_bench::driver::exspeed::ExspeedClient;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[tokio::test]
async fn connects_and_ensures_stream_is_idempotent() {
    let srv = start().await;
    let mut client = ExspeedClient::connect(&srv.tcp_addr).await.unwrap();
    client.ensure_stream("bench-stream").await.unwrap();
    // Second call must succeed (broker returns an Error for duplicate create,
    // which ensure_stream should treat as success).
    client.ensure_stream("bench-stream").await.unwrap();
}

#[tokio::test]
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
    ).await.unwrap();
    assert!(stats.messages > 0, "producer sent 0 messages");
    assert_eq!(stats.messages, count.load(Ordering::Relaxed));
}

#[tokio::test]
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
        ).await
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
    ).await.unwrap();

    let cstats = consumer.await.unwrap().unwrap();
    assert!(cstats.messages > 0, "consumer received 0");
    let p50 = cstats.latency_histogram.value_at_percentile(50.0);
    assert!(p50 > 0 && p50 < 1_000_000, "p50 {p50} us outside sanity range");
}
