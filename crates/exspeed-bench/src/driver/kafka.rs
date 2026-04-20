use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::driver::exspeed::{ConsumerStats, ProducerStats};

const PUBLISH_TS_HEADER: &str = "bench.publish_us";

/// Auto-create a Kafka topic by sending one throwaway message.
/// Requires `auto.create.topics.enable=true` on the broker (docker-compose default).
pub async fn ensure_topic(brokers: &str, topic: &str) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("ensure_topic: create producer")?;

    let record: FutureRecord<str, str> = FutureRecord::to(topic)
        .payload("")
        .key("init");

    producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(e, _)| anyhow::anyhow!("ensure_topic: send failed: {e}"))?;

    Ok(())
}

/// Spawn `tasks` producer tasks. Each owns one FutureProducer and publishes
/// `payload_bytes`-sized records at unlimited rate for `duration`.
pub async fn run_producer(
    brokers: &str,
    topic: &str,
    payload_bytes: usize,
    duration: Duration,
    tasks: usize,
    origin: Instant,
    shared_count: Arc<AtomicU64>,
) -> Result<ProducerStats> {
    let payload: Vec<u8> = vec![b'x'; payload_bytes];
    let start = Instant::now();
    let mut handles = Vec::with_capacity(tasks);

    for _ in 0..tasks {
        let brokers = brokers.to_owned();
        let topic = topic.to_owned();
        let payload = payload.clone();
        let shared_count = shared_count.clone();

        handles.push(tokio::spawn(async move {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set("message.timeout.ms", "5000")
                // Linger + batch size for max throughput (mirrors producer tuning docs)
                .set("linger.ms", "5")
                .set("batch.size", "131072")
                .create()
                .context("run_producer: create FutureProducer")?;

            let deadline = Instant::now() + duration;
            let mut local: u64 = 0;

            while Instant::now() < deadline {
                let us = origin.elapsed().as_micros() as u64;
                let ts_str = us.to_string();
                let headers = OwnedHeaders::new()
                    .insert(rdkafka::message::Header {
                        key: PUBLISH_TS_HEADER,
                        value: Some(ts_str.as_bytes()),
                    });

                let record: FutureRecord<str, [u8]> = FutureRecord::to(&topic)
                    .payload(payload.as_slice())
                    .headers(headers);

                producer
                    .send(record, Duration::ZERO)
                    .await
                    .map_err(|(e, _)| anyhow::anyhow!("producer send: {e}"))?;

                local += 1;
                if local % 256 == 0 {
                    shared_count.fetch_add(256, Ordering::Relaxed);
                }
            }
            shared_count.fetch_add(local % 256, Ordering::Relaxed);
            Ok::<u64, anyhow::Error>(local)
        }));
    }

    let mut total: u64 = 0;
    for h in handles {
        total += h.await??;
    }
    let wall_secs = start.elapsed().as_secs_f64();

    Ok(ProducerStats {
        messages: total,
        bytes: total * payload_bytes as u64,
        wall_secs,
    })
}

/// Subscribe to `topic` from the latest offset, receive messages for `duration`,
/// extract `bench.publish_us` header, and record end-to-end latency in microseconds.
pub async fn run_consumer(
    brokers: &str,
    topic: &str,
    group: &str,
    duration: Duration,
    origin: Instant,
) -> Result<ConsumerStats> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .set("session.timeout.ms", "6000")
        .create()
        .context("run_consumer: create StreamConsumer")?;

    consumer
        .subscribe(&[topic])
        .context("run_consumer: subscribe")?;

    let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
        .expect("histogram bounds");
    let mut messages: u64 = 0;
    let deadline = Instant::now() + duration;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let msg = match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(anyhow::anyhow!("kafka consumer error: {e}")),
            Err(_) => break, // deadline reached
        };

        // Extract bench.publish_us header
        if let Some(headers) = msg.headers() {
            for i in 0..headers.count() {
                let h = headers.get(i);
                if h.key == PUBLISH_TS_HEADER {
                    if let Some(value_bytes) = h.value {
                        if let Ok(ts_str) = std::str::from_utf8(value_bytes) {
                            if let Ok(sent_us) = ts_str.parse::<u64>() {
                                let now_us = origin.elapsed().as_micros() as u64;
                                if now_us > sent_us {
                                    let _ = hist.record(now_us - sent_us);
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }

        messages += 1;
    }

    Ok(ConsumerStats {
        messages,
        latency_histogram: hist,
    })
}
