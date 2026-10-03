//! Exspeed benchmark driver, built on `exspeed-client`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use hdrhistogram::Histogram;

use exspeed_client::{
    code, Client, ConnectOptions, ConsumerSpec, DeliverPolicy, PublishRecord, StreamSpec,
};

pub const PUBLISH_TS_HEADER: &str = "bench.publish_us";

/// Setup connection used by scenarios (stream creation etc.).
pub struct ExspeedClient {
    pub client: Client,
}

impl ExspeedClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let client = Client::connect(addr, ConnectOptions::default().client_id("exspeed-bench"))
            .await
            .context("connect")?;
        Ok(Self { client })
    }

    /// Create the stream if it doesn't exist (any existing config is kept).
    pub async fn ensure_stream(&mut self, name: &str) -> Result<()> {
        match self.client.create_stream(StreamSpec::named(name)).await {
            Ok(()) => Ok(()),
            Err(e) if e.code() == Some(code::CONFLICT) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

fn bench_record(payload: &Bytes, origin: Instant) -> PublishRecord {
    let us = origin.elapsed().as_micros() as u64;
    PublishRecord::new("bench", payload.clone()).header(PUBLISH_TS_HEADER, us.to_string())
}

pub struct ProducerStats {
    pub messages: u64,
    pub bytes: u64,
    pub wall_secs: f64,
}

/// `tasks` producer loops sharing one coalescing publisher, each keeping 64
/// publishes in flight. `origin` is the shared start instant for the
/// publish-time header so consumers can compute end-to-end latency.
pub async fn run_producer(
    addr: &str,
    stream: &str,
    payload_bytes: usize,
    duration: Duration,
    tasks: usize,
    origin: Instant,
    shared_count: Arc<AtomicU64>,
) -> Result<ProducerStats> {
    let payload = Bytes::from(vec![b'x'; payload_bytes]);
    let client = ExspeedClient::connect(addr).await?.client;
    let publisher = client
        .publisher()
        .max_batch_records(512)
        .batch_window(Duration::from_micros(100))
        .max_in_flight(8192)
        .build();
    let start = Instant::now();
    let in_flight_per_task = 64;
    let mut handles = Vec::with_capacity(tasks);
    for _ in 0..tasks {
        let publisher = publisher.clone();
        let stream = stream.to_string();
        let payload = payload.clone();
        let shared_count = shared_count.clone();
        handles.push(tokio::spawn(async move {
            let deadline = Instant::now() + duration;
            let mut local: u64 = 0;
            let mut in_flight = FuturesUnordered::new();
            loop {
                while in_flight.len() < in_flight_per_task && Instant::now() < deadline {
                    let p = publisher.clone();
                    let rec = bench_record(&payload, origin);
                    let s = stream.clone();
                    in_flight.push(async move { p.publish(&s, rec).await });
                }
                match in_flight.next().await {
                    Some(r) => {
                        r?;
                        local += 1;
                        shared_count.fetch_add(1, Ordering::Relaxed);
                    }
                    None => break,
                }
            }
            Ok::<u64, anyhow::Error>(local)
        }));
    }
    let mut total = 0;
    for h in handles {
        total += h.await??;
    }
    publisher.close().await.ok();
    Ok(ProducerStats {
        messages: total,
        bytes: total * payload_bytes as u64,
        wall_secs: start.elapsed().as_secs_f64(),
    })
}

pub async fn run_producer_at_rate(
    addr: &str,
    stream: &str,
    payload_bytes: usize,
    duration: Duration,
    rate_per_sec: u64,
    origin: Instant,
) -> Result<ProducerStats> {
    let payload = Bytes::from(vec![b'x'; payload_bytes]);
    let client = ExspeedClient::connect(addr).await?.client;
    let publisher = client
        .publisher()
        .batch_window(Duration::from_micros(100))
        .build();
    let interval_ns = 1_000_000_000u64 / rate_per_sec.max(1);
    let mut ticker = tokio::time::interval(Duration::from_nanos(interval_ns));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let start = Instant::now();
    let deadline = start + duration;
    let mut sent: u64 = 0;
    while Instant::now() < deadline {
        ticker.tick().await;
        let p = publisher.clone();
        let rec = bench_record(&payload, origin);
        let s = stream.to_string();
        // Fire and forget: the rate is set by the ticker, not by acks.
        tokio::spawn(async move {
            let _ = p.publish(&s, rec).await;
        });
        sent += 1;
    }
    publisher.close().await.ok();
    Ok(ProducerStats {
        messages: sent,
        bytes: sent * payload_bytes as u64,
        wall_secs: start.elapsed().as_secs_f64(),
    })
}

pub struct ConsumerStats {
    pub messages: u64,
    pub latency_histogram: Histogram<u64>,
}

/// Create (or reuse) consumer `consumer_name` delivering only new records,
/// subscribe, and record end-to-end latency (µs) until `duration` elapses.
/// Acks are batched and sent without waiting for replies.
pub async fn run_consumer(
    addr: &str,
    stream: &str,
    consumer_name: &str,
    duration: Duration,
    origin: Instant,
) -> Result<ConsumerStats> {
    let client = ExspeedClient::connect(addr).await?.client;
    let spec = ConsumerSpec {
        deliver: DeliverPolicy::New,
        max_ack_pending: 100_000,
        ..ConsumerSpec::new(consumer_name, stream)
    };
    match client.create_consumer(spec).await {
        Ok(_) => {}
        Err(e) if e.code() == Some(code::CONFLICT) => {}
        Err(e) => return Err(e.into()),
    }
    let mut sub = client.subscribe(consumer_name, 4096).await?;

    let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("histogram bounds");
    let mut messages = 0u64;
    let deadline = Instant::now() + duration;
    const ACK_BATCH: usize = 256;
    const ACK_WINDOW: Duration = Duration::from_millis(5);
    let mut pending_acks = Vec::with_capacity(ACK_BATCH);
    let mut last_ack = Instant::now();

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let Some(msg) = sub.next_timeout(remaining.min(ACK_WINDOW)).await else {
            if sub.end_reason().is_some() {
                break;
            }
            if !pending_acks.is_empty() {
                client
                    .ack_nowait(consumer_name, std::mem::take(&mut pending_acks))
                    .await?;
                last_ack = Instant::now();
            }
            continue;
        };
        if let Some(sent_us) = msg
            .record
            .headers
            .iter()
            .find(|(k, _)| k == PUBLISH_TS_HEADER)
            .and_then(|(_, v)| v.parse::<u64>().ok())
        {
            let now_us = origin.elapsed().as_micros() as u64;
            if now_us > sent_us {
                let _ = hist.record(now_us - sent_us);
            }
        }
        messages += 1;
        pending_acks.push(msg.record.offset);
        if pending_acks.len() >= ACK_BATCH || last_ack.elapsed() >= ACK_WINDOW {
            client
                .ack_nowait(consumer_name, std::mem::take(&mut pending_acks))
                .await?;
            last_ack = Instant::now();
        }
    }
    if !pending_acks.is_empty() {
        client.ack_nowait(consumer_name, pending_acks).await?;
    }
    Ok(ConsumerStats {
        messages,
        latency_histogram: hist,
    })
}
