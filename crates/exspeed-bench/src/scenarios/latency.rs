use std::time::Instant;

use anyhow::Result;

use crate::driver::exspeed::{self, ExspeedClient};
use crate::profile::Profile;
use crate::report::{LatencyPercentiles, LatencyResult};

pub async fn run(addr: &str, profile: &Profile) -> Result<LatencyResult> {
    let stream = "bench-latency";
    let payload_bytes: usize = 1024;

    let mut setup = ExspeedClient::connect(addr).await?;
    setup.ensure_stream(stream).await?;

    let origin = Instant::now();
    let consumer_addr = addr.to_owned();
    let duration = profile.latency_duration;

    // Start the consumer first so the subscription is live before publishes start.
    let consumer = tokio::spawn(async move {
        exspeed::run_consumer(
            &consumer_addr,
            stream,
            "bench-latency-consumer",
            duration + std::time::Duration::from_millis(500),
            origin,
        ).await
    });
    // Small delay so the consumer has subscribed and the broker will start
    // delivering records as soon as they are published.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let _producer = exspeed::run_producer_at_rate(
        addr,
        stream,
        payload_bytes,
        duration,
        profile.latency_target_rate,
        origin,
    ).await?;

    let cstats = consumer.await??;
    let h = &cstats.latency_histogram;
    let latency_us = LatencyPercentiles {
        p50: h.value_at_percentile(50.0),
        p90: h.value_at_percentile(90.0),
        p99: h.value_at_percentile(99.0),
        p999: h.value_at_percentile(99.9),
        p9999: h.value_at_percentile(99.99),
        max: h.max(),
    };

    Ok(LatencyResult {
        payload_bytes,
        target_rate: profile.latency_target_rate,
        duration_sec: duration.as_secs_f64(),
        latency_us,
    })
}
