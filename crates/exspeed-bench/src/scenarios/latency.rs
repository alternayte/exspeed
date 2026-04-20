use std::time::Instant;

use anyhow::Result;

use crate::driver::Target;
use crate::profile::Profile;
use crate::report::{LatencyPercentiles, LatencyResult};
use crate::scenarios::dispatch;

pub async fn run(target: Target, addr: &str, profile: &Profile) -> Result<LatencyResult> {
    let stream = "bench-latency";
    let payload_bytes: usize = 1024;

    dispatch::ensure_stream(target, addr, stream).await?;

    let origin = Instant::now();
    let consumer_addr = addr.to_owned();
    let duration = profile.latency_duration;

    // Start the consumer first so the subscription is live before publishes start.
    let consumer = tokio::spawn(async move {
        dispatch::run_consumer(
            target,
            &consumer_addr,
            stream,
            "bench-latency-consumer",
            duration + std::time::Duration::from_millis(500),
            origin,
        )
        .await
    });
    // Small delay so the consumer has subscribed and the broker will start
    // delivering records as soon as they are published.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let _producer = dispatch::run_producer_at_rate(
        target,
        addr,
        stream,
        payload_bytes,
        duration,
        profile.latency_target_rate,
        origin,
    )
    .await?;

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
