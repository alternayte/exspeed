use std::time::Instant;

use anyhow::Result;

use crate::driver::exspeed::{self, ExspeedClient};
use crate::profile::Profile;
use crate::report::FanoutResult;

pub async fn run(addr: &str, profile: &Profile) -> Result<Vec<FanoutResult>> {
    let mut out = Vec::with_capacity(profile.fanout_consumer_counts.len());
    let payload_bytes: usize = 1024;

    for &n in &profile.fanout_consumer_counts {
        // Fresh stream per rung so consumer offsets start from zero.
        let stream = format!("bench-fanout-{n}");
        let mut setup = ExspeedClient::connect(addr).await?;
        setup.ensure_stream(&stream).await?;

        let origin = Instant::now();
        let duration = profile.fanout_duration;

        // Spawn N consumers, each as its own subscription.
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let consumer_addr = addr.to_owned();
            let consumer_name = format!("bench-fanout-c-{n}-{i}");
            let s = stream.clone();
            handles.push(tokio::spawn(async move {
                exspeed::run_consumer(
                    &consumer_addr,
                    &s,
                    &consumer_name,
                    duration + std::time::Duration::from_millis(500),
                    origin,
                ).await
            }));
        }

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        let pstats = exspeed::run_producer_at_rate(
            addr,
            &stream,
            payload_bytes,
            duration,
            profile.fanout_producer_rate,
            origin,
        ).await?;

        let mut total_received: u64 = 0;
        let mut max_lag: u64 = 0;
        for h in handles {
            let c = h.await??;
            total_received += c.messages;
            let expected = pstats.messages;
            let lag = expected.saturating_sub(c.messages);
            if lag > max_lag {
                max_lag = lag;
            }
        }
        let aggregate_consumer_rate = total_received as f64 / pstats.wall_secs;

        out.push(FanoutResult {
            consumers: n,
            producer_rate: profile.fanout_producer_rate,
            aggregate_consumer_rate,
            max_lag_msgs: max_lag,
        });
    }

    Ok(out)
}
