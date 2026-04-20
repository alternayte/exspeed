use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use anyhow::Result;

use crate::driver::exspeed::{self, ExspeedClient};
use crate::profile::Profile;
use crate::report::PublishResult;

pub async fn run(addr: &str, profile: &Profile) -> Result<Vec<PublishResult>> {
    let mut out = Vec::with_capacity(profile.publish_payload_sizes.len());

    for &payload_bytes in &profile.publish_payload_sizes {
        let stream = format!("bench-publish-{payload_bytes}");
        let mut setup = ExspeedClient::connect(addr).await?;
        setup.ensure_stream(&stream).await?;

        let origin = Instant::now();
        let shared = Arc::new(AtomicU64::new(0));
        let stats = exspeed::run_producer(
            addr,
            &stream,
            payload_bytes,
            profile.publish_duration,
            4,
            origin,
            shared,
        ).await?;

        let msg_per_sec = stats.messages as f64 / stats.wall_secs;
        let mb_per_sec = stats.bytes as f64 / stats.wall_secs / 1_000_000.0;

        out.push(PublishResult {
            payload_bytes,
            msg_per_sec,
            mb_per_sec,
            duration_sec: stats.wall_secs,
        });
    }

    Ok(out)
}
