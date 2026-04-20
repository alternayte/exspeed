use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use anyhow::Result;

use crate::driver::Target;
use crate::profile::Profile;
use crate::report::PublishResult;
use crate::scenarios::dispatch;

pub async fn run(target: Target, addr: &str, profile: &Profile) -> Result<Vec<PublishResult>> {
    let mut out = Vec::with_capacity(profile.publish_payload_sizes.len());

    for &payload_bytes in &profile.publish_payload_sizes {
        let stream = format!("bench-publish-{payload_bytes}");
        dispatch::ensure_stream(target, addr, &stream).await?;

        let origin = Instant::now();
        let shared = Arc::new(AtomicU64::new(0));
        let stats = dispatch::run_producer(
            target,
            addr,
            &stream,
            payload_bytes,
            profile.publish_duration,
            4,
            origin,
            shared,
        )
        .await?;

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
