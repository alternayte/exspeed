use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::driver::Target;
use crate::driver::exspeed::{ConsumerStats, ProducerStats};

pub async fn ensure_stream(target: Target, addr: &str, stream: &str) -> Result<()> {
    match target {
        Target::Exspeed => {
            let mut c = crate::driver::exspeed::ExspeedClient::connect(addr).await?;
            c.ensure_stream(stream).await
        }
        #[cfg(feature = "comparison")]
        Target::Kafka => crate::driver::kafka::ensure_topic(addr, stream).await,
    }
}

// dispatch layer: parameters mirror the underlying driver APIs; bundling
// them into a struct would only add ceremony for the routing match below.
#[allow(clippy::too_many_arguments)]
pub async fn run_producer(
    target: Target,
    addr: &str,
    stream: &str,
    payload_bytes: usize,
    duration: Duration,
    tasks: usize,
    origin: Instant,
    shared: Arc<AtomicU64>,
) -> Result<ProducerStats> {
    match target {
        Target::Exspeed => {
            crate::driver::exspeed::run_producer(
                addr, stream, payload_bytes, duration, tasks, origin, shared,
            )
            .await
        }
        #[cfg(feature = "comparison")]
        Target::Kafka => {
            crate::driver::kafka::run_producer(
                addr, stream, payload_bytes, duration, tasks, origin, shared,
            )
            .await
        }
    }
}

pub async fn run_producer_at_rate(
    target: Target,
    addr: &str,
    stream: &str,
    payload_bytes: usize,
    duration: Duration,
    rate: u64,
    origin: Instant,
) -> Result<ProducerStats> {
    match target {
        Target::Exspeed => {
            crate::driver::exspeed::run_producer_at_rate(
                addr, stream, payload_bytes, duration, rate, origin,
            )
            .await
        }
        #[cfg(feature = "comparison")]
        Target::Kafka => {
            let shared = Arc::new(AtomicU64::new(0));
            crate::driver::kafka::run_producer(
                addr, stream, payload_bytes, duration, 1, origin, shared,
            )
            .await
        }
    }
}

pub async fn run_consumer(
    target: Target,
    addr: &str,
    stream: &str,
    name: &str,
    duration: Duration,
    origin: Instant,
) -> Result<ConsumerStats> {
    match target {
        Target::Exspeed => {
            crate::driver::exspeed::run_consumer(addr, stream, name, duration, origin).await
        }
        #[cfg(feature = "comparison")]
        Target::Kafka => {
            crate::driver::kafka::run_consumer(addr, stream, name, duration, origin).await
        }
    }
}
