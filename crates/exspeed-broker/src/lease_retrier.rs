//! Periodic task that re-attempts ownership of unheld leases for the
//! connector and query registries. Runs every TTL/3 (default 10s when TTL
//! is the default 30s). This is the failover mechanism: when a pod's lease
//! expires, peers pick up the work on the next tick.
//!
//! The retrier is backend-agnostic — it only calls into
//! [`LeaseRetrierTarget`]. Concrete impls live in `exspeed-connectors` and
//! `exspeed-processing` (continuous queries).

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{interval, MissedTickBehavior};
use tracing::trace;

/// Interface the retrier calls into. Implemented by `ConnectorManager` and
/// `ExqlEngine`. Kept as a trait so the retrier does not need to depend on
/// those crates (avoids circular dependencies with exspeed-broker).
#[async_trait::async_trait]
pub trait LeaseRetrierTarget: Send + Sync {
    /// Attempt to acquire leases for any resources this pod is not
    /// currently running. Called once per tick. Implementations must be
    /// idempotent and cheap when there's nothing to do.
    async fn attempt_acquire_unheld(&self);

    /// Short static name for logging (e.g. "connectors", "queries").
    fn name(&self) -> &'static str;
}

/// Spawn a background task that ticks every `TTL/3` (min 1s) and calls
/// [`LeaseRetrierTarget::attempt_acquire_unheld`] on each target. The
/// task runs until the process exits.
///
/// Callers typically gate this behind `lease.supports_coordination()` —
/// the Noop backend doesn't need a retrier because it always grants.
pub fn spawn_lease_retrier(targets: Vec<Arc<dyn LeaseRetrierTarget>>) {
    let tick = crate::lease::ttl_from_env() / 3;
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(tick.as_secs().max(1)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await; // skip t=0 so the first real tick is a full period in
        loop {
            ticker.tick().await;
            for t in &targets {
                trace!(target = t.name(), "lease retrier tick");
                t.attempt_acquire_unheld().await;
            }
        }
    });
}
