//! `ClusterLeadership` — thin wrapper over the `LeaderLease` trait that
//! owns a single cluster-wide `cluster:leader` lease and exposes:
//!   - `is_leader: watch::Receiver<bool>` — subscribe to promotions /
//!     demotions.
//!   - `current_child_token()` — returns a fresh child of the current
//!     leader token. Pre-cancelled when we are not leader, so tasks
//!     awaiting it exit cleanly.
//!
//! The internal retry loop ticks every `TTL/3`. While holding the lease,
//! the underlying `LeaseGuard`'s own heartbeat task keeps the lease
//! alive; we observe its `on_lost` watcher to flip state on loss.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::lease::{LeaderLease, LeaseGuard};
use exspeed_common::Metrics;

const LEASE_NAME: &str = "cluster:leader";

/// Handle to the cluster-leader-lease state machine. Clone freely — all
/// internal state is `Arc`'d.
#[derive(Clone)]
pub struct ClusterLeadership {
    /// Subscribes to `false → true` on promotion and `true → false` on
    /// demotion. Callers should use `wait_for(|&v| v)` rather than busy
    /// looping.
    pub is_leader: watch::Receiver<bool>,
    /// This pod's stable identity for lease ownership. Emitted via
    /// `/healthz` and `/api/v1/leases` — not a secret.
    pub holder_id: Uuid,
    // Internal shared state.
    inner: Arc<Inner>,
}

struct Inner {
    lease: Arc<dyn LeaderLease>,
    metrics: Arc<Metrics>,
    is_leader_tx: watch::Sender<bool>,
    // Stores the currently-held guard while we are leader (`None` otherwise).
    // Held behind a `Mutex` because the background retry task and `demote`
    // both touch it.
    guard: Mutex<Option<LeaseGuard>>,
    // The live leader token. Rotated on each promotion so that callers
    // who used `current_child_token()` during a previous tenure don't
    // inherit the new one. Callers should always read via
    // `ClusterLeadership::current_child_token()`.
    current_token: Mutex<CancellationToken>,
    holder_id: Uuid,
}

impl ClusterLeadership {
    /// Spawn the leadership state machine and return a handle. The retry
    /// task runs in the background for the lifetime of the returned value
    /// (kept alive via `Arc`).
    pub async fn spawn(lease: Arc<dyn LeaderLease>, metrics: Arc<Metrics>) -> Self {
        let holder_id = Uuid::new_v4();
        let (is_leader_tx, is_leader_rx) = watch::channel(false);

        let inner = Arc::new(Inner {
            lease,
            metrics,
            is_leader_tx,
            guard: Mutex::new(None),
            // Pre-cancelled token so standbys that call
            // current_child_token() immediately receive a cancelled child.
            current_token: Mutex::new({
                let t = CancellationToken::new();
                t.cancel();
                t
            }),
            holder_id,
        });

        // Spawn the retry loop.
        let inner_for_task = Arc::clone(&inner);
        tokio::spawn(async move {
            run_retry_loop(inner_for_task).await;
        });

        Self {
            is_leader: is_leader_rx,
            holder_id,
            inner,
        }
    }

    /// Non-blocking snapshot of whether this pod is currently leader.
    pub fn is_currently_leader(&self) -> bool {
        *self.is_leader.borrow()
    }

    /// Return a fresh child of the *current* leader token. If this pod
    /// is not currently leader, the returned token is already cancelled,
    /// so tasks awaiting it will exit immediately — a safe default.
    /// Always prefer this over trying to hold a long-lived token
    /// reference across a demote/re-promote cycle.
    pub async fn current_child_token(&self) -> CancellationToken {
        let tok = self.inner.current_token.lock().await;
        tok.child_token()
    }
}

/// Periodic acquire + heartbeat-observation loop. Ticks every `TTL/3`.
/// Only spawned once per `ClusterLeadership`.
async fn run_retry_loop(inner: Arc<Inner>) {
    let ttl = crate::lease::ttl_from_env();
    let tick = std::cmp::max(ttl / 3, Duration::from_secs(1));
    let mut ticker = tokio::time::interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        // If we already hold leadership, skip re-acquire; the LeaseGuard's
        // own heartbeat task keeps the lease alive and fires on_lost on
        // failure. Demotion is handled via the watcher task below.
        {
            let guard = inner.guard.lock().await;
            if guard.is_some() {
                continue;
            }
        }

        // Not currently leader — attempt to acquire.
        // Plan G: `None` — this pod has no replication endpoint to advertise
        // yet. Extended to `Some(endpoint)` in the follow-on Task 13 commit.
        match inner.lease.try_acquire(LEASE_NAME, ttl, None).await {
            Ok(Some(lg)) => {
                // Install fresh token BEFORE signalling is_leader, so
                // subscribers that wake up on is_leader=true and call
                // current_child_token() see the new token, not the
                // pre-cancelled one.
                let new_token = CancellationToken::new();
                {
                    let mut tok = inner.current_token.lock().await;
                    *tok = new_token.clone();
                }

                // Clone on_lost before we move lg into the guard slot.
                let mut on_lost = lg.on_lost.clone();

                // Stash the guard. The LeaseGuard's own heartbeat task
                // keeps the lease alive while it's held here.
                {
                    let mut g = inner.guard.lock().await;
                    *g = Some(lg);
                }

                inner.metrics.set_is_leader(true);
                inner.metrics.set_lease_held(LEASE_NAME, true);
                inner.metrics.record_leader_transition("acquired");
                inner.metrics.record_lease_acquire_attempt(LEASE_NAME, "acquired");
                info!(
                    holder = %inner.holder_id,
                    role = "leader",
                    "cluster:leader acquired — this pod is now the leader"
                );
                let _ = inner.is_leader_tx.send(true);

                // Hook up on_lost watcher: when the guard's heartbeat
                // fails, run demote() to cancel the token + clear state.
                let inner_for_watch = Arc::clone(&inner);
                tokio::spawn(async move {
                    let _ = on_lost.wait_for(|&v| v).await;
                    demote(inner_for_watch).await;
                });
            }
            Ok(None) => {
                inner.metrics.record_lease_acquire_attempt(LEASE_NAME, "rejected");
                debug!("cluster:leader held by another pod; staying standby");
            }
            Err(e) => {
                inner.metrics.record_lease_acquire_attempt(LEASE_NAME, "error");
                warn!(error = %e, "cluster:leader acquire failed");
            }
        }
    }
}

async fn demote(inner: Arc<Inner>) {
    // Cancel the current token (so background tasks exit), drop the
    // guard (so peers can acquire), flip metrics + watcher.
    let token = {
        let tok = inner.current_token.lock().await;
        tok.clone()
    };
    token.cancel();

    {
        let mut g = inner.guard.lock().await;
        *g = None;
    }

    inner.metrics.set_is_leader(false);
    inner.metrics.set_lease_held(LEASE_NAME, false);
    inner.metrics.record_leader_transition("lost");
    inner.metrics.record_lease_lost(LEASE_NAME);
    let _ = inner.is_leader_tx.send(false);
    warn!(
        holder = %inner.holder_id,
        role = "standby",
        "cluster:leader lost — this pod is now a standby"
    );
}
