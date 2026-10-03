//! `ClusterLeadership` competes for the `cluster:leader` lease and drives
//! this node's role:
//!
//! * **follower** — not the holder. The [`RoleHooks`] keep a replica of the
//!   leader's log (see `crate::cluster`).
//! * **promoting** — the lease was just acquired. [`RoleHooks::promote`]
//!   stops the follower, stamps the new epoch on every stream and rebuilds
//!   the dedup state. Writes are still closed.
//! * **leader** — `is_leader` is `true`: the write path is open and leader
//!   work (consumers, connectors, continuous queries, retention) runs under
//!   [`ClusterLeadership::current_child_token`].
//!
//! Losing the lease (the heartbeat found another holder or missed its local
//! deadline) cancels the leader token, closes writes and hands the node back
//! to the follower.
//!
//! Standbys poll the lease record every heartbeat interval, so they know the
//! current leader's endpoints (for client redirects) and take over as soon
//! as the lease expires.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{watch, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::lease::{self, AcquireRequest, Heartbeat, LeaderLease, LeaseGuard, LeaseRecord};
use exspeed_common::Metrics;

pub use crate::lease::CLUSTER_LEASE as LEASE_NAME;

/// Called on role changes. Implemented by the replication layer.
#[async_trait]
pub trait RoleHooks: Send + Sync {
    /// This node is (or became) a follower of whoever holds the lease.
    async fn follow(&self);
    /// The lease was acquired. Prepare to lead: stop following, fence the
    /// log with the new epoch, rebuild dedup state. An error releases the
    /// lease again.
    async fn promote(&self, lease: &LeaseRecord) -> Result<(), String>;
    /// Leadership ended (the leader token is already cancelled and writes
    /// are closed).
    async fn demoted(&self);
}

/// Settings for [`ClusterLeadership::start`].
#[derive(Debug, Clone)]
pub struct LeadershipOptions {
    /// This node's stable id (persisted in the data dir by the server).
    pub node_id: String,
    pub ttl: Duration,
    pub heartbeat: Duration,
    /// Where followers replicate from this node.
    pub replication_endpoint: Option<String>,
    /// Where clients reach this node (sent to clients as a leader hint).
    pub client_endpoint: Option<String>,
    /// Only take the lease when this node is in the published ISR (or the
    /// ISR is empty). Disabling it allows unclean elections.
    pub require_isr: bool,
}

impl LeadershipOptions {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            ttl: Duration::from_secs(15),
            heartbeat: Duration::from_secs(3),
            replication_endpoint: None,
            client_endpoint: None,
            require_isr: true,
        }
    }
}

/// Handle to the leadership state machine. Clone freely.
#[derive(Clone)]
pub struct ClusterLeadership {
    /// `true` while this node is the leader and accepts writes.
    pub is_leader: watch::Receiver<bool>,
    /// This node's stable id.
    pub node_id: String,
    inner: Arc<Inner>,
}

struct Inner {
    lease: Arc<dyn LeaderLease>,
    metrics: Arc<Metrics>,
    opts: LeadershipOptions,
    hooks: Option<Arc<dyn RoleHooks>>,
    is_leader_tx: watch::Sender<bool>,
    /// The latest lease record seen: ours while leading, the leader's
    /// while following.
    known_tx: watch::Sender<Option<LeaseRecord>>,
    guard: Mutex<Option<LeaseGuard>>,
    /// Rotated on each promotion; pre-cancelled while not leader.
    current_token: Mutex<CancellationToken>,
    /// Our epoch while leader; 0 otherwise.
    epoch: AtomicU64,
    resigned: AtomicBool,
    /// Serializes promotion, demotion and resignation.
    transition: Mutex<()>,
}

impl ClusterLeadership {
    /// Convenience for tests and single-node setups: random node id, default
    /// timing, no role hooks.
    pub async fn spawn(
        lease: Arc<dyn LeaderLease>,
        metrics: Arc<Metrics>,
        replication_endpoint: Option<String>,
    ) -> Self {
        let mut opts = LeadershipOptions::new(uuid::Uuid::new_v4().to_string());
        opts.replication_endpoint = replication_endpoint;
        Self::start(lease, metrics, opts, None)
    }

    /// Start competing for the lease.
    pub fn start(
        lease: Arc<dyn LeaderLease>,
        metrics: Arc<Metrics>,
        opts: LeadershipOptions,
        hooks: Option<Arc<dyn RoleHooks>>,
    ) -> Self {
        let (is_leader_tx, is_leader_rx) = watch::channel(false);
        let (known_tx, _) = watch::channel(None);
        let node_id = opts.node_id.clone();
        let inner = Arc::new(Inner {
            lease,
            metrics,
            opts,
            hooks,
            is_leader_tx,
            known_tx,
            guard: Mutex::new(None),
            current_token: Mutex::new({
                let t = CancellationToken::new();
                t.cancel();
                t
            }),
            epoch: AtomicU64::new(0),
            resigned: AtomicBool::new(false),
            transition: Mutex::new(()),
        });
        tokio::spawn(run_loop(inner.clone()));
        Self {
            is_leader: is_leader_rx,
            node_id,
            inner,
        }
    }

    /// Step down for good (graceful shutdown): stop competing, cancel the
    /// leader token, close writes and release the lease so a peer can take
    /// over at once instead of waiting out the TTL.
    pub async fn resign(&self) {
        self.inner.resigned.store(true, Ordering::SeqCst);
        let _t = self.inner.transition.lock().await;
        self.inner.current_token.lock().await.cancel();
        let had = self.inner.guard.lock().await.take().is_some();
        let _ = self.inner.is_leader_tx.send(false);
        self.inner.epoch.store(0, Ordering::SeqCst);
        if had {
            self.inner.metrics.set_is_leader(false);
            self.inner.metrics.set_lease_held(LEASE_NAME, false);
            self.inner.metrics.record_leader_transition("resigned");
            info!(node = %self.node_id, "cluster:leader released (shutdown)");
        }
    }

    /// Whether this node is currently the leader (writes open).
    pub fn is_currently_leader(&self) -> bool {
        *self.is_leader.borrow()
    }

    /// Our leader epoch, or 0 when not leading.
    pub fn epoch(&self) -> u64 {
        self.inner.epoch.load(Ordering::SeqCst)
    }

    /// A fresh child of the current leader token; already cancelled when
    /// this node isn't the leader.
    pub async fn current_child_token(&self) -> CancellationToken {
        self.inner.current_token.lock().await.child_token()
    }

    /// The latest lease record seen (ours while leading).
    pub fn lease_record(&self) -> Option<LeaseRecord> {
        self.inner.known_tx.borrow().clone()
    }

    /// Subscribe to lease record updates.
    pub fn watch_lease(&self) -> watch::Receiver<Option<LeaseRecord>> {
        self.inner.known_tx.subscribe()
    }

    /// Client endpoint of the current leader when that is another live node
    /// — the hint sent to clients that reach a follower.
    pub fn leader_hint(&self) -> Option<String> {
        if self.is_currently_leader() {
            return None;
        }
        let rec = self.lease_record()?;
        if rec.holder == self.node_id || !rec.is_live() {
            return None;
        }
        rec.client_endpoint
    }

    /// The lease backend.
    pub fn lease(&self) -> &Arc<dyn LeaderLease> {
        &self.inner.lease
    }

    pub fn options(&self) -> &LeadershipOptions {
        &self.inner.opts
    }
}

fn heartbeat(opts: &LeadershipOptions) -> Heartbeat {
    Heartbeat::new(opts.ttl, opts.heartbeat)
}

async fn run_loop(inner: Arc<Inner>) {
    let coordinated = inner.lease.supports_coordination();
    if coordinated {
        if let Some(h) = &inner.hooks {
            h.follow().await;
        }
    }
    let hb = heartbeat(&inner.opts);
    let mut tick = tokio::time::interval(hb.interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if inner.resigned.load(Ordering::SeqCst) {
            return;
        }
        if inner.guard.lock().await.is_some() {
            continue; // leading; the guard's heartbeat keeps the lease
        }
        // Standby: look at the record first so we know the leader, and only
        // try to acquire when it looks free (or is ours from before a
        // restart).
        if coordinated {
            match inner.lease.get(LEASE_NAME).await {
                Ok(rec) => {
                    let free = rec
                        .as_ref()
                        .is_none_or(|r| !r.is_live() || r.holder == inner.opts.node_id);
                    inner.known_tx.send_replace(rec);
                    if !free {
                        inner
                            .metrics
                            .record_lease_acquire_attempt(LEASE_NAME, "rejected");
                        continue;
                    }
                }
                Err(e) => {
                    debug!(error = %e, "lease backend unavailable");
                    continue;
                }
            }
        }
        let req = AcquireRequest {
            name: LEASE_NAME.to_string(),
            holder: inner.opts.node_id.clone(),
            ttl: inner.opts.ttl,
            replication_endpoint: inner.opts.replication_endpoint.clone(),
            client_endpoint: inner.opts.client_endpoint.clone(),
            require_isr: inner.opts.require_isr,
        };
        match lease::acquire(inner.lease.clone(), &req, hb).await {
            Ok(Some(guard)) => promote(&inner, guard).await,
            Ok(None) => {
                inner
                    .metrics
                    .record_lease_acquire_attempt(LEASE_NAME, "rejected");
                debug!("cluster:leader held by another node (or not in the ISR)");
            }
            Err(e) => {
                inner
                    .metrics
                    .record_lease_acquire_attempt(LEASE_NAME, "error");
                warn!(error = %e, "cluster:leader acquire failed");
            }
        }
    }
}

async fn promote(inner: &Arc<Inner>, guard: LeaseGuard) {
    let _t = inner.transition.lock().await;
    if inner.resigned.load(Ordering::SeqCst) {
        return; // dropping the guard releases the lease
    }
    let record = guard.record.clone();
    let mut on_lost = guard.on_lost.clone();
    inner
        .metrics
        .record_lease_acquire_attempt(LEASE_NAME, "acquired");
    info!(node = %inner.opts.node_id, epoch = record.epoch, "cluster:leader acquired; promoting");
    inner.known_tx.send_replace(Some(record.clone()));

    if let Some(h) = &inner.hooks {
        let res = tokio::select! {
            r = h.promote(&record) => r,
            _ = on_lost.wait_for(|&l| l) => Err("lease lost during promotion".into()),
        };
        if let Err(e) = res {
            error!(error = %e, "promotion failed; releasing the lease");
            drop(guard);
            h.follow().await;
            return;
        }
    }

    let token = CancellationToken::new();
    *inner.current_token.lock().await = token.clone();
    inner.epoch.store(record.epoch, Ordering::SeqCst);
    *inner.guard.lock().await = Some(guard);
    inner.metrics.set_is_leader(true);
    inner.metrics.set_lease_held(LEASE_NAME, true);
    inner.metrics.record_leader_transition("acquired");
    let _ = inner.is_leader_tx.send(true);
    info!(node = %inner.opts.node_id, epoch = record.epoch, role = "leader", "this node is now the leader");

    let inner2 = inner.clone();
    tokio::spawn(async move {
        let _ = on_lost.wait_for(|&l| l).await;
        demote(inner2, record.epoch).await;
    });
}

async fn demote(inner: Arc<Inner>, epoch: u64) {
    let _t = inner.transition.lock().await;
    if inner.resigned.load(Ordering::SeqCst) || inner.epoch.load(Ordering::SeqCst) != epoch {
        return;
    }
    inner.current_token.lock().await.cancel();
    let _ = inner.is_leader_tx.send(false);
    inner.epoch.store(0, Ordering::SeqCst);
    *inner.guard.lock().await = None;
    inner.metrics.set_is_leader(false);
    inner.metrics.set_lease_held(LEASE_NAME, false);
    inner.metrics.record_leader_transition("lost");
    inner.metrics.record_lease_lost(LEASE_NAME);
    warn!(node = %inner.opts.node_id, epoch, role = "follower", "cluster:leader lost; this node is now a follower");
    if let Some(h) = &inner.hooks {
        h.demoted().await;
        h.follow().await;
    }
}
