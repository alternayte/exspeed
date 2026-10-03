//! The cluster-leader lease. Exactly one node of a cluster holds it; only
//! that node accepts writes and runs consumers, connectors and continuous
//! queries. The others replicate from it.
//!
//! A lease is one record per name, stored in a shared backend (Postgres or
//! Redis; an in-process [`memory`] backend for tests):
//!
//! * `holder` — the stable node id of the current (or last) holder;
//! * `epoch` — incremented on every acquisition. It is the fencing token:
//!   replication and stream epoch histories are keyed by it, so a deposed
//!   leader can never be mistaken for the current one;
//! * `expires_at` — backend clock; the lease is free once it passes;
//! * `replication_endpoint` / `client_endpoint` — where followers and clients
//!   reach the holder;
//! * `isr` — the in-sync replica set the holder last published. When the
//!   cluster requires it, only a member may acquire the lease next, so a node
//!   that is missing acknowledged writes can't become leader.
//!
//! Releasing a lease expires the record instead of deleting it, so its epoch
//! and ISR survive.
//!
//! Without a backend every node uses [`NoopLeaderLease`], which always grants
//! the lease: single-node mode.

pub mod memory;
pub mod noop;
pub mod postgres;
pub mod redis;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;
use tracing::{debug, info, warn};

pub use memory::MemoryLeaseBackend;
pub use noop::NoopLeaderLease;

/// The lease every cluster node competes for.
pub const CLUSTER_LEASE: &str = "cluster:leader";

#[derive(Debug, Clone, thiserror::Error)]
pub enum LeaseError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("lease backend call timed out")]
    Timeout,
}

/// A lease record as stored in the backend.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeaseRecord {
    pub name: String,
    /// Node id of the current (or, once expired, the last) holder.
    pub holder: String,
    /// Fencing token: incremented on every acquisition.
    pub epoch: u64,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    /// Where followers replicate from the holder (`host:port`).
    #[serde(default)]
    pub replication_endpoint: Option<String>,
    /// Where clients reach the holder (`host:port` of the client protocol).
    #[serde(default)]
    pub client_endpoint: Option<String>,
    /// In-sync replicas (node ids, the holder included) as last published by
    /// the holder. Empty means "unknown": anyone may acquire.
    #[serde(default)]
    pub isr: Vec<String>,
}

impl LeaseRecord {
    /// Whether the lease is still held at `now` (backend clock assumed close
    /// to ours; only used for display and hints, never for safety).
    pub fn is_live(&self) -> bool {
        self.expires_at > chrono::Utc::now()
    }
}

/// Parameters of an acquisition attempt.
#[derive(Debug, Clone)]
pub struct AcquireRequest {
    pub name: String,
    /// This node's stable id.
    pub holder: String,
    pub ttl: Duration,
    pub replication_endpoint: Option<String>,
    pub client_endpoint: Option<String>,
    /// Only acquire if the stored ISR is empty or contains `holder`.
    pub require_isr: bool,
}

impl AcquireRequest {
    pub fn new(name: &str, holder: &str, ttl: Duration) -> Self {
        Self {
            name: name.to_string(),
            holder: holder.to_string(),
            ttl,
            replication_endpoint: None,
            client_endpoint: None,
            require_isr: false,
        }
    }
}

/// Outcome of a refresh.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refresh {
    /// Still ours; the expiry was extended.
    Held,
    /// Another holder (or a newer epoch) owns the lease, or it expired.
    Lost,
}

/// A lease backend. Backends implement the atomic primitives; the heartbeat
/// and loss detection live in [`acquire`] and are the same for every backend.
///
/// Every mutating call is conditional on `(holder, epoch)`, so a node whose
/// lease was taken over can't extend, release or rewrite the new holder's
/// record.
#[async_trait]
pub trait LeaderLease: Send + Sync {
    /// Whether this backend coordinates across nodes. False for Noop.
    fn supports_coordination(&self) -> bool;

    /// Take the lease if it is free (expired, never held, or already held by
    /// `req.holder` — a restarted node may take its own lease back), subject
    /// to `require_isr`. On success the epoch is incremented and the new
    /// record returned. `Ok(None)` means someone else holds it, or the ISR
    /// check failed.
    async fn try_acquire(&self, req: &AcquireRequest) -> Result<Option<LeaseRecord>, LeaseError>;

    /// Extend the lease by `ttl` if `(holder, epoch)` still owns it.
    async fn refresh(
        &self,
        name: &str,
        holder: &str,
        epoch: u64,
        ttl: Duration,
    ) -> Result<Refresh, LeaseError>;

    /// Give the lease up: expire it now if `(holder, epoch)` still owns it.
    /// The record (epoch, ISR) is kept.
    async fn release(&self, name: &str, holder: &str, epoch: u64) -> Result<(), LeaseError>;

    /// Replace the stored ISR if `(holder, epoch)` still owns the lease.
    /// Returns `false` when it doesn't.
    async fn set_isr(
        &self,
        name: &str,
        holder: &str,
        epoch: u64,
        isr: &[String],
    ) -> Result<bool, LeaseError>;

    /// The record for `name`, live or expired. `None` if never acquired.
    async fn get(&self, name: &str) -> Result<Option<LeaseRecord>, LeaseError>;

    /// All live leases, for operators. Empty for Noop.
    async fn list_all(&self) -> Result<Vec<LeaseRecord>, LeaseError>;
}

/// Held by the lease owner. A heartbeat task keeps the lease alive while the
/// guard exists. Dropping the guard stops the heartbeat and releases the
/// lease (expiring it in the backend).
pub struct LeaseGuard {
    /// The record as acquired (epoch included).
    pub record: LeaseRecord,
    /// Flips to `true` once the lease is lost: the backend reported another
    /// holder, or the heartbeat couldn't refresh it before the local
    /// deadline. Never flips back.
    pub on_lost: watch::Receiver<bool>,
    _stop: oneshot::Sender<()>,
}

impl LeaseGuard {
    pub fn epoch(&self) -> u64 {
        self.record.epoch
    }

    pub fn is_lost(&self) -> bool {
        *self.on_lost.borrow()
    }
}

/// Heartbeat timing for [`acquire`].
#[derive(Debug, Clone, Copy)]
pub struct Heartbeat {
    pub ttl: Duration,
    /// How often the lease is refreshed. Must be well under `ttl`.
    pub interval: Duration,
}

impl Heartbeat {
    pub fn new(ttl: Duration, interval: Duration) -> Self {
        let interval = interval.min(ttl / 3).max(Duration::from_millis(10));
        Self { ttl, interval }
    }

    /// How long after the last successful refresh *was sent* the holder
    /// considers the lease lost. Two thirds of the TTL: the holder stops
    /// acting as leader well before the backend lets anyone else in, which
    /// covers clock-rate differences and in-flight requests.
    pub fn local_deadline(&self) -> Duration {
        self.ttl * 2 / 3
    }
}

/// Try to acquire a lease. On success returns a guard whose heartbeat task
/// keeps the lease alive.
///
/// Loss detection: the lease is reported lost on the first refresh that
/// finds another holder, or once [`Heartbeat::local_deadline`] passes since
/// the last refresh that succeeded (measured from when it was sent).
/// Failed refreshes are retried every `interval / 4`; each call is bounded by
/// the time left before the deadline.
pub async fn acquire(
    backend: Arc<dyn LeaderLease>,
    req: &AcquireRequest,
    hb: Heartbeat,
) -> Result<Option<LeaseGuard>, LeaseError> {
    let sent = Instant::now();
    let record = match tokio::time::timeout(hb.ttl / 3, backend.try_acquire(req)).await {
        Ok(r) => r?,
        Err(_) => return Err(LeaseError::Timeout),
    };
    let Some(record) = record else {
        return Ok(None);
    };
    let (lost_tx, lost_rx) = watch::channel(false);
    let (stop_tx, stop_rx) = oneshot::channel();
    if backend.supports_coordination() {
        tokio::spawn(heartbeat(
            backend,
            record.clone(),
            hb,
            sent,
            lost_tx,
            stop_rx,
        ));
    } else {
        // Noop: never lost; keep the sender alive with the task.
        tokio::spawn(async move {
            let _ = stop_rx.await;
            drop(lost_tx);
        });
    }
    Ok(Some(LeaseGuard {
        record,
        on_lost: lost_rx,
        _stop: stop_tx,
    }))
}

async fn heartbeat(
    backend: Arc<dyn LeaderLease>,
    record: LeaseRecord,
    hb: Heartbeat,
    acquired_sent: Instant,
    lost_tx: watch::Sender<bool>,
    mut stop: oneshot::Receiver<()>,
) {
    let name = record.name.clone();
    let holder = record.holder.clone();
    let epoch = record.epoch;
    let mut deadline = acquired_sent + hb.local_deadline();
    let mut next = Instant::now() + hb.interval;
    loop {
        tokio::select! {
            _ = &mut stop => {
                // Graceful release. Bounded so shutdown can't hang on a dead
                // backend; the lease then simply expires.
                match tokio::time::timeout(
                    Duration::from_secs(5),
                    backend.release(&name, &holder, epoch),
                ).await {
                    Ok(Ok(())) => info!(lease = %name, epoch, "lease released"),
                    Ok(Err(e)) => warn!(lease = %name, error = %e, "lease release failed"),
                    Err(_) => warn!(lease = %name, "lease release timed out"),
                }
                return;
            }
            _ = tokio::time::sleep_until(deadline) => {
                warn!(lease = %name, epoch, "lease not refreshed before the local deadline; stepping down");
                let _ = lost_tx.send(true);
                return;
            }
            _ = tokio::time::sleep_until(next) => {}
        }
        let sent = Instant::now();
        let budget = deadline.saturating_duration_since(sent);
        let res =
            tokio::time::timeout(budget, backend.refresh(&name, &holder, epoch, hb.ttl)).await;
        match res {
            Ok(Ok(Refresh::Held)) => {
                deadline = sent + hb.local_deadline();
                next = sent + hb.interval;
            }
            Ok(Ok(Refresh::Lost)) => {
                warn!(lease = %name, epoch, "lease taken over by another holder; stepping down");
                let _ = lost_tx.send(true);
                return;
            }
            Ok(Err(e)) => {
                debug!(lease = %name, error = %e, "lease refresh failed; retrying");
                next = Instant::now() + hb.interval / 4;
            }
            Err(_) => {
                debug!(lease = %name, "lease refresh timed out; retrying");
                next = Instant::now() + hb.interval / 4;
            }
        }
    }
}

/// Explicit lease settings (the server's `[cluster]` config section).
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// `none`, `postgres`, `redis` or `memory` (in-process, tests only).
    pub backend: String,
    pub postgres_url: Option<String>,
    pub postgres_schema: String,
    pub redis_url: Option<String>,
    pub redis_key_prefix: String,
    /// Namespace for the `memory` backend: nodes in one process that use
    /// the same namespace share a lease table.
    pub memory_namespace: String,
    /// Per-call timeout for backend requests.
    pub call_timeout: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            backend: "none".into(),
            postgres_url: None,
            postgres_schema: "public".into(),
            redis_url: None,
            redis_key_prefix: "exspeed:lease:".into(),
            memory_namespace: "default".into(),
            call_timeout: Duration::from_secs(5),
        }
    }
}

impl LeaseConfig {
    /// Settings from the environment (used by tests and tools; the server
    /// resolves its config file + env + flags instead).
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            backend: std::env::var("EXSPEED_LEASE_BACKEND").unwrap_or(d.backend),
            postgres_url: std::env::var("EXSPEED_LEASE_POSTGRES_URL").ok(),
            postgres_schema: std::env::var("EXSPEED_LEASE_POSTGRES_SCHEMA")
                .unwrap_or(d.postgres_schema),
            redis_url: std::env::var("EXSPEED_LEASE_REDIS_URL").ok(),
            redis_key_prefix: std::env::var("EXSPEED_LEASE_REDIS_KEY_PREFIX")
                .unwrap_or(d.redis_key_prefix),
            ..d
        }
    }
}

/// Build a lease backend from explicit settings. Noop for backend `none`.
pub async fn from_config(cfg: &LeaseConfig) -> Result<Arc<dyn LeaderLease>, LeaseError> {
    match cfg.backend.as_str() {
        "postgres" => {
            let url = cfg.postgres_url.as_deref().ok_or_else(|| {
                LeaseError::Connection("the postgres lease backend needs a URL".into())
            })?;
            let b = postgres::PostgresLeaseBackend::connect(
                url,
                &cfg.postgres_schema,
                cfg.call_timeout,
            )
            .await?;
            Ok(Arc::new(b))
        }
        "redis" => {
            let url = cfg.redis_url.as_deref().ok_or_else(|| {
                LeaseError::Connection("the redis lease backend needs a URL".into())
            })?;
            let b = redis::RedisLeaseBackend::connect(url, &cfg.redis_key_prefix, cfg.call_timeout)
                .await?;
            Ok(Arc::new(b))
        }
        "memory" => Ok(MemoryLeaseBackend::named(&cfg.memory_namespace)),
        "none" | "" => Ok(Arc::new(NoopLeaderLease::new())),
        other => Err(LeaseError::Connection(format!(
            "unknown lease backend `{other}` (expected none, postgres or redis)"
        ))),
    }
}

/// Shared conformance checks every coordinating backend must pass. Called
/// from each backend's tests (the Postgres and Redis ones need a server).
#[doc(hidden)]
pub mod conformance {
    use super::*;

    fn req(holder: &str, ttl_ms: u64) -> AcquireRequest {
        AcquireRequest::new("conf:lease", holder, Duration::from_millis(ttl_ms))
    }

    pub async fn run(b: Arc<dyn LeaderLease>) {
        // Fresh: acquire wins with epoch 1 (or more if the table is reused).
        let a = b
            .try_acquire(&req("a", 400))
            .await
            .unwrap()
            .expect("a acquires");
        assert_eq!(a.holder, "a");
        let e1 = a.epoch;
        // Held: b is rejected.
        assert!(b.try_acquire(&req("b", 400)).await.unwrap().is_none());
        // Refresh by the holder works; by a wrong epoch doesn't.
        assert_eq!(
            b.refresh("conf:lease", "a", e1, Duration::from_millis(400))
                .await
                .unwrap(),
            Refresh::Held
        );
        assert_eq!(
            b.refresh("conf:lease", "a", e1 + 7, Duration::from_millis(400))
                .await
                .unwrap(),
            Refresh::Lost
        );
        // ISR publication.
        assert!(b
            .set_isr("conf:lease", "a", e1, &["a".to_string(), "c".to_string()])
            .await
            .unwrap());
        assert!(!b.set_isr("conf:lease", "b", e1, &[]).await.unwrap());
        let got = b.get("conf:lease").await.unwrap().unwrap();
        assert_eq!(got.isr, vec!["a".to_string(), "c".to_string()]);
        assert_eq!(
            b.list_all()
                .await
                .unwrap()
                .iter()
                .filter(|r| r.name == "conf:lease")
                .count(),
            1
        );
        // Release keeps epoch + ISR but frees the lease.
        b.release("conf:lease", "a", e1).await.unwrap();
        let got = b.get("conf:lease").await.unwrap().unwrap();
        assert_eq!(got.epoch, e1);
        assert_eq!(got.isr.len(), 2);
        assert!(b
            .list_all()
            .await
            .unwrap()
            .iter()
            .all(|r| r.name != "conf:lease"));
        // ISR-gated acquisition: b is not in {a, c}.
        let mut rb = req("b", 400);
        rb.require_isr = true;
        assert!(b.try_acquire(&rb).await.unwrap().is_none());
        let mut rc = req("c", 300);
        rc.require_isr = true;
        rc.client_endpoint = Some("c:5933".into());
        let c = b.try_acquire(&rc).await.unwrap().expect("c is in the ISR");
        assert_eq!(c.epoch, e1 + 1);
        assert_eq!(c.client_endpoint.as_deref(), Some("c:5933"));
        // The old holder's refresh is fenced.
        assert_eq!(
            b.refresh("conf:lease", "a", e1, Duration::from_millis(400))
                .await
                .unwrap(),
            Refresh::Lost
        );
        // Expiry frees it for anyone (without the ISR gate).
        tokio::time::sleep(Duration::from_millis(450)).await;
        let bb = b
            .try_acquire(&req("b", 400))
            .await
            .unwrap()
            .expect("expired");
        assert_eq!(bb.epoch, e1 + 2);
        // A holder may re-take its own live lease (restart), bumping the epoch.
        let again = b
            .try_acquire(&req("b", 400))
            .await
            .unwrap()
            .expect("own lease");
        assert_eq!(again.epoch, e1 + 3);
        b.release("conf:lease", "b", again.epoch).await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn guard_heartbeat_keeps_and_release_frees() {
        let b = MemoryLeaseBackend::new();
        let hb = Heartbeat::new(Duration::from_millis(300), Duration::from_millis(50));
        let g = acquire(b.clone(), &AcquireRequest::new("x", "n1", hb.ttl), hb)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(!g.is_lost(), "heartbeat keeps it");
        assert!(b
            .try_acquire(&AcquireRequest::new("x", "n2", hb.ttl))
            .await
            .unwrap()
            .is_none());
        drop(g);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(b
            .try_acquire(&AcquireRequest::new("x", "n2", hb.ttl))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn partitioned_holder_steps_down_before_expiry() {
        let b = MemoryLeaseBackend::new();
        let hb = Heartbeat::new(Duration::from_millis(600), Duration::from_millis(100));
        let mut g = acquire(b.clone(), &AcquireRequest::new("x", "n1", hb.ttl), hb)
            .await
            .unwrap()
            .unwrap();
        b.set_partitioned("n1", true);
        let t0 = std::time::Instant::now();
        g.on_lost.wait_for(|&l| l).await.unwrap();
        // Lost at the local deadline (2/3 TTL), before the backend expiry.
        assert!(
            t0.elapsed() < Duration::from_millis(600),
            "{:?}",
            t0.elapsed()
        );
        assert!(b
            .try_acquire(&AcquireRequest::new("x", "n2", hb.ttl))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn stolen_lease_is_lost_on_next_refresh() {
        let b = MemoryLeaseBackend::new();
        let hb = Heartbeat::new(Duration::from_millis(3000), Duration::from_millis(50));
        let mut g = acquire(b.clone(), &AcquireRequest::new("x", "n1", hb.ttl), hb)
            .await
            .unwrap()
            .unwrap();
        b.force_expire("x");
        let other = b
            .try_acquire(&AcquireRequest::new("x", "n2", hb.ttl))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(other.epoch, g.epoch() + 1);
        tokio::time::timeout(Duration::from_millis(500), g.on_lost.wait_for(|&l| l))
            .await
            .expect("lost quickly")
            .unwrap();
    }

    #[tokio::test]
    async fn memory_backend_conformance() {
        conformance::run(MemoryLeaseBackend::new()).await;
    }
}
