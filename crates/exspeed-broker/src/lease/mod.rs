//! Distributed leader-lease primitive. Used to ensure exactly one pod in a
//! multi-pod deployment runs each source connector, continuous query, and
//! ungrouped sink consumer.
//!
//! Backend dispatched from `EXSPEED_CONSUMER_STORE` (same env var as
//! [`crate::work_coordinator`]). File-backed deployments fall through to
//! [`NoopLeaderLease`] which always succeeds — preserving current single-pod
//! behavior.

pub mod noop;
pub mod postgres;
pub mod redis;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

pub use noop::NoopLeaderLease;

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("backend error: {0}")]
    Backend(String),
}

/// Information about a currently-held lease, used by `GET /api/v1/leases`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LeaseInfo {
    pub name: String,
    pub holder: Uuid,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

/// Held by the lease owner. While this struct is alive, an internal
/// heartbeat task extends the TTL. Dropping it cancels the heartbeat and
/// best-effort releases the lease in the backend.
pub struct LeaseGuard {
    pub name: String,
    pub holder_id: Uuid,
    /// Watcher signaling lease loss. `false` while held; flips to `true`
    /// if the heartbeat task fails to refresh the lease. Owner code should
    /// select on `on_lost.changed()` to stop work cleanly.
    pub on_lost: watch::Receiver<bool>,
    /// Keeps the `on_lost` sender alive for the lifetime of the guard.
    /// Backends that don't expose lost-signaling (e.g. Noop) store the
    /// sender here so `on_lost.changed()` stays pending indefinitely
    /// instead of resolving with `Err(_)` the instant the sender drops.
    /// Backends that do signal lost (postgres/redis) move the real sender
    /// into their heartbeat task and set this to `None`.
    pub _lost_tx: Option<watch::Sender<bool>>,
    /// Dropping the guard sends on this, stopping the heartbeat task.
    _cancel_heartbeat: oneshot::Sender<()>,
}

#[async_trait]
pub trait LeaderLease: Send + Sync {
    /// Whether this backend coordinates across pods. False for Noop.
    fn supports_coordination(&self) -> bool;

    /// Try to acquire the named lease with the given TTL.
    ///
    /// Returns:
    /// - `Ok(Some(LeaseGuard))` — this caller is now the holder. The guard
    ///   owns an internal heartbeat task; dropping the guard releases.
    /// - `Ok(None)` — another holder is alive.
    /// - `Err(LeaseError)` — backend failure (connection, serialization).
    async fn try_acquire(
        &self,
        name: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseGuard>, LeaseError>;

    /// List all currently-held leases for operator visibility. Returns an
    /// empty vec for Noop. Order is backend-defined; callers should not
    /// assume stability.
    async fn list_all(&self) -> Result<Vec<LeaseInfo>, LeaseError>;
}

/// Read `EXSPEED_LEASE_TTL_SECS` (default 30).
pub fn ttl_from_env() -> Duration {
    let secs: u64 = std::env::var("EXSPEED_LEASE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    Duration::from_secs(secs)
}

/// Read `EXSPEED_LEASE_HEARTBEAT_SECS` (default 10).
pub fn heartbeat_interval_from_env() -> Duration {
    let secs: u64 = std::env::var("EXSPEED_LEASE_HEARTBEAT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    Duration::from_secs(secs)
}

/// Build a `LeaderLease` from `EXSPEED_CONSUMER_STORE`. Mirrors
/// [`crate::work_coordinator::from_env`]. Defaults to Noop when unset or file.
pub async fn from_env() -> Result<Arc<dyn LeaderLease>, LeaseError> {
    let backend = std::env::var("EXSPEED_CONSUMER_STORE")
        .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE"))
        .unwrap_or_else(|_| "file".to_string());

    match backend.as_str() {
        "postgres" => {
            let b = postgres::PostgresLeaseBackend::from_env().await?;
            Ok(Arc::new(b))
        }
        "redis" => {
            let b = redis::RedisLeaseBackend::from_env().await?;
            Ok(Arc::new(b))
        }
        _ => Ok(Arc::new(NoopLeaderLease::new())),
    }
}
