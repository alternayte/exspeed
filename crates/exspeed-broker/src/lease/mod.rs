//! Distributed leader-lease primitive. Exactly one pod in a multi-pod
//! deployment holds the cluster lease; only it accepts writes and runs
//! consumers, connectors and continuous queries.
//!
//! Backend chosen by `EXSPEED_LEASE_BACKEND` (`postgres` | `redis` |
//! `none`, default `none`; see [`backend_from_env`]). Without one, every pod
//! uses [`NoopLeaderLease`], which always succeeds: single-node mode.

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
    /// Where followers should dial this holder for replication. `None` for
    /// leases that do not carry a replication state (Noop backend; a leader
    /// that was started without a cluster-bind endpoint; or any non-cluster
    /// lease such as connector-group holders). Emitted alongside `holder` so
    /// followers can discover the leader without a separate registry.
    ///
    /// Serialized unconditionally (as `null` when absent) so operator
    /// tooling pinned to the JSON shape can reliably distinguish "no
    /// replication endpoint advertised" from "field missing due to schema
    /// change". See `GET /api/v1/leases`.
    #[serde(default)]
    pub replication_endpoint: Option<String>,
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
    /// Exposed (but underscore-prefixed) so external test backends can
    /// construct a guard. Production callers should not touch this field.
    pub _cancel_heartbeat: oneshot::Sender<()>,
}

#[async_trait]
pub trait LeaderLease: Send + Sync {
    /// Whether this backend coordinates across pods. False for Noop.
    fn supports_coordination(&self) -> bool;

    /// Try to acquire the named lease with the given TTL.
    ///
    /// `replication_endpoint` is written into the lease row so followers can
    /// discover the leader via `list_all()` without a separate registry.
    /// Pass `None` for leases that do not expose a replication endpoint
    /// (anything other than `cluster:leader`, or the cluster-leader lease
    /// itself when the server is started without a cluster-bind endpoint).
    /// Backends that don't support coordination (Noop) ignore it.
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
        replication_endpoint: Option<&str>,
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

/// The configured lease backend: `EXSPEED_LEASE_BACKEND`, falling back to
/// the deprecated `EXSPEED_CONSUMER_STORE`. Returns `"postgres"`, `"redis"`
/// or `"none"`.
pub fn backend_from_env() -> String {
    let raw = match std::env::var("EXSPEED_LEASE_BACKEND") {
        Ok(v) => v,
        Err(_) => match std::env::var("EXSPEED_CONSUMER_STORE") {
            Ok(v) => {
                tracing::warn!(
                    "EXSPEED_CONSUMER_STORE is deprecated for selecting the lease backend; \
                     use EXSPEED_LEASE_BACKEND"
                );
                v
            }
            Err(_) => String::new(),
        },
    };
    match raw.as_str() {
        "postgres" | "redis" => raw,
        _ => "none".to_string(),
    }
}

/// Explicit lease settings (the server's `[cluster]` config section).
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// `none`, `postgres` or `redis`.
    pub backend: String,
    pub postgres_url: Option<String>,
    pub postgres_schema: String,
    pub redis_url: Option<String>,
    pub redis_key_prefix: String,
    pub ttl: Duration,
    pub heartbeat: Duration,
}

impl LeaseConfig {
    /// Settings from the environment (used by tests and tools; the server
    /// resolves its config file + env + flags instead).
    pub fn from_env() -> Self {
        Self {
            backend: backend_from_env(),
            postgres_url: std::env::var("EXSPEED_LEASE_POSTGRES_URL")
                .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL"))
                .ok(),
            postgres_schema: std::env::var("EXSPEED_LEASE_POSTGRES_SCHEMA")
                .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA"))
                .unwrap_or_else(|_| "public".into()),
            redis_url: std::env::var("EXSPEED_LEASE_REDIS_URL")
                .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE_REDIS_URL"))
                .ok(),
            redis_key_prefix: std::env::var("EXSPEED_LEASE_REDIS_KEY_PREFIX")
                .unwrap_or_else(|_| "exspeed:lease:".into()),
            ttl: ttl_from_env(),
            heartbeat: heartbeat_interval_from_env(),
        }
    }
}

/// Build a `LeaderLease` from the environment. Noop when no backend is set.
pub async fn from_env() -> Result<Arc<dyn LeaderLease>, LeaseError> {
    from_config(&LeaseConfig::from_env()).await
}

/// Build a `LeaderLease` from explicit settings. Noop for backend `none`.
pub async fn from_config(cfg: &LeaseConfig) -> Result<Arc<dyn LeaderLease>, LeaseError> {
    match cfg.backend.as_str() {
        "postgres" => {
            let url = cfg.postgres_url.as_deref().ok_or_else(|| {
                LeaseError::Connection("the postgres lease backend needs a URL".into())
            })?;
            let b =
                postgres::PostgresLeaseBackend::connect(url, &cfg.postgres_schema, cfg.heartbeat)
                    .await?;
            Ok(Arc::new(b))
        }
        "redis" => {
            let url = cfg.redis_url.as_deref().ok_or_else(|| {
                LeaseError::Connection("the redis lease backend needs a URL".into())
            })?;
            let b = redis::RedisLeaseBackend::connect(url, &cfg.redis_key_prefix, cfg.heartbeat)
                .await?;
            Ok(Arc::new(b))
        }
        _ => Ok(Arc::new(NoopLeaderLease::new())),
    }
}
