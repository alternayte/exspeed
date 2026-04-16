pub mod noop;
pub mod postgres;
pub mod redis;

use async_trait::async_trait;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum WorkCoordinatorError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// A message claimed from the pending queue for delivery to a subscriber.
#[derive(Debug, Clone)]
pub struct ClaimedMessage {
    /// Offset in the source stream.
    pub offset: u64,
    /// Number of delivery attempts (1 for first attempt, 2+ for redeliveries).
    pub attempts: u32,
}

#[async_trait]
pub trait WorkCoordinator: Send + Sync {
    /// Whether this backend supports multi-pod coordination.
    /// Returns false for Noop; true for Postgres and Redis.
    fn supports_coordination(&self) -> bool;

    /// Claim up to `n` available (or expired) messages for this group.
    /// Returns empty vec if nothing available.
    async fn claim_batch(
        &self,
        group: &str,
        subscriber_id: &str,
        n: usize,
        ack_timeout_secs: u64,
    ) -> Result<Vec<ClaimedMessage>, WorkCoordinatorError>;

    /// Add newly-read offsets from storage to the pending queue.
    /// Also advances `delivery_head` to `max(offsets)` if higher.
    async fn enqueue(
        &self,
        group: &str,
        offsets: &[u64],
    ) -> Result<(), WorkCoordinatorError>;

    /// Current delivery_head (highest offset loaded into pending).
    async fn delivery_head(&self, group: &str) -> Result<u64, WorkCoordinatorError>;

    /// Count of currently-pending messages (delivered + undelivered).
    async fn pending_count(&self, group: &str) -> Result<usize, WorkCoordinatorError>;

    /// Ack a message. Removes from pending. Advances committed_offset
    /// if gap-free prefix.
    async fn ack(&self, group: &str, offset: u64) -> Result<(), WorkCoordinatorError>;

    /// Nack a message. Increments attempts. Returns new attempt count.
    async fn nack(&self, group: &str, offset: u64) -> Result<u32, WorkCoordinatorError>;

    /// Current committed offset for the group.
    async fn committed_offset(&self, group: &str) -> Result<u64, WorkCoordinatorError>;
}

/// Build a WorkCoordinator from the `EXSPEED_CONSUMER_STORE` env var.
/// Defaults to the same backend as `EXSPEED_OFFSET_STORE`, then noop.
pub async fn from_env() -> Result<Arc<dyn WorkCoordinator>, WorkCoordinatorError> {
    let backend = std::env::var("EXSPEED_CONSUMER_STORE")
        .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE"))
        .unwrap_or_else(|_| "file".to_string());

    match backend.as_str() {
        "postgres" => {
            let c = postgres::PostgresWorkCoordinator::from_env().await?;
            Ok(Arc::new(c))
        }
        "redis" => {
            let c = redis::RedisWorkCoordinator::from_env().await?;
            Ok(Arc::new(c))
        }
        _ => Ok(Arc::new(noop::NoopWorkCoordinator)),
    }
}
