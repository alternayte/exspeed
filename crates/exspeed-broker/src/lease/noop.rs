use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use super::{LeaderLease, LeaseError, LeaseGuard, LeaseInfo};

/// Always-succeed lease backend used when no shared coordination backend
/// is configured (`EXSPEED_CONSUMER_STORE=file` or unset). Each pod acts as
/// its own leader — preserving current single-pod behavior exactly.
pub struct NoopLeaderLease;

impl NoopLeaderLease {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoopLeaderLease {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LeaderLease for NoopLeaderLease {
    fn supports_coordination(&self) -> bool {
        false
    }

    async fn try_acquire(
        &self,
        name: &str,
        _ttl: Duration,
    ) -> Result<Option<LeaseGuard>, LeaseError> {
        let (cancel_tx, _cancel_rx) = oneshot::channel::<()>();
        let (lost_tx, lost_rx) = watch::channel(false);
        Ok(Some(LeaseGuard {
            name: name.to_string(),
            holder_id: Uuid::new_v4(),
            on_lost: lost_rx,
            // Keep the sender alive so `on_lost.changed()` stays pending.
            // Without this, owner tasks that select on `on_lost.changed()`
            // would stop immediately because the dropped sender causes
            // `changed()` to resolve with `Err(_)`.
            _lost_tx: Some(lost_tx),
            _cancel_heartbeat: cancel_tx,
        }))
    }

    async fn list_all(&self) -> Result<Vec<LeaseInfo>, LeaseError> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn noop_always_acquires() {
        let b = NoopLeaderLease::new();
        let guard = b.try_acquire("foo", Duration::from_secs(30)).await.unwrap();
        assert!(guard.is_some());
        let g2 = b.try_acquire("foo", Duration::from_secs(30)).await.unwrap();
        assert!(g2.is_some(), "noop never rejects");
    }

    #[tokio::test]
    async fn noop_list_is_empty() {
        let b = NoopLeaderLease::new();
        let _g = b.try_acquire("x", Duration::from_secs(30)).await.unwrap();
        assert!(b.list_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn noop_on_lost_never_fires() {
        let b = NoopLeaderLease::new();
        let guard = b
            .try_acquire("x", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        // Spin briefly — on_lost should remain false.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(*guard.on_lost.borrow(), false);
    }

    #[tokio::test]
    async fn noop_reports_no_coordination() {
        assert!(!NoopLeaderLease::new().supports_coordination());
    }
}
