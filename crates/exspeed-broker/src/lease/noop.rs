use std::time::Duration;

use async_trait::async_trait;

use super::{AcquireRequest, LeaderLease, LeaseError, LeaseRecord, Refresh};

/// Always-grant lease backend used when no coordination backend is
/// configured: every node is its own leader (single-node mode).
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

    async fn try_acquire(&self, req: &AcquireRequest) -> Result<Option<LeaseRecord>, LeaseError> {
        Ok(Some(LeaseRecord {
            name: req.name.clone(),
            holder: req.holder.clone(),
            epoch: 0,
            expires_at: chrono::DateTime::<chrono::Utc>::MAX_UTC,
            replication_endpoint: None,
            client_endpoint: req.client_endpoint.clone(),
            isr: Vec::new(),
        }))
    }

    async fn refresh(&self, _: &str, _: &str, _: u64, _: Duration) -> Result<Refresh, LeaseError> {
        Ok(Refresh::Held)
    }

    async fn release(&self, _: &str, _: &str, _: u64) -> Result<(), LeaseError> {
        Ok(())
    }

    async fn set_isr(&self, _: &str, _: &str, _: u64, _: &[String]) -> Result<bool, LeaseError> {
        Ok(true)
    }

    async fn get(&self, _: &str) -> Result<Option<LeaseRecord>, LeaseError> {
        Ok(None)
    }

    async fn list_all(&self) -> Result<Vec<LeaseRecord>, LeaseError> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn noop_always_grants_and_never_loses() {
        let b: Arc<dyn LeaderLease> = Arc::new(NoopLeaderLease::new());
        let hb = super::super::Heartbeat::new(Duration::from_millis(60), Duration::from_millis(10));
        let req = AcquireRequest::new("x", "n", hb.ttl);
        let g = super::super::acquire(b.clone(), &req, hb)
            .await
            .unwrap()
            .unwrap();
        let g2 = super::super::acquire(b.clone(), &req, hb).await.unwrap();
        assert!(g2.is_some(), "noop never rejects");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!g.is_lost());
        assert!(b.list_all().await.unwrap().is_empty());
        assert!(!b.supports_coordination());
    }
}
