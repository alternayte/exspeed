use async_trait::async_trait;

use super::{ClaimedMessage, WorkCoordinator, WorkCoordinatorError};

pub struct NoopWorkCoordinator;

#[async_trait]
impl WorkCoordinator for NoopWorkCoordinator {
    fn supports_coordination(&self) -> bool {
        false
    }

    async fn claim_batch(
        &self,
        _group: &str,
        _subscriber_id: &str,
        _n: usize,
        _ack_timeout_secs: u64,
    ) -> Result<Vec<ClaimedMessage>, WorkCoordinatorError> {
        Ok(Vec::new())
    }

    async fn enqueue(
        &self,
        _group: &str,
        _offsets: &[u64],
    ) -> Result<(), WorkCoordinatorError> {
        Ok(())
    }

    async fn delivery_head(&self, _group: &str) -> Result<u64, WorkCoordinatorError> {
        Ok(0)
    }

    async fn pending_count(&self, _group: &str) -> Result<usize, WorkCoordinatorError> {
        Ok(0)
    }

    async fn ack(&self, _group: &str, _offset: u64) -> Result<(), WorkCoordinatorError> {
        Ok(())
    }

    async fn nack(&self, _group: &str, _offset: u64) -> Result<u32, WorkCoordinatorError> {
        Ok(0)
    }

    async fn committed_offset(&self, _group: &str) -> Result<u64, WorkCoordinatorError> {
        Ok(0)
    }
}
