use async_trait::async_trait;

use super::{ClaimedMessage, WorkCoordinator, WorkCoordinatorError};

pub struct PostgresWorkCoordinator;

impl PostgresWorkCoordinator {
    pub async fn from_env() -> Result<Self, WorkCoordinatorError> {
        Err(WorkCoordinatorError::Connection(
            "postgres coordinator not yet implemented (Task 3)".to_string(),
        ))
    }
}

#[async_trait]
impl WorkCoordinator for PostgresWorkCoordinator {
    fn supports_coordination(&self) -> bool {
        true
    }
    async fn claim_batch(
        &self,
        _group: &str,
        _subscriber_id: &str,
        _n: usize,
        _ack_timeout_secs: u64,
    ) -> Result<Vec<ClaimedMessage>, WorkCoordinatorError> {
        unimplemented!()
    }
    async fn enqueue(&self, _g: &str, _o: &[u64]) -> Result<(), WorkCoordinatorError> {
        unimplemented!()
    }
    async fn delivery_head(&self, _g: &str) -> Result<u64, WorkCoordinatorError> {
        unimplemented!()
    }
    async fn pending_count(&self, _g: &str) -> Result<usize, WorkCoordinatorError> {
        unimplemented!()
    }
    async fn ack(&self, _g: &str, _o: u64) -> Result<(), WorkCoordinatorError> {
        unimplemented!()
    }
    async fn nack(&self, _g: &str, _o: u64) -> Result<u32, WorkCoordinatorError> {
        unimplemented!()
    }
    async fn committed_offset(&self, _g: &str) -> Result<u64, WorkCoordinatorError> {
        unimplemented!()
    }
}
