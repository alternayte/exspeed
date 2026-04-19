//! RedisLeaseBackend — implemented in Plan D Task 3. This file is a stub.

use std::time::Duration;

use async_trait::async_trait;

use super::{LeaderLease, LeaseError, LeaseGuard, LeaseInfo};

pub struct RedisLeaseBackend;

impl RedisLeaseBackend {
    pub async fn from_env() -> Result<Self, LeaseError> {
        Err(LeaseError::Backend(
            "RedisLeaseBackend not yet implemented (Plan D Task 3)".into(),
        ))
    }
}

#[async_trait]
impl LeaderLease for RedisLeaseBackend {
    fn supports_coordination(&self) -> bool {
        true
    }

    async fn try_acquire(
        &self,
        _name: &str,
        _ttl: Duration,
    ) -> Result<Option<LeaseGuard>, LeaseError> {
        Err(LeaseError::Backend("unimplemented".into()))
    }

    async fn list_all(&self) -> Result<Vec<LeaseInfo>, LeaseError> {
        Err(LeaseError::Backend("unimplemented".into()))
    }
}
