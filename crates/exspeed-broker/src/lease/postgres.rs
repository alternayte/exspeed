//! PostgresLeaseBackend — implemented in Plan D Task 2. This file is a stub
//! that allows Task 1's `from_env` dispatch to compile; the actual impl
//! lands in Task 2.

use std::time::Duration;

use async_trait::async_trait;

use super::{LeaderLease, LeaseError, LeaseGuard, LeaseInfo};

pub struct PostgresLeaseBackend;

impl PostgresLeaseBackend {
    pub async fn from_env() -> Result<Self, LeaseError> {
        Err(LeaseError::Backend(
            "PostgresLeaseBackend not yet implemented (Plan D Task 2)".into(),
        ))
    }
}

#[async_trait]
impl LeaderLease for PostgresLeaseBackend {
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
