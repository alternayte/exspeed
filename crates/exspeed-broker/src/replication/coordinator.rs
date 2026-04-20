//! Leader-side fan-out coordinator.

use std::sync::Arc;
use exspeed_common::Metrics;

pub struct ReplicationCoordinator {
    _metrics: Arc<Metrics>,
}

impl ReplicationCoordinator {
    pub fn new(metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self { _metrics: metrics })
    }
}
