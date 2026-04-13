use std::sync::Arc;
use std::time::Instant;

use exspeed_broker::Broker;
use exspeed_common::Metrics;
use exspeed_storage::file::FileStorage;
use prometheus::Registry;

pub struct AppState {
    pub broker: Arc<Broker>,
    pub storage: Arc<FileStorage>,
    pub metrics: Arc<Metrics>,
    pub start_time: Instant,
    pub prometheus_registry: Registry,
}
