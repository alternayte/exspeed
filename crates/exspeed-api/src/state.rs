use std::sync::Arc;
use std::time::Instant;

use exspeed_broker::leadership::ClusterLeadership;
use exspeed_broker::Broker;
use exspeed_broker::LeaderLease;
use exspeed_common::Metrics;
use exspeed_connectors::ConnectorManager;
use exspeed_processing::ExqlEngine;
use exspeed_storage::file::FileStorage;
use prometheus::Registry;

pub struct AppState {
    pub broker: Arc<Broker>,
    pub storage: Arc<FileStorage>,
    pub metrics: Arc<Metrics>,
    pub start_time: Instant,
    pub prometheus_registry: Registry,
    pub connector_manager: Arc<ConnectorManager>,
    pub exql: Arc<ExqlEngine>,
    /// Shared bearer token required on protected HTTP endpoints.
    /// When None, all HTTP endpoints are unauthenticated.
    pub auth_token: Option<Arc<String>>,
    /// Distributed leader-lease primitive. Used to coordinate exactly-one
    /// execution of connectors and continuous queries across pods. Exposed
    /// for `/api/v1/leases` (Task 7).
    pub lease: Arc<dyn LeaderLease>,
    /// Cluster-leader lease handle. Used by `/healthz` and the
    /// `leader_gate` middleware (added in Task 5) to decide whether to
    /// accept client traffic.
    pub leadership: Arc<ClusterLeadership>,
}
