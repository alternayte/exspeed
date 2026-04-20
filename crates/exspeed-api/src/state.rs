use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use exspeed_broker::leadership::ClusterLeadership;
use exspeed_broker::replication::ReplicationCoordinator;
use exspeed_broker::Broker;
use exspeed_broker::LeaderLease;
use exspeed_common::auth::CredentialStore;
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
    /// Multi-tenant credential store. When `Some`, HTTP middleware requires a
    /// bearer token that hashes to a known identity; handlers then gate on
    /// per-identity scoped-admin / global-admin checks. When `None`, auth is
    /// globally disabled and every HTTP request passes through.
    pub credential_store: Option<Arc<CredentialStore>>,
    /// Distributed leader-lease primitive. Used to coordinate exactly-one
    /// execution of connectors and continuous queries across pods. Exposed
    /// for `/api/v1/leases` (Task 7).
    pub lease: Arc<dyn LeaderLease>,
    /// Cluster-leader lease handle. Used by `/healthz` and the
    /// `leader_gate` middleware (added in Task 5) to decide whether to
    /// accept client traffic.
    pub leadership: Arc<ClusterLeadership>,
    /// Set to true after the HTTP API server has been spawned and the
    /// startup pipeline has completed. Until then, `/readyz` returns 503
    /// with `{"status": "starting"}`.
    pub ready: Arc<AtomicBool>,
    /// Used by the `/readyz` probe to verify the data dir is writable
    /// (touch+remove probe on each request). Independent of `/healthz`,
    /// which only reports cluster-leader status.
    pub data_dir: PathBuf,
    /// Leader-side replication fan-out coordinator. `Some(_)` only when
    /// this pod was started in multi-pod mode
    /// (`EXSPEED_CONSUMER_STORE=postgres|redis`); `None` on single-pod
    /// deployments. `GET /api/v1/cluster/followers` gates on presence
    /// and returns 503 when absent.
    pub replication_coordinator: Option<Arc<ReplicationCoordinator>>,
}
