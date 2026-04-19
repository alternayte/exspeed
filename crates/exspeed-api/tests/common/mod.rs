use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use exspeed_broker::lease::{LeaderLease, LeaseError, LeaseGuard, LeaseInfo};
use exspeed_broker::leadership::ClusterLeadership;
use exspeed_common::Metrics;

/// Lease backend that always rejects. Used to put a `ClusterLeadership`
/// into a permanent standby state for tests.
pub struct AlwaysRejectLease;

#[async_trait]
impl LeaderLease for AlwaysRejectLease {
    fn supports_coordination(&self) -> bool {
        true
    }

    async fn try_acquire(
        &self,
        _name: &str,
        _ttl: Duration,
    ) -> Result<Option<LeaseGuard>, LeaseError> {
        Ok(None)
    }

    async fn list_all(&self) -> Result<Vec<LeaseInfo>, LeaseError> {
        Ok(vec![])
    }
}

/// Build an AppState with the desired leadership state.
pub async fn make_state_with_leader(leader: bool) -> Arc<exspeed_api::AppState> {
    use exspeed_broker::broker_append::BrokerAppend;
    use exspeed_broker::{consumer_store, work_coordinator, Broker};
    use exspeed_connectors::{offset_store, ConnectorManager};
    use exspeed_processing::ExqlEngine;
    use exspeed_storage::file::FileStorage;

    let tmp = tempfile::tempdir().unwrap();
    let storage = Arc::new(FileStorage::open(tmp.path()).unwrap());
    let storage_dyn: Arc<dyn exspeed_streams::StorageEngine> = storage.clone();

    let (metrics, registry) = Metrics::new();
    let metrics = Arc::new(metrics);

    let lease: Arc<dyn LeaderLease> = if leader {
        Arc::new(exspeed_broker::lease::NoopLeaderLease::new())
    } else {
        Arc::new(AlwaysRejectLease)
    };

    let leadership = Arc::new(ClusterLeadership::spawn(lease.clone(), metrics.clone()).await);

    if leader {
        // Wait for Noop promotion (~1 tick = max(TTL/3, 1s)). Default
        // TTL is 30s so bound wait at 3s to be safe in case env is set.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while !leadership.is_currently_leader() {
            if tokio::time::Instant::now() > deadline {
                panic!("Noop-backed leadership never promoted");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    } else {
        // AlwaysRejectLease never promotes. Give the retry loop a moment
        // to run a tick so metrics settle, but we don't actually wait for
        // any promotion signal.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!leadership.is_currently_leader());
    }

    let ba = Arc::new(BrokerAppend::new(storage_dyn.clone(), 60));
    let cs = consumer_store::from_env(tmp.path())
        .await
        .expect("consumer store");
    let wc = work_coordinator::from_env()
        .await
        .expect("work coordinator");
    let broker = Arc::new(Broker::new(
        storage_dyn.clone(),
        ba.clone(),
        tmp.path().to_path_buf(),
        cs,
        wc,
        lease.clone(),
        metrics.clone(),
    ));

    let oss = offset_store::from_env(tmp.path(), storage_dyn.clone())
        .await
        .expect("offset store");
    let cm = Arc::new(ConnectorManager::new(
        storage_dyn.clone(),
        ba,
        tmp.path().to_path_buf(),
        metrics.clone(),
        oss,
        leadership.clone(),
    ));

    let exql = Arc::new(ExqlEngine::new(
        storage_dyn.clone(),
        tmp.path().to_path_buf(),
        leadership.clone(),
        metrics.clone(),
    ));

    let data_dir = tmp.path().to_path_buf();

    // Leak the tempdir so file storage stays alive for the test body.
    // Tests are short-lived; this is acceptable.
    std::mem::forget(tmp);

    Arc::new(exspeed_api::AppState {
        broker,
        storage,
        metrics,
        start_time: std::time::Instant::now(),
        prometheus_registry: registry,
        connector_manager: cm,
        exql,
        auth_token: None,
        lease,
        leadership,
        // Pre-flip ready=true: tests that exercise /healthz or other
        // routes assume startup is complete. Tests that exercise
        // /readyz directly should set this themselves.
        ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        data_dir,
    })
}
