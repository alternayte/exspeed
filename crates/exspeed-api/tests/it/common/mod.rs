use std::sync::Arc;
use std::time::Duration;

use exspeed_broker::leadership::ClusterLeadership;
use exspeed_broker::lease::{AcquireRequest, LeaderLease, MemoryLeaseBackend, CLUSTER_LEASE};
use exspeed_common::Metrics;

/// A lease backend whose cluster lease is held by another node for an
/// hour: a `ClusterLeadership` on it stays a follower.
pub async fn held_elsewhere() -> Arc<dyn LeaderLease> {
    let b = MemoryLeaseBackend::new();
    b.try_acquire(&AcquireRequest::new(
        CLUSTER_LEASE,
        "someone-else",
        Duration::from_secs(3600),
    ))
    .await
    .unwrap()
    .unwrap();
    b
}

/// Build an AppState with the desired leadership state.
pub async fn make_state_with_leader(leader: bool) -> Arc<exspeed_api::AppState> {
    use exspeed_broker::broker_append::BrokerAppend;
    use exspeed_broker::Broker;
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
        held_elsewhere().await
    };

    let leadership = Arc::new(ClusterLeadership::spawn(lease.clone(), metrics.clone(), None).await);

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
    let broker = Arc::new(Broker::new(
        storage_dyn.clone(),
        ba.clone(),
        tmp.path().to_path_buf(),
        lease.clone(),
        metrics.clone(),
    ));
    // Mark dedup rebuild as complete — the helper represents a fully started
    // server for test purposes. Tests that want to exercise the not-ready
    // branch should override this after calling `make_state_with_leader`.
    broker
        .dedup_ready
        .store(true, std::sync::atomic::Ordering::Release);

    let oss = offset_store::from_env(tmp.path(), broker.log.clone()).expect("offset store");
    let cm = Arc::new(ConnectorManager::new(
        storage_dyn.clone(),
        broker.log.clone(),
        tmp.path().to_path_buf(),
        metrics.clone(),
        oss,
        leadership.clone(),
    ));

    let exql = Arc::new(
        ExqlEngine::new(
            broker.log.clone(),
            tmp.path().to_path_buf(),
            leadership.clone(),
            metrics.clone(),
            exspeed_processing::ExqlConfig::default(),
        )
        .expect("exql engine"),
    );

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
        credential_store: None,
        lease,
        leadership,
        // Pre-flip ready=true: tests that exercise /healthz or other
        // routes assume startup is complete. Tests that exercise
        // /readyz directly should set this themselves.
        ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        data_dir,
        cluster: None,
    })
}
