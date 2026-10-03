//! High availability: leader election, log replication and failover.
//!
//! A cluster is N nodes, each with its own data directory, sharing a lease
//! backend (Postgres or Redis). The lease holder is the **leader**: it alone
//! accepts writes and runs consumers, connectors and continuous queries. The
//! other nodes are **followers**: they pull the leader's log — every stream,
//! internal ones included, with the same offsets, timestamps, keys and
//! headers — and are ready to take over.
//!
//! * **Replication** is pull-based ([`follower`] fetches, [`leader`] serves)
//!   over the cluster port, with long polling.
//! * **Fencing**: the lease epoch increments on every takeover. Each stream
//!   keeps an epoch history ([`epochs`]); a follower whose log diverged from
//!   the new leader's (writes a deposed leader accepted but never
//!   replicated) truncates the divergent suffix (KIP-101).
//! * **Durability**: with `acks = all` a write is acknowledged once every
//!   in-sync replica has it ([`tracker`]); the ISR is published in the lease,
//!   and only an ISR member can be elected. `min_insync_replicas` rejects
//!   writes when too few replicas are in sync.
//! * **Promotion** ([`Cluster::promote`]): stop following, stamp the new
//!   epoch on every stream, rebuild the dedup state, then open writes.

pub mod epochs;
pub mod follower;
pub mod leader;
pub mod tracker;
pub mod wire;

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use exspeed_common::auth::CredentialStore;
use exspeed_common::{Metrics, StreamName};
use exspeed_streams::StorageEngine;

use crate::leadership::{ClusterLeadership, RoleHooks};
use crate::lease::{LeaderLease, LeaseRecord};
use crate::log::{Log, LogError, MetadataChange, ReplicaSync};

pub use epochs::{EpochStore, StreamEpochs};
pub use tracker::{AckError, FollowerStatus, ReplicaTracker, TrackerConfig};

/// Cluster settings.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_id: String,
    /// `true`: acknowledge writes once every in-sync replica has them.
    /// `false`: acknowledge after the leader's local write.
    pub acks_all: bool,
    /// Minimum in-sync replicas (leader included) for `acks = all` writes.
    pub min_insync_replicas: usize,
    /// A follower that hasn't caught up for this long leaves the ISR.
    pub replica_lag_max: Duration,
    /// How long an `acks = all` write waits for replication.
    pub ack_timeout: Duration,
    /// Token followers present to the leader (needs the `replicate` action
    /// when auth is on).
    pub replicator_token: Option<String>,
    pub fetch_max_bytes: u32,
    pub fetch_max_wait: Duration,
}

impl ClusterConfig {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            acks_all: true,
            min_insync_replicas: 1,
            replica_lag_max: Duration::from_secs(10),
            ack_timeout: Duration::from_secs(10),
            replicator_token: None,
            fetch_max_bytes: 8 * 1024 * 1024,
            fetch_max_wait: Duration::from_millis(1000),
        }
    }

    fn tracker(&self) -> TrackerConfig {
        TrackerConfig {
            acks_all: self.acks_all,
            min_insync_replicas: self.min_insync_replicas.max(1),
            replica_lag_max: self.replica_lag_max,
            ack_timeout: self.ack_timeout,
        }
    }
}

/// Read this node's id from `{data_dir}/node_id`, creating it on first use.
pub fn load_or_create_node_id(data_dir: &Path) -> std::io::Result<String> {
    let path = data_dir.join("node_id");
    match std::fs::read_to_string(&path) {
        Ok(s) if !s.trim().is_empty() => Ok(s.trim().to_string()),
        _ => {
            std::fs::create_dir_all(data_dir)?;
            let id = uuid::Uuid::new_v4().to_string();
            let tmp = data_dir.join("node_id.tmp");
            std::fs::write(&tmp, &id)?;
            std::fs::rename(&tmp, &path)?;
            Ok(id)
        }
    }
}

/// What a follower knows about its replication session (for the API).
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct FollowerInfo {
    pub leader: Option<String>,
    pub leader_epoch: u64,
    pub connected: bool,
    /// Records behind the leader at the last fetch, summed over streams.
    pub lag_records: u64,
    pub last_error: Option<String>,
}

/// The cluster layer of one node. Implements the role hooks (driven by
/// [`ClusterLeadership`]) and the write-path hooks ([`ReplicaSync`]).
pub struct Cluster {
    pub(crate) cfg: ClusterConfig,
    pub(crate) storage: Arc<dyn StorageEngine>,
    pub(crate) log: Arc<Log>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) epochs: EpochStore,
    pub(crate) credentials: Option<Arc<CredentialStore>>,
    pub(crate) lease: Arc<dyn LeaderLease>,
    dedup_ready: Arc<AtomicBool>,
    leadership: OnceLock<ClusterLeadership>,
    tracker: RwLock<Option<Arc<ReplicaTracker>>>,
    follower: tokio::sync::Mutex<Option<(CancellationToken, JoinHandle<()>)>>,
    pub(crate) follower_info: Mutex<FollowerInfo>,
    shutdown: CancellationToken,
}

impl Cluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: ClusterConfig,
        data_dir: &Path,
        storage: Arc<dyn StorageEngine>,
        log: Arc<Log>,
        metrics: Arc<Metrics>,
        credentials: Option<Arc<CredentialStore>>,
        lease: Arc<dyn LeaderLease>,
        dedup_ready: Arc<AtomicBool>,
    ) -> std::io::Result<Arc<Self>> {
        let epochs = EpochStore::open(data_dir)?;
        let cluster = Arc::new(Self {
            cfg,
            storage,
            log: log.clone(),
            metrics,
            epochs,
            credentials,
            lease,
            dedup_ready,
            leadership: OnceLock::new(),
            tracker: RwLock::new(None),
            follower: tokio::sync::Mutex::new(None),
            follower_info: Mutex::new(FollowerInfo::default()),
            shutdown: CancellationToken::new(),
        });
        log.set_replica_sync(cluster.clone());
        Ok(cluster)
    }

    /// Attach the leadership handle (once, right after starting it).
    pub fn set_leadership(&self, leadership: ClusterLeadership) {
        let _ = self.leadership.set(leadership);
    }

    pub fn leadership(&self) -> Option<&ClusterLeadership> {
        self.leadership.get()
    }

    pub fn node_id(&self) -> &str {
        &self.cfg.node_id
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.cfg
    }

    pub(crate) fn tracker(&self) -> Option<Arc<ReplicaTracker>> {
        self.tracker.read().clone()
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    /// Serve replication fetches on `listener` until shutdown.
    pub fn serve(self: &Arc<Self>, listener: TcpListener) -> JoinHandle<()> {
        let me = self.clone();
        let cancel = self.shutdown.clone();
        tokio::spawn(async move { leader::serve(me, listener, cancel).await })
    }

    /// Stop following and serving. Call during shutdown, after resigning.
    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        self.stop_follower().await;
        if let Some(t) = self.tracker.write().take() {
            t.close();
        }
    }

    async fn stop_follower(&self) {
        let running = self.follower.lock().await.take();
        if let Some((cancel, handle)) = running {
            cancel.cancel();
            let _ = handle.await;
            self.follower_info.lock().connected = false;
        }
    }

    /// Cluster status for the HTTP API.
    pub async fn status(&self) -> serde_json::Value {
        let leadership = self.leadership.get();
        let is_leader = leadership.is_some_and(|l| l.is_currently_leader());
        let lease = leadership.and_then(|l| l.lease_record());
        let mut v = serde_json::json!({
            "node_id": self.cfg.node_id,
            "role": if is_leader { "leader" } else { "follower" },
            "epoch": lease.as_ref().map(|l| l.epoch),
            "leader": lease.as_ref().filter(|l| is_leader || l.is_live()).map(|l| serde_json::json!({
                "node_id": l.holder,
                "client_endpoint": l.client_endpoint,
                "replication_endpoint": l.replication_endpoint,
            })),
            "acks": if self.cfg.acks_all { "all" } else { "leader" },
            "min_insync_replicas": self.cfg.min_insync_replicas,
        });
        if let Some(t) = self.tracker() {
            let hws = leader::high_watermarks(self).await;
            v["isr"] = serde_json::json!(t.isr());
            v["followers"] = serde_json::json!(t.followers(&hws));
        } else {
            v["replication"] = serde_json::json!(self.follower_info.lock().clone());
        }
        v
    }

    /// Rebuild every stream's dedup map from the log (snapshots ignored:
    /// a follower's snapshots don't describe what it replicated).
    fn rebuild_dedup(self: &Arc<Self>) {
        self.dedup_ready.store(false, Ordering::Release);
        let me = self.clone();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            let streams = me.storage.list_streams().await.unwrap_or_default();
            for s in &streams {
                let cfg = me.storage.stream_config(s).await.unwrap_or_default();
                let dedup = me.log.dedup();
                dedup.forget_stream(s).await;
                dedup
                    .configure_stream(s, cfg.dedup_window_secs, cfg.dedup_max_entries)
                    .await;
                if let Err(e) = dedup.rebuild_stream_from_log(s).await {
                    warn!(stream = %s, error = %e, "dedup rebuild failed");
                }
            }
            me.dedup_ready.store(true, Ordering::Release);
            info!(
                streams = streams.len(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "dedup state rebuilt after promotion"
            );
        });
    }
}

#[async_trait]
impl RoleHooks for Arc<Cluster> {
    async fn follow(&self) {
        if self.is_shutting_down() {
            return;
        }
        let mut slot = self.follower.lock().await;
        if slot.as_ref().is_some_and(|(_, h)| !h.is_finished()) {
            return;
        }
        let cancel = self.shutdown.child_token();
        let handle = tokio::spawn(follower::run(self.clone(), cancel.clone()));
        *slot = Some((cancel, handle));
        self.metrics.set_replication_role("follower");
    }

    async fn promote(&self, lease: &LeaseRecord) -> Result<(), String> {
        self.stop_follower().await;
        // Stamp the new epoch on every stream: records from here on belong
        // to it.
        let streams = self
            .storage
            .list_streams()
            .await
            .map_err(|e| format!("list streams: {e}"))?;
        for s in &streams {
            let (_, next) = self
                .storage
                .stream_bounds(s)
                .await
                .map_err(|e| format!("bounds of {s}: {e}"))?;
            self.epochs
                .update(s.as_str(), |h| {
                    h.push(lease.epoch, next.0);
                    true
                })
                .map_err(|e| format!("epoch history of {s}: {e}"))?;
        }
        self.rebuild_dedup();
        let tracker = ReplicaTracker::new(
            self.cfg.node_id.clone(),
            lease.epoch,
            self.lease.clone(),
            self.cfg.tracker(),
            &lease.isr,
        );
        if let Some(old) = self.tracker.write().replace(tracker) {
            old.close();
        }
        self.metrics.set_replication_role("leader");
        info!(
            epoch = lease.epoch,
            streams = streams.len(),
            "promotion complete"
        );
        Ok(())
    }

    async fn demoted(&self) {
        if let Some(t) = self.tracker.write().take() {
            t.close();
        }
    }
}

#[async_trait]
impl ReplicaSync for Cluster {
    fn precheck(&self) -> Result<(), LogError> {
        match self.tracker() {
            Some(t) => t.precheck().map_err(ack_error),
            None => Ok(()),
        }
    }

    async fn wait_replicated(&self, stream: &StreamName, last_offset: u64) -> Result<(), LogError> {
        match self.tracker() {
            Some(t) => t
                .wait_replicated(stream.as_str(), last_offset)
                .await
                .map_err(ack_error),
            None => Err(LogError::NotLeader),
        }
    }

    fn metadata_changed(&self, change: MetadataChange<'_>) {
        match change {
            MetadataChange::Created(s) => {
                let epoch = self.tracker().map_or(0, |t| t.epoch());
                let mut h = StreamEpochs::new(rand::random::<u64>() | 1);
                if epoch > 0 {
                    h.push(epoch, 0);
                }
                if let Err(e) = self.epochs.put(s.as_str(), h) {
                    warn!(stream = %s, error = %e, "could not write the epoch history");
                }
            }
            MetadataChange::Deleted(s) => self.epochs.remove(s.as_str()),
            MetadataChange::Updated(_) => {}
        }
    }
}

fn ack_error(e: AckError) -> LogError {
    match e {
        AckError::NotEnoughReplicas { in_sync, required } => {
            LogError::NotEnoughReplicas { in_sync, required }
        }
        AckError::Timeout => LogError::ReplicationTimeout,
        AckError::NotLeader => LogError::NotLeader,
    }
}
