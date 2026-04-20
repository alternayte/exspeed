//! Leader-side fan-out. Each connected follower registers a channel;
//! `emit()` pushes the event onto every channel. A follower whose queue
//! fills (slow apply, slow network, stuck task) is dropped — the
//! client write path is never blocked on any follower.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::warn;
use uuid::Uuid;

use exspeed_common::Metrics;

use crate::replication::ReplicationEvent;

/// Per-follower metadata kept alongside the fan-out channel. Extended
/// over time with lag / apply counters; v1 tracks just the wall-clock
/// time of registration so `GET /api/v1/cluster/followers` can surface
/// "who's connected + for how long".
struct FollowerState {
    tx: mpsc::Sender<ReplicationEvent>,
    registered_at: DateTime<Utc>,
}

/// Operator-facing snapshot of one registered follower. Returned in the
/// JSON body of `GET /api/v1/cluster/followers`. Field set is intentionally
/// minimal at v1 — richer data (cursor summary, lag seconds, records
/// applied) lands in a later wave when we have lag-tracking plumbed in.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FollowerSnapshot {
    pub follower_id: Uuid,
    pub registered_at: DateTime<Utc>,
}

pub struct ReplicationCoordinator {
    /// Per-follower state (fan-out sender + registration timestamp).
    /// The receiver end is owned by the per-connection task in
    /// `server.rs` and is dropped when that task exits.
    followers: RwLock<HashMap<Uuid, FollowerState>>,
    metrics: Arc<Metrics>,
    queue_capacity: usize,
}

impl ReplicationCoordinator {
    pub fn new(metrics: Arc<Metrics>, queue_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            followers: RwLock::new(HashMap::new()),
            metrics,
            queue_capacity,
        })
    }

    /// Register a new follower. Returns the follower's id + a receiver
    /// for events. The caller must drop the receiver when the
    /// connection closes to eagerly surface the deregistration.
    pub fn register_follower(&self) -> (Uuid, mpsc::Receiver<ReplicationEvent>) {
        let id = Uuid::new_v4();
        let (tx, rx) = mpsc::channel(self.queue_capacity);
        {
            let mut map = self.followers.write();
            map.insert(
                id,
                FollowerState {
                    tx,
                    registered_at: Utc::now(),
                },
            );
            self.metrics
                .replication_connected_followers
                .record(map.len() as i64, &[]);
        }
        (id, rx)
    }

    pub fn deregister_follower(&self, id: Uuid) {
        let mut map = self.followers.write();
        map.remove(&id);
        self.metrics
            .replication_connected_followers
            .record(map.len() as i64, &[]);
    }

    pub fn is_registered(&self, id: Uuid) -> bool {
        self.followers.read().contains_key(&id)
    }

    pub fn connected_followers(&self) -> usize {
        self.followers.read().len()
    }

    /// Operator snapshot of every currently-registered follower. Holds
    /// the read lock only for the duration of the iteration (the
    /// `DateTime<Utc>` copy is cheap; no I/O inside the lock). Returned
    /// by `GET /api/v1/cluster/followers`.
    pub fn snapshot(&self) -> Vec<FollowerSnapshot> {
        let map = self.followers.read();
        map.iter()
            .map(|(id, state)| FollowerSnapshot {
                follower_id: *id,
                registered_at: state.registered_at,
            })
            .collect()
    }

    /// Best-effort broadcast. Followers whose queues are full are
    /// dropped; their connection task will see the closed channel on
    /// next recv and tear down its TCP socket.
    pub fn emit(&self, event: ReplicationEvent) {
        let mut to_drop: Vec<Uuid> = Vec::new();
        {
            let map = self.followers.read();
            for (id, state) in map.iter() {
                match state.tx.try_send(event.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!(follower_id = %id, "follower queue full — dropping connection");
                        self.metrics
                            .inc_replication_follower_queue_drop(&id.to_string());
                        to_drop.push(*id);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // Receiver is gone — connection task has already exited.
                        to_drop.push(*id);
                    }
                }
            }
        }
        if !to_drop.is_empty() {
            let mut map = self.followers.write();
            for id in &to_drop {
                map.remove(id);
            }
            self.metrics
                .replication_connected_followers
                .record(map.len() as i64, &[]);
        }
    }
}
