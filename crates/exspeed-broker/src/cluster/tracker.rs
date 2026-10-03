//! Leader-side follower tracking: replication progress, the in-sync replica
//! set (ISR) and `acks = all` waits.
//!
//! A follower is **in sync** while it caught up with the leader within the
//! last `replica_lag_max`: a fetch counts as caught up when its positions
//! reach the high watermarks the leader reported in its previous response to
//! that follower.
//!
//! The ISR is published in the lease record so that, after a failover, only
//! a node holding every acknowledged write can be elected. To keep that
//! true, acknowledgements wait for every follower in `desired ∪ published`:
//! a follower leaving the ISR keeps being waited for until its removal is
//! published.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::lease::{LeaderLease, CLUSTER_LEASE};

/// Why a write could not be acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckError {
    NotEnoughReplicas { in_sync: usize, required: usize },
    Timeout,
    NotLeader,
}

#[derive(Debug, Clone)]
pub struct TrackerConfig {
    pub acks_all: bool,
    /// Minimum in-sync replicas, the leader included.
    pub min_insync_replicas: usize,
    pub replica_lag_max: Duration,
    pub ack_timeout: Duration,
}

struct Follower {
    /// Per-stream next offset the follower has durably written.
    offsets: HashMap<String, u64>,
    last_fetch: Instant,
    last_caught_up: Option<Instant>,
    /// High watermarks sent in the last response, and when.
    pending: Option<(Instant, HashMap<String, u64>)>,
}

impl Follower {
    fn new() -> Self {
        Self {
            offsets: HashMap::new(),
            last_fetch: Instant::now(),
            last_caught_up: None,
            pending: None,
        }
    }
}

struct State {
    followers: HashMap<String, Follower>,
    /// Followers currently considered in sync.
    desired: BTreeSet<String>,
    /// Followers in the ISR published in the lease (leader excluded).
    published: BTreeSet<String>,
    /// A publication is in flight.
    publishing: bool,
    closed: bool,
}

/// Status of one follower, for the HTTP API.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FollowerStatus {
    pub node_id: String,
    pub in_sync: bool,
    pub last_fetch_ms_ago: u64,
    /// Records behind the leader, summed over streams.
    pub lag_records: u64,
}

pub struct ReplicaTracker {
    node_id: String,
    epoch: u64,
    lease: Arc<dyn LeaderLease>,
    cfg: TrackerConfig,
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}

impl ReplicaTracker {
    /// `published` is the ISR from the lease record at promotion. Its members
    /// get a grace period of `replica_lag_max` to reconnect.
    pub fn new(
        node_id: String,
        epoch: u64,
        lease: Arc<dyn LeaderLease>,
        cfg: TrackerConfig,
        published: &[String],
    ) -> Arc<Self> {
        let now = Instant::now();
        let mut followers = HashMap::new();
        let mut desired = BTreeSet::new();
        for id in published.iter().filter(|id| **id != node_id) {
            let mut f = Follower::new();
            f.last_caught_up = Some(now);
            followers.insert(id.clone(), f);
            desired.insert(id.clone());
        }
        let t = Arc::new(Self {
            node_id,
            epoch,
            lease,
            cfg,
            state: Mutex::new(State {
                followers,
                published: desired.clone(),
                desired,
                // Forces the first publication (the leader itself must be
                // in the published set).
                publishing: false,
                closed: false,
            }),
            changed: watch::channel(0).0,
        });
        t.publish(true);
        let weak = Arc::downgrade(&t);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tick.tick().await;
                let Some(t) = weak.upgrade() else { return };
                if t.state.lock().closed {
                    return;
                }
                t.evaluate();
            }
        });
        t
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().closed
    }

    /// Stop: wake every waiter with `NotLeader`.
    pub fn close(&self) {
        self.state.lock().closed = true;
        self.changed.send_modify(|v| *v += 1);
    }

    /// A fetch arrived with these positions (`stream -> next`, already
    /// capped at the leader's end for diverged followers).
    pub fn on_fetch(self: &Arc<Self>, follower: &str, positions: &HashMap<String, u64>) {
        {
            let mut st = self.state.lock();
            let f = st
                .followers
                .entry(follower.to_string())
                .or_insert_with(Follower::new);
            f.last_fetch = Instant::now();
            for (s, &o) in positions {
                f.offsets.insert(s.clone(), o);
            }
            if let Some((at, hws)) = &f.pending {
                let caught_up = hws
                    .iter()
                    .all(|(s, &hw)| hw == 0 || positions.get(s).is_some_and(|&o| o >= hw));
                if caught_up {
                    f.last_caught_up = Some(*at);
                }
            }
        }
        self.evaluate();
        self.changed.send_modify(|v| *v += 1);
    }

    /// A response was sent to `follower`, reporting these high watermarks
    /// (for every stream on the leader).
    pub fn on_response(&self, follower: &str, hws: HashMap<String, u64>, sent_at: Instant) {
        let mut st = self.state.lock();
        if let Some(f) = st.followers.get_mut(follower) {
            f.pending = Some((sent_at, hws));
        }
    }

    /// Recompute the ISR and publish it when it changed.
    fn evaluate(self: &Arc<Self>) {
        let now = Instant::now();
        let changed = {
            let mut st = self.state.lock();
            let desired: BTreeSet<String> = st
                .followers
                .iter()
                .filter(|(_, f)| {
                    f.last_caught_up
                        .is_some_and(|t| now.duration_since(t) < self.cfg.replica_lag_max)
                })
                .map(|(id, _)| id.clone())
                .collect();
            let changed = desired != st.desired;
            if changed {
                info!(isr = ?desired, "in-sync replicas changed");
                st.desired = desired;
            }
            changed || st.desired != st.published
        };
        if changed {
            self.changed.send_modify(|v| *v += 1);
            self.publish(false);
        }
    }

    fn publish(self: &Arc<Self>, force: bool) {
        let target = {
            let mut st = self.state.lock();
            if st.publishing || st.closed || (!force && st.desired == st.published) {
                return;
            }
            st.publishing = true;
            st.desired.clone()
        };
        let me = self.clone();
        tokio::spawn(async move {
            let mut isr: Vec<String> = vec![me.node_id.clone()];
            isr.extend(target.iter().cloned());
            let res = me
                .lease
                .set_isr(CLUSTER_LEASE, &me.node_id, me.epoch, &isr)
                .await;
            let again = {
                let mut st = me.state.lock();
                st.publishing = false;
                match res {
                    Ok(true) => {
                        st.published = target;
                        st.desired != st.published
                    }
                    Ok(false) => {
                        warn!("could not publish the ISR: the lease is no longer ours");
                        false
                    }
                    Err(e) => {
                        warn!(error = %e, "could not publish the ISR; retrying");
                        true
                    }
                }
            };
            me.changed.send_modify(|v| *v += 1);
            if again {
                tokio::time::sleep(Duration::from_millis(200)).await;
                me.publish(false);
            }
        });
    }

    /// Fail fast before writing when too few replicas are in sync.
    pub fn precheck(&self) -> Result<(), AckError> {
        if !self.cfg.acks_all {
            return Ok(());
        }
        let st = self.state.lock();
        if st.closed {
            return Err(AckError::NotLeader);
        }
        let in_sync = st.desired.len() + 1;
        if in_sync < self.cfg.min_insync_replicas {
            return Err(AckError::NotEnoughReplicas {
                in_sync,
                required: self.cfg.min_insync_replicas,
            });
        }
        Ok(())
    }

    /// Wait until every in-sync follower has `stream` up to `offset`
    /// (inclusive). No-op unless `acks = all`.
    pub async fn wait_replicated(&self, stream: &str, offset: u64) -> Result<(), AckError> {
        if !self.cfg.acks_all {
            return Ok(());
        }
        let deadline = Instant::now() + self.cfg.ack_timeout;
        let mut rx = self.changed.subscribe();
        loop {
            {
                let st = self.state.lock();
                if st.closed {
                    return Err(AckError::NotLeader);
                }
                let in_sync = st.desired.len() + 1;
                if in_sync < self.cfg.min_insync_replicas {
                    return Err(AckError::NotEnoughReplicas {
                        in_sync,
                        required: self.cfg.min_insync_replicas,
                    });
                }
                let done = st.desired.union(&st.published).all(|id| {
                    st.followers
                        .get(id)
                        .and_then(|f| f.offsets.get(stream))
                        .is_some_and(|&o| o > offset)
                });
                if done {
                    return Ok(());
                }
            }
            rx.borrow_and_update();
            tokio::select! {
                r = rx.changed() => if r.is_err() { return Err(AckError::NotLeader) },
                _ = tokio::time::sleep_until(deadline) => return Err(AckError::Timeout),
            }
        }
    }

    /// The ISR as currently used for acknowledgements (leader first).
    pub fn isr(&self) -> Vec<String> {
        let st = self.state.lock();
        let mut v = vec![self.node_id.clone()];
        v.extend(st.desired.iter().cloned());
        v
    }

    pub fn followers(&self, leader_hws: &HashMap<String, u64>) -> Vec<FollowerStatus> {
        let st = self.state.lock();
        let now = Instant::now();
        let mut v: Vec<FollowerStatus> = st
            .followers
            .iter()
            .map(|(id, f)| FollowerStatus {
                node_id: id.clone(),
                in_sync: st.desired.contains(id),
                last_fetch_ms_ago: now.duration_since(f.last_fetch).as_millis() as u64,
                lag_records: leader_hws
                    .iter()
                    .map(|(s, &hw)| hw.saturating_sub(f.offsets.get(s).copied().unwrap_or(0)))
                    .sum(),
            })
            .collect();
        v.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::{AcquireRequest, MemoryLeaseBackend};

    async fn tracker(
        acks_all: bool,
        min_isr: usize,
        published: &[&str],
    ) -> (Arc<ReplicaTracker>, Arc<MemoryLeaseBackend>) {
        let lease = MemoryLeaseBackend::new();
        let rec = lease
            .try_acquire(&AcquireRequest::new(
                CLUSTER_LEASE,
                "L",
                Duration::from_secs(30),
            ))
            .await
            .unwrap()
            .unwrap();
        let published: Vec<String> = published.iter().map(|s| s.to_string()).collect();
        let t = ReplicaTracker::new(
            "L".into(),
            rec.epoch,
            lease.clone(),
            TrackerConfig {
                acks_all,
                min_insync_replicas: min_isr,
                replica_lag_max: Duration::from_millis(400),
                ack_timeout: Duration::from_millis(1500),
            },
            &published,
        );
        (t, lease)
    }

    fn pos(s: &str, o: u64) -> HashMap<String, u64> {
        HashMap::from([(s.to_string(), o)])
    }

    #[tokio::test]
    async fn ack_waits_for_isr_follower() {
        let (t, lease) = tracker(true, 1, &["F"]).await;
        let t2 = t.clone();
        let w = tokio::spawn(async move { t2.wait_replicated("s", 4).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!w.is_finished());
        t.on_fetch("F", &pos("s", 5));
        assert_eq!(w.await.unwrap(), Ok(()));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let rec = lease.get(CLUSTER_LEASE).await.unwrap().unwrap();
        assert_eq!(rec.isr, vec!["L".to_string(), "F".to_string()]);
    }

    #[tokio::test]
    async fn lagging_follower_leaves_isr_and_unblocks_acks() {
        let (t, lease) = tracker(true, 1, &["F"]).await;
        // F never fetches: after the grace period it drops out.
        let res = t.wait_replicated("s", 0).await;
        assert_eq!(res, Ok(()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let rec = lease.get(CLUSTER_LEASE).await.unwrap().unwrap();
        assert_eq!(rec.isr, vec!["L".to_string()]);
        assert!(t.isr() == vec!["L".to_string()]);
    }

    #[tokio::test]
    async fn min_insync_rejects() {
        let (t, _) = tracker(true, 2, &[]).await;
        assert!(matches!(
            t.precheck(),
            Err(AckError::NotEnoughReplicas {
                in_sync: 1,
                required: 2
            })
        ));
        // A follower catches up: fetch, response with HWs, fetch reaching them.
        t.on_fetch("F", &pos("s", 0));
        t.on_response("F", pos("s", 3), Instant::now());
        t.on_fetch("F", &pos("s", 3));
        assert!(t.precheck().is_ok());
        assert_eq!(t.isr(), vec!["L".to_string(), "F".to_string()]);
    }

    #[tokio::test]
    async fn acks_leader_never_waits() {
        let (t, _) = tracker(false, 3, &["F"]).await;
        assert!(t.precheck().is_ok());
        assert_eq!(t.wait_replicated("s", 100).await, Ok(()));
    }

    #[tokio::test]
    async fn close_wakes_waiters() {
        let (t, _) = tracker(true, 1, &["F"]).await;
        let t2 = t.clone();
        let w = tokio::spawn(async move { t2.wait_replicated("s", 9).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        t.close();
        assert_eq!(w.await.unwrap(), Err(AckError::NotLeader));
    }
}
