//! Two in-process nodes (FileStorage + Log + Cluster + ClusterLeadership on
//! the in-memory lease) replicating over real TCP. Covers retention
//! mirroring and a follower that is behind the leader's earliest offset.

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use exspeed_broker::broker_append::BrokerAppend;
use exspeed_broker::cluster::{Cluster, ClusterConfig};
use exspeed_broker::leadership::{ClusterLeadership, LeadershipOptions, RoleHooks};
use exspeed_broker::lease::{LeaderLease, MemoryLeaseBackend};
use exspeed_broker::log::Log;
use exspeed_common::{Metrics, Offset, StreamName};
use exspeed_storage::file::FileStorage;
use exspeed_streams::{ReadLimits, Record, StorageEngine, StreamConfig};

struct Node {
    storage: Arc<FileStorage>,
    log: Arc<Log>,
    leadership: ClusterLeadership,
    cluster: Arc<Cluster>,
}

impl Node {
    async fn start(lease: Arc<dyn LeaderLease>, id: &str, dir: &Path) -> Node {
        let storage = Arc::new(FileStorage::open(dir).unwrap());
        let engine: Arc<dyn StorageEngine> = storage.clone();
        let metrics = Arc::new(Metrics::new().0);
        let ready = Arc::new(AtomicBool::new(false));
        let dedup = Arc::new(BrokerAppend::new(engine.clone(), 300));
        let log = Arc::new(Log::new(
            engine.clone(),
            dedup,
            metrics.clone(),
            ready.clone(),
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let mut cfg = ClusterConfig::new(id);
        cfg.acks_all = false;
        cfg.fetch_max_wait = Duration::from_millis(200);
        let cluster = Cluster::new(
            cfg,
            dir,
            engine,
            log.clone(),
            metrics.clone(),
            None,
            lease.clone(),
            ready,
        )
        .unwrap();
        let mut opts = LeadershipOptions::new(id);
        opts.ttl = Duration::from_millis(1500);
        opts.heartbeat = Duration::from_millis(150);
        opts.replication_endpoint = Some(addr);
        let leadership = ClusterLeadership::start(
            lease,
            metrics,
            opts,
            Some(Arc::new(cluster.clone()) as Arc<dyn RoleHooks>),
        );
        log.set_write_gate(Arc::new(leadership.clone()));
        cluster.set_leadership(leadership.clone());
        cluster.serve(listener);
        Node {
            storage,
            log,
            leadership,
            cluster,
        }
    }

    async fn stop(self) {
        self.leadership.resign().await;
        self.cluster.shutdown().await;
        let s = self.storage.clone();
        tokio::task::spawn_blocking(move || s.close())
            .await
            .unwrap();
    }

    async fn bounds(&self, s: &str) -> Option<(u64, u64)> {
        self.storage
            .stream_bounds(&sn(s))
            .await
            .ok()
            .map(|(a, b)| (a.0, b.0))
    }

    async fn values(&self, s: &str) -> Vec<(u64, String)> {
        let mut out = Vec::new();
        let mut from = self.bounds(s).await.map_or(0, |b| b.0);
        loop {
            let b = self
                .storage
                .read_batch(&sn(s), Offset(from), ReadLimits::default())
                .await
                .unwrap();
            if b.records.is_empty() {
                return out;
            }
            from = b.next_offset.0;
            out.extend(
                b.records
                    .into_iter()
                    .map(|r| (r.offset.0, String::from_utf8(r.value.to_vec()).unwrap())),
            );
        }
    }

    async fn append(&self, s: &str, range: std::ops::Range<u64>) {
        for i in range {
            self.log
                .append(
                    &sn(s),
                    Record {
                        key: None,
                        value: Bytes::from(format!("v{i}")),
                        subject: "x".into(),
                        headers: vec![],
                        timestamp_ns: None,
                    },
                )
                .await
                .unwrap();
        }
    }
}

fn sn(s: &str) -> StreamName {
    StreamName::try_from(s).unwrap()
}

async fn wait_until<F, Fut>(what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !f().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_mirrors_the_leaders_trims() {
    let lease = MemoryLeaseBackend::new();
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = Node::start(lease.clone(), "a", da.path()).await;
    wait_until("a leads", || async { a.leadership.is_currently_leader() }).await;
    let b = Node::start(lease.clone(), "b", db.path()).await;

    a.log
        .create_stream(&sn("s"), &StreamConfig::default())
        .await
        .unwrap();
    // Small segments, so a trim (retention) can drop some of them.
    assert!(a.storage.set_stream_segment_max_bytes("s", 512));
    wait_until("b has the stream", || async {
        b.bounds("s").await.is_some()
    })
    .await;
    assert!(b.storage.set_stream_segment_max_bytes("s", 512));
    for i in 0..100 {
        a.append("s", i..i + 1).await;
    }
    wait_until("b has everything", || async {
        b.bounds("s").await == Some((0, 100))
    })
    .await;

    // Retention on the leader (here: a direct trim) drops whole segments
    // and moves its earliest offset; the follower trims up to it.
    a.storage.trim_up_to(&sn("s"), Offset(40)).await.unwrap();
    let (leader_earliest, _) = a.bounds("s").await.unwrap();
    assert!(
        leader_earliest > 0 && leader_earliest <= 40,
        "{leader_earliest}"
    );
    wait_until("b trims too", || async {
        b.bounds("s").await.unwrap().0 > 0
    })
    .await;
    let (follower_earliest, next) = b.bounds("s").await.unwrap();
    assert!(follower_earliest <= leader_earliest);
    assert_eq!(next, 100);
    let leader = a.values("s").await;
    let follower = b.values("s").await;
    assert_eq!(
        follower
            .iter()
            .filter(|(o, _)| *o >= leader_earliest)
            .cloned()
            .collect::<Vec<_>>(),
        leader
    );

    b.stop().await;
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_behind_the_leaders_earliest_offset_recovers() {
    let lease = MemoryLeaseBackend::new();
    let (da, db, dc) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let a = Node::start(lease.clone(), "a", da.path()).await;
    wait_until("a leads", || async { a.leadership.is_currently_leader() }).await;
    let b = Node::start(lease.clone(), "b", db.path()).await;
    a.log
        .create_stream(&sn("s"), &StreamConfig::default())
        .await
        .unwrap();
    assert!(a.storage.set_stream_segment_max_bytes("s", 512));
    a.append("s", 0..20).await;
    wait_until("b has the first 20", || async {
        b.bounds("s").await == Some((0, 20))
    })
    .await;

    // b goes away; the leader moves on and trims past b's position.
    b.stop().await;
    for i in 20..100 {
        a.append("s", i..i + 1).await;
    }
    a.storage.trim_up_to(&sn("s"), Offset(60)).await.unwrap();
    let (leader_earliest, _) = a.bounds("s").await.unwrap();
    assert!(leader_earliest > 20, "{leader_earliest}");

    // b comes back with its log ending before the leader's earliest offset
    // (it holds records the leader dropped, then a gap); a brand-new node c
    // starts empty. Both end up with exactly the leader's records.
    let b = Node::start(lease.clone(), "b", db.path()).await;
    let c = Node::start(lease.clone(), "c", dc.path()).await;
    let leader = a.values("s").await;
    for (n, node) in [("b", &b), ("c", &c)] {
        wait_until(&format!("{n} catches up"), || async {
            node.bounds("s").await.map(|b| b.1) == Some(100)
        })
        .await;
        assert_eq!(node.values("s").await, leader, "{n}");
    }

    // And they keep following new writes.
    a.append("s", 100..110).await;
    wait_until("b follows on", || async {
        b.bounds("s").await.map(|b| b.1) == Some(110)
    })
    .await;
    assert_eq!(b.values("s").await, a.values("s").await);

    c.stop().await;
    b.stop().await;
    a.stop().await;
}
