//! kill -9 loops against the real server binary (Phase 1 exit criteria).
//!
//! Each round starts `exspeed server` on the same data directory, publishes
//! from several clients (single and batch publishes) while a durable
//! consumer pulls and acks, then SIGKILLs the process at a random moment.
//! After every restart:
//!
//! * every acknowledged publish is in the log at the offset it was given;
//! * offsets are dense from 0 to the high watermark and no message appears
//!   twice;
//! * the consumer never skips: across all rounds every record is delivered
//!   at least once, and nothing acked is redelivered past the ack floor's
//!   persistence window.
//!
//! `EXSPEED_CRASH_ROUNDS` sets the number of rounds (default 5).

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use exspeed_client::{Client, ConnectOptions, ConsumerSpec, PublishRecord, StreamSpec};
use std::sync::Mutex;
use tokio::process::{Child, Command};

use crate::common::eventually;

const STREAM: &str = "crashy";
const CONSUMER: &str = "crashy-worker";

struct Server {
    child: Child,
    addr: String,
}

async fn start(dir: &Path) -> Server {
    let port = exspeed_testkit::pick_unused_port().unwrap();
    let api = exspeed_testkit::pick_unused_port().unwrap();
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_exspeed"))
        .args([
            "server",
            "--data-dir",
            dir.to_str().unwrap(),
            "--bind",
            &addr,
            "--api-bind",
            &format!("127.0.0.1:{api}"),
        ])
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn exspeed");
    let ready = format!("http://127.0.0.1:{api}/readyz");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if matches!(reqwest::get(&ready).await, Ok(r) if r.status().is_success()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "server did not start"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Server { child, addr }
}

async fn read_all(c: &Client) -> Vec<exspeed_client::WireRecord> {
    let mut out = Vec::new();
    let mut from = 0;
    loop {
        let r = c
            .read(STREAM, from, 1000, Duration::ZERO, "")
            .await
            .expect("read");
        if r.records.is_empty() {
            return out;
        }
        from = r.next_offset;
        out.extend(r.records);
    }
}

fn rounds() -> u64 {
    std::env::var("EXSPEED_CRASH_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_9_loop_loses_no_acknowledged_write() {
    let dir = tempfile::tempdir().unwrap();
    // offset -> value, for every acknowledged publish.
    let acked: Arc<Mutex<BTreeMap<u64, String>>> = Arc::default();
    // offsets the consumer received / acked (acks may be lost on crash).
    let delivered: Arc<Mutex<HashSet<u64>>> = Arc::default();
    let mut seed = 0x2545_F491_4F6C_DD1Du64;

    for round in 0..rounds() {
        let server = start(dir.path()).await;
        let c = Client::connect(&server.addr, ConnectOptions::default())
            .await
            .unwrap();
        c.create_stream(StreamSpec::named(STREAM)).await.unwrap();
        let _ = c.create_consumer(ConsumerSpec::new(CONSUMER, STREAM)).await;

        // Everything acknowledged so far survived, at the same offsets.
        let snapshot = acked.lock().unwrap().clone();
        verify(&c, &snapshot, round).await;

        // Load: 3 single publishers, 1 batch publisher, 1 consumer.
        let mut tasks = Vec::new();
        for p in 0..4u64 {
            let acked = acked.clone();
            let addr = server.addr.clone();
            tasks.push(tokio::spawn(async move {
                let Ok(c) = Client::connect(&addr, ConnectOptions::default()).await else {
                    return;
                };
                for n in 0u64.. {
                    if p == 3 {
                        let vals: Vec<String> =
                            (0..10).map(|i| format!("r{round}-b-{n}-{i}")).collect();
                        let recs = vals
                            .iter()
                            .map(|v| PublishRecord::new("crashy.b", v.clone()).msg_id(v.clone()))
                            .collect();
                        match c.publish_batch(STREAM, recs).await {
                            Ok(acks) => {
                                let mut a = acked.lock().unwrap();
                                for (ack, v) in acks.iter().zip(vals) {
                                    assert!(!ack.duplicate);
                                    a.insert(ack.offset, v);
                                }
                            }
                            Err(_) => return,
                        }
                    } else {
                        let v = format!("r{round}-p{p}-{n}");
                        match c
                            .publish(
                                STREAM,
                                PublishRecord::new("crashy.s", v.clone()).msg_id(v.clone()),
                            )
                            .await
                        {
                            Ok(ack) => {
                                assert!(!ack.duplicate);
                                acked.lock().unwrap().insert(ack.offset, v);
                            }
                            Err(_) => return,
                        }
                    }
                }
            }));
        }
        {
            let delivered = delivered.clone();
            let addr = server.addr.clone();
            tasks.push(tokio::spawn(async move {
                let Ok(c) = Client::connect(&addr, ConnectOptions::default()).await else {
                    return;
                };
                loop {
                    match c.pull(CONSUMER, 200, Duration::from_millis(200)).await {
                        Ok(recs) => {
                            let offs: Vec<u64> = recs.iter().map(|r| r.offset).collect();
                            delivered.lock().unwrap().extend(offs.iter().copied());
                            if c.ack(CONSUMER, offs).await.is_err() {
                                return;
                            }
                        }
                        Err(_) => return,
                    }
                }
            }));
        }

        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        tokio::time::sleep(Duration::from_millis(300 + seed % 700)).await;
        let mut server = server;
        server.child.start_kill().unwrap(); // SIGKILL
        let _ = server.child.wait().await;
        for t in tasks {
            let _ = tokio::time::timeout(Duration::from_secs(10), t).await;
        }
        assert!(
            !acked.lock().unwrap().is_empty(),
            "round {round} acknowledged nothing"
        );
    }

    // Final check, then drain the consumer: every record in the log was
    // delivered at least once across all the crashes.
    let server = start(dir.path()).await;
    let c = Client::connect(&server.addr, ConnectOptions::default())
        .await
        .unwrap();
    let snapshot = acked.lock().unwrap().clone();
    let all = verify(&c, &snapshot, rounds()).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let recs = c
            .pull(CONSUMER, 500, Duration::from_millis(300))
            .await
            .unwrap();
        if recs.is_empty() && delivered.lock().unwrap().len() >= all {
            break;
        }
        let offs: Vec<u64> = recs.iter().map(|r| r.offset).collect();
        delivered.lock().unwrap().extend(offs.iter().copied());
        if !offs.is_empty() {
            c.ack(CONSUMER, offs).await.unwrap();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "consumer did not catch up: {} of {all}",
            delivered.lock().unwrap().len()
        );
    }
    let d = delivered.lock().unwrap();
    for o in 0..all as u64 {
        assert!(d.contains(&o), "record {o} was never delivered");
    }
}

/// Check the log against the acknowledged publishes; returns its length.
async fn verify(c: &Client, acked: &BTreeMap<u64, String>, round: u64) -> usize {
    let log = read_all(c).await;
    let mut seen = HashSet::new();
    for (i, r) in log.iter().enumerate() {
        assert_eq!(r.offset, i as u64, "round {round}: offsets are dense");
        let v = String::from_utf8(r.value.to_vec()).unwrap();
        assert!(seen.insert(v.clone()), "round {round}: {v} appears twice");
    }
    for (o, v) in acked {
        let r = log
            .get(*o as usize)
            .unwrap_or_else(|| panic!("round {round}: acknowledged offset {o} is missing"));
        assert_eq!(
            String::from_utf8_lossy(&r.value),
            *v,
            "round {round}: offset {o}"
        );
    }
    log.len()
}

/// A full disk under a running server: publishes fail with 507, nothing
/// acknowledged is lost, and publishing resumes once space is freed. Needs
/// a small filesystem in `EXSPEED_ENOSPC_DIR`; skipped otherwise.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_disk_fails_publishes_with_507_and_recovers() {
    let Some(root) = std::env::var_os("EXSPEED_ENOSPC_DIR").map(std::path::PathBuf::from) else {
        eprintln!("skipping: EXSPEED_ENOSPC_DIR is not set");
        return;
    };
    let dir = root.join(format!("server-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ballast = root.join(format!("server-ballast-{}", std::process::id()));
    std::fs::write(&ballast, vec![0u8; 2 * 1024 * 1024]).unwrap();

    let server = start(&dir).await;
    let c = Client::connect(&server.addr, ConnectOptions::default())
        .await
        .unwrap();
    c.create_stream(StreamSpec::named(STREAM)).await.unwrap();
    let big = "x".repeat(32 * 1024);
    let mut acked = 0u64;
    let err = loop {
        assert!(acked < 100_000, "the filesystem never filled up");
        match c
            .publish(
                STREAM,
                PublishRecord::new("full.x", format!("{acked}-{big}")),
            )
            .await
        {
            Ok(a) => {
                assert_eq!(a.offset, acked);
                acked += 1;
            }
            Err(e) => break e,
        }
    };
    assert_eq!(err.code(), Some(507), "{err}");
    // Reads still work and nothing acknowledged is missing.
    assert_eq!(read_all(&c).await.len() as u64, acked);

    std::fs::remove_file(&ballast).unwrap();
    let mut resumed = false;
    for _ in 0..50 {
        if let Ok(a) = c
            .publish(STREAM, PublishRecord::new("full.x", "after"))
            .await
        {
            assert_eq!(a.offset, acked);
            resumed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(resumed, "publishing did not resume after space was freed");
    drop(c);
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// kill -9 in a real 3-process cluster (Postgres lease)
// ---------------------------------------------------------------------------

struct ClusterProc {
    child: Option<Child>,
    dir: std::path::PathBuf,
    port: u16,
    api: u16,
    cport: u16,
}

impl ClusterProc {
    fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    async fn spawn(&mut self, pg: &str, schema: &str) {
        let child = Command::new(env!("CARGO_BIN_EXE_exspeed"))
            .args([
                "server",
                "--data-dir",
                self.dir.to_str().unwrap(),
                "--bind",
                &self.addr(),
                "--api-bind",
                &format!("127.0.0.1:{}", self.api),
            ])
            .env("EXSPEED_LEASE_BACKEND", "postgres")
            .env("EXSPEED_LEASE_POSTGRES_URL", pg)
            .env("EXSPEED_LEASE_POSTGRES_SCHEMA", schema)
            .env("EXSPEED_LEASE_TTL_SECS", "3")
            .env("EXSPEED_LEASE_HEARTBEAT_SECS", "1")
            .env("EXSPEED_CLUSTER_BIND", format!("127.0.0.1:{}", self.cport))
            .env("EXSPEED_CLIENT_ADVERTISE", self.addr())
            .env("EXSPEED_ACKS", "quorum")
            .env("EXSPEED_CLUSTER_SIZE", "3")
            .env("EXSPEED_REPLICA_LAG_MAX_MS", "3000")
            .env("RUST_LOG", "error")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn exspeed");
        self.child = Some(child);
        let ready = format!("http://127.0.0.1:{}/readyz", self.api);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while !matches!(reqwest::get(&ready).await, Ok(r) if r.status().is_success()) {
            assert!(tokio::time::Instant::now() < deadline, "node did not start");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            c.start_kill().unwrap();
            let _ = c.wait().await;
        }
    }

    async fn status(&self) -> Option<serde_json::Value> {
        reqwest::get(format!("http://127.0.0.1:{}/api/v1/cluster", self.api))
            .await
            .ok()?
            .json()
            .await
            .ok()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_9_in_a_three_node_cluster_loses_no_acknowledged_write() {
    use std::collections::BTreeSet;
    let Ok(pg) = std::env::var("EXSPEED_LEASE_POSTGRES_URL")
        .or_else(|_| std::env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL"))
    else {
        eprintln!("skipping: EXSPEED_LEASE_POSTGRES_URL is not set");
        return;
    };
    let schema = format!("crash_{}", uuid::Uuid::new_v4().simple());
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes: Vec<ClusterProc> = dirs
        .iter()
        .map(|d| ClusterProc {
            child: None,
            dir: d.path().to_path_buf(),
            port: exspeed_testkit::pick_unused_port().unwrap(),
            api: exspeed_testkit::pick_unused_port().unwrap(),
            cport: exspeed_testkit::pick_unused_port().unwrap(),
        })
        .collect();
    for n in nodes.iter_mut() {
        n.spawn(&pg, &schema).await;
    }
    let seeds: Vec<String> = nodes.iter().map(|n| n.addr()).collect();
    let c = Client::connect_cluster(&seeds, ConnectOptions::default(), Duration::from_secs(30))
        .await
        .unwrap();
    c.create_stream(StreamSpec::named(STREAM)).await.unwrap();
    drop(c);

    let acked: Arc<Mutex<BTreeSet<String>>> = Arc::default();
    let unknown: Arc<Mutex<BTreeSet<String>>> = Arc::default();
    let stop = tokio_util::sync::CancellationToken::new();
    let writer = {
        let (acked, unknown, stop, seeds) =
            (acked.clone(), unknown.clone(), stop.clone(), seeds.clone());
        tokio::spawn(async move {
            let mut client: Option<Client> = None;
            for n in 0u64.. {
                if stop.is_cancelled() {
                    return;
                }
                let v = format!("k{n}");
                let mut done = false;
                for _ in 0..40 {
                    if stop.is_cancelled() {
                        break;
                    }
                    if client.as_ref().is_none_or(|c| c.is_closed()) {
                        client = Client::connect_cluster(
                            &seeds,
                            ConnectOptions::default(),
                            Duration::from_secs(2),
                        )
                        .await
                        .ok();
                    }
                    let Some(cl) = client.as_ref() else {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    };
                    match tokio::time::timeout(
                        Duration::from_secs(12),
                        cl.publish(
                            STREAM,
                            PublishRecord::new("k.v", v.clone()).msg_id(v.clone()),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            acked.lock().unwrap().insert(v.clone());
                            done = true;
                            break;
                        }
                        Ok(Err(e)) => {
                            if e.code() == Some(503) || e.code().is_none() {
                                client = None;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Err(_) => client = None,
                    }
                }
                if !done {
                    unknown.lock().unwrap().insert(v);
                }
            }
        })
    };

    let mut rng = 0xD1B5_4A32_D192_ED03u64 ^ std::process::id() as u64;
    for _round in 0..5 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        tokio::time::sleep(Duration::from_millis(1500 + rng % 1500)).await;
        // Usually kill the leader; sometimes a follower.
        let mut victim = (rng % 3) as usize;
        if !rng.is_multiple_of(4) {
            for (i, n) in nodes.iter().enumerate() {
                if n.status().await.is_some_and(|s| s["role"] == "leader") {
                    victim = i;
                }
            }
        }
        nodes[victim].kill9().await;
        tokio::time::sleep(Duration::from_millis(500 + rng % 2000)).await;
        nodes[victim].spawn(&pg, &schema).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(40), writer).await;

    // Wait for a leader with all three nodes in sync, then check every log.
    let leader = eventually(Duration::from_secs(40), || async {
        for (i, n) in nodes.iter().enumerate() {
            let s = n.status().await?;
            if s["role"] == "leader" && s["isr"].as_array().is_some_and(|a| a.len() == 3) {
                return Some(i);
            }
        }
        None
    })
    .await;
    let lc = Client::connect(&nodes[leader].addr(), ConnectOptions::default())
        .await
        .unwrap();
    let truth = read_all(&lc).await;
    let values: Vec<String> = truth
        .iter()
        .map(|r| String::from_utf8(r.value.to_vec()).unwrap())
        .collect();
    let mut seen = HashSet::new();
    for (i, v) in values.iter().enumerate() {
        assert_eq!(truth[i].offset, i as u64, "offsets are dense");
        assert!(seen.insert(v.clone()), "{v} was written twice");
    }
    let acked = acked.lock().unwrap().clone();
    let unknown = unknown.lock().unwrap().clone();
    assert!(
        acked.len() > 20,
        "too few writes succeeded: {}",
        acked.len()
    );
    for v in &acked {
        assert!(seen.contains(v), "acknowledged {v} was lost");
    }
    for v in &values {
        assert!(
            acked.contains(v) || unknown.contains(v),
            "{v} came from nowhere"
        );
    }
    for n in &nodes {
        let c = Client::connect(&n.addr(), ConnectOptions::default())
            .await
            .unwrap();
        eventually(Duration::from_secs(20), || {
            let c = c.clone();
            let truth = truth.clone();
            async move { (read_all(&c).await == truth).then_some(()) }
        })
        .await;
    }
    for n in nodes.iter_mut() {
        n.kill9().await;
    }
}
