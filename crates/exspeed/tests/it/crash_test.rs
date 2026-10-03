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
