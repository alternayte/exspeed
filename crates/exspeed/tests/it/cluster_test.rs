//! Multi-node clusters in one process: nodes share an in-memory lease
//! backend (`cluster.lease = "memory"`) and replicate over real TCP.
//!
//! Covers: replication of records (offsets, timestamps, keys, headers),
//! stream metadata and consumer state; failover without losing acknowledged
//! writes; the deposed leader truncating its divergent suffix; follower
//! restart; `min_insync_replicas`; leader hints and `connect_cluster`; the
//! cluster status endpoint.

use std::time::Duration;

use exspeed::cli::server::ServerArgs;
use exspeed_broker::lease::MemoryLeaseBackend;
use exspeed_client::{Client, ConnectOptions, ConsumerSpec, PublishRecord, StreamSpec};

use crate::common::{eventually, TestServer};

#[derive(Clone)]
struct Opts {
    namespace: String,
    acks: &'static str,
    min_isr: usize,
    unclean: bool,
    /// (cert, key, trust roots) for TLS on every listener.
    tls: Option<(std::path::PathBuf, std::path::PathBuf, std::path::PathBuf)>,
}

impl Opts {
    fn new() -> Self {
        Self {
            namespace: format!("cluster-test-{}", uuid::Uuid::new_v4()),
            acks: "all",
            min_isr: 1,
            unclean: false,
            tls: None,
        }
    }

    fn apply(&self, a: &mut ServerArgs) {
        a.cluster.lease = "memory".into();
        a.cluster.memory_namespace = self.namespace.clone();
        a.cluster.bind = "127.0.0.1:0".into();
        a.cluster.lease_ttl_ms = Some(1500);
        a.cluster.lease_heartbeat_ms = Some(150);
        a.cluster.replica_lag_max_ms = 1500;
        a.cluster.ack_timeout_ms = 5000;
        a.cluster.acks = self.acks.into();
        a.cluster.min_insync_replicas = self.min_isr;
        a.cluster.unclean_leader_election = self.unclean;
        if let Some((cert, key, ca)) = &self.tls {
            a.tls_cert = Some(cert.clone());
            a.tls_key = Some(key.clone());
            a.cluster.tls = true;
            a.cluster.tls_ca = Some(ca.clone());
        }
    }

    fn backend(&self) -> std::sync::Arc<MemoryLeaseBackend> {
        MemoryLeaseBackend::named(&self.namespace)
    }
}

async fn start_node(o: &Opts, dir: &std::path::Path) -> TestServer {
    let o = o.clone();
    TestServer::builder()
        .data_dir(dir)
        .with(move |a| o.apply(a))
        .start()
        .await
}

fn node_id(n: &TestServer) -> String {
    std::fs::read_to_string(n.data_dir.join("node_id"))
        .unwrap()
        .trim()
        .to_string()
}

async fn is_leader(n: &TestServer) -> bool {
    matches!(reqwest::get(n.api_url("/healthz")).await, Ok(r) if r.status().is_success())
}

/// Index of the node that is leader, waiting for one to emerge.
async fn leader_of(nodes: &[&TestServer]) -> usize {
    eventually(Duration::from_secs(20), || async {
        for (i, n) in nodes.iter().enumerate() {
            if is_leader(n).await {
                return Some(i);
            }
        }
        None
    })
    .await
}

async fn read_all(client: &Client, stream: &str) -> Vec<exspeed_client::WireRecord> {
    let mut out = Vec::new();
    let mut from = 0;
    loop {
        let r = client
            .read(stream, from, 1000, Duration::ZERO, "")
            .await
            .expect("read");
        if r.records.is_empty() {
            return out;
        }
        from = r.next_offset;
        out.extend(r.records);
    }
}

/// Wait until `node` has exactly the records `want` in `stream`.
async fn wait_replicated(node: &TestServer, stream: &str, want: &[exspeed_client::WireRecord]) {
    let c = node.client().await;
    eventually(Duration::from_secs(20), || {
        let c = c.clone();
        async move {
            let got = match c.stream_info(stream).await {
                Ok(_) => read_all(&c, stream).await,
                Err(_) => return None,
            };
            (got == want).then_some(())
        }
    })
    .await;
}

struct Dirs(Vec<tempfile::TempDir>);

impl Dirs {
    fn new(n: usize) -> Self {
        Self((0..n).map(|_| tempfile::tempdir().unwrap()).collect())
    }
    fn path(&self, i: usize) -> &std::path::Path {
        self.0[i].path()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicates_records_metadata_and_consumer_state() {
    let o = Opts::new();
    let dirs = Dirs::new(3);
    let a = start_node(&o, dirs.path(0)).await;
    assert!(is_leader(&a).await, "the first node leads");
    let b = start_node(&o, dirs.path(1)).await;
    let c = start_node(&o, dirs.path(2)).await;
    assert!(!is_leader(&b).await && !is_leader(&c).await);

    let la = a.client().await;
    la.create_stream(StreamSpec {
        name: "orders".into(),
        max_age_secs: 3600,
        ..Default::default()
    })
    .await
    .unwrap();
    let batch: Vec<PublishRecord> = (0..50)
        .map(|i| {
            PublishRecord::new(format!("orders.{}", i % 3), format!(r#"{{"i":{i}}}"#))
                .key(format!("k{}", i % 7))
                .header("trace", format!("t{i}"))
                .msg_id(format!("m{i}"))
        })
        .collect();
    la.publish_batch("orders", batch).await.unwrap();
    la.create_consumer(ConsumerSpec::new("billing", "orders"))
        .await
        .unwrap();
    let got = la
        .pull("billing", 10, Duration::from_secs(2))
        .await
        .unwrap();
    la.ack("billing", got.iter().map(|r| r.offset).collect())
        .await
        .unwrap();

    let want = read_all(&la, "orders").await;
    assert_eq!(want.len(), 50);
    wait_replicated(&b, "orders", &want).await;
    wait_replicated(&c, "orders", &want).await;

    // Config changes and the consumer state stream replicate too.
    la.update_stream(StreamSpec {
        name: "orders".into(),
        max_age_secs: 7200,
        ..Default::default()
    })
    .await
    .unwrap();
    let lb = b.client().await;
    eventually(Duration::from_secs(10), || {
        let lb = lb.clone();
        async move {
            let info = lb.stream_info("orders").await.ok()?;
            (info["config"]["max_age_secs"] == 7200 || info["max_age_secs"] == 7200).then_some(())
        }
    })
    .await;
    let consumers = read_all(&la, "__consumers").await;
    assert!(!consumers.is_empty());
    wait_replicated(&b, "__consumers", &consumers).await;

    // Followers refuse writes and point at the leader.
    let err = lb
        .publish("orders", PublishRecord::new("orders.x", "{}"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(503));
    assert_eq!(err.leader_hint().as_deref(), Some(a.addr.as_str()));
    assert_eq!(lb.server_info().leader.as_deref(), Some(a.addr.as_str()));

    // Deletes replicate.
    la.create_stream(StreamSpec::named("scratch"))
        .await
        .unwrap();
    eventually(Duration::from_secs(10), || {
        let lb = lb.clone();
        async move { lb.stream_info("scratch").await.ok().map(|_| ()) }
    })
    .await;
    la.delete_stream("scratch").await.unwrap();
    eventually(Duration::from_secs(10), || {
        let lb = lb.clone();
        async move { lb.stream_info("scratch").await.err().map(|_| ()) }
    })
    .await;

    // The cluster endpoint shows the ISR on the leader and the session on
    // a follower.
    let status: serde_json::Value = reqwest::get(a.api_url("/api/v1/cluster"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["role"], "leader");
    let isr = eventually(Duration::from_secs(10), || async {
        let s: serde_json::Value = reqwest::get(a.api_url("/api/v1/cluster"))
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        (s["isr"].as_array()?.len() == 3).then_some(s["isr"].clone())
    })
    .await;
    assert!(isr.as_array().unwrap().contains(&node_id(&b).into()));
    let fstatus: serde_json::Value = reqwest::get(b.api_url("/api/v1/cluster"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fstatus["role"], "follower");
    assert_eq!(fstatus["replication"]["connected"], true);
    assert_eq!(fstatus["leader"]["node_id"], node_id(&a));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_keeps_acknowledged_writes_and_consumer_progress() {
    let o = Opts::new();
    let dirs = Dirs::new(3);
    let a = start_node(&o, dirs.path(0)).await;
    let b = start_node(&o, dirs.path(1)).await;
    let c = start_node(&o, dirs.path(2)).await;
    let nodes = [&a, &b, &c];
    assert_eq!(leader_of(&nodes).await, 0);

    // Wait for a full ISR so acks=all covers every node.
    eventually(Duration::from_secs(10), || async {
        let s: serde_json::Value = reqwest::get(a.api_url("/api/v1/cluster"))
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        (s["isr"].as_array()?.len() == 3).then_some(())
    })
    .await;

    let la = a.client().await;
    la.create_stream(StreamSpec::named("events")).await.unwrap();
    for i in 0..200 {
        la.publish(
            "events",
            PublishRecord::new("events.e", format!("{i}")).msg_id(format!("id-{i}")),
        )
        .await
        .unwrap();
    }
    la.create_consumer(ConsumerSpec::new("worker", "events"))
        .await
        .unwrap();
    let first = la.pull("worker", 50, Duration::from_secs(2)).await.unwrap();
    assert_eq!(first.len(), 50);
    la.ack("worker", first.iter().map(|r| r.offset).collect())
        .await
        .unwrap();
    let acked = read_all(&la, "events").await;

    // Cut the leader off from the lease backend: it steps down, a follower
    // in the ISR takes over.
    o.backend().set_partitioned(&node_id(&a), true);
    let new_leader = eventually(Duration::from_secs(20), || async {
        for (i, n) in [&b, &c].iter().enumerate() {
            if is_leader(n).await {
                return Some(i + 1);
            }
        }
        None
    })
    .await;
    assert!(!is_leader(&a).await);
    let seeds = [a.addr.clone(), b.addr.clone(), c.addr.clone()];
    let lc = Client::connect_cluster(&seeds, ConnectOptions::default(), Duration::from_secs(20))
        .await
        .unwrap();
    assert_eq!(lc.server_info().node_id, node_id(nodes[new_leader]));
    // Every acknowledged record survived, at the same offsets.
    assert_eq!(read_all(&lc, "events").await, acked);
    // Dedup state was rebuilt: a retried publish is a duplicate.
    let dup = lc
        .publish("events", PublishRecord::new("events.e", "7").msg_id("id-7"))
        .await
        .unwrap();
    assert!(dup.duplicate);
    // The consumer resumes after its acked records.
    let next = eventually(Duration::from_secs(10), || {
        let lc = lc.clone();
        async move {
            let r = lc
                .pull("worker", 10, Duration::from_millis(500))
                .await
                .ok()?;
            (!r.is_empty()).then_some(r)
        }
    })
    .await;
    assert!(
        next[0].offset >= 50,
        "redelivered acked records: {:?}",
        next[0].offset
    );

    // New writes on the new leader; the old leader rejoins as a follower.
    lc.publish("events", PublishRecord::new("events.e", "after"))
        .await
        .unwrap();
    o.backend().set_partitioned(&node_id(&a), false);
    let want = read_all(&lc, "events").await;
    wait_replicated(&a, "events", &want).await;
    assert!(!is_leader(&a).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deposed_leader_truncates_its_divergent_records() {
    let mut o = Opts::new();
    o.acks = "leader";
    o.unclean = true;
    let dirs = Dirs::new(2);
    let a = start_node(&o, dirs.path(0)).await;
    let mut b = start_node(&o, dirs.path(1)).await;
    assert!(is_leader(&a).await);
    let la = a.client().await;
    la.create_stream(StreamSpec::named("s")).await.unwrap();
    crate::common::publish_n(&la, "s", "s.x", 10).await;
    let common_prefix = read_all(&la, "s").await;
    wait_replicated(&b, "s", &common_prefix).await;

    // b goes away; a keeps writing (acks=leader) records b never sees.
    b.stop().await;
    for i in 0..5 {
        la.publish("s", PublishRecord::new("s.a-only", format!("{i}")))
            .await
            .unwrap();
    }
    // a loses the lease; b comes back and (unclean election) takes over,
    // writing different records at the same offsets.
    o.backend().set_partitioned(&node_id(&a), true);
    // Wait until a has stepped down, so b can't copy a's extra records first.
    eventually(Duration::from_secs(10), || async {
        (!is_leader(&a).await).then_some(())
    })
    .await;
    let b2 = start_node(&o, dirs.path(1)).await;
    b = b2;
    eventually(Duration::from_secs(20), || async {
        is_leader(&b).await.then_some(())
    })
    .await;
    let lb = b.client().await;
    for i in 0..3 {
        lb.publish("s", PublishRecord::new("s.b-only", format!("{i}")))
            .await
            .unwrap();
    }
    let truth = read_all(&lb, "s").await;
    assert_eq!(truth.len(), 13);

    // a rejoins: its 5 divergent records are truncated, b's 3 copied.
    o.backend().set_partitioned(&node_id(&a), false);
    wait_replicated(&a, "s", &truth).await;
    let on_a = read_all(&a.client().await, "s").await;
    assert!(on_a.iter().all(|r| r.subject != "s.a-only"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_restart_catches_up() {
    let o = Opts::new();
    let dirs = Dirs::new(2);
    let a = start_node(&o, dirs.path(0)).await;
    let mut b = start_node(&o, dirs.path(1)).await;
    let la = a.client().await;
    la.create_stream(StreamSpec::named("s")).await.unwrap();
    crate::common::publish_n(&la, "s", "s.x", 20).await;
    b.stop().await;
    crate::common::publish_n(&la, "s", "s.y", 30).await;
    la.create_stream(StreamSpec::named("t")).await.unwrap();
    crate::common::publish_n(&la, "t", "t.x", 5).await;
    let b = start_node(&o, dirs.path(1)).await;
    wait_replicated(&b, "s", &read_all(&la, "s").await).await;
    wait_replicated(&b, "t", &read_all(&la, "t").await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn min_insync_replicas_rejects_writes_without_followers() {
    let mut o = Opts::new();
    o.min_isr = 2;
    let dirs = Dirs::new(2);
    let a = start_node(&o, dirs.path(0)).await;
    let mut b = start_node(&o, dirs.path(1)).await;
    let la = a.client().await;
    // Stream creation is metadata (not acks-gated); records need 2 replicas.
    la.create_stream(StreamSpec::named("s")).await.unwrap();
    eventually(Duration::from_secs(10), || {
        let la = la.clone();
        async move {
            la.publish("s", PublishRecord::new("s.x", "1"))
                .await
                .ok()
                .map(|_| ())
        }
    })
    .await;
    b.stop().await;
    let err = eventually(Duration::from_secs(10), || {
        let la = la.clone();
        async move { la.publish("s", PublishRecord::new("s.x", "2")).await.err() }
    })
    .await;
    assert_eq!(err.code(), Some(503), "{err}");
    let _b = start_node(&o, dirs.path(1)).await;
    eventually(Duration::from_secs(15), || {
        let la = la.clone();
        async move {
            la.publish("s", PublishRecord::new("s.x", "3"))
                .await
                .ok()
                .map(|_| ())
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queries_and_connectors_move_to_the_new_leader() {
    use serde_json::{json, Value};
    let http = reqwest::Client::new();
    async fn post(http: &reqwest::Client, url: String, body: Value) -> (u16, Value) {
        let r = http.post(url).json(&body).send().await.unwrap();
        let s = r.status().as_u16();
        (s, r.json().await.unwrap_or(Value::Null))
    }
    async fn count(http: &reqwest::Client, node: &TestServer, stream: &str) -> Option<i64> {
        let r = http
            .post(node.api_url("/api/v1/queries"))
            .json(&json!({"sql": format!("SELECT COUNT(*) FROM {stream}")}))
            .send()
            .await
            .ok()?;
        let v: Value = r.json().await.ok()?;
        v["rows"][0][0].as_i64()
    }

    let o = Opts::new();
    let dirs = Dirs::new(2);
    let a = start_node(&o, dirs.path(0)).await;
    let b = start_node(&o, dirs.path(1)).await;
    let (s, body) = post(&http, a.api_url("/api/v1/streams"), json!({"name": "src"})).await;
    assert_eq!(s, 201, "{body}");
    let (s, body) = post(
        &http,
        a.api_url("/api/v1/queries"),
        json!({"sql": "CREATE STREAM copy AS SELECT payload->>'v' AS v FROM src"}),
    )
    .await;
    assert_eq!(s, 201, "{body}");
    let qid = body["query_id"].as_str().unwrap().to_string();
    let (s, body) = post(
        &http,
        a.api_url("/api/v1/connectors"),
        json!({"name": "hook", "type": "source", "plugin": "http_webhook", "stream": "src",
               "settings": {"path": "hook", "auth_type": "none"}}),
    )
    .await;
    assert_eq!(s, 201, "{body}");
    for v in 0..3 {
        let r = http
            .post(a.api_url("/webhooks/hook"))
            .json(&json!({ "v": v }))
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success());
    }
    eventually(Duration::from_secs(15), || async {
        (count(&http, &a, "copy").await == Some(3)).then_some(())
    })
    .await;

    o.backend().set_partitioned(&node_id(&a), true);
    eventually(Duration::from_secs(20), || async {
        is_leader(&b).await.then_some(())
    })
    .await;

    // The new leader runs the same query and webhook connector.
    eventually(Duration::from_secs(15), || async {
        let q: Value = http
            .get(b.api_url(&format!("/api/v1/queries/{qid}")))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        let c: Value = http
            .get(b.api_url("/api/v1/connectors/hook"))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        (q["status"] == "running" && c["status"] == "running").then_some(())
    })
    .await;
    for v in 3..5 {
        let r = http
            .post(b.api_url("/webhooks/hook"))
            .json(&json!({ "v": v }))
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success(), "{}", r.status());
    }
    // The query resumes from its replicated checkpoint: 5 rows, no repeats.
    eventually(Duration::from_secs(15), || async {
        (count(&http, &b, "copy").await == Some(5)).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(count(&http, &b, "copy").await, Some(5));
}

fn self_signed(dir: &std::path::Path, name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let c =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let cert = dir.join(format!("{name}.pem"));
    let key = dir.join(format!("{name}.key"));
    std::fs::write(&cert, c.cert.pem()).unwrap();
    std::fs::write(&key, c.key_pair.serialize_pem()).unwrap();
    (cert, key)
}

async fn https_json(url: String) -> Option<serde_json::Value> {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_over_tls_and_untrusted_peers_are_refused() {
    let certs = tempfile::tempdir().unwrap();
    let (cert, key) = self_signed(certs.path(), "node");
    let (other, _) = self_signed(certs.path(), "other");
    let mut o = Opts::new();
    o.tls = Some((cert.clone(), key.clone(), cert.clone()));
    let dirs = Dirs::new(3);
    let a = start_node(&o, dirs.path(0)).await;
    let b = start_node(&o, dirs.path(1)).await;

    // b replicates from a over TLS.
    let status = eventually(Duration::from_secs(15), || async {
        let s = https_json(format!("https://{}/api/v1/cluster", a.api_addr)).await?;
        (s["isr"].as_array()?.len() == 2).then_some(s)
    })
    .await;
    assert_eq!(status["role"], "leader");

    // A node that doesn't trust a's certificate can't replicate from it.
    let mut wrong = o.clone();
    wrong.tls = Some((cert.clone(), key.clone(), other));
    let c = start_node(&wrong, dirs.path(2)).await;
    let err = eventually(Duration::from_secs(15), || async {
        let s = https_json(format!("https://{}/api/v1/cluster", c.api_addr)).await?;
        let e = s["replication"]["last_error"].as_str()?.to_string();
        (s["replication"]["connected"] == false && !e.is_empty()).then_some(e)
    })
    .await;
    assert!(err.contains("TLS"), "{err}");
    let fs = https_json(format!("https://{}/api/v1/cluster", a.api_addr))
        .await
        .unwrap();
    assert_eq!(
        fs["isr"].as_array().unwrap().len(),
        2,
        "the untrusting node never joined"
    );
    drop((b, c));
}

// ---------------------------------------------------------------------------
// Jepsen-style randomized faults
// ---------------------------------------------------------------------------

/// A TCP proxy in front of a node's replication listener. Peers dial the
/// proxy (it is the node's advertised endpoint), so cutting it isolates the
/// node's log from followers: open connections are dropped and new ones
/// refused until it is healed.
struct FaultProxy {
    addr: String,
    cut: std::sync::Arc<std::sync::atomic::AtomicBool>,
    generation: std::sync::Arc<tokio::sync::watch::Sender<u64>>,
}

impl FaultProxy {
    async fn start(target: String) -> Self {
        use std::sync::atomic::Ordering;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let cut = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let generation = std::sync::Arc::new(tokio::sync::watch::channel(0u64).0);
        let (c, g) = (cut.clone(), generation.clone());
        tokio::spawn(async move {
            loop {
                let Ok((inbound, _)) = listener.accept().await else {
                    return;
                };
                if c.load(Ordering::SeqCst) {
                    drop(inbound);
                    continue;
                }
                let target = target.clone();
                let mut killed = g.subscribe();
                tokio::spawn(async move {
                    let Ok(outbound) = tokio::net::TcpStream::connect(&target).await else {
                        return;
                    };
                    let (mut ri, mut wi) = inbound.into_split();
                    let (mut ro, mut wo) = outbound.into_split();
                    tokio::select! {
                        _ = tokio::io::copy(&mut ri, &mut wo) => {}
                        _ = tokio::io::copy(&mut ro, &mut wi) => {}
                        _ = killed.changed() => {}
                    }
                });
            }
        });
        Self {
            addr,
            cut,
            generation,
        }
    }

    fn set_cut(&self, cut: bool) {
        self.cut.store(cut, std::sync::atomic::Ordering::SeqCst);
        if cut {
            self.generation.send_modify(|g| *g += 1);
        }
    }
}

struct JNode {
    server: Option<TestServer>,
    dir: std::path::PathBuf,
    proxy: FaultProxy,
    bind: String,
    id: String,
}

async fn start_jnode(o: &Opts, dir: &std::path::Path, bind: &str, advertise: &str) -> TestServer {
    let o = o.clone();
    let (bind, advertise) = (bind.to_string(), advertise.to_string());
    TestServer::builder()
        .data_dir(dir)
        .with(move |a| {
            o.apply(a);
            a.cluster.bind = bind;
            a.cluster.advertise = Some(advertise);
        })
        .start()
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn randomized_partitions_and_restarts_lose_no_acknowledged_write() {
    use std::collections::{BTreeSet, HashSet};
    let mut o = Opts::new();
    o.min_isr = 2;
    let dirs = Dirs::new(3);
    let mut nodes = Vec::new();
    for i in 0..3 {
        let port = exspeed_testkit::pick_unused_port().unwrap();
        let bind = format!("127.0.0.1:{port}");
        let proxy = FaultProxy::start(bind.clone()).await;
        let server = start_jnode(&o, dirs.path(i), &bind, &proxy.addr).await;
        let id = node_id(&server);
        nodes.push(JNode {
            server: Some(server),
            dir: dirs.path(i).to_path_buf(),
            proxy,
            bind,
            id,
        });
    }
    let seeds: Vec<String> = nodes
        .iter()
        .map(|n| n.server.as_ref().unwrap().addr.clone())
        .collect();
    {
        let c = Client::connect_cluster(&seeds, ConnectOptions::default(), Duration::from_secs(20))
            .await
            .unwrap();
        c.create_stream(StreamSpec::named("jep")).await.unwrap();
    }

    // Writer: unique values, each retried with its msg_id until acknowledged
    // or given up on (outcome unknown).
    let acked: std::sync::Arc<std::sync::Mutex<BTreeSet<String>>> = Default::default();
    let unknown: std::sync::Arc<std::sync::Mutex<BTreeSet<String>>> = Default::default();
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
                let v = format!("v{n}");
                let mut done = false;
                for _attempt in 0..20 {
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
                    let Some(c) = client.as_ref() else {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    };
                    match tokio::time::timeout(
                        Duration::from_secs(8),
                        c.publish(
                            "jep",
                            PublishRecord::new("jep.v", v.clone()).msg_id(v.clone()),
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
                            if e.code() == Some(503) && e.leader_hint().is_some() {
                                client = None; // follow the leader
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

    // Nemesis.
    let backend = o.backend();
    let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ std::process::id() as u64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for _round in 0..8 {
        let victim = (next() % 3) as usize;
        match next() % 4 {
            // Cut the victim off the lease backend and from its followers.
            0 => {
                backend.set_partitioned(&nodes[victim].id, true);
                nodes[victim].proxy.set_cut(true);
            }
            // Cut only the replication link (acks=all writes stall; the ISR
            // shrinks; with min_insync_replicas = 2 writes may be refused).
            1 => nodes[victim].proxy.set_cut(true),
            // Restart the victim.
            2 => {
                if let Some(mut s) = nodes[victim].server.take() {
                    s.stop().await;
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
                let n = &nodes[victim];
                nodes[victim].server = Some(start_jnode(&o, &n.dir, &n.bind, &n.proxy.addr).await);
            }
            // Calm.
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(1500 + next() % 1500)).await;
        for n in &nodes {
            backend.set_partitioned(&n.id, false);
            n.proxy.set_cut(false);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), writer).await;

    // Healed: wait for a leader with the full ISR, then check every log.
    let leader = eventually(Duration::from_secs(30), || async {
        for (i, n) in nodes.iter().enumerate() {
            let s = n.server.as_ref()?;
            let st: serde_json::Value = reqwest::get(s.api_url("/api/v1/cluster"))
                .await
                .ok()?
                .json()
                .await
                .ok()?;
            if st["role"] == "leader" && st["isr"].as_array().is_some_and(|a| a.len() == 3) {
                return Some(i);
            }
        }
        None
    })
    .await;
    let lc = nodes[leader].server.as_ref().unwrap().client().await;
    let truth = read_all(&lc, "jep").await;
    let values: Vec<String> = truth
        .iter()
        .map(|r| String::from_utf8(r.value.to_vec()).unwrap())
        .collect();
    let mut seen = HashSet::new();
    for v in &values {
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
        let s = n.server.as_ref().unwrap();
        wait_replicated(s, "jep", &truth).await;
    }
}
