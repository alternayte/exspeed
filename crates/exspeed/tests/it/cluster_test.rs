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
}

impl Opts {
    fn new() -> Self {
        Self {
            namespace: format!("cluster-test-{}", uuid::Uuid::new_v4()),
            acks: "all",
            min_isr: 1,
            unclean: false,
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
