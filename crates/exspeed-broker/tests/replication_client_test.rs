#![cfg(test)]
//! Integration tests for `ReplicationClient`. We spin up a minimal TCP
//! listener (stub leader) in each test and drive the wire directly —
//! no real `ReplicationServer` needed, which lets us inject exact frame
//! sequences including silent idle periods.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use exspeed_broker::replication::client::ReplicationClient;
use exspeed_common::types::StreamName;
use exspeed_common::{Metrics, Offset};
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{ConnectRequest, ConnectResponse};
use exspeed_protocol::messages::replicate::{
    ClusterManifest, RecordsAppended, ReplicateResume, ReplicatedRecord, RetentionTrimmedEvent,
    StreamCreatedEvent, StreamDeletedEvent, StreamReseedEvent, StreamSummary,
};
use exspeed_protocol::opcodes::OpCode;
use exspeed_storage::memory::MemoryStorage;
use exspeed_streams::{Record, StorageEngine};

// ---------------------------------------------------------------------------
// Test harness: a stub leader that performs the handshake and then lets the
// test driver write arbitrary follow-up frames through a channel.
// ---------------------------------------------------------------------------

type FramedW = FramedWrite<tokio::io::WriteHalf<TcpStream>, ExspeedCodec>;
type FramedR = FramedRead<tokio::io::ReadHalf<TcpStream>, ExspeedCodec>;

/// Accept one follower, complete the handshake (ConnectOk + expect
/// ReplicateResume), send the given manifest, then hand the framed
/// writer back to the test so it can drive subsequent events.
///
/// Returns:
///   - the local port (so the test can hand it as an "endpoint")
///   - a oneshot receiver that yields the `(FramedR, FramedW, ReplicateResume)`
///     after the handshake completes so the test can inject frames and
///     verify what the client sent.
async fn spawn_stub_leader(
    manifest: ClusterManifest,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Receiver<(FramedR, FramedW, ReplicateResume)>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let (handshake_tx, handshake_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        let (socket, _peer) = listener.accept().await.expect("accept");
        let (r, w) = tokio::io::split(socket);
        let mut framed_read = FramedRead::new(r, ExspeedCodec::new());
        let mut framed_write = FramedWrite::new(w, ExspeedCodec::new());

        // Expect Connect.
        let connect_frame = framed_read
            .next()
            .await
            .expect("connect frame")
            .expect("decoded");
        assert_eq!(connect_frame.opcode, OpCode::Connect);
        let _ = ConnectRequest::decode(connect_frame.payload).expect("connect decode");

        // Send ConnectOk.
        let mut payload = BytesMut::new();
        ConnectResponse { server_version: 2 }.encode(&mut payload);
        framed_write
            .send(Frame::new(
                OpCode::ConnectOk,
                connect_frame.correlation_id,
                payload.freeze(),
            ))
            .await
            .expect("send ConnectOk");

        // Expect ReplicateResume.
        let resume_frame = framed_read
            .next()
            .await
            .expect("resume frame")
            .expect("decoded");
        assert_eq!(resume_frame.opcode, OpCode::ReplicateResume);
        let resume: ReplicateResume = bincode::deserialize(&resume_frame.payload).unwrap();

        // Send ClusterManifest.
        let bytes = bincode::serialize(&manifest).unwrap();
        framed_write
            .send(Frame::new(OpCode::ClusterManifest, 0, Bytes::from(bytes)))
            .await
            .expect("send manifest");

        // Hand channel back to the test.
        let _ = handshake_tx.send((framed_read, framed_write, resume));
    });

    (addr, handshake_rx)
}

/// Push a ReplicationEvent-style payload to the stub's writer.
async fn push_frame<T: serde::Serialize>(
    framed_write: &mut FramedW,
    opcode: OpCode,
    payload: &T,
) {
    let bytes = bincode::serialize(payload).unwrap();
    framed_write
        .send(Frame::new(opcode, 0, Bytes::from(bytes)))
        .await
        .expect("send stub frame");
}

/// Wrapper that spawns the client in the background so tests can observe
/// storage state while it's running.
struct ClientHandle {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    _tmp: TempDir, // owned for the duration of the test
}

impl ClientHandle {
    async fn shutdown(self) {
        self.cancel.cancel();
        // The client may be parked in a `sleep(backoff)`; give it a
        // healthy deadline.
        let _ = tokio::time::timeout(Duration::from_secs(5), self.task).await;
    }
}

fn metrics() -> Arc<Metrics> {
    let (m, _r) = Metrics::new();
    Arc::new(m)
}

/// Build `Metrics` + the prometheus `Registry` so a test can assert on
/// specific counter/label-set values after the client emits them.
fn metrics_with_registry() -> (Arc<Metrics>, prometheus::Registry) {
    let (m, r) = Metrics::new();
    (Arc::new(m), r)
}

/// Look up a counter by name + required label filters. Returns the
/// summed value across matching series, or `0.0` if no series matched.
/// Kept small rather than extracting to a helper crate — one metric,
/// one test, per the reviewer's guidance.
fn counter_value(
    registry: &prometheus::Registry,
    metric_name: &str,
    required_labels: &[(&str, &str)],
) -> f64 {
    let mut total = 0.0;
    for mf in registry.gather() {
        if mf.get_name() != metric_name {
            continue;
        }
        for m in mf.get_metric() {
            let mut matches = true;
            for (k, v) in required_labels {
                let hit = m
                    .get_label()
                    .iter()
                    .any(|lp: &prometheus::proto::LabelPair| {
                        lp.get_name() == *k && lp.get_value() == *v
                    });
                if !hit {
                    matches = false;
                    break;
                }
            }
            if matches {
                total += m.get_counter().get_value();
            }
        }
    }
    total
}

/// Look up a gauge by name + required label filters. Returns the last
/// series value that matched the required labels, or `None` if no series
/// matched. OTel exports gauges as Prometheus gauges — a single sample
/// per label-set, so "last match wins" is equivalent to "the value".
fn gauge_value(
    registry: &prometheus::Registry,
    metric_name: &str,
    required_labels: &[(&str, &str)],
) -> Option<f64> {
    for mf in registry.gather() {
        if mf.get_name() != metric_name {
            continue;
        }
        for m in mf.get_metric() {
            let mut matches = true;
            for (k, v) in required_labels {
                let hit = m
                    .get_label()
                    .iter()
                    .any(|lp: &prometheus::proto::LabelPair| {
                        lp.get_name() == *k && lp.get_value() == *v
                    });
                if !hit {
                    matches = false;
                    break;
                }
            }
            if matches {
                return Some(m.get_gauge().get_value());
            }
        }
    }
    None
}

/// Build a client with `MemoryStorage` and spawn its run loop at `addr`.
/// The endpoint getter returns `addr` forever — the client dials, the
/// handshake runs, and then the test controls the stream.
fn spawn_client(
    storage: Arc<dyn StorageEngine>,
    cursor_path: std::path::PathBuf,
    addr: std::net::SocketAddr,
    metrics: Arc<Metrics>,
    tmp: TempDir,
) -> ClientHandle {
    let client =
        ReplicationClient::new(storage, cursor_path, metrics).expect("build ReplicationClient");
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    let task = tokio::spawn(async move {
        let endpoint = format!("{}:{}", addr.ip(), addr.port());
        client
            .run(
                move || {
                    let ep = endpoint.clone();
                    async move { Some(ep) }
                },
                "test-token".to_string(),
                c2,
            )
            .await;
    });
    ClientHandle {
        cancel,
        task,
        _tmp: tmp,
    }
}

async fn wait_for<F, Fut>(cond: F, deadline: Duration, what: &str)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while !cond().await {
        if start.elapsed() > deadline {
            panic!("timed out waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn stream_summary(name: &str, earliest: u64, latest: u64) -> StreamSummary {
    StreamSummary {
        name: name.to_string(),
        max_age_secs: 0,
        max_bytes: 0,
        earliest_offset: earliest,
        latest_offset: latest,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn applies_records_appended_to_storage() {
    let storage = Arc::new(MemoryStorage::new());
    let tmp = TempDir::new().unwrap();
    let cursor_path = tmp.path().join("cursor.json");

    // Leader advertises "orders" with 10 records already present at its tail.
    let manifest = ClusterManifest {
        streams: vec![stream_summary("orders", 0, 10)],
        leader_holder_id: Uuid::new_v4(),
    };
    let (addr, handshake_rx) = spawn_stub_leader(manifest).await;

    let handle = spawn_client(
        storage.clone() as Arc<dyn StorageEngine>,
        cursor_path.clone(),
        addr,
        metrics(),
        tmp,
    );

    let (_framed_read, mut framed_write, _resume) = handshake_rx.await.expect("handshake");

    // Push a batch of 10 records with base_offset 0.
    let records: Vec<ReplicatedRecord> = (0..10u64)
        .map(|i| ReplicatedRecord {
            subject: "orders.placed".into(),
            payload: format!("rec-{i}").into_bytes(),
            headers: vec![],
            timestamp_ms: 1_700_000_000_000 + i,
            msg_id: None,
        })
        .collect();
    let batch = RecordsAppended {
        stream: "orders".into(),
        base_offset: 0,
        records,
    };
    push_frame(&mut framed_write, OpCode::RecordsAppended, &batch).await;

    // Assert storage has 10 records at offsets 0..10.
    let name = StreamName::try_from("orders").unwrap();
    wait_for(
        || async {
            storage
                .stream_bounds(&name)
                .await
                .map(|(_, next)| next.0 == 10)
                .unwrap_or(false)
        },
        Duration::from_secs(3),
        "storage to have 10 records",
    )
    .await;
    let all = storage.read(&name, Offset(0), 100).await.unwrap();
    assert_eq!(all.len(), 10);
    for (i, r) in all.iter().enumerate() {
        assert_eq!(r.offset.0, i as u64);
    }

    // Assert cursor advanced to 10.
    wait_for(
        || async {
            let raw = std::fs::read_to_string(&cursor_path).unwrap_or_default();
            raw.contains("\"orders\"") && raw.contains("10")
        },
        Duration::from_secs(3),
        "cursor to advance to 10",
    )
    .await;

    handle.shutdown().await;
}

#[tokio::test]
async fn creates_stream_on_stream_created_event() {
    let storage = Arc::new(MemoryStorage::new());
    let tmp = TempDir::new().unwrap();
    let cursor_path = tmp.path().join("cursor.json");

    // Manifest has NO streams; follower starts empty.
    let manifest = ClusterManifest {
        streams: vec![],
        leader_holder_id: Uuid::new_v4(),
    };
    let (addr, handshake_rx) = spawn_stub_leader(manifest).await;

    let handle = spawn_client(
        storage.clone() as Arc<dyn StorageEngine>,
        cursor_path,
        addr,
        metrics(),
        tmp,
    );

    let (_r, mut w, _) = handshake_rx.await.expect("handshake");

    let ev = StreamCreatedEvent {
        name: "orders".into(),
        max_age_secs: 3600,
        max_bytes: 1_000_000,
    };
    push_frame(&mut w, OpCode::StreamCreatedEvent, &ev).await;

    let name = StreamName::try_from("orders").unwrap();
    wait_for(
        || async { storage.stream_bounds(&name).await.is_ok() },
        Duration::from_secs(3),
        "stream 'orders' to exist",
    )
    .await;

    handle.shutdown().await;
}

#[tokio::test]
async fn deletes_stream_on_stream_deleted_event() {
    let storage = Arc::new(MemoryStorage::new());
    let name = StreamName::try_from("orders").unwrap();
    storage.create_stream(&name, 0, 0).await.unwrap();
    for i in 0..5u32 {
        storage
            .append(
                &name,
                &Record {
                    key: None,
                    value: Bytes::from(format!("v{i}")),
                    subject: "s".into(),
                    headers: vec![],
                    timestamp_ns: None,
                },
            )
            .await
            .unwrap();
    }

    let tmp = TempDir::new().unwrap();
    let cursor_path = tmp.path().join("cursor.json");
    // Pre-seed the cursor so we can observe the entry getting removed.
    {
        let mut initial = BTreeMap::new();
        initial.insert("orders".to_string(), 5u64);
        let s = serde_json::to_string(&initial).unwrap();
        std::fs::write(&cursor_path, s).unwrap();
    }

    let manifest = ClusterManifest {
        streams: vec![stream_summary("orders", 0, 5)],
        leader_holder_id: Uuid::new_v4(),
    };
    let (addr, handshake_rx) = spawn_stub_leader(manifest).await;

    let handle = spawn_client(
        storage.clone() as Arc<dyn StorageEngine>,
        cursor_path.clone(),
        addr,
        metrics(),
        tmp,
    );

    let (_r, mut w, _) = handshake_rx.await.expect("handshake");

    push_frame(
        &mut w,
        OpCode::StreamDeletedEvent,
        &StreamDeletedEvent {
            name: "orders".into(),
        },
    )
    .await;

    wait_for(
        || async {
            matches!(
                storage.stream_bounds(&name).await,
                Err(exspeed_streams::StorageError::StreamNotFound(_))
            )
        },
        Duration::from_secs(3),
        "stream to be deleted",
    )
    .await;

    // Cursor entry for orders should be removed.
    wait_for(
        || async {
            let raw = std::fs::read_to_string(&cursor_path).unwrap_or_default();
            !raw.contains("\"orders\"")
        },
        Duration::from_secs(3),
        "cursor entry to be removed",
    )
    .await;

    handle.shutdown().await;
}

#[tokio::test]
async fn trims_on_retention_trimmed_event() {
    let storage = Arc::new(MemoryStorage::new());
    let name = StreamName::try_from("orders").unwrap();
    storage.create_stream(&name, 0, 0).await.unwrap();
    for i in 0..10u32 {
        storage
            .append(
                &name,
                &Record {
                    key: None,
                    value: Bytes::from(format!("v{i}")),
                    subject: "s".into(),
                    headers: vec![],
                    timestamp_ns: None,
                },
            )
            .await
            .unwrap();
    }

    let tmp = TempDir::new().unwrap();
    let cursor_path = tmp.path().join("cursor.json");

    let manifest = ClusterManifest {
        streams: vec![stream_summary("orders", 0, 10)],
        leader_holder_id: Uuid::new_v4(),
    };
    let (addr, handshake_rx) = spawn_stub_leader(manifest).await;

    let handle = spawn_client(
        storage.clone() as Arc<dyn StorageEngine>,
        cursor_path,
        addr,
        metrics(),
        tmp,
    );

    let (_r, mut w, _) = handshake_rx.await.expect("handshake");

    push_frame(
        &mut w,
        OpCode::RetentionTrimmedEvent,
        &RetentionTrimmedEvent {
            stream: "orders".into(),
            new_earliest_offset: 5,
        },
    )
    .await;

    wait_for(
        || async {
            storage
                .stream_bounds(&name)
                .await
                .map(|(e, _)| e.0 == 5)
                .unwrap_or(false)
        },
        Duration::from_secs(3),
        "earliest to advance to 5",
    )
    .await;
    let all = storage.read(&name, Offset(0), 100).await.unwrap();
    assert!(
        all.iter().all(|r| r.offset.0 >= 5),
        "all records must be >= 5, got: {:?}",
        all.iter().map(|r| r.offset.0).collect::<Vec<_>>()
    );
    assert_eq!(all.len(), 5);

    handle.shutdown().await;
}

#[tokio::test]
async fn reseeds_on_stream_reseed_event() {
    let storage = Arc::new(MemoryStorage::new());
    let name = StreamName::try_from("orders").unwrap();
    storage.create_stream(&name, 0, 0).await.unwrap();
    for i in 0..10u32 {
        storage
            .append(
                &name,
                &Record {
                    key: None,
                    value: Bytes::from(format!("v{i}")),
                    subject: "s".into(),
                    headers: vec![],
                    timestamp_ns: None,
                },
            )
            .await
            .unwrap();
    }

    let tmp = TempDir::new().unwrap();
    let cursor_path = tmp.path().join("cursor.json");

    // Seed cursor at 10 so the reseed bumps it to 100.
    {
        let mut initial = BTreeMap::new();
        initial.insert("orders".to_string(), 10u64);
        std::fs::write(&cursor_path, serde_json::to_string(&initial).unwrap()).unwrap();
    }

    let manifest = ClusterManifest {
        streams: vec![stream_summary("orders", 0, 10)],
        leader_holder_id: Uuid::new_v4(),
    };
    let (addr, handshake_rx) = spawn_stub_leader(manifest).await;

    let handle = spawn_client(
        storage.clone() as Arc<dyn StorageEngine>,
        cursor_path.clone(),
        addr,
        metrics(),
        tmp,
    );

    let (_r, mut w, _) = handshake_rx.await.expect("handshake");

    push_frame(
        &mut w,
        OpCode::StreamReseedEvent,
        &StreamReseedEvent {
            stream: "orders".into(),
            new_earliest_offset: 100,
        },
    )
    .await;

    // After reseed: stream recreated empty (next == earliest == 0 in memory
    // storage — reseed intentionally resets offset assignment too).
    wait_for(
        || async {
            match storage.stream_bounds(&name).await {
                Ok((_, next)) => next.0 == 0,
                Err(_) => false,
            }
        },
        Duration::from_secs(3),
        "stream to be recreated empty",
    )
    .await;
    let all = storage.read(&name, Offset(0), 100).await.unwrap();
    assert!(all.is_empty(), "stream should be empty after reseed");

    // Cursor should now point at 100.
    wait_for(
        || async {
            let raw = std::fs::read_to_string(&cursor_path).unwrap_or_default();
            raw.contains("\"orders\"") && raw.contains("100")
        },
        Duration::from_secs(3),
        "cursor to advance to 100",
    )
    .await;

    handle.shutdown().await;
}

#[tokio::test]
async fn follower_preserves_leader_timestamp() {
    // Leader sends a RecordsAppended with an explicit ms timestamp; after
    // apply, the follower's StoredRecord.timestamp (ns) must equal the
    // leader's ms * 1_000_000 exactly. Without the `Record::timestamp_ns`
    // override the follower would mint a fresh wall-clock ns and
    // `seek_by_time` would diverge across the cluster.
    let storage = Arc::new(MemoryStorage::new());
    let tmp = TempDir::new().unwrap();
    let cursor_path = tmp.path().join("cursor.json");

    let manifest = ClusterManifest {
        streams: vec![stream_summary("orders", 0, 1)],
        leader_holder_id: Uuid::new_v4(),
    };
    let (addr, handshake_rx) = spawn_stub_leader(manifest).await;

    let handle = spawn_client(
        storage.clone() as Arc<dyn StorageEngine>,
        cursor_path,
        addr,
        metrics(),
        tmp,
    );

    let (_framed_read, mut framed_write, _resume) = handshake_rx.await.expect("handshake");

    // Arbitrary known ms value; the exact number is what matters.
    const LEADER_TS_MS: u64 = 1_700_000_000_000;
    let batch = RecordsAppended {
        stream: "orders".into(),
        base_offset: 0,
        records: vec![ReplicatedRecord {
            subject: "orders.placed".into(),
            payload: b"first-record".to_vec(),
            headers: vec![],
            timestamp_ms: LEADER_TS_MS,
            msg_id: None,
        }],
    };
    push_frame(&mut framed_write, OpCode::RecordsAppended, &batch).await;

    let name = StreamName::try_from("orders").unwrap();
    wait_for(
        || async {
            storage
                .stream_bounds(&name)
                .await
                .map(|(_, next)| next.0 == 1)
                .unwrap_or(false)
        },
        Duration::from_secs(3),
        "follower to apply 1 record",
    )
    .await;

    let all = storage.read(&name, Offset(0), 10).await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(
        all[0].timestamp,
        LEADER_TS_MS * 1_000_000,
        "follower must persist leader's timestamp exactly (ms → ns round-trip)"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn divergent_history_truncation_on_manifest() {
    // Follower has cursor=50 for stream X; leader's manifest says
    // latest_offset=30. Must truncate_from(30) and reset cursor to 30.
    let storage = Arc::new(MemoryStorage::new());
    let name = StreamName::try_from("orders").unwrap();
    storage.create_stream(&name, 0, 0).await.unwrap();
    for i in 0..50u32 {
        storage
            .append(
                &name,
                &Record {
                    key: None,
                    value: Bytes::from(format!("v{i}")),
                    subject: "s".into(),
                    headers: vec![],
                    timestamp_ns: None,
                },
            )
            .await
            .unwrap();
    }

    let tmp = TempDir::new().unwrap();
    let cursor_path = tmp.path().join("cursor.json");
    {
        let mut initial = BTreeMap::new();
        initial.insert("orders".to_string(), 50u64);
        std::fs::write(&cursor_path, serde_json::to_string(&initial).unwrap()).unwrap();
    }

    let manifest = ClusterManifest {
        streams: vec![stream_summary("orders", 0, 30)],
        leader_holder_id: Uuid::new_v4(),
    };
    let (addr, handshake_rx) = spawn_stub_leader(manifest).await;

    let (metrics, registry) = metrics_with_registry();
    let handle = spawn_client(
        storage.clone() as Arc<dyn StorageEngine>,
        cursor_path.clone(),
        addr,
        metrics,
        tmp,
    );

    let _ = handshake_rx.await.expect("handshake");

    // Storage should shrink from 50 records to 30.
    wait_for(
        || async {
            storage
                .stream_bounds(&name)
                .await
                .map(|(_, next)| next.0 == 30)
                .unwrap_or(false)
        },
        Duration::from_secs(3),
        "storage next to become 30",
    )
    .await;
    let all = storage.read(&name, Offset(0), 100).await.unwrap();
    assert_eq!(all.len(), 30);

    // Cursor should be at 30.
    wait_for(
        || async {
            let raw = std::fs::read_to_string(&cursor_path).unwrap_or_default();
            raw.contains("\"orders\"") && raw.contains("30")
        },
        Duration::from_secs(3),
        "cursor to be 30",
    )
    .await;

    // Metric should record the 20 dropped records (50 → 30). Note the
    // OTel-Prometheus exporter appends an extra `_total` suffix to any
    // counter whose declared name already ends in `_total`, so the full
    // series name becomes `exspeed_replication_truncated_records_total_total`.
    let dropped = counter_value(
        &registry,
        "exspeed_replication_truncated_records_total_total",
        &[("stream", "orders")],
    );
    assert!(
        dropped >= 20.0,
        "expected truncated_records_total_total ≥ 20 for stream=orders, got {dropped}"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn manifest_creates_unknown_streams_with_retention() {
    // Leader's manifest advertises `orders` with retention=(3600, 1_000_000)
    // and offsets=(0, 0). Follower has no pre-existing stream. After the
    // handshake completes — before any RecordsAppended arrives — the
    // follower must have created `orders` locally and its bounds must
    // report (0, 0).
    let storage = Arc::new(MemoryStorage::new());
    let tmp = TempDir::new().unwrap();
    let cursor_path = tmp.path().join("cursor.json");

    let manifest = ClusterManifest {
        streams: vec![StreamSummary {
            name: "orders".into(),
            max_age_secs: 3600,
            max_bytes: 1_000_000,
            earliest_offset: 0,
            latest_offset: 0,
        }],
        leader_holder_id: Uuid::new_v4(),
    };
    let (addr, handshake_rx) = spawn_stub_leader(manifest).await;

    let handle = spawn_client(
        storage.clone() as Arc<dyn StorageEngine>,
        cursor_path,
        addr,
        metrics(),
        tmp,
    );

    let _ = handshake_rx.await.expect("handshake");

    let name = StreamName::try_from("orders").unwrap();
    // Wait for the pre-create to land — the follower does it in
    // `reconcile_manifest`, which runs after handshake but before the
    // apply loop. `list_streams()` is the external signal.
    wait_for(
        || async {
            storage
                .list_streams()
                .await
                .map(|s| s.iter().any(|n| n.as_str() == "orders"))
                .unwrap_or(false)
        },
        Duration::from_secs(3),
        "manifest pre-create of 'orders'",
    )
    .await;

    let (earliest, next) = storage.stream_bounds(&name).await.unwrap();
    assert_eq!(
        (earliest.0, next.0),
        (0, 0),
        "pre-created stream should report bounds (0, 0)"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn idle_timeout_drops_connection() {
    // Set a tight idle_timeout so the test completes fast. The client reads
    // this env var once per session on enter.
    std::env::set_var("EXSPEED_REPLICATION_IDLE_TIMEOUT_SECS", "2");

    let storage = Arc::new(MemoryStorage::new());
    let tmp = TempDir::new().unwrap();
    let cursor_path = tmp.path().join("cursor.json");

    // We'll accept TWO connections to prove the client reconnects after
    // the idle-timeout on the first.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let connection_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = connection_count.clone();
    tokio::spawn(async move {
        // Accept up to 2 connections. For each, do the handshake + manifest,
        // then go silent (do NOT drop the socket — we want to verify the
        // CLIENT tears down on idle, not the server-side close).
        for _ in 0..2 {
            let (socket, _peer) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (r, w) = tokio::io::split(socket);
            let mut fr = FramedRead::new(r, ExspeedCodec::new());
            let mut fw = FramedWrite::new(w, ExspeedCodec::new());

            // Connect.
            let c = fr.next().await.unwrap().unwrap();
            assert_eq!(c.opcode, OpCode::Connect);
            let _ = ConnectRequest::decode(c.payload).unwrap();
            let mut payload = BytesMut::new();
            ConnectResponse { server_version: 2 }.encode(&mut payload);
            fw.send(Frame::new(
                OpCode::ConnectOk,
                c.correlation_id,
                payload.freeze(),
            ))
            .await
            .unwrap();

            // ReplicateResume.
            let rr = fr.next().await.unwrap().unwrap();
            assert_eq!(rr.opcode, OpCode::ReplicateResume);

            // Manifest (empty — we don't need records for this test).
            let manifest = ClusterManifest {
                streams: vec![],
                leader_holder_id: Uuid::new_v4(),
            };
            let bytes = bincode::serialize(&manifest).unwrap();
            fw.send(Frame::new(OpCode::ClusterManifest, 0, Bytes::from(bytes)))
                .await
                .unwrap();

            // Now go silent. Hold the socket open long enough that the
            // client's idle timer fires and tears down. Keep the writer
            // alive by holding the binding; the client will close.
            // Wait until the client disconnects, then loop back to accept
            // the reconnect.
            while fr.next().await.is_some() {}
        }
    });

    let handle = spawn_client(
        storage as Arc<dyn StorageEngine>,
        cursor_path,
        addr,
        metrics(),
        tmp,
    );

    // First connection: client dials, handshakes, reads manifest, goes
    // into apply loop, idle-times-out after 2s, reconnects.
    //
    // We should see 2 connections within a bounded window (idle_timeout +
    // backoff + epsilon). Initial backoff is 1s, idle is 2s → ~3.5s lower
    // bound; allow 10s ceiling.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while connection_count.load(std::sync::atomic::Ordering::SeqCst) < 2 {
        if std::time::Instant::now() > deadline {
            panic!(
                "expected 2 connections within 10s (first + reconnect), got {}",
                connection_count.load(std::sync::atomic::Ordering::SeqCst)
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    std::env::remove_var("EXSPEED_REPLICATION_IDLE_TIMEOUT_SECS");
    handle.shutdown().await;
}

#[tokio::test]
async fn apply_path_moves_records_applied_and_lag_seconds() {
    // After a RecordsAppended batch lands on the follower, the
    // `records_applied_total{stream}` counter must reflect the batch size
    // and the `lag_seconds{stream}` gauge must be observable (i.e. the
    // series exists with a numeric value; the exact magnitude is
    // wall-clock-dependent). This is the blocker alert-path: without these
    // metrics moving, `rate(exspeed_replication_lag_seconds[1m]) > 10`
    // alerts would never fire on a live follower.
    let storage = Arc::new(MemoryStorage::new());
    let tmp = TempDir::new().unwrap();
    let cursor_path = tmp.path().join("cursor.json");

    let manifest = ClusterManifest {
        streams: vec![stream_summary("orders", 0, 5)],
        leader_holder_id: Uuid::new_v4(),
    };
    let (addr, handshake_rx) = spawn_stub_leader(manifest).await;

    let (metrics, registry) = metrics_with_registry();
    let handle = spawn_client(
        storage.clone() as Arc<dyn StorageEngine>,
        cursor_path,
        addr,
        metrics,
        tmp,
    );

    let (_r, mut w, _) = handshake_rx.await.expect("handshake");

    // Choose a leader timestamp strictly in the past so `lag_seconds`
    // observably > 0 — proves the gauge is not just descriptor-only.
    let leader_ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        - 10_000; // 10 seconds behind

    let records: Vec<ReplicatedRecord> = (0..5u64)
        .map(|i| ReplicatedRecord {
            subject: "orders.placed".into(),
            payload: format!("rec-{i}").into_bytes(),
            headers: vec![],
            timestamp_ms: leader_ts_ms + i,
            msg_id: None,
        })
        .collect();
    let batch = RecordsAppended {
        stream: "orders".into(),
        base_offset: 0,
        records,
    };
    push_frame(&mut w, OpCode::RecordsAppended, &batch).await;

    // Wait for the batch to land.
    let name = StreamName::try_from("orders").unwrap();
    wait_for(
        || async {
            storage
                .stream_bounds(&name)
                .await
                .map(|(_, next)| next.0 == 5)
                .unwrap_or(false)
        },
        Duration::from_secs(3),
        "follower to apply 5 records",
    )
    .await;

    // Counter: `exspeed_replication_records_applied_total_total{stream="orders"}`
    // should be exactly 5. Note the `_total_total` suffix — the
    // OTel-to-Prometheus exporter appends `_total` to counters whose
    // declared name already ends in `_total`.
    let applied = counter_value(
        &registry,
        "exspeed_replication_records_applied_total_total",
        &[("stream", "orders")],
    );
    assert!(
        applied >= 5.0,
        "expected records_applied_total_total >= 5 for stream=orders, got {applied}"
    );

    // Gauge: `exspeed_replication_lag_seconds{stream="orders"}` must be
    // present with a non-zero, roughly-plausible value. The leader ts was
    // 10s in the past; after batch apply the gauge should be ≥ 9.0 and
    // ≤ some generous ceiling to tolerate slow CI.
    let lag_secs = gauge_value(
        &registry,
        "exspeed_replication_lag_seconds",
        &[("stream", "orders")],
    )
    .expect("lag_seconds series must exist after apply");
    assert!(
        (9.0..=120.0).contains(&lag_secs),
        "expected lag_seconds in [9.0, 120.0] for stream=orders, got {lag_secs}"
    );

    handle.shutdown().await;
}
