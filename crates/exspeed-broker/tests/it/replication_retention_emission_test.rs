//! Wave 2 integration tests for the RetentionTask pre/post emission diff.
//!
//! The retention loop snapshots each stream's `(earliest, _)` before
//! `enforce_all_retention`, then re-reads after, and emits a
//! `RetentionTrimmed` event for every stream whose earliest offset
//! moved forward. These tests exercise that diff directly by driving
//! `retention_task::enforce_once` (the one-pass entry point exposed
//! precisely so these tests can run without the 60-second background
//! tick).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use exspeed_broker::replication::{ReplicationCoordinator, ReplicationEvent};
use exspeed_broker::retention_task;
use exspeed_common::{Metrics, StreamName};
use exspeed_storage::file::FileStorage;
use exspeed_streams::{Record, StorageEngine};
use tempfile::TempDir;
use tokio::sync::mpsc;

fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new().0)
}

fn record(value: &[u8]) -> Record {
    Record {
        key: None,
        value: Bytes::copy_from_slice(value),
        subject: "retention.test".into(),
        headers: vec![],
        timestamp_ns: None,
    }
}

async fn drain(rx: &mut mpsc::Receiver<ReplicationEvent>) -> Vec<ReplicationEvent> {
    let mut out = Vec::new();
    while let Ok(Some(ev)) =
        tokio::time::timeout(Duration::from_millis(100), rx.recv()).await
    {
        out.push(ev);
    }
    out
}

/// Write enough records to `stream` under `storage` — with a tiny
/// segment threshold — to force `num_sealed_segments` sealed segments
/// plus an active segment. The active segment is the only one retention
/// refuses to drop, so we need at least one sealed segment per stream
/// we want retention to advance.
async fn fill_stream_with_sealed_segments(
    storage: &Arc<FileStorage>,
    stream: &str,
    max_age_secs: u64,
    num_sealed_segments: usize,
) {
    let storage_trait: Arc<dyn StorageEngine> = storage.clone();
    let name = StreamName::try_from(stream).unwrap();
    storage_trait
        .create_stream(&name, max_age_secs, 0)
        .await
        .unwrap();
    // Override segment rolling threshold so small records still seal.
    assert!(storage.set_stream_segment_max_bytes(stream, 64));

    // Write enough records that we roll past `num_sealed_segments` — each
    // record is ~20 bytes so ~4 records per segment. 20 records will seal
    // several segments, leaving 1 active.
    let records_per_segment = 4;
    let total = records_per_segment * num_sealed_segments + records_per_segment;
    for i in 0..total {
        storage_trait
            .append(&name, &record(format!("data-{i:04}").as_bytes()))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn retention_trim_emits_one_event_per_stream_advanced() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(FileStorage::new(dir.path()).unwrap());

    // Two streams with 1-second retention + forced rolling. Wait >1s so
    // sealed segments age out.
    fill_stream_with_sealed_segments(&storage, "alpha", 1, 2).await;
    fill_stream_with_sealed_segments(&storage, "beta", 1, 2).await;

    // Snapshot earliest offsets before — should both be 0 (head).
    let storage_trait: Arc<dyn StorageEngine> = storage.clone();
    let pre_alpha = storage_trait
        .stream_bounds(&StreamName::try_from("alpha").unwrap())
        .await
        .unwrap()
        .0;
    let pre_beta = storage_trait
        .stream_bounds(&StreamName::try_from("beta").unwrap())
        .await
        .unwrap()
        .0;
    assert_eq!(pre_alpha.0, 0);
    assert_eq!(pre_beta.0, 0);

    // Let sealed segments age past the 1-second cutoff.
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let coord = ReplicationCoordinator::new(metrics(), 100);
    let (_id, mut rx) = coord.register_follower();

    retention_task::enforce_once(&storage, Some(&coord)).await;

    let events = drain(&mut rx).await;
    assert_eq!(
        events.len(),
        2,
        "expected one RetentionTrimmed per advanced stream, got {events:?}"
    );
    let mut seen_alpha = false;
    let mut seen_beta = false;
    for ev in events {
        match ev {
            ReplicationEvent::RetentionTrimmed(t) => {
                match t.stream.as_str() {
                    "alpha" => {
                        assert!(t.new_earliest_offset > 0);
                        seen_alpha = true;
                    }
                    "beta" => {
                        assert!(t.new_earliest_offset > 0);
                        seen_beta = true;
                    }
                    other => panic!("unexpected stream in retention event: {other}"),
                }
            }
            other => panic!("expected RetentionTrimmed, got {other:?}"),
        }
    }
    assert!(seen_alpha && seen_beta, "both streams should emit");
}

#[tokio::test]
async fn retention_pass_with_no_advance_emits_nothing() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(FileStorage::new(dir.path()).unwrap());

    // Very long retention — nothing ages out, nothing trims.
    fill_stream_with_sealed_segments(&storage, "alpha", 86_400, 2).await;

    let coord = ReplicationCoordinator::new(metrics(), 100);
    let (_id, mut rx) = coord.register_follower();

    retention_task::enforce_once(&storage, Some(&coord)).await;

    let events = drain(&mut rx).await;
    assert!(
        events.is_empty(),
        "no stream advanced; expected no events, got {events:?}"
    );
}

#[tokio::test]
async fn retention_on_deleted_stream_does_not_emit() {
    // Edge case: a stream exists at the start of a pass but is deleted
    // before the post-pass bounds read. The code path falls through to
    // a debug! on `stream_bounds` Err and must NOT emit a spurious event.
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(FileStorage::new(dir.path()).unwrap());

    let storage_trait: Arc<dyn StorageEngine> = storage.clone();
    fill_stream_with_sealed_segments(&storage, "ephemeral", 86_400, 2).await;

    // Delete the stream outright — the pre-retention `list_streams` in
    // `enforce_once` will still see it if the delete lands after the
    // snapshot, but for a synchronous delete before `enforce_once` the
    // stream is simply not seen. Either way, no event is emitted.
    storage_trait
        .delete_stream(&StreamName::try_from("ephemeral").unwrap())
        .await
        .unwrap();

    let coord = ReplicationCoordinator::new(metrics(), 100);
    let (_id, mut rx) = coord.register_follower();

    retention_task::enforce_once(&storage, Some(&coord)).await;

    let events = drain(&mut rx).await;
    assert!(
        events.is_empty(),
        "deleted stream must not produce a retention event, got {events:?}"
    );
}
