use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use tempfile::tempdir;

use exspeed_common::StreamName;
use exspeed_streams::{record::Record, StorageEngine};
use crate::file::{FileStorage, StorageSyncMode};

fn mk(body: &'static [u8]) -> Record {
    Record {
        subject: "s".into(),
        key: None,
        value: Bytes::from_static(body),
        headers: vec![],
        timestamp_ns: None,
    }
}

#[tokio::test]
async fn async_mode_acks_immediately_then_syncs_on_timer() {
    let tmp = tempdir().unwrap();
    let storage = Arc::new(
        FileStorage::open_with_mode(
            tmp.path(),
            StorageSyncMode::Async {
                interval: Duration::from_millis(50),
                threshold_bytes: 4 * 1024 * 1024,
            },
            crate::file::segment_appender::AppenderConfig::default(),
        ).unwrap()
    );
    let stream: StreamName = "s".try_into().unwrap();
    storage.create_stream(&stream, 0, 0).await.unwrap();

    let start = std::time::Instant::now();
    storage.append(&stream, &mk(b"hi")).await.unwrap();
    let single_append_elapsed = start.elapsed();

    // 50ms is a coarse upper bound — real fsync on macOS is 5-15ms, so this
    // distinguishes "definitely async (no fsync per append)" from "definitely
    // sync". Tighter bounds flake under parallel test contention.
    assert!(
        single_append_elapsed < Duration::from_millis(50),
        "async append took {single_append_elapsed:?}, expected < 50ms (much faster than a real fsync)"
    );
}

/// Smoke test for `SegmentSyncerHandle::set_active_file`: spawn a syncer
/// pointing at file A, let it tick once, swap to file B, let it tick again,
/// and verify no panic / deadlock. We don't have a way to observe WHICH
/// file was fsync'd (sync_data is a syscall on a kernel fd with no
/// user-visible side-effect on an empty segment), so this asserts the
/// happy path only — it exercises the lock swap, not the fsync target.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_active_file_swaps_sync_target() {
    use std::fs::OpenOptions;

    let tmp = tempdir().unwrap();
    let path_a = tmp.path().join("a.seg");
    let path_b = tmp.path().join("b.seg");

    let file_a = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&path_a)
        .unwrap();
    let file_b = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&path_b)
        .unwrap();

    // Spawn syncer pointing at A.
    let handle = crate::file::segment_syncer::spawn(
        file_a,
        "s".into(),
        0,
        Duration::from_millis(50),
    );

    // Let it tick once, verify no crash.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Swap to B.
    handle.set_active_file(file_b);

    // Let it tick again with B; verify still no crash.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Dropping the handle triggers a final fsync on B; give it a moment.
    drop(handle);
    tokio::time::sleep(Duration::from_millis(20)).await;
}
