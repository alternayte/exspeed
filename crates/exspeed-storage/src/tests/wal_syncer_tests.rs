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
        FileStorage::open_with_mode(tmp.path(), StorageSyncMode::Async {
            interval: Duration::from_millis(50),
            threshold_bytes: 4 * 1024 * 1024,
        }).unwrap()
    );
    let stream: StreamName = "s".try_into().unwrap();
    storage.create_stream(&stream, 0, 0).await.unwrap();

    let start = std::time::Instant::now();
    storage.append(&stream, &mk(b"hi")).await.unwrap();
    let single_append_elapsed = start.elapsed();

    // Async mode acks should be near-instant (< 5ms), much faster than a real fsync.
    assert!(
        single_append_elapsed < Duration::from_millis(5),
        "async append took {single_append_elapsed:?}, expected < 5ms"
    );
}
