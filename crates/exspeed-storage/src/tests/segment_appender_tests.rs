use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tempfile::tempdir;

use exspeed_common::StreamName;
use exspeed_streams::{record::Record, StorageEngine};
use crate::file::FileStorage;

fn mk_record(body: &[u8]) -> Record {
    Record {
        subject: "bench".into(),
        key: None,
        value: Bytes::copy_from_slice(body),
        headers: vec![],
        timestamp_ns: None,
    }
}

#[tokio::test]
async fn concurrent_appends_get_sequential_offsets() {
    let tmp = tempdir().unwrap();
    let storage = Arc::new(FileStorage::open(tmp.path()).unwrap());
    let stream: StreamName = "bench-stream".try_into().unwrap();
    storage.create_stream(&stream, 0, 0).await.unwrap();

    // Fire 100 concurrent appends.
    let mut handles = Vec::new();
    for i in 0..100u64 {
        let s = storage.clone();
        let st = stream.clone();
        let body_str = format!("body-{i}");
        handles.push(tokio::spawn(async move {
            let body = body_str.into_bytes();
            s.append(&st, &mk_record(&body)).await
        }));
    }

    let mut offsets: Vec<u64> = Vec::new();
    for h in handles {
        let (offset, _ts) = h.await.unwrap().unwrap();
        offsets.push(offset.0);
    }
    offsets.sort();
    // Must be exactly 0..100, no gaps, no duplicates.
    assert_eq!(offsets, (0..100).collect::<Vec<u64>>());
}

#[tokio::test]
async fn concurrent_appends_complete_faster_than_n_serial_fsyncs() {
    // Group commit's whole point: 100 concurrent appends should complete in
    // far less time than 100 × fsync_latency. We pick a generous bound:
    // even at 10ms/fsync × 100 = 1000ms serial, group commit should easily
    // complete in under 500ms because they batch.
    let tmp = tempdir().unwrap();
    let storage = Arc::new(FileStorage::open(tmp.path()).unwrap());
    let stream: StreamName = "bench-stream".try_into().unwrap();
    storage.create_stream(&stream, 0, 0).await.unwrap();

    let start = std::time::Instant::now();
    let mut handles = Vec::new();
    for i in 0..100u64 {
        let s = storage.clone();
        let st = stream.clone();
        let body_str = format!("b{i}");
        handles.push(tokio::spawn(async move {
            let body = body_str.into_bytes();
            s.append(&st, &mk_record(&body)).await.unwrap()
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "100 concurrent appends took {elapsed:?}, expected < 500ms (group commit working)"
    );
}

#[tokio::test]
async fn explicit_batch_through_appender_handle() {
    let tmp = tempdir().unwrap();
    let storage = Arc::new(FileStorage::open(tmp.path()).unwrap());
    let stream: StreamName = "batch-stream".try_into().unwrap();
    storage.create_stream(&stream, 0, 0).await.unwrap();

    let records: Vec<Record> = (0..10)
        .map(|i| Record {
            subject: "bench".into(),
            key: None,
            value: Bytes::from(format!("v{i}").into_bytes()),
            headers: vec![],
            timestamp_ns: None,
        })
        .collect();
    let results = storage.append_batch(&stream, records).await.unwrap();
    assert_eq!(results.len(), 10);
    for (i, (offset, _ts)) in results.iter().enumerate() {
        assert_eq!(offset.0, i as u64);
    }
}
