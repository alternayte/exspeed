use std::thread;
use std::time::Duration;

use bytes::Bytes;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine};
use tempfile::TempDir;

use crate::file::partition::Partition;
use crate::file::FileStorage;

fn stream(name: &str) -> StreamName {
    StreamName::try_from(name).unwrap()
}

fn record(value: &[u8]) -> Record {
    Record {
        key: Some(Bytes::from_static(b"key")),
        value: Bytes::copy_from_slice(value),
        subject: "test.subject".into(),
        headers: vec![],
        timestamp_ns: None,
    }
}

/// Create FileStorage, create stream, write 10 records, drop (simulate crash),
/// reopen with FileStorage::open, read all 10, verify offsets 0-9.
#[tokio::test]
async fn crash_recovery_replays_wal() {
    let dir = TempDir::new().unwrap();

    // Phase 1: write records, then drop to simulate crash.
    {
        let storage = FileStorage::new(dir.path()).unwrap();
        storage.create_stream(&stream("crash"), 0, 0).await.unwrap();
        for i in 0u64..10 {
            let val = format!("val-{}", i);
            let (offset, _) = storage
                .append(&stream("crash"), &record(val.as_bytes()))
                .await
                .unwrap();
            assert_eq!(offset, Offset(i));
        }
        // drop storage — simulates a crash
    }

    // Phase 2: reopen and verify all records survived.
    {
        let storage = FileStorage::open(dir.path()).unwrap();
        let records = storage.read(&stream("crash"), Offset(0), 100).await.unwrap();
        assert_eq!(records.len(), 10);
        for i in 0u64..10 {
            assert_eq!(records[i as usize].offset, Offset(i));
            let expected = format!("val-{}", i);
            assert_eq!(records[i as usize].value, Bytes::from(expected));
        }
    }
}

/// Create storage, create stream, write 1 record,
/// drop, reopen with FileStorage::open, read back, verify data.
#[tokio::test]
async fn data_persists_across_restart() {
    let dir = TempDir::new().unwrap();

    {
        let storage = FileStorage::new(dir.path()).unwrap();
        storage.create_stream(&stream("persist"), 0, 0).await.unwrap();
        storage
            .append(&stream("persist"), &record(b"p0-data"))
            .await
            .unwrap();
    }

    {
        let storage = FileStorage::open(dir.path()).unwrap();

        let p0 = storage.read(&stream("persist"), Offset(0), 10).await.unwrap();
        assert_eq!(p0.len(), 1);
        assert_eq!(p0[0].offset, Offset(0));
        assert_eq!(p0[0].value, Bytes::from_static(b"p0-data"));
    }
}

/// Create storage, create stream, write 1000 records, read all 1000 back,
/// verify offsets 0-999 and first/last values.
#[tokio::test]
async fn segment_rolling() {
    let dir = TempDir::new().unwrap();
    let storage = FileStorage::new(dir.path()).unwrap();
    storage.create_stream(&stream("rolling"), 0, 0).await.unwrap();

    for i in 0u64..1000 {
        let val = format!("rec-{:04}", i);
        let (offset, _) = storage
            .append(&stream("rolling"), &record(val.as_bytes()))
            .await
            .unwrap();
        assert_eq!(offset, Offset(i));
    }

    let records = storage.read(&stream("rolling"), Offset(0), 1000).await.unwrap();
    assert_eq!(records.len(), 1000);
    assert_eq!(records[0].offset, Offset(0));
    assert_eq!(records[0].value, Bytes::from(String::from("rec-0000")));
    assert_eq!(records[999].offset, Offset(999));
    assert_eq!(records[999].value, Bytes::from(String::from("rec-0999")));
}

/// THE MILESTONE TEST: 10,000 records, crash, recover, verify everything.
#[tokio::test]
async fn milestone_10k_records_crash_recover() {
    let dir = TempDir::new().unwrap();

    // Phase 1: write 10,000 records then drop (crash).
    {
        let storage = FileStorage::new(dir.path()).unwrap();
        storage.create_stream(&stream("milestone"), 0, 0).await.unwrap();

        for i in 0u64..10_000 {
            let val = format!("record-{:05}", i);
            let (offset, _) = storage
                .append(&stream("milestone"), &record(val.as_bytes()))
                .await
                .unwrap();
            assert_eq!(offset, Offset(i));
        }
    }

    // Phase 2: reopen and verify all 10,000 records.
    {
        let storage = FileStorage::open(dir.path()).unwrap();
        let records = storage
            .read(&stream("milestone"), Offset(0), 10_000)
            .await
            .unwrap();

        assert_eq!(
            records.len(),
            10_000,
            "expected 10000 records after recovery"
        );

        // First record
        assert_eq!(records[0].offset, Offset(0));
        assert_eq!(records[0].value, Bytes::from("record-00000"));

        // Middle record
        assert_eq!(records[5000].offset, Offset(5000));
        assert_eq!(records[5000].value, Bytes::from("record-05000"));

        // Last record
        assert_eq!(records[9999].offset, Offset(9999));
        assert_eq!(records[9999].value, Bytes::from("record-09999"));
    }
}

/// Test age-based retention: create a partition, write records, force a segment
/// roll, wait for the records to age past the cutoff, then enforce retention.
#[test]
fn retention_deletes_old_segments_by_age() {
    let dir = TempDir::new().unwrap();
    let part_dir = dir.path().join("part0");

    let mut partition = Partition::create(&part_dir, "test-stream", 0).unwrap();

    // Set a very small segment max so that writes trigger rolling.
    partition.set_segment_max_bytes(64);

    // Write enough records to fill at least 2 sealed segments.
    for i in 0u64..20 {
        let val = format!("age-{:04}", i);
        partition
            .append(&Record {
                key: None,
                value: Bytes::from(val),
                subject: "test.subject".into(),
                headers: vec![],
                timestamp_ns: None,
            })
            .unwrap();
    }

    // At this point there should be at least 1 sealed segment.
    let total_before = partition.total_bytes();
    assert!(total_before > 0, "partition should have some bytes written");

    // Wait just over 1 second so the sealed segments age out.
    thread::sleep(Duration::from_millis(1500));

    // Enforce retention with 1-second max age and very large max bytes.
    let stats = partition.enforce_retention(1, u64::MAX).unwrap();
    assert!(
        stats.segments_deleted > 0,
        "expected at least one sealed segment to be deleted by age retention"
    );
    assert!(stats.bytes_reclaimed > 0, "expected bytes to be reclaimed");

    // Total bytes should have decreased.
    let total_after = partition.total_bytes();
    assert!(
        total_after < total_before,
        "total bytes should decrease after retention: before={}, after={}",
        total_before,
        total_after
    );
}

/// Test size-based retention: create sealed segments and enforce with a small
/// max_bytes limit.
#[test]
fn retention_deletes_oldest_segments_by_size() {
    let dir = TempDir::new().unwrap();
    let part_dir = dir.path().join("part0");

    let mut partition = Partition::create(&part_dir, "test-stream", 0).unwrap();

    // Set a very small segment max so writes trigger rolling.
    partition.set_segment_max_bytes(64);

    // Write enough records to create multiple sealed segments.
    for i in 0u64..30 {
        let val = format!("size-{:04}", i);
        partition
            .append(&Record {
                key: None,
                value: Bytes::from(val),
                subject: "test.subject".into(),
                headers: vec![],
                timestamp_ns: None,
            })
            .unwrap();
    }

    let total_before = partition.total_bytes();

    // Set max_bytes to a small value that should trigger size-based deletion.
    // Use a max_bytes just slightly larger than the active segment.
    let active_size = partition.total_bytes();
    let max_bytes = active_size / 3; // force deletion of most sealed segments

    // Enforce retention with very large max_age (so age-based won't trigger) and small max_bytes.
    let stats = partition.enforce_retention(999_999_999, max_bytes).unwrap();
    assert!(
        stats.segments_deleted > 0,
        "expected at least one segment deleted by size retention"
    );

    let total_after = partition.total_bytes();
    assert!(
        total_after <= max_bytes,
        "total bytes ({}) should be at or below max_bytes ({})",
        total_after,
        max_bytes
    );
    assert!(
        total_after < total_before,
        "total bytes should decrease after size retention"
    );
}

#[tokio::test]
async fn list_streams() {
    let dir = TempDir::new().unwrap();
    let storage = FileStorage::new(dir.path()).unwrap();
    storage.create_stream(&stream("alpha"), 0, 0).await.unwrap();
    storage.create_stream(&stream("beta"), 0, 0).await.unwrap();
    let names = storage.list_streams();
    assert_eq!(names, vec!["alpha", "beta"]);
}

#[tokio::test]
async fn stream_storage_bytes_and_head_offset() {
    let dir = TempDir::new().unwrap();
    let storage = FileStorage::new(dir.path()).unwrap();
    storage
        .create_stream(&stream("metrics-test"), 0, 0)
        .await
        .unwrap();
    assert_eq!(storage.stream_head_offset("metrics-test"), Some(0));
    storage
        .append(&stream("metrics-test"), &record(b"data"))
        .await
        .unwrap();
    assert_eq!(storage.stream_head_offset("metrics-test"), Some(1));
    assert!(storage.stream_storage_bytes("metrics-test").unwrap() > 0);
}

#[tokio::test]
async fn nonexistent_stream_query_returns_none() {
    let dir = TempDir::new().unwrap();
    let storage = FileStorage::new(dir.path()).unwrap();
    assert!(storage.stream_storage_bytes("nope").is_none());
    assert!(storage.stream_head_offset("nope").is_none());
}

/// `truncate_from` with `drop_from` inside a sealed segment: the
/// straddling sealed segment is rewritten, later sealed segments + the
/// old active are deleted, and subsequent appends resume at `drop_from`.
#[test]
fn truncate_from_inside_sealed_segment() {
    let dir = TempDir::new().unwrap();
    let part_dir = dir.path().join("part0");

    let mut partition = Partition::create(&part_dir, "test-stream", 0).unwrap();
    partition.set_segment_max_bytes(64);

    // Write enough records to produce several sealed segments + an active.
    for i in 0u64..20 {
        let val = format!("seg-{:04}", i);
        partition
            .append(&Record {
                key: None,
                value: Bytes::from(val),
                subject: "test.subject".into(),
                headers: vec![],
                timestamp_ns: None,
            })
            .unwrap();
    }
    assert_eq!(partition.next_offset(), 20);

    // Truncate at offset 7 — somewhere inside a sealed segment.
    partition.truncate_from(7).unwrap();
    assert_eq!(partition.next_offset(), 7);

    // All surviving records should read back cleanly.
    let records = partition.read(Offset(0), 100).unwrap();
    assert_eq!(records.len(), 7);
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.offset, Offset(i as u64));
        let expected = format!("seg-{:04}", i);
        assert_eq!(r.value, Bytes::from(expected));
    }

    // Next append assigns exactly 7.
    let (new_off, _) = partition
        .append(&Record {
            key: None,
            value: Bytes::from_static(b"post-truncate"),
            subject: "test.subject".into(),
            headers: vec![],
            timestamp_ns: None,
        })
        .unwrap();
    assert_eq!(new_off, Offset(7));

    // read_from(Offset(7)) returns the newly-appended record.
    let tail = partition.read(Offset(7), 10).unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].offset, Offset(7));
    assert_eq!(tail[0].value, Bytes::from_static(b"post-truncate"));
}

/// `truncate_from` with `drop_from` inside the active segment: old active
/// is rewritten in place (same base_offset, records `< drop_from`
/// preserved); next append resumes at `drop_from`.
#[test]
fn truncate_from_inside_active_segment() {
    let dir = TempDir::new().unwrap();
    let part_dir = dir.path().join("part0");

    let mut partition = Partition::create(&part_dir, "test-stream", 0).unwrap();
    // Large enough that all 10 records land in the active segment.
    for i in 0u64..10 {
        let val = format!("act-{:04}", i);
        partition
            .append(&Record {
                key: None,
                value: Bytes::from(val),
                subject: "test.subject".into(),
                headers: vec![],
                timestamp_ns: None,
            })
            .unwrap();
    }

    partition.truncate_from(4).unwrap();
    assert_eq!(partition.next_offset(), 4);

    let records = partition.read(Offset(0), 100).unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(records[3].offset, Offset(3));

    // Appending resumes at 4.
    let (new_off, _) = partition
        .append(&Record {
            key: None,
            value: Bytes::from_static(b"tail"),
            subject: "test.subject".into(),
            headers: vec![],
            timestamp_ns: None,
        })
        .unwrap();
    assert_eq!(new_off, Offset(4));
}

/// `truncate_from(0)` with sealed segments: all segments are rebuilt
/// empty and the next append starts at 0.
#[test]
fn truncate_from_zero_wipes_all_segments() {
    let dir = TempDir::new().unwrap();
    let part_dir = dir.path().join("part0");

    let mut partition = Partition::create(&part_dir, "test-stream", 0).unwrap();
    partition.set_segment_max_bytes(64);

    for i in 0u64..20 {
        let val = format!("wipe-{:04}", i);
        partition
            .append(&Record {
                key: None,
                value: Bytes::from(val),
                subject: "test.subject".into(),
                headers: vec![],
                timestamp_ns: None,
            })
            .unwrap();
    }

    partition.truncate_from(0).unwrap();
    assert_eq!(partition.next_offset(), 0);

    let records = partition.read(Offset(0), 100).unwrap();
    assert!(records.is_empty());

    // Append resumes at 0.
    let (new_off, _) = partition
        .append(&Record {
            key: None,
            value: Bytes::from_static(b"fresh"),
            subject: "test.subject".into(),
            headers: vec![],
            timestamp_ns: None,
        })
        .unwrap();
    assert_eq!(new_off, Offset(0));
}

/// `truncate_from` is durable: after a rewrite, closing and re-opening
/// the storage replays WAL (which has been truncated) and the stream
/// bounds reflect `next == drop_from`.
#[tokio::test]
async fn truncate_from_survives_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let storage = FileStorage::new(dir.path()).unwrap();
        storage.create_stream(&stream("trunc-reopen"), 0, 0).await.unwrap();
        for i in 0u64..10 {
            let val = format!("val-{}", i);
            storage
                .append(&stream("trunc-reopen"), &record(val.as_bytes()))
                .await
                .unwrap();
        }
        storage.truncate_from(&stream("trunc-reopen"), Offset(6)).await.unwrap();
    }

    {
        let storage = FileStorage::open(dir.path()).unwrap();
        let (earliest, next) = storage.stream_bounds(&stream("trunc-reopen")).await.unwrap();
        assert_eq!(earliest, Offset(0));
        assert_eq!(next, Offset(6));

        let records = storage.read(&stream("trunc-reopen"), Offset(0), 100).await.unwrap();
        assert_eq!(records.len(), 6);
        assert_eq!(records.last().unwrap().offset, Offset(5));

        let (new_off, _) = storage
            .append(&stream("trunc-reopen"), &record(b"post-reopen"))
            .await
            .unwrap();
        assert_eq!(new_off, Offset(6));
    }
}

/// Test that retention never deletes the active segment.
#[test]
fn retention_never_deletes_active_segment() {
    let dir = TempDir::new().unwrap();
    let part_dir = dir.path().join("part0");

    let mut partition = Partition::create(&part_dir, "test-stream", 0).unwrap();

    // Write a record to the active segment.
    partition
        .append(&Record {
            key: None,
            value: Bytes::from("active-data"),
            subject: "test.subject".into(),
            headers: vec![],
            timestamp_ns: None,
        })
        .unwrap();

    // Enforce very aggressive retention -- 0 max age, 0 max bytes.
    // With no sealed segments, nothing should be deleted.
    let stats = partition.enforce_retention(0, 0).unwrap();
    assert_eq!(
        stats.segments_deleted, 0,
        "should not delete the active segment"
    );

    // Active segment data should still be readable.
    let records = partition.read(Offset(0), 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].value, Bytes::from("active-data"));
}
