use bytes::Bytes;
use exspeed_common::{Offset, PartitionId, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn stream(name: &str) -> StreamName {
    StreamName::try_from(name).unwrap()
}

fn partition(id: u32) -> PartitionId {
    PartitionId(id)
}

fn record(subject: &str, value: &[u8]) -> Record {
    Record {
        key: None,
        value: Bytes::copy_from_slice(value),
        subject: subject.to_string(),
        headers: Vec::new(),
    }
}

#[allow(dead_code)]
fn record_with_key(subject: &str, key: &[u8], value: &[u8]) -> Record {
    Record {
        key: Some(Bytes::copy_from_slice(key)),
        value: Bytes::copy_from_slice(value),
        subject: subject.to_string(),
        headers: Vec::new(),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Create a stream with 3 partitions, append to partition 0, assert offset is 0.
pub fn test_create_and_append(engine: &impl StorageEngine) {
    let s = stream("test-create");
    engine.create_stream(&s, 3).unwrap();
    let offset = engine.append(&s, partition(0), &record("events", b"hello")).unwrap();
    assert_eq!(offset, Offset(0));
}

/// Create a stream, append a record with key + headers, read back, verify all fields match.
pub fn test_append_and_read_back(engine: &impl StorageEngine) {
    let s = stream("test-readback");
    engine.create_stream(&s, 1).unwrap();

    let rec = Record {
        key: Some(Bytes::from_static(b"mykey")),
        value: Bytes::from_static(b"myvalue"),
        subject: "orders.created".to_string(),
        headers: vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("correlation-id".to_string(), "abc-123".to_string()),
        ],
    };

    engine.append(&s, partition(0), &rec).unwrap();

    let results = engine.read(&s, partition(0), Offset(0), 10).unwrap();
    assert_eq!(results.len(), 1);

    let stored = &results[0];
    assert_eq!(stored.offset, Offset(0));
    assert_eq!(stored.subject, "orders.created");
    assert_eq!(stored.key, Some(Bytes::from_static(b"mykey")));
    assert_eq!(stored.value, Bytes::from_static(b"myvalue"));
    assert_eq!(stored.headers.len(), 2);
    assert_eq!(stored.headers[0], ("content-type".to_string(), "application/json".to_string()));
    assert_eq!(stored.headers[1], ("correlation-id".to_string(), "abc-123".to_string()));
}

/// Append 5 records, verify offsets are 0, 1, 2, 3, 4.
pub fn test_sequential_offsets(engine: &impl StorageEngine) {
    let s = stream("test-offsets");
    engine.create_stream(&s, 1).unwrap();

    for i in 0u64..5 {
        let offset = engine.append(&s, partition(0), &record("events", b"data")).unwrap();
        assert_eq!(offset, Offset(i), "expected offset {i} on append #{i}");
    }
}

/// Append 10 records, read from offset 3 with max 4, verify records 3, 4, 5, 6 are returned.
pub fn test_read_range(engine: &impl StorageEngine) {
    let s = stream("test-range");
    engine.create_stream(&s, 1).unwrap();

    for i in 0u8..10 {
        engine.append(&s, partition(0), &record("events", &[i])).unwrap();
    }

    let results = engine.read(&s, partition(0), Offset(3), 4).unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].offset, Offset(3));
    assert_eq!(results[1].offset, Offset(4));
    assert_eq!(results[2].offset, Offset(5));
    assert_eq!(results[3].offset, Offset(6));
}

/// Create a stream, read from an empty partition, get an empty vec.
pub fn test_read_empty_partition(engine: &impl StorageEngine) {
    let s = stream("test-empty");
    engine.create_stream(&s, 1).unwrap();

    let results = engine.read(&s, partition(0), Offset(0), 100).unwrap();
    assert!(results.is_empty());
}

/// Append 1 record, read from offset 999, get an empty vec.
pub fn test_read_past_end(engine: &impl StorageEngine) {
    let s = stream("test-past-end");
    engine.create_stream(&s, 1).unwrap();
    engine.append(&s, partition(0), &record("events", b"only")).unwrap();

    let results = engine.read(&s, partition(0), Offset(999), 10).unwrap();
    assert!(results.is_empty());
}

/// Append to a nonexistent stream, get StreamNotFound error.
pub fn test_stream_not_found(engine: &impl StorageEngine) {
    let s = stream("nonexistent");
    let err = engine.append(&s, partition(0), &record("events", b"data")).unwrap_err();
    assert!(
        matches!(err, StorageError::StreamNotFound(_)),
        "expected StreamNotFound, got {err:?}"
    );
}

/// Create a stream with 2 partitions, append to partition 99, get PartitionNotFound error.
pub fn test_partition_not_found(engine: &impl StorageEngine) {
    let s = stream("test-partnotfound");
    engine.create_stream(&s, 2).unwrap();

    let err = engine.append(&s, partition(99), &record("events", b"data")).unwrap_err();
    assert!(
        matches!(err, StorageError::PartitionNotFound { .. }),
        "expected PartitionNotFound, got {err:?}"
    );
}

/// Create the same stream twice, get StreamAlreadyExists error.
pub fn test_stream_already_exists(engine: &impl StorageEngine) {
    let s = stream("test-duplicate");
    engine.create_stream(&s, 1).unwrap();

    let err = engine.create_stream(&s, 1).unwrap_err();
    assert!(
        matches!(err, StorageError::StreamAlreadyExists(_)),
        "expected StreamAlreadyExists, got {err:?}"
    );
}

/// Create 3 partitions, append to each, verify independent offsets and data.
pub fn test_multiple_partitions(engine: &impl StorageEngine) {
    let s = stream("test-multipart");
    engine.create_stream(&s, 3).unwrap();

    // Append different numbers of records to each partition.
    engine.append(&s, partition(0), &record("p0", b"a")).unwrap();
    engine.append(&s, partition(0), &record("p0", b"b")).unwrap();

    engine.append(&s, partition(1), &record("p1", b"x")).unwrap();

    engine.append(&s, partition(2), &record("p2", b"m")).unwrap();
    engine.append(&s, partition(2), &record("p2", b"n")).unwrap();
    engine.append(&s, partition(2), &record("p2", b"o")).unwrap();

    let p0 = engine.read(&s, partition(0), Offset(0), 10).unwrap();
    assert_eq!(p0.len(), 2);
    assert_eq!(p0[0].value, Bytes::from_static(b"a"));
    assert_eq!(p0[1].value, Bytes::from_static(b"b"));

    let p1 = engine.read(&s, partition(1), Offset(0), 10).unwrap();
    assert_eq!(p1.len(), 1);
    assert_eq!(p1[0].value, Bytes::from_static(b"x"));

    let p2 = engine.read(&s, partition(2), Offset(0), 10).unwrap();
    assert_eq!(p2.len(), 3);
    assert_eq!(p2[2].value, Bytes::from_static(b"o"));

    // Offsets within each partition are independent.
    assert_eq!(p0[0].offset, Offset(0));
    assert_eq!(p0[1].offset, Offset(1));
    assert_eq!(p1[0].offset, Offset(0));
    assert_eq!(p2[0].offset, Offset(0));
    assert_eq!(p2[2].offset, Offset(2));
}

/// Append 2 records, verify the second timestamp is >= the first.
pub fn test_timestamps_increasing(engine: &impl StorageEngine) {
    let s = stream("test-timestamps");
    engine.create_stream(&s, 1).unwrap();

    engine.append(&s, partition(0), &record("events", b"first")).unwrap();
    engine.append(&s, partition(0), &record("events", b"second")).unwrap();

    let results = engine.read(&s, partition(0), Offset(0), 10).unwrap();
    assert_eq!(results.len(), 2);
    assert!(
        results[1].timestamp >= results[0].timestamp,
        "expected second timestamp ({}) >= first ({})",
        results[1].timestamp,
        results[0].timestamp
    );
}
