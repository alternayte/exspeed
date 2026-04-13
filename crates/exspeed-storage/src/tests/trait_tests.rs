use bytes::Bytes;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError};

// -- Helpers ------------------------------------------------------------------

fn stream(name: &str) -> StreamName {
    StreamName::try_from(name).unwrap()
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

// -- Tests --------------------------------------------------------------------

/// Create a stream, append one record, assert offset is 0.
pub fn test_create_and_append(engine: &impl StorageEngine) {
    let s = stream("test-create");
    engine.create_stream(&s, 0, 0).unwrap();
    let offset = engine.append(&s, &record("events", b"hello")).unwrap();
    assert_eq!(offset, Offset(0));
}

/// Create a stream, append a record with key + headers, read back, verify all fields match.
pub fn test_append_and_read_back(engine: &impl StorageEngine) {
    let s = stream("test-readback");
    engine.create_stream(&s, 0, 0).unwrap();

    let rec = Record {
        key: Some(Bytes::from_static(b"mykey")),
        value: Bytes::from_static(b"myvalue"),
        subject: "orders.created".to_string(),
        headers: vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("correlation-id".to_string(), "abc-123".to_string()),
        ],
    };

    engine.append(&s, &rec).unwrap();

    let results = engine.read(&s, Offset(0), 10).unwrap();
    assert_eq!(results.len(), 1);

    let stored = &results[0];
    assert_eq!(stored.offset, Offset(0));
    assert_eq!(stored.subject, "orders.created");
    assert_eq!(stored.key, Some(Bytes::from_static(b"mykey")));
    assert_eq!(stored.value, Bytes::from_static(b"myvalue"));
    assert_eq!(stored.headers.len(), 2);
    assert_eq!(
        stored.headers[0],
        ("content-type".to_string(), "application/json".to_string())
    );
    assert_eq!(
        stored.headers[1],
        ("correlation-id".to_string(), "abc-123".to_string())
    );
}

/// Append 5 records, verify offsets are 0, 1, 2, 3, 4.
pub fn test_sequential_offsets(engine: &impl StorageEngine) {
    let s = stream("test-offsets");
    engine.create_stream(&s, 0, 0).unwrap();

    for i in 0u64..5 {
        let offset = engine.append(&s, &record("events", b"data")).unwrap();
        assert_eq!(offset, Offset(i), "expected offset {i} on append #{i}");
    }
}

/// Append 10 records, read from offset 3 with max 4, verify records 3, 4, 5, 6 are returned.
pub fn test_read_range(engine: &impl StorageEngine) {
    let s = stream("test-range");
    engine.create_stream(&s, 0, 0).unwrap();

    for i in 0u8..10 {
        engine.append(&s, &record("events", &[i])).unwrap();
    }

    let results = engine.read(&s, Offset(3), 4).unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].offset, Offset(3));
    assert_eq!(results[1].offset, Offset(4));
    assert_eq!(results[2].offset, Offset(5));
    assert_eq!(results[3].offset, Offset(6));
}

/// Create a stream, read from an empty stream, get an empty vec.
pub fn test_read_empty_stream(engine: &impl StorageEngine) {
    let s = stream("test-empty");
    engine.create_stream(&s, 0, 0).unwrap();

    let results = engine.read(&s, Offset(0), 100).unwrap();
    assert!(results.is_empty());
}

/// Append 1 record, read from offset 999, get an empty vec.
pub fn test_read_past_end(engine: &impl StorageEngine) {
    let s = stream("test-past-end");
    engine.create_stream(&s, 0, 0).unwrap();
    engine.append(&s, &record("events", b"only")).unwrap();

    let results = engine.read(&s, Offset(999), 10).unwrap();
    assert!(results.is_empty());
}

/// Append to a nonexistent stream, get StreamNotFound error.
pub fn test_stream_not_found(engine: &impl StorageEngine) {
    let s = stream("nonexistent");
    let err = engine.append(&s, &record("events", b"data")).unwrap_err();
    assert!(
        matches!(err, StorageError::StreamNotFound(_)),
        "expected StreamNotFound, got {err:?}"
    );
}

/// Create the same stream twice, get StreamAlreadyExists error.
pub fn test_stream_already_exists(engine: &impl StorageEngine) {
    let s = stream("test-duplicate");
    engine.create_stream(&s, 0, 0).unwrap();

    let err = engine.create_stream(&s, 0, 0).unwrap_err();
    assert!(
        matches!(err, StorageError::StreamAlreadyExists(_)),
        "expected StreamAlreadyExists, got {err:?}"
    );
}

/// Seek by timestamp: timestamp 0 returns offset 0; far future returns end of stream.
pub fn test_seek_by_time(engine: &impl StorageEngine) {
    let s = stream("test-seek");
    engine.create_stream(&s, 0, 0).unwrap();

    // Publish records -- we can't control timestamps in MemoryStorage (they use now()),
    // so just verify seek returns a valid offset
    engine.append(&s, &record("events", b"first")).unwrap();
    engine.append(&s, &record("events", b"second")).unwrap();

    // Seek to timestamp 0 (before all records) should return offset 0
    let offset = engine.seek_by_time(&s, 0).unwrap();
    assert_eq!(offset, Offset(0));

    // Seek to far future should return end of stream
    let offset = engine.seek_by_time(&s, u64::MAX).unwrap();
    assert!(offset.0 >= 2); // at or past the end
}

/// Append 2 records, verify the second timestamp is >= the first.
pub fn test_timestamps_increasing(engine: &impl StorageEngine) {
    let s = stream("test-timestamps");
    engine.create_stream(&s, 0, 0).unwrap();

    engine.append(&s, &record("events", b"first")).unwrap();
    engine.append(&s, &record("events", b"second")).unwrap();

    let results = engine.read(&s, Offset(0), 10).unwrap();
    assert_eq!(results.len(), 2);
    assert!(
        results[1].timestamp >= results[0].timestamp,
        "expected second timestamp ({}) >= first ({})",
        results[1].timestamp,
        results[0].timestamp
    );
}
