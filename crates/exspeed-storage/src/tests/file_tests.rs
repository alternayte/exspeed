use bytes::Bytes;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine};
use tempfile::TempDir;

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
    }
}

/// Create FileStorage, create stream, write 10 records, drop (simulate crash),
/// reopen with FileStorage::open, read all 10, verify offsets 0-9.
#[test]
fn crash_recovery_replays_wal() {
    let dir = TempDir::new().unwrap();

    // Phase 1: write records, then drop to simulate crash.
    {
        let storage = FileStorage::new(dir.path()).unwrap();
        storage.create_stream(&stream("crash"), 0, 0).unwrap();
        for i in 0u64..10 {
            let val = format!("val-{}", i);
            let offset = storage
                .append(&stream("crash"), &record(val.as_bytes()))
                .unwrap();
            assert_eq!(offset, Offset(i));
        }
        // drop storage — simulates a crash
    }

    // Phase 2: reopen and verify all records survived.
    {
        let storage = FileStorage::open(dir.path()).unwrap();
        let records = storage.read(&stream("crash"), Offset(0), 100).unwrap();
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
#[test]
fn data_persists_across_restart() {
    let dir = TempDir::new().unwrap();

    {
        let storage = FileStorage::new(dir.path()).unwrap();
        storage.create_stream(&stream("persist"), 0, 0).unwrap();
        storage
            .append(&stream("persist"), &record(b"p0-data"))
            .unwrap();
    }

    {
        let storage = FileStorage::open(dir.path()).unwrap();

        let p0 = storage.read(&stream("persist"), Offset(0), 10).unwrap();
        assert_eq!(p0.len(), 1);
        assert_eq!(p0[0].offset, Offset(0));
        assert_eq!(p0[0].value, Bytes::from_static(b"p0-data"));
    }
}

/// Create storage, create stream, write 1000 records, read all 1000 back,
/// verify offsets 0-999 and first/last values.
#[test]
fn segment_rolling() {
    let dir = TempDir::new().unwrap();
    let storage = FileStorage::new(dir.path()).unwrap();
    storage.create_stream(&stream("rolling"), 0, 0).unwrap();

    for i in 0u64..1000 {
        let val = format!("rec-{:04}", i);
        let offset = storage
            .append(&stream("rolling"), &record(val.as_bytes()))
            .unwrap();
        assert_eq!(offset, Offset(i));
    }

    let records = storage.read(&stream("rolling"), Offset(0), 1000).unwrap();
    assert_eq!(records.len(), 1000);
    assert_eq!(records[0].offset, Offset(0));
    assert_eq!(records[0].value, Bytes::from(String::from("rec-0000")));
    assert_eq!(records[999].offset, Offset(999));
    assert_eq!(records[999].value, Bytes::from(String::from("rec-0999")));
}

/// THE MILESTONE TEST: 10,000 records, crash, recover, verify everything.
#[test]
fn milestone_10k_records_crash_recover() {
    let dir = TempDir::new().unwrap();

    // Phase 1: write 10,000 records then drop (crash).
    {
        let storage = FileStorage::new(dir.path()).unwrap();
        storage.create_stream(&stream("milestone"), 0, 0).unwrap();

        for i in 0u64..10_000 {
            let val = format!("record-{:05}", i);
            let offset = storage
                .append(&stream("milestone"), &record(val.as_bytes()))
                .unwrap();
            assert_eq!(offset, Offset(i));
        }
    }

    // Phase 2: reopen and verify all 10,000 records.
    {
        let storage = FileStorage::open(dir.path()).unwrap();
        let records = storage
            .read(&stream("milestone"), Offset(0), 10_000)
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
