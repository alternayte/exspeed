// Tests for Partition::open's tail-scan recovery (replaces WAL replay).

use std::fs::OpenOptions;
use std::io::Write;

use bytes::Bytes;
use exspeed_common::Offset;
use exspeed_streams::record::Record;
use tempfile::TempDir;

use crate::file::partition::Partition;

fn mk(body: &'static [u8]) -> Record {
    Record {
        subject: "bench".into(),
        key: None,
        value: Bytes::from_static(body),
        headers: vec![],
        timestamp_ns: None,
    }
}

#[test]
fn open_fresh_partition_starts_at_offset_zero() {
    let tmp = TempDir::new().unwrap();
    let p = Partition::create(tmp.path(), "s", 0).unwrap();
    // Reopen the same dir; should see the fresh (empty) segment.
    drop(p);
    let p2 = Partition::open(tmp.path(), "s", 0).unwrap();
    // A fresh partition's next_offset is 0.
    let records = p2.read(Offset(0), 100).unwrap();
    assert_eq!(records.len(), 0);
}

#[test]
fn open_recovers_all_appended_records() {
    let tmp = TempDir::new().unwrap();
    let mut p = Partition::create(tmp.path(), "s", 0).unwrap();
    for i in 0..20 {
        p.append(&mk(
            Box::leak(format!("v{i}").into_boxed_str()).as_bytes()
        )).unwrap();
    }
    drop(p);

    let p2 = Partition::open(tmp.path(), "s", 0).unwrap();
    let records = p2.read(Offset(0), 100).unwrap();
    assert_eq!(records.len(), 20);
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.offset.0, i as u64);
    }
    // Next append should continue at offset 20.
    drop(p2);
    let mut p3 = Partition::open(tmp.path(), "s", 0).unwrap();
    let (offset, _) = p3.append(&mk(b"next")).unwrap();
    assert_eq!(offset, Offset(20));
}

#[test]
fn open_truncates_partial_tail_length_prefix() {
    let tmp = TempDir::new().unwrap();
    let mut p = Partition::create(tmp.path(), "s", 0).unwrap();
    for i in 0..5 {
        p.append(&mk(
            Box::leak(format!("v{i}").into_boxed_str()).as_bytes()
        )).unwrap();
    }
    drop(p);

    // Find the single .seg file and append garbage bytes.
    let entries: Vec<_> = std::fs::read_dir(tmp.path()).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("seg"))
        .collect();
    assert_eq!(entries.len(), 1);
    let seg_path = entries[0].path();
    let size_before = std::fs::metadata(&seg_path).unwrap().len();
    let mut f = OpenOptions::new().append(true).open(&seg_path).unwrap();
    f.write_all(&[0xFF, 0xFF]).unwrap();
    drop(f);

    // Re-open should detect + truncate the garbage.
    let p2 = Partition::open(tmp.path(), "s", 0).unwrap();
    let records = p2.read(Offset(0), 100).unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), size_before);
}

#[test]
fn open_rejects_legacy_wal_log() {
    // If a legacy wal.log from v0.1.x is found, Partition::open must fail
    // fast rather than silently ignore it.
    let tmp = TempDir::new().unwrap();
    let p = Partition::create(tmp.path(), "s", 0).unwrap();
    drop(p);

    // Drop a bogus wal.log into the partition directory.
    std::fs::write(tmp.path().join("wal.log"), b"legacy data").unwrap();

    let err = match Partition::open(tmp.path(), "s", 0) {
        Ok(_) => panic!("expected Partition::open to reject legacy wal.log"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    let msg = format!("{err}");
    assert!(
        msg.contains("wal.log") || msg.contains("v0.2.0"),
        "error message should mention wal.log or v0.2.0, got: {msg}"
    );
}
