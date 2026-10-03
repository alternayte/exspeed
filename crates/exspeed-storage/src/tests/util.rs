//! Shared helpers for the storage tests.

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{ReadLimits, Record, StorageEngine, StoredRecord};

use crate::file::{FileStorage, StorageOptions, StorageSyncMode};

pub fn stream(name: &str) -> StreamName {
    StreamName::try_from(name).unwrap()
}

pub fn now_ns() -> u64 {
    crate::file::writer::now_nanos()
}

/// A record whose value encodes its expected offset (`v-{i}`).
pub fn plain(i: u64) -> Record {
    Record {
        key: None,
        value: Bytes::from(format!("v-{i}")),
        subject: "test.subject".into(),
        headers: vec![],
        timestamp_ns: None,
    }
}

pub fn plain_at(i: u64, ts: u64) -> Record {
    Record {
        timestamp_ns: Some(ts),
        ..plain(i)
    }
}

pub fn keyed(key: &str, value: &str) -> Record {
    Record {
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        value: Bytes::copy_from_slice(value.as_bytes()),
        subject: "kv".into(),
        headers: vec![],
        timestamp_ns: None,
    }
}

pub fn options(segment_max_bytes: u64) -> StorageOptions {
    StorageOptions {
        segment_max_bytes,
        compaction_interval: Duration::ZERO,
        ..StorageOptions::default()
    }
}

/// Storage with tiny segments and no background compactor.
pub fn small_segments(dir: &Path, segment_max_bytes: u64) -> FileStorage {
    FileStorage::open_with_options(dir, options(segment_max_bytes)).unwrap()
}

pub fn small_segments_async(dir: &Path, segment_max_bytes: u64) -> FileStorage {
    FileStorage::open_with_options(
        dir,
        StorageOptions {
            sync_mode: StorageSyncMode::Async {
                interval: Duration::from_millis(5),
                threshold_bytes: 0,
            },
            ..options(segment_max_bytes)
        },
    )
    .unwrap()
}

/// Append `plain(start..start+n)` one by one, asserting the offsets.
pub async fn append_n(s: &impl StorageEngine, st: &StreamName, start: u64, n: u64) {
    for i in start..start + n {
        let (o, _) = s.append(st, &plain(i)).await.unwrap();
        assert_eq!(o, Offset(i));
    }
}

/// Read everything from `from` to the high watermark with `read_batch`.
pub async fn read_all(s: &impl StorageEngine, st: &StreamName, from: u64) -> Vec<StoredRecord> {
    let mut out = Vec::new();
    let mut next = Offset(from);
    loop {
        let b = s
            .read_batch(
                st,
                next,
                ReadLimits {
                    max_records: 97,
                    max_bytes: 4096,
                },
            )
            .await
            .unwrap();
        out.extend(b.records);
        if b.next_offset >= b.high_watermark {
            return out;
        }
        assert!(b.next_offset > next, "read_batch made no progress");
        next = b.next_offset;
    }
}

/// Every record's value is `v-{offset}`.
pub fn check_values(recs: &[StoredRecord]) {
    for r in recs {
        assert_eq!(r.value, Bytes::from(format!("v-{}", r.offset.0)));
    }
}

pub fn segment_count(s: &FileStorage, name: &str) -> usize {
    s.shared(name).unwrap().segments().len()
}

/// Tiny deterministic PRNG for seeded tests.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}
