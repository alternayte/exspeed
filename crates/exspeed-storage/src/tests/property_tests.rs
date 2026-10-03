//! Model-based property tests (Phase 1 exit criteria).
//!
//! Random sequences of appends, clean restarts, crashes with a torn tail,
//! retention trims and reads run against both a `FileStorage` with tiny
//! segments and a simple model. After every step:
//!
//! * offsets are strictly increasing and dense from the earliest retained
//!   offset to the high watermark: no gaps, no duplicates;
//! * every acknowledged record that retention has not removed reads back
//!   byte-for-byte at its offset;
//! * the high watermark never moves backwards and no offset is ever handed
//!   out twice, across restarts and crashes;
//! * the earliest offset never moves backwards, and retention only removes
//!   a prefix.
//!
//! Cases are seeded and reproducible; set `EXSPEED_PROPTEST_CASES` to run
//! more (default 16).

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use bytes::Bytes;
use exspeed_common::Offset;
use exspeed_streams::{ReadLimits, Record, StorageEngine};

use super::util::{options, stream, Rng};
use crate::file::FileStorage;

const STREAM: &str = "prop";

fn cases() -> u64 {
    std::env::var("EXSPEED_PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16)
}

fn open(dir: &Path) -> FileStorage {
    FileStorage::open_with_options(dir, options(2048)).unwrap()
}

fn value(seed: u64, offset: u64, rng: &mut Rng) -> Bytes {
    let len = 1 + rng.below(300) as usize;
    let mut v = format!("{seed}:{offset}:").into_bytes();
    v.resize(v.len() + len, b'a' + (offset % 26) as u8);
    Bytes::from(v)
}

/// The newest segment file of the stream (where a crash tears the tail).
fn active_segment(dir: &Path) -> std::path::PathBuf {
    let part = dir
        .join("streams")
        .join(STREAM)
        .join("partitions")
        .join("0");
    let mut segs: Vec<_> = std::fs::read_dir(&part)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("seg"))
        .collect();
    segs.sort();
    segs.pop().expect("a segment")
}

struct Model {
    /// offset -> value, for retained records.
    records: BTreeMap<u64, Bytes>,
    next: u64,
    earliest: u64,
}

async fn check(storage: &FileStorage, m: &mut Model, step: &str) {
    let s = stream(STREAM);
    let (earliest, next) = storage.stream_bounds(&s).await.unwrap();
    assert_eq!(next.0, m.next, "{step}: high watermark");
    assert!(earliest.0 >= m.earliest, "{step}: earliest moved backwards");
    // Retention (segment-granular) may have removed a prefix; drop it from
    // the model too.
    m.records = m.records.split_off(&earliest.0);
    m.earliest = earliest.0;

    let mut got = Vec::new();
    let mut from = Offset(earliest.0);
    loop {
        let b = storage
            .read_batch(
                &s,
                from,
                ReadLimits {
                    max_records: 37,
                    max_bytes: 3000,
                },
            )
            .await
            .unwrap();
        got.extend(b.records);
        if b.next_offset >= b.high_watermark {
            break;
        }
        assert!(b.next_offset > from, "{step}: read made no progress");
        from = b.next_offset;
    }
    assert_eq!(got.len(), m.records.len(), "{step}: record count");
    let mut prev: Option<u64> = None;
    for (r, (o, v)) in got.iter().zip(m.records.iter()) {
        if let Some(p) = prev {
            assert_eq!(r.offset.0, p + 1, "{step}: offsets dense and increasing");
        }
        prev = Some(r.offset.0);
        assert_eq!(r.offset.0, *o, "{step}: offset");
        assert_eq!(&r.value, v, "{step}: value at {o}");
    }

    // A random point read agrees with the model.
    if let Some((&o, v)) = m.records.iter().nth(m.records.len() / 2) {
        let r = storage.read(&s, Offset(o), 1).await.unwrap();
        assert_eq!(r[0].offset.0, o);
        assert_eq!(&r[0].value, v);
    }
}

async fn run_case(seed: u64) {
    let dir = tempfile::tempdir().unwrap();
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let s = stream(STREAM);
    let mut storage = open(dir.path());
    storage.create_stream(&s, 0, 0).await.unwrap();
    let mut m = Model {
        records: BTreeMap::new(),
        next: 0,
        earliest: 0,
    };

    for step in 0..60 {
        let op = rng.below(100);
        let label = format!("seed {seed} step {step} op {op}");
        match op {
            // Append a batch.
            0..=54 => {
                let n = 1 + rng.below(25);
                let recs: Vec<Record> = (0..n)
                    .map(|i| Record {
                        value: value(seed, m.next + i, &mut rng),
                        subject: format!("p.{}", rng.below(4)),
                        ..Default::default()
                    })
                    .collect();
                let values: Vec<Bytes> = recs.iter().map(|r| r.value.clone()).collect();
                let res = storage.append_batch(&s, recs).await.unwrap();
                for ((o, _), v) in res.into_iter().zip(values) {
                    assert_eq!(o.0, m.next, "{label}: assigned offset");
                    m.records.insert(o.0, v);
                    m.next += 1;
                }
            }
            // Clean restart.
            55..=64 => {
                drop(storage);
                storage = open(dir.path());
            }
            // Crash with a torn, unacknowledged tail: garbage after the last
            // committed frame must be discarded by recovery.
            65..=74 => {
                drop(storage);
                let seg = active_segment(dir.path());
                let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
                let junk: Vec<u8> = (0..1 + rng.below(200)).map(|_| rng.next() as u8).collect();
                f.write_all(&junk).unwrap();
                f.sync_all().unwrap();
                drop(f);
                storage = open(dir.path());
            }
            // Retention: keep from a random offset.
            75..=84 => {
                if m.next > 0 {
                    let keep_from = m.earliest + rng.below(m.next - m.earliest + 1);
                    storage.trim_up_to(&s, Offset(keep_from)).await.unwrap();
                }
            }
            // Read only.
            _ => {}
        }
        check(&storage, &mut m, &label).await;
    }
}

#[tokio::test]
async fn offsets_and_contents_survive_restarts_crashes_and_retention() {
    for seed in 1..=cases() {
        run_case(seed).await;
    }
}
