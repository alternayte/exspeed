//! Read path: seeks across segments, concurrent readers, high watermark,
//! `watch_appends`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use exspeed_common::Offset;
use exspeed_streams::{ReadLimits, StorageEngine};
use tempfile::TempDir;

use super::util::*;
use crate::file::FileStorage;

const T0: u64 = 1_000_000;

/// Expected `seek_by_time` result by brute force.
fn brute_seek(ts: &[u64], target: u64) -> u64 {
    ts.iter()
        .position(|&t| t >= target)
        .map_or(ts.len() as u64, |p| p as u64)
}

async fn check_seeks(storage: &FileStorage, name: &str, ts: &[u64], targets: &[u64]) {
    let s = stream(name);
    for &t in targets {
        let got = storage.seek_by_time(&s, t).await.unwrap();
        assert_eq!(got.0, brute_seek(ts, t), "seek_by_time({t})");
    }
}

#[tokio::test]
async fn seek_by_time_across_many_segments() {
    let dir = TempDir::new().unwrap();
    let n = 600u64;
    let ts: Vec<u64> = (0..n).map(|i| T0 + 10 * i).collect();
    let name = "seek";
    let s = stream(name);
    let targets: Vec<u64>;
    {
        let storage = small_segments(dir.path(), 300);
        storage.create_stream(&s, 0, 0).await.unwrap();
        for (i, &t) in ts.iter().enumerate() {
            storage.append(&s, &plain_at(i as u64, t)).await.unwrap();
        }
        assert!(segment_count(&storage, name) > 20);
        let bases: Vec<u64> = storage
            .shared(name)
            .unwrap()
            .segments()
            .iter()
            .map(|seg| seg.base_offset)
            .collect();
        let mut tg = vec![0, T0 - 1, T0, T0 + 1, T0 + 10 * n, u64::MAX];
        for b in bases.iter().filter(|&&b| b < n) {
            let t = ts[*b as usize];
            tg.extend([t - 1, t, t + 1]); // on and around every boundary
        }
        for i in (0..n).step_by(7) {
            tg.push(ts[i as usize] + 5); // between records
        }
        targets = tg;
        check_seeks(&storage, name, &ts, &targets).await;
    }
    // After a restart the sealed indexes come from disk.
    let storage = small_segments(dir.path(), 300);
    check_seeks(&storage, name, &ts, &targets).await;
}

/// Out-of-order timestamps: the result is still the first record (by
/// offset) whose timestamp is `>= target`.
#[tokio::test]
async fn seek_by_time_with_unordered_timestamps() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 400);
    let s = stream("unordered");
    storage.create_stream(&s, 0, 0).await.unwrap();
    let mut rng = Rng(99);
    let ts: Vec<u64> = (0..500).map(|_| T0 + rng.below(10_000)).collect();
    for (i, &t) in ts.iter().enumerate() {
        storage.append(&s, &plain_at(i as u64, t)).await.unwrap();
    }
    let targets: Vec<u64> = (0..300).map(|_| T0 + rng.below(10_500)).collect();
    check_seeks(&storage, "unordered", &ts, &targets).await;
}

#[tokio::test]
async fn read_batch_respects_limits_and_reports_hwm() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 512);
    let s = stream("rb");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 100).await;
    let b = storage
        .read_batch(
            &s,
            Offset(10),
            ReadLimits {
                max_records: 5,
                max_bytes: 1 << 20,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        b.records.iter().map(|r| r.offset.0).collect::<Vec<_>>(),
        (10..15).collect::<Vec<_>>()
    );
    assert_eq!(b.next_offset, Offset(15));
    assert_eq!(b.high_watermark, Offset(100));
    // A byte limit smaller than one record still returns one record.
    let b = storage
        .read_batch(
            &s,
            Offset(50),
            ReadLimits {
                max_records: 100,
                max_bytes: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(b.records.len(), 1);
    // Caught up.
    let b = storage
        .read_batch(&s, Offset(100), ReadLimits::default())
        .await
        .unwrap();
    assert!(b.records.is_empty());
    assert_eq!(b.next_offset, b.high_watermark);
}

/// Readers running while the writer appends (and rolls segments) never see
/// a torn or partial record, never see anything at or beyond the high
/// watermark they were given, and the high watermark never goes backwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_readers_never_see_partial_or_unpublished_records() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 8192);
    let s = stream("conc");
    storage.create_stream(&s, 0, 0).await.unwrap();
    let total = 4000u64;
    let done = Arc::new(AtomicBool::new(false));

    let mut readers = Vec::new();
    for r in 0..3 {
        let st = storage.clone();
        let s = s.clone();
        let done = done.clone();
        readers.push(tokio::spawn(async move {
            let mut next = 0u64;
            let mut last_hwm = 0u64;
            let mut seen = 0u64;
            loop {
                let finished = done.load(Ordering::SeqCst);
                let b = st
                    .read_batch(
                        &s,
                        Offset(next),
                        ReadLimits {
                            max_records: 50 + r * 31,
                            max_bytes: 1 << 16,
                        },
                    )
                    .await
                    .unwrap();
                assert!(b.high_watermark.0 >= last_hwm, "hwm went backwards");
                last_hwm = b.high_watermark.0;
                for rec in &b.records {
                    assert_eq!(rec.offset.0, next, "gap or duplicate");
                    assert!(rec.offset < b.high_watermark, "record beyond hwm");
                    assert_eq!(rec.value, bytes::Bytes::from(format!("v-{next}")));
                    next += 1;
                    seen += 1;
                }
                // Raw stream_bounds must agree too.
                let (_, hwm) = st.stream_bounds(&s).await.unwrap();
                assert!(hwm.0 >= next);
                if finished && next == total {
                    return seen;
                }
                if b.records.is_empty() {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }

    let mut rng = Rng(42);
    let mut i = 0u64;
    while i < total {
        let n = (1 + rng.below(20)).min(total - i);
        let recs = (i..i + n).map(plain).collect();
        storage.append_batch(&s, recs).await.unwrap();
        i += n;
    }
    done.store(true, Ordering::SeqCst);
    for r in readers {
        assert_eq!(r.await.unwrap(), total);
    }
    assert!(segment_count(&storage, "conc") > 5);
}

#[tokio::test]
async fn watch_appends_wakes_on_append() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 1 << 20);
    let s = stream("w");
    storage.create_stream(&s, 0, 0).await.unwrap();
    let mut rx = storage
        .watch_appends(&s)
        .expect("file storage supports watch");
    assert_eq!(*rx.borrow(), 0);
    let waiter = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), rx.changed())
            .await
            .expect("woken")
            .unwrap();
        let v = *rx.borrow();
        v
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    storage.append(&s, &plain(0)).await.unwrap();
    assert_eq!(waiter.await.unwrap(), 1);
    let rx = storage.watch_appends(&s).unwrap();
    storage
        .append_batch(&s, vec![plain(1), plain(2)])
        .await
        .unwrap();
    assert_eq!(*rx.borrow(), 3);
}

/// Replication hook: with a floor set the high watermark is held at
/// min(committed, floor); reads and seeks never pass it.
#[tokio::test]
async fn replication_floor_holds_back_the_high_watermark() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 1 << 20);
    let s = stream("floor");
    storage.create_stream(&s, 0, 0).await.unwrap();
    assert!(storage.set_replication_floor("floor", Some(5)));
    for i in 0..10 {
        storage.append(&s, &plain_at(i, T0 + i)).await.unwrap();
    }
    assert_eq!(storage.stream_bounds(&s).await.unwrap().1, Offset(5));
    assert_eq!(read_all(&storage, &s, 0).await.len(), 5);
    assert_eq!(storage.seek_by_time(&s, T0 + 8).await.unwrap(), Offset(5));
    let rx = storage.watch_appends(&s).unwrap();
    storage.set_replication_floor("floor", Some(8));
    assert_eq!(*rx.borrow(), 8);
    storage.set_replication_floor("floor", None);
    assert_eq!(storage.stream_bounds(&s).await.unwrap().1, Offset(10));
    assert_eq!(read_all(&storage, &s, 0).await.len(), 10);
}
