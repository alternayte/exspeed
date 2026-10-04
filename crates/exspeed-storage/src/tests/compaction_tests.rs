//! Log compaction.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bytes::Bytes;
use exspeed_common::Offset;
use exspeed_streams::{ReadLimits, Record, StorageEngine, StoredRecord, StreamConfig};
use tempfile::TempDir;

use super::util::*;
use crate::file::{FileStorage, StorageOptions};

fn compacted(tombstone_retention_secs: u64) -> StreamConfig {
    StreamConfig {
        compaction: true,
        tombstone_retention_secs,
        ..StreamConfig::default()
    }
}

fn keyless(i: u64) -> Record {
    Record {
        key: None,
        value: Bytes::from(format!("nokey-{i}")),
        subject: "kv".into(),
        headers: vec![],
        timestamp_ns: None,
    }
}

/// Write `rounds` updates for keys k0..k9 interleaved with keyless records.
/// Returns everything written, by offset.
async fn fill(storage: &FileStorage, name: &str, rounds: u64) -> Vec<StoredRecord> {
    let s = stream(name);
    let mut written = Vec::new();
    for r in 0..rounds {
        for k in 0..10u64 {
            let rec = if (r * 10 + k) % 7 == 0 {
                keyless(r * 10 + k)
            } else {
                keyed(&format!("k{k}"), &format!("r{r}"))
            };
            let (o, ts) = storage.append(&s, &rec).await.unwrap();
            written.push(StoredRecord {
                offset: o,
                timestamp: ts,
                subject: rec.subject,
                key: rec.key,
                value: rec.value,
                headers: rec.headers,
            });
        }
    }
    written
}

/// What must survive compaction: keyless records, the latest record per
/// key, and everything in the active segment.
fn assert_compaction_invariants(
    written: &[StoredRecord],
    after: &[StoredRecord],
    active_base: u64,
) {
    let mut latest: HashMap<Bytes, u64> = HashMap::new();
    for r in written {
        if let Some(k) = &r.key {
            latest.insert(k.clone(), r.offset.0);
        }
    }
    let by_offset: HashMap<u64, &StoredRecord> = written.iter().map(|r| (r.offset.0, r)).collect();
    let mut prev = None;
    for r in after {
        assert!(
            prev.is_none_or(|p| r.offset.0 > p),
            "offsets strictly increasing"
        );
        prev = Some(r.offset.0);
        let orig = by_offset[&r.offset.0];
        assert_eq!(
            r.value, orig.value,
            "offset {} keeps its record",
            r.offset.0
        );
        assert_eq!(r.key, orig.key);
        assert_eq!(r.timestamp, orig.timestamp);
        if let Some(k) = &r.key {
            if r.offset.0 < active_base {
                assert_eq!(latest[k], r.offset.0, "superseded record survived");
            }
        }
    }
    let kept: HashSet<u64> = after.iter().map(|r| r.offset.0).collect();
    for r in written {
        let must = r.key.is_none()
            || latest[r.key.as_ref().unwrap()] == r.offset.0
            || r.offset.0 >= active_base;
        if must {
            assert!(
                kept.contains(&r.offset.0),
                "offset {} was removed",
                r.offset.0
            );
        }
    }
}

#[tokio::test]
async fn keeps_latest_value_per_key_and_preserves_offsets() {
    let dir = TempDir::new().unwrap();
    let name = "compact";
    let s = stream(name);
    let storage = small_segments(dir.path(), 400);
    storage
        .create_stream_with(&s, &compacted(86_400))
        .await
        .unwrap();
    let written = fill(&storage, name, 30).await;
    let before = storage.stream_storage_bytes(name).unwrap();
    let active_base = storage
        .shared(name)
        .unwrap()
        .segments()
        .last()
        .unwrap()
        .base_offset;

    let stats = storage.compact_stream(name).unwrap();
    assert!(stats.segments_rewritten > 0 && stats.records_removed > 0);
    assert!(storage.stream_storage_bytes(name).unwrap() < before);
    let after = read_all(&storage, &s, 0).await;
    assert!(after.len() < written.len());
    assert_compaction_invariants(&written, &after, active_base);
    // A second pass has nothing left to do.
    assert_eq!(storage.compact_stream(name).unwrap().records_removed, 0);

    // Bounds and the next offset are unchanged.
    assert_eq!(
        storage.stream_bounds(&s).await.unwrap(),
        (Offset(0), Offset(300))
    );
    let (o, _) = storage.append(&s, &keyed("k1", "new")).await.unwrap();
    assert_eq!(o, Offset(300));

    // Reads starting inside gaps land on the next surviving record.
    let kept: Vec<u64> = after.iter().map(|r| r.offset.0).collect();
    for from in 0..300u64 {
        let b = storage
            .read_batch(
                &s,
                Offset(from),
                ReadLimits {
                    max_records: 1,
                    max_bytes: 1 << 20,
                },
            )
            .await
            .unwrap();
        let expect = kept.iter().copied().find(|&o| o >= from).unwrap_or(300);
        assert_eq!(
            b.records.first().map_or(300, |r| r.offset.0),
            expect,
            "from {from}"
        );
        let legacy = storage.read(&s, Offset(from), 1).await.unwrap();
        assert_eq!(legacy.first().map_or(300, |r| r.offset.0), expect);
    }
    // Same view after a restart.
    drop(storage);
    let storage = small_segments(dir.path(), 400);
    let again = read_all(&storage, &s, 0).await;
    assert_eq!(
        again.iter().map(|r| r.offset.0).collect::<Vec<_>>(),
        after
            .iter()
            .map(|r| r.offset.0)
            .chain([300])
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn tombstones_delete_keys_and_expire() {
    let dir = TempDir::new().unwrap();
    let s = stream("tomb");
    let storage = small_segments(dir.path(), 200);
    // Long tombstone retention first.
    storage
        .create_stream_with(&s, &compacted(86_400))
        .await
        .unwrap();
    for i in 0..5 {
        storage
            .append(&s, &keyed("a", &format!("a{i}")))
            .await
            .unwrap();
        storage
            .append(&s, &keyed("b", &format!("b{i}")))
            .await
            .unwrap();
    }
    let (tomb, _) = storage.append(&s, &keyed("a", "")).await.unwrap();
    // Push the tombstone into a sealed segment.
    for i in 0..10 {
        storage.append(&s, &keyless(i)).await.unwrap();
    }
    storage.compact_stream("tomb").unwrap();
    let recs = read_all(&storage, &s, 0).await;
    let a: Vec<&StoredRecord> = recs
        .iter()
        .filter(|r| r.key.as_deref() == Some(b"a"))
        .collect();
    assert_eq!(a.len(), 1, "only the tombstone is left for key a");
    assert_eq!(a[0].offset, tomb);
    assert!(a[0].value.is_empty());
    let b: Vec<&StoredRecord> = recs
        .iter()
        .filter(|r| r.key.as_deref() == Some(b"b"))
        .collect();
    assert_eq!(b.len(), 1);
    assert_eq!(&b[0].value[..], b"b4");

    // With zero retention the (already old) tombstone goes too.
    storage
        .update_stream_config(&s, &compacted(0))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    storage.compact_stream("tomb").unwrap();
    let recs = read_all(&storage, &s, 0).await;
    assert!(
        recs.iter().all(|r| r.key.as_deref() != Some(b"a")),
        "key a is gone"
    );
    assert_eq!(recs.iter().filter(|r| r.key.is_none()).count(), 10);
}

#[tokio::test]
async fn non_compacted_streams_are_never_compacted() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 200);
    let s = stream("plain");
    storage.create_stream(&s, 0, 0).await.unwrap();
    for i in 0..50 {
        storage
            .append(&s, &keyed("same", &format!("{i}")))
            .await
            .unwrap();
    }
    assert_eq!(storage.compact_stream("plain").unwrap().records_removed, 0);
    assert_eq!(storage.compact_all().records_removed, 0);
    assert_eq!(read_all(&storage, &s, 0).await.len(), 50);
}

#[tokio::test]
async fn seek_works_across_compaction_gaps() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 200);
    let s = stream("seekgap");
    storage
        .create_stream_with(&s, &compacted(86_400))
        .await
        .unwrap();
    for i in 0..100u64 {
        let mut r = keyed(&format!("k{}", i % 3), "v");
        r.timestamp_ns = Some(1_000 + i);
        storage.append(&s, &r).await.unwrap();
    }
    storage.compact_stream("seekgap").unwrap();
    let recs = read_all(&storage, &s, 0).await;
    for target in 1_000..1_101u64 {
        let expect = recs
            .iter()
            .find(|r| r.timestamp >= target)
            .map_or(100, |r| r.offset.0);
        assert_eq!(storage.seek_by_time(&s, target).await.unwrap().0, expect);
    }
}

/// A crash before the rewrite is installed leaves `.compacting` files that
/// recovery deletes; the original data is intact.
#[tokio::test]
async fn crash_before_install_keeps_original_data() {
    let dir = TempDir::new().unwrap();
    let name = "crash1";
    let s = stream(name);
    let written;
    {
        let storage = small_segments(dir.path(), 400);
        storage
            .create_stream_with(&s, &compacted(86_400))
            .await
            .unwrap();
        written = fill(&storage, name, 20).await;
        let shared = storage.shared(name).unwrap();
        let jobs = crate::file::compaction::prepare(&shared, &compacted(86_400), now_ns()).unwrap();
        assert!(!jobs.is_empty());
        // "Crash": never install.
    }
    let part = dir.path().join("streams/crash1/partitions/0");
    let leftovers = |p: &std::path::Path| {
        std::fs::read_dir(p)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".compacting")
            })
            .count()
    };
    assert!(leftovers(&part) > 0);
    let storage = small_segments(dir.path(), 400);
    assert_eq!(leftovers(&part), 0, "recovery removes leftovers");
    let after = read_all(&storage, &s, 0).await;
    assert_eq!(after.len(), written.len());
}

/// A crash after the compacted segment was renamed into place but before
/// its index and metadata were: recovery notices the stale `.meta` and
/// rebuilds them from the compacted file.
#[tokio::test]
async fn crash_mid_install_is_recovered() {
    let dir = TempDir::new().unwrap();
    let name = "crash2";
    let s = stream(name);
    let written;
    let active_base;
    let expected: Vec<u64>;
    {
        let storage = small_segments(dir.path(), 400);
        storage
            .create_stream_with(&s, &compacted(86_400))
            .await
            .unwrap();
        written = fill(&storage, name, 20).await;
        let shared = storage.shared(name).unwrap();
        active_base = shared.segments().last().unwrap().base_offset;
        let jobs = crate::file::compaction::prepare(&shared, &compacted(86_400), now_ns()).unwrap();
        let mut removed: HashSet<u64> = HashSet::new();
        for (job, _) in &jobs {
            // Only the segment file is renamed before the "crash".
            let base = job.meta.base_offset;
            let part = &shared.dir;
            let old: HashSet<u64> = {
                let seg = job.old.clone();
                let mut it =
                    crate::file::segment::FrameIter::new(seg.file(), 16, seg.len(), 1 << 20);
                let mut v = HashSet::new();
                while let Ok(Some(f)) = it.next_frame() {
                    v.insert(f.offset);
                }
                v
            };
            std::fs::rename(&job.tmp_seg, part.join(format!("{base:020}.seg"))).unwrap();
            let file = std::fs::File::open(part.join(format!("{base:020}.seg"))).unwrap();
            let mut it = crate::file::segment::FrameIter::new(&file, 16, job.meta.len, 1 << 20);
            let mut new = HashSet::new();
            while let Ok(Some(f)) = it.next_frame() {
                new.insert(f.offset);
            }
            removed.extend(old.difference(&new));
        }
        assert!(!removed.is_empty());
        expected = written
            .iter()
            .map(|r| r.offset.0)
            .filter(|o| !removed.contains(o))
            .collect();
    }
    let storage = small_segments(dir.path(), 400);
    let after = read_all(&storage, &s, 0).await;
    assert_eq!(
        after.iter().map(|r| r.offset.0).collect::<Vec<_>>(),
        expected
    );
    assert_compaction_invariants(&written, &after, active_base);
    // Reads from every offset work through the rebuilt indexes.
    for from in (0..200u64).step_by(13) {
        let b = storage
            .read_batch(
                &s,
                Offset(from),
                ReadLimits {
                    max_records: 1,
                    max_bytes: 1 << 20,
                },
            )
            .await
            .unwrap();
        let want = expected.iter().copied().find(|&o| o >= from).unwrap_or(200);
        assert_eq!(b.records.first().map_or(200, |r| r.offset.0), want);
    }
}

#[tokio::test]
async fn background_compactor_runs() {
    let dir = TempDir::new().unwrap();
    let storage = FileStorage::open_with_options(
        dir.path(),
        StorageOptions {
            compaction_interval: Duration::from_millis(30),
            ..options(300)
        },
    )
    .unwrap();
    let s = stream("bg");
    storage
        .create_stream_with(&s, &compacted(86_400))
        .await
        .unwrap();
    for i in 0..100 {
        storage
            .append(&s, &keyed("only", &format!("{i}")))
            .await
            .unwrap();
    }
    for _ in 0..200 {
        if read_all(&storage, &s, 0).await.len() < 20 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("background compactor never compacted the stream");
}
