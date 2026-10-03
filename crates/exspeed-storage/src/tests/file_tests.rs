//! FileStorage behaviour: persistence, rolling, retention, trim, truncate.

use bytes::Bytes;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine, StorageError, StreamConfig};
use tempfile::TempDir;

use super::util::*;
use crate::file::FileStorage;

fn record(value: &[u8]) -> Record {
    Record {
        key: Some(Bytes::from_static(b"key")),
        value: Bytes::copy_from_slice(value),
        subject: "test.subject".into(),
        headers: vec![],
        timestamp_ns: None,
    }
}

async fn offsets(s: &FileStorage, st: &StreamName, from: u64) -> Vec<u64> {
    read_all(s, st, from)
        .await
        .iter()
        .map(|r| r.offset.0)
        .collect()
}

#[tokio::test]
async fn data_persists_across_restart() {
    let dir = TempDir::new().unwrap();
    let s = stream("persist");
    {
        let storage = FileStorage::new(dir.path()).unwrap();
        storage.create_stream(&s, 0, 0).await.unwrap();
        for i in 0u64..10 {
            let (o, _) = storage
                .append(&s, &record(format!("val-{i}").as_bytes()))
                .await
                .unwrap();
            assert_eq!(o, Offset(i));
        }
    }
    let storage = FileStorage::open(dir.path()).unwrap();
    let recs = storage.read(&s, Offset(0), 100).await.unwrap();
    assert_eq!(recs.len(), 10);
    for (i, r) in recs.iter().enumerate() {
        assert_eq!(r.offset, Offset(i as u64));
        assert_eq!(r.value, Bytes::from(format!("val-{i}")));
        assert_eq!(r.key.as_deref(), Some(&b"key"[..]));
    }
    let (o, _) = storage.append(&s, &record(b"next")).await.unwrap();
    assert_eq!(o, Offset(10));
}

#[tokio::test]
async fn many_segments_read_back_in_order_and_survive_restart() {
    let dir = TempDir::new().unwrap();
    let s = stream("rolling");
    {
        let storage = small_segments(dir.path(), 512);
        storage.create_stream(&s, 0, 0).await.unwrap();
        append_n(&storage, &s, 0, 1000).await;
        assert!(segment_count(&storage, "rolling") > 20);
        assert_eq!(
            offsets(&storage, &s, 0).await,
            (0..1000).collect::<Vec<_>>()
        );
    }
    let storage = small_segments(dir.path(), 512);
    assert_eq!(
        offsets(&storage, &s, 0).await,
        (0..1000).collect::<Vec<_>>()
    );
    // Reads starting mid-way, in every segment.
    for from in [1u64, 37, 500, 998, 999] {
        assert_eq!(
            offsets(&storage, &s, from).await,
            (from..1000).collect::<Vec<_>>()
        );
    }
    let (o, _) = storage.append(&s, &plain(1000)).await.unwrap();
    assert_eq!(o, Offset(1000));
}

#[tokio::test]
async fn startup_reads_sealed_metadata_not_data() {
    let dir = TempDir::new().unwrap();
    let s = stream("meta");
    {
        let storage = small_segments(dir.path(), 512);
        storage.create_stream(&s, 0, 0).await.unwrap();
        append_n(&storage, &s, 0, 200).await;
    }
    let part = dir.path().join("streams/meta/partitions/0");
    let metas = std::fs::read_dir(&part)
        .unwrap()
        .filter(|e| e.as_ref().unwrap().path().extension().unwrap() == "meta")
        .count();
    let segs = std::fs::read_dir(&part)
        .unwrap()
        .filter(|e| e.as_ref().unwrap().path().extension().unwrap() == "seg")
        .count();
    assert_eq!(metas, segs - 1, "every sealed segment has a .meta sidecar");
    // Corrupt the *data* of the first sealed segment in a way that only a
    // full decode would notice; open must not decode sealed data.
    let first = part.join(format!("{:020}.seg", 0));
    let mut bytes = std::fs::read(&first).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&first, &bytes).unwrap();
    let storage = small_segments(dir.path(), 512);
    let (_, next) = storage.stream_bounds(&s).await.unwrap();
    assert_eq!(next, Offset(200));
    // Reading the damaged record reports corruption instead of garbage.
    let err = storage.read(&s, Offset(0), 1000).await.unwrap_err();
    assert!(
        matches!(err, StorageError::CorruptedRecord { .. }),
        "{err:?}"
    );
}

#[tokio::test]
async fn stale_meta_is_rebuilt_by_scan() {
    let dir = TempDir::new().unwrap();
    let s = stream("stale");
    {
        let storage = small_segments(dir.path(), 512);
        storage.create_stream(&s, 0, 0).await.unwrap();
        append_n(&storage, &s, 0, 100).await;
    }
    let part = dir.path().join("streams/stale/partitions/0");
    std::fs::remove_file(part.join(format!("{:020}.meta", 0))).unwrap();
    std::fs::remove_file(part.join(format!("{:020}.idx", 0))).unwrap();
    let storage = small_segments(dir.path(), 512);
    assert_eq!(offsets(&storage, &s, 0).await, (0..100).collect::<Vec<_>>());
    assert!(part.join(format!("{:020}.meta", 0)).exists());
}

#[tokio::test]
async fn list_streams_and_metrics_helpers() {
    let dir = TempDir::new().unwrap();
    let storage = FileStorage::new(dir.path()).unwrap();
    storage.create_stream(&stream("beta"), 0, 0).await.unwrap();
    storage.create_stream(&stream("alpha"), 0, 0).await.unwrap();
    assert_eq!(storage.list_streams(), vec!["alpha", "beta"]);
    assert_eq!(storage.stream_head_offset("alpha"), Some(0));
    storage
        .append(&stream("alpha"), &record(b"data"))
        .await
        .unwrap();
    assert_eq!(storage.stream_head_offset("alpha"), Some(1));
    assert!(storage.stream_storage_bytes("alpha").unwrap() > 16);
    assert!(storage.stream_storage_bytes("nope").is_none());
    assert!(storage.stream_head_offset("nope").is_none());
}

#[tokio::test]
async fn retention_by_age_deletes_old_sealed_segments_only() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 256);
    let s = stream("age");
    let cfg = StreamConfig {
        max_age_secs: 1,
        dedup_window_secs: 1,
        ..StreamConfig::default()
    };
    storage.create_stream_with(&s, &cfg).await.unwrap();
    // Old records (timestamps far in the past) fill sealed segments...
    let old = now_ns() - 3_600_000_000_000;
    for i in 0..30u64 {
        storage.append(&s, &plain_at(i, old + i)).await.unwrap();
    }
    // ...and fresh ones stay.
    let base_fresh = 30u64;
    for i in 0..30u64 {
        storage
            .append(&s, &plain_at(base_fresh + i, now_ns()))
            .await
            .unwrap();
    }
    let before = storage.stream_storage_bytes("age").unwrap();
    storage.enforce_all_retention().unwrap();
    let after = storage.stream_storage_bytes("age").unwrap();
    assert!(after < before);
    let (earliest, next) = storage.stream_bounds(&s).await.unwrap();
    assert!(
        earliest.0 > 0 && earliest.0 <= base_fresh,
        "earliest={earliest:?}"
    );
    assert_eq!(next, Offset(60));
    // Every fresh record survives.
    let got = offsets(&storage, &s, earliest.0).await;
    assert_eq!(got, (earliest.0..60).collect::<Vec<_>>());
}

#[tokio::test]
async fn retention_by_age_only_removes_a_prefix() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 256);
    let s = stream("prefix");
    let cfg = StreamConfig {
        max_age_secs: 1,
        dedup_window_secs: 1,
        ..StreamConfig::default()
    };
    storage.create_stream_with(&s, &cfg).await.unwrap();
    // Fresh records first, then old (producer-supplied) timestamps, then
    // fresh again: the old segments in the middle must not be deleted.
    let old = now_ns() - 3_600_000_000_000;
    for i in 0..20u64 {
        storage.append(&s, &plain_at(i, now_ns())).await.unwrap();
    }
    for i in 20..40u64 {
        storage.append(&s, &plain_at(i, old + i)).await.unwrap();
    }
    for i in 40..60u64 {
        storage.append(&s, &plain_at(i, now_ns())).await.unwrap();
    }
    storage.enforce_all_retention().unwrap();
    let (earliest, next) = storage.stream_bounds(&s).await.unwrap();
    assert_eq!(earliest, Offset(0));
    assert_eq!(next, Offset(60));
    assert_eq!(offsets(&storage, &s, 0).await, (0..60).collect::<Vec<_>>());
}

#[tokio::test]
async fn retention_by_size_keeps_newest_and_never_the_active_segment() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 256);
    let s = stream("size");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 60).await;
    let total = storage.stream_storage_bytes("size").unwrap();
    let cfg = StreamConfig {
        max_bytes: total / 3,
        ..StreamConfig::default()
    };
    storage.update_stream_config(&s, &cfg).await.unwrap();
    storage.enforce_all_retention().unwrap();
    assert!(storage.stream_storage_bytes("size").unwrap() <= total / 3);
    let (earliest, next) = storage.stream_bounds(&s).await.unwrap();
    assert!(earliest.0 > 0);
    assert_eq!(next, Offset(60));
    assert_eq!(
        offsets(&storage, &s, earliest.0).await,
        (earliest.0..60).collect::<Vec<_>>()
    );

    // Most aggressive retention: only the active segment is left.
    let cfg = StreamConfig {
        max_bytes: 1,
        max_age_secs: 1,
        dedup_window_secs: 1,
        ..StreamConfig::default()
    };
    storage.update_stream_config(&s, &cfg).await.unwrap();
    storage.enforce_all_retention().unwrap();
    assert_eq!(segment_count(&storage, "size"), 1);
    let (earliest, next) = storage.stream_bounds(&s).await.unwrap();
    assert_eq!(next, Offset(60));
    assert_eq!(
        offsets(&storage, &s, earliest.0).await,
        (earliest.0..60).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn read_below_earliest_after_retention_returns_out_of_range() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 256);
    let s = stream("oor");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 30).await;
    storage.trim_up_to(&s, Offset(15)).await.unwrap();
    let (earliest, _) = storage.stream_bounds(&s).await.unwrap();
    assert!(earliest.0 > 0 && earliest.0 <= 15);
    match storage.read(&s, Offset(0), 10).await.unwrap_err() {
        StorageError::OffsetOutOfRange {
            requested,
            earliest: e,
        } => {
            assert_eq!(requested, 0);
            assert_eq!(e, earliest.0);
        }
        other => panic!("expected OffsetOutOfRange, got {other:?}"),
    }
    // read_batch clamps instead.
    let b = storage
        .read_batch(&s, Offset(0), exspeed_streams::ReadLimits::default())
        .await
        .unwrap();
    assert_eq!(b.records[0].offset, earliest);
}

/// Regression (blocker 1): retention deleting every sealed segment leaves an
/// empty active segment; the next offset must survive a restart.
#[tokio::test]
async fn restart_after_retention_with_empty_active_segment_keeps_offsets_monotonic() {
    let dir = TempDir::new().unwrap();
    let s = stream("mono");
    {
        // A 1-byte limit rolls after every commit: the active segment is
        // always empty.
        let storage = small_segments(dir.path(), 1);
        storage.create_stream(&s, 0, 0).await.unwrap();
        append_n(&storage, &s, 0, 20).await;
        storage.trim_up_to(&s, Offset(1_000)).await.unwrap();
        // The writer rolled before handling the trim (same queue).
        assert_eq!(segment_count(&storage, "mono"), 1);
        let shared = storage.shared("mono").unwrap();
        assert_eq!(shared.segments()[0].stats().records, 0);
        assert_eq!(shared.segments()[0].base_offset, 20);
    }
    for round in 0..3u64 {
        let storage = small_segments(dir.path(), 1);
        let (earliest, next) = storage.stream_bounds(&s).await.unwrap();
        assert_eq!(next.0, 20 + round, "next offset must survive restart");
        assert!(earliest <= next);
        let (o, _) = storage.append(&s, &plain(next.0)).await.unwrap();
        assert_eq!(o, next);
        storage.trim_up_to(&s, Offset(u64::MAX)).await.unwrap();
    }
}

#[tokio::test]
async fn truncate_from_inside_sealed_segment() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 128);
    let s = stream("t1");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 40).await;
    assert!(segment_count(&storage, "t1") > 4);
    storage.truncate_from(&s, Offset(7)).await.unwrap();
    assert_eq!(storage.stream_bounds(&s).await.unwrap().1, Offset(7));
    assert_eq!(offsets(&storage, &s, 0).await, (0..7).collect::<Vec<_>>());
    let (o, _) = storage.append(&s, &plain(7)).await.unwrap();
    assert_eq!(o, Offset(7));
    drop(storage);
    let storage = small_segments(dir.path(), 128);
    assert_eq!(offsets(&storage, &s, 0).await, (0..8).collect::<Vec<_>>());
    check_values(&read_all(&storage, &s, 0).await);
}

#[tokio::test]
async fn truncate_from_inside_active_segment_and_reopen() {
    let dir = TempDir::new().unwrap();
    let s = stream("t2");
    {
        let storage = FileStorage::new(dir.path()).unwrap();
        storage.create_stream(&s, 0, 0).await.unwrap();
        append_n(&storage, &s, 0, 10).await;
        storage.truncate_from(&s, Offset(6)).await.unwrap();
        // At or past next is a no-op.
        storage.truncate_from(&s, Offset(6)).await.unwrap();
        storage.truncate_from(&s, Offset(100)).await.unwrap();
    }
    let storage = FileStorage::open(dir.path()).unwrap();
    assert_eq!(
        storage.stream_bounds(&s).await.unwrap(),
        (Offset(0), Offset(6))
    );
    assert_eq!(offsets(&storage, &s, 0).await, (0..6).collect::<Vec<_>>());
    let (o, _) = storage.append(&s, &plain(6)).await.unwrap();
    assert_eq!(o, Offset(6));
}

#[tokio::test]
async fn truncate_from_zero_wipes_all_segments() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 128);
    let s = stream("t3");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 20).await;
    storage.truncate_from(&s, Offset(0)).await.unwrap();
    assert_eq!(
        storage.stream_bounds(&s).await.unwrap(),
        (Offset(0), Offset(0))
    );
    assert!(read_all(&storage, &s, 0).await.is_empty());
    assert_eq!(segment_count(&storage, "t3"), 1);
    let (o, _) = storage.append(&s, &plain(0)).await.unwrap();
    assert_eq!(o, Offset(0));
}

/// A crash in the middle of `truncate_from` (marker written, files half
/// processed) is finished by recovery.
#[tokio::test]
async fn truncate_from_is_crash_safe() {
    let dir = TempDir::new().unwrap();
    let s = stream("tcrash");
    {
        let storage = small_segments(dir.path(), 128);
        storage.create_stream(&s, 0, 0).await.unwrap();
        append_n(&storage, &s, 0, 40).await;
    }
    let part = dir.path().join("streams/tcrash/partitions/0");
    // Simulate a crash right after the intent marker was written and the
    // newest segment was deleted.
    crate::file::partition::write_truncate_marker(&part, 9).unwrap();
    let mut bases = crate::file::partition::list_segment_bases(&part).unwrap();
    let newest = bases.pop().unwrap();
    std::fs::remove_file(part.join(format!("{newest:020}.seg"))).unwrap();

    let storage = small_segments(dir.path(), 128);
    assert!(!part.join("truncate.json").exists());
    assert_eq!(storage.stream_bounds(&s).await.unwrap().1, Offset(9));
    assert_eq!(offsets(&storage, &s, 0).await, (0..9).collect::<Vec<_>>());
    let (o, _) = storage.append(&s, &plain(9)).await.unwrap();
    assert_eq!(o, Offset(9));
}

/// Crash points inside `apply_truncation`'s rewrite of the segment that
/// holds `drop_from`, reproduced on disk: (1) right after the cut (the file
/// is shortened, its `.meta`/`.idx` still describe the old length and the
/// new active segment doesn't exist yet), (2) right after the sidecars are
/// rewritten (`.meta` removed or rewritten, fresh `.idx`). With the intent
/// marker present, recovery finishes the job either way.
#[tokio::test]
async fn truncate_from_crash_after_cut_and_after_meta_write() {
    for after_meta in [false, true] {
        let dir = TempDir::new().unwrap();
        let name = if after_meta { "tmeta" } else { "tcut" };
        let s = stream(name);
        {
            let storage = small_segments(dir.path(), 128);
            storage.create_stream(&s, 0, 0).await.unwrap();
            append_n(&storage, &s, 0, 40).await;
        }
        let part = dir.path().join(format!("streams/{name}/partitions/0"));
        let bases = crate::file::partition::list_segment_bases(&part).unwrap();
        // An offset strictly inside a sealed segment (not a segment base).
        let drop_from = (1..bases[bases.len() - 1])
            .rev()
            .find(|o| !bases.contains(o))
            .unwrap();
        let base = *bases.iter().rev().find(|&&b| b < drop_from).unwrap();

        // What a complete truncation produces, on a copy.
        let reference = TempDir::new().unwrap();
        for e in std::fs::read_dir(&part).unwrap() {
            let e = e.unwrap();
            std::fs::copy(e.path(), reference.path().join(e.file_name())).unwrap();
        }
        crate::file::partition::apply_truncation(reference.path(), drop_from).unwrap();
        let file = |d: &std::path::Path, ext: &str| d.join(format!("{base:020}.{ext}"));
        let cut_len = std::fs::metadata(file(reference.path(), "seg"))
            .unwrap()
            .len();

        // The crashed state.
        crate::file::partition::write_truncate_marker(&part, drop_from).unwrap();
        for &b in bases.iter().filter(|&&b| b >= drop_from) {
            for ext in ["seg", "idx", "meta"] {
                let _ = std::fs::remove_file(part.join(format!("{b:020}.{ext}")));
            }
        }
        std::fs::OpenOptions::new()
            .write(true)
            .open(file(&part, "seg"))
            .unwrap()
            .set_len(cut_len)
            .unwrap();
        if after_meta {
            // The sidecars as the finished truncation leaves them (with
            // contiguous offsets the cut segment ends at `drop_from` and
            // becomes the active one: no `.meta`, fresh `.idx`).
            for ext in ["meta", "idx"] {
                let want = file(reference.path(), ext);
                if want.exists() {
                    std::fs::copy(want, file(&part, ext)).unwrap();
                } else {
                    let _ = std::fs::remove_file(file(&part, ext));
                }
            }
        }

        let storage = small_segments(dir.path(), 128);
        assert!(!part.join("truncate.json").exists(), "{name}");
        assert_eq!(
            storage.stream_bounds(&s).await.unwrap().1,
            Offset(drop_from),
            "{name}"
        );
        assert_eq!(
            offsets(&storage, &s, 0).await,
            (0..drop_from).collect::<Vec<_>>(),
            "{name}"
        );
        let (o, _) = storage.append(&s, &plain(drop_from)).await.unwrap();
        assert_eq!(o, Offset(drop_from), "{name}");
        drop(storage);
        // And it survives another restart.
        let storage = small_segments(dir.path(), 128);
        assert_eq!(
            storage.stream_bounds(&s).await.unwrap().1,
            Offset(drop_from + 1),
            "{name}"
        );
    }
}

#[tokio::test]
async fn delete_and_recreate_stream() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 128);
    let s = stream("del");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 20).await;
    storage.delete_stream(&s).await.unwrap();
    assert!(!dir.path().join("streams/del").exists());
    assert!(matches!(
        storage.append(&s, &plain(0)).await,
        Err(StorageError::StreamNotFound(_))
    ));
    storage.create_stream(&s, 0, 0).await.unwrap();
    assert_eq!(
        storage.stream_bounds(&s).await.unwrap(),
        (Offset(0), Offset(0))
    );
    drop(storage);
    let storage = small_segments(dir.path(), 128);
    assert_eq!(
        storage.stream_bounds(&s).await.unwrap(),
        (Offset(0), Offset(0))
    );
}

#[tokio::test]
async fn corrupt_stream_json_only_affects_that_stream() {
    let dir = TempDir::new().unwrap();
    {
        let storage = small_segments(dir.path(), 128);
        for name in ["good", "bad"] {
            let cfg = StreamConfig {
                max_age_secs: 1,
                dedup_window_secs: 1,
                ..StreamConfig::default()
            };
            storage
                .create_stream_with(&stream(name), &cfg)
                .await
                .unwrap();
            let old = now_ns() - 3_600_000_000_000;
            for i in 0..20 {
                storage
                    .append(&stream(name), &plain_at(i, old))
                    .await
                    .unwrap();
            }
        }
    }
    std::fs::write(dir.path().join("streams/bad/stream.json"), b"{not json").unwrap();
    let storage = small_segments(dir.path(), 128);
    storage.enforce_all_retention().unwrap();
    assert_eq!(
        segment_count(&storage, "good"),
        1,
        "good stream was trimmed"
    );
    assert!(
        segment_count(&storage, "bad") > 1,
        "bad stream is left alone"
    );
    assert!(storage.stream_config(&stream("bad")).await.is_err());
    // The broken stream is still readable and writable.
    assert_eq!(
        offsets(&storage, &stream("bad"), 0).await,
        (0..20).collect::<Vec<_>>()
    );
    storage.append(&stream("bad"), &plain(20)).await.unwrap();
}

#[tokio::test]
async fn stream_config_roundtrip_with_compaction_fields() {
    let dir = TempDir::new().unwrap();
    let s = stream("cfg");
    let cfg = StreamConfig {
        compaction: true,
        tombstone_retention_secs: 42,
        ..StreamConfig::default()
    };
    {
        let storage = FileStorage::new(dir.path()).unwrap();
        storage.create_stream_with(&s, &cfg).await.unwrap();
        assert_eq!(storage.stream_config(&s).await.unwrap(), cfg);
    }
    let storage = FileStorage::open(dir.path()).unwrap();
    assert_eq!(storage.stream_config(&s).await.unwrap(), cfg);
}
