//! Crash recovery, torn writes, mid-file corruption and fault injection.

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use exspeed_common::Offset;
use exspeed_streams::{StorageEngine, StorageError};
use tempfile::TempDir;

use super::util::*;
use crate::file::partition::{list_segment_bases, PartitionStatus};
use crate::file::writer::{IoHooks, WriteFault};
use crate::file::FileStorage;

fn active_segment(dir: &Path, stream: &str) -> PathBuf {
    let part = dir.join("streams").join(stream).join("partitions/0");
    let base = *list_segment_bases(&part).unwrap().last().unwrap();
    part.join(format!("{base:020}.seg"))
}

/// Offsets readable after reopen; asserts they form `0..n` and returns `n`.
async fn assert_contiguous_prefix(storage: &FileStorage, name: &str) -> u64 {
    let s = stream(name);
    let recs = read_all(storage, &s, 0).await;
    for (i, r) in recs.iter().enumerate() {
        assert_eq!(
            r.offset,
            Offset(i as u64),
            "offsets must be a contiguous prefix"
        );
    }
    check_values(&recs);
    let n = recs.len() as u64;
    let (earliest, next) = storage.stream_bounds(&s).await.unwrap();
    assert_eq!(earliest, Offset(0));
    assert_eq!(
        next,
        Offset(n),
        "next offset = one past the last surviving record"
    );
    n
}

async fn write_mixed(storage: &FileStorage, name: &str, n: u64, rng: &mut Rng) {
    let s = stream(name);
    storage.create_stream(&s, 0, 0).await.unwrap();
    let mut i = 0;
    while i < n {
        let batch = (1 + rng.below(8)).min(n - i);
        if batch == 1 {
            storage.append(&s, &plain(i)).await.unwrap();
        } else {
            let recs = (i..i + batch).map(plain).collect();
            let res = storage.append_batch(&s, recs).await.unwrap();
            assert_eq!(res[0].0, Offset(i));
        }
        i += batch;
    }
}

/// Truncate the active segment at random byte positions (many seeds), then
/// reopen: a contiguous prefix survives, every read works, and the next
/// append gets the right offset.
#[tokio::test]
async fn torn_tail_at_random_positions_recovers_contiguous_prefix() {
    for seed in 1..=60u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let dir = TempDir::new().unwrap();
        let n = 150;
        let sealed_records;
        {
            let storage = small_segments(dir.path(), 2048);
            write_mixed(&storage, "torn", n, &mut rng).await;
            let shared = storage.shared("torn").unwrap();
            sealed_records = shared.segments().last().unwrap().base_offset;
        }
        let seg = active_segment(dir.path(), "torn");
        let len = std::fs::metadata(&seg).unwrap().len();
        let cut = rng.below(len + 1);
        OpenOptions::new()
            .write(true)
            .open(&seg)
            .unwrap()
            .set_len(cut)
            .unwrap();

        let storage = small_segments(dir.path(), 2048);
        let k = assert_contiguous_prefix(&storage, "torn").await;
        assert!(
            k >= sealed_records,
            "sealed data must never be lost (seed {seed})"
        );
        assert!(k <= n);
        let (o, _) = storage.append(&stream("torn"), &plain(k)).await.unwrap();
        assert_eq!(o, Offset(k), "seed {seed}");
        drop(storage);
        // Stable across another restart.
        let storage = small_segments(dir.path(), 2048);
        assert_eq!(assert_contiguous_prefix(&storage, "torn").await, k + 1);
    }
}

/// Garbage after the last good frame (a torn write of random bytes, or a
/// zero-filled tail) is cut off.
#[tokio::test]
async fn garbage_tail_is_truncated() {
    for (seed, zeros) in [(7u64, false), (11, true), (13, false), (17, true)] {
        let mut rng = Rng(seed);
        let dir = TempDir::new().unwrap();
        {
            let storage = small_segments(dir.path(), 1 << 20);
            write_mixed(&storage, "g", 40, &mut rng).await;
        }
        let seg = active_segment(dir.path(), "g");
        let junk: Vec<u8> = (0..1 + rng.below(300))
            .map(|_| if zeros { 0 } else { rng.next() as u8 })
            .collect();
        OpenOptions::new()
            .append(true)
            .open(&seg)
            .unwrap()
            .write_all(&junk)
            .unwrap();
        let storage = small_segments(dir.path(), 1 << 20);
        assert_eq!(assert_contiguous_prefix(&storage, "g").await, 40);
    }
}

fn corrupt_middle_of_active(dir: &Path, name: &str) {
    let seg = active_segment(dir, name);
    let mut bytes = std::fs::read(&seg).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x5a;
    std::fs::write(&seg, &bytes).unwrap();
}

/// Sync mode: every acknowledged record was fsynced, so a bad frame with
/// valid frames after it is real corruption — recovery refuses to drop the
/// data and fails loudly.
#[tokio::test]
async fn sync_mode_fails_loudly_on_mid_file_corruption() {
    let dir = TempDir::new().unwrap();
    {
        let storage = small_segments(dir.path(), 1 << 20);
        write_mixed(&storage, "mid", 100, &mut Rng(3)).await;
    }
    corrupt_middle_of_active(dir.path(), "mid");
    let err = FileStorage::open_with_options(dir.path(), options(1 << 20))
        .err()
        .expect("open must fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("corruption"), "{err}");
    // Nothing was truncated.
    let seg = active_segment(dir.path(), "mid");
    assert!(std::fs::metadata(&seg).unwrap().len() > 2000);
}

/// Async mode: pages may reach disk out of order before a crash, so the log
/// is cut at the first bad frame.
#[tokio::test]
async fn async_mode_truncates_at_mid_file_corruption() {
    let dir = TempDir::new().unwrap();
    {
        let storage = small_segments_async(dir.path(), 1 << 20);
        write_mixed(&storage, "mid", 100, &mut Rng(5)).await;
    }
    corrupt_middle_of_active(dir.path(), "mid");
    let storage = small_segments_async(dir.path(), 1 << 20);
    let k = assert_contiguous_prefix(&storage, "mid").await;
    assert!(k > 0 && k < 100, "k = {k}");
}

#[derive(Default)]
struct Faults {
    fail_write: AtomicBool,
    partial_write: AtomicUsize,
    fail_sync: AtomicBool,
    fail_truncate: AtomicBool,
    sync_calls: AtomicUsize,
}

impl IoHooks for Faults {
    fn on_write(&self, len: usize) -> Option<WriteFault> {
        if self.fail_write.swap(false, Ordering::SeqCst) {
            return Some(WriteFault::Fail(io::Error::from_raw_os_error(28)));
        }
        let p = self.partial_write.swap(0, Ordering::SeqCst);
        if p > 0 {
            return Some(WriteFault::Partial(
                p.min(len - 1),
                io::Error::other("injected short write"),
            ));
        }
        None
    }
    fn on_sync(&self) -> io::Result<()> {
        self.sync_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_sync.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("injected fsync failure"));
        }
        Ok(())
    }
    fn on_truncate(&self) -> io::Result<()> {
        if self.fail_truncate.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected truncate failure"));
        }
        Ok(())
    }
}

async fn setup_faults(dir: &Path) -> (FileStorage, Arc<Faults>) {
    let storage = small_segments(dir, 4096);
    let s = stream("f");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 10).await;
    let faults = Arc::new(Faults::default());
    assert!(storage.set_io_hooks("f", Some(faults.clone())));
    (storage, faults)
}

fn file_len_matches_committed(storage: &FileStorage, dir: &Path) {
    let shared = storage.shared("f").unwrap();
    let active = shared.segments().last().unwrap().clone();
    let on_disk = std::fs::metadata(active_segment(dir, "f")).unwrap().len();
    assert_eq!(
        on_disk,
        active.len(),
        "no bytes beyond the committed length"
    );
}

/// A failed write (nothing or part of the batch reached the file) or a
/// failed fsync rolls the file back to the last committed length. The
/// batch was never visible, so its offsets are reassigned; no offset ever
/// appears twice in the log.
#[tokio::test]
async fn write_and_fsync_failures_roll_back_and_never_duplicate_offsets() {
    let dir = TempDir::new().unwrap();
    let (storage, faults) = setup_faults(dir.path()).await;
    let s = stream("f");
    let mut next = 10u64;
    for case in 0..3 {
        match case {
            0 => faults.fail_write.store(true, Ordering::SeqCst),
            1 => faults.partial_write.store(13, Ordering::SeqCst),
            _ => faults.fail_sync.store(true, Ordering::SeqCst),
        }
        let batch = (next..next + 5).map(plain).collect();
        let err = storage.append_batch(&s, batch).await.unwrap_err();
        assert!(matches!(err, StorageError::Io(_)), "case {case}: {err:?}");
        assert_eq!(
            storage.partition_status("f"),
            Some(PartitionStatus::Healthy)
        );
        assert_eq!(storage.stream_bounds(&s).await.unwrap().1, Offset(next));
        file_len_matches_committed(&storage, dir.path());
        // Never visible.
        assert_eq!(read_all(&storage, &s, 0).await.len() as u64, next);
        // The next append gets the same offset: nothing at it exists.
        let (o, _) = storage.append(&s, &plain(next)).await.unwrap();
        assert_eq!(o, Offset(next));
        next += 1;
    }
    let recs = read_all(&storage, &s, 0).await;
    assert_eq!(
        recs.iter().map(|r| r.offset.0).collect::<Vec<_>>(),
        (0..next).collect::<Vec<_>>()
    );
    drop(storage);
    let storage = small_segments(dir.path(), 4096);
    assert_eq!(assert_contiguous_prefix(&storage, "f").await, next);
}

/// When the rollback itself fails, bytes of unknown state may sit past the
/// committed length: the partition is fenced read-only, reports Failed,
/// rejects every later write (so no offset can be reused) and still serves
/// reads.
#[tokio::test]
async fn failed_rollback_fences_the_partition() {
    let dir = TempDir::new().unwrap();
    let (storage, faults) = setup_faults(dir.path()).await;
    let s = stream("f");
    faults.fail_truncate.store(true, Ordering::SeqCst);
    faults.partial_write.store(20, Ordering::SeqCst);
    let err = storage.append(&s, &plain(10)).await.unwrap_err();
    assert!(
        matches!(err, StorageError::PartitionFailed { .. }),
        "{err:?}"
    );
    match storage.partition_status("f").unwrap() {
        PartitionStatus::Failed { reason } => assert!(reason.contains("truncat"), "{reason}"),
        other => panic!("expected Failed, got {other:?}"),
    }
    assert_eq!(storage.failed_streams().len(), 1);
    for _ in 0..3 {
        assert!(matches!(
            storage.append(&s, &plain(10)).await,
            Err(StorageError::PartitionFailed { .. })
        ));
    }
    assert!(matches!(
        storage.truncate_from(&s, Offset(5)).await,
        Err(StorageError::PartitionFailed { .. })
    ));
    // Reads still work and see only committed data.
    assert_eq!(read_all(&storage, &s, 0).await.len(), 10);
    assert_eq!(storage.stream_bounds(&s).await.unwrap().1, Offset(10));
    drop(storage);
    // Recovery removes the torn bytes; the log is healthy again.
    let storage = small_segments(dir.path(), 4096);
    assert_eq!(assert_contiguous_prefix(&storage, "f").await, 10);
    assert_eq!(
        storage.partition_status("f"),
        Some(PartitionStatus::Healthy)
    );
    let (o, _) = storage.append(&s, &plain(10)).await.unwrap();
    assert_eq!(o, Offset(10));
}

/// In async mode a failed background fsync can't be rolled back (the data
/// is already acknowledged), so the partition is fenced.
#[tokio::test]
async fn async_background_fsync_failure_fences() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments_async(dir.path(), 1 << 20);
    let s = stream("f");
    storage.create_stream(&s, 0, 0).await.unwrap();
    let faults = Arc::new(Faults::default());
    faults.fail_sync.store(true, Ordering::SeqCst);
    storage.set_io_hooks("f", Some(faults.clone()));
    storage.append(&s, &plain(0)).await.unwrap();
    for _ in 0..200 {
        if storage.partition_status("f") != Some(PartitionStatus::Healthy) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(matches!(
        storage.partition_status("f"),
        Some(PartitionStatus::Failed { .. })
    ));
    assert!(matches!(
        storage.append(&s, &plain(1)).await,
        Err(StorageError::PartitionFailed { .. })
    ));
}

/// Group commit: concurrent single appends share fsyncs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_share_fsyncs() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 1 << 20);
    let s = stream("gc");
    storage.create_stream(&s, 0, 0).await.unwrap();
    let faults = Arc::new(Faults::default());
    storage.set_io_hooks("gc", Some(faults.clone()));
    let mut tasks = Vec::new();
    for t in 0..50u64 {
        let st = storage.clone();
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            st.append(&s, &plain(t)).await.unwrap().0 .0
        }));
    }
    let mut offs: Vec<u64> = Vec::new();
    for t in tasks {
        offs.push(t.await.unwrap());
    }
    offs.sort_unstable();
    assert_eq!(offs, (0..50).collect::<Vec<_>>());
    let syncs = faults.sync_calls.load(Ordering::SeqCst);
    assert!(
        syncs < 50,
        "expected group commit, got {syncs} fsyncs for 50 appends"
    );
}

/// Encoders refuse records the format can't represent instead of
/// truncating lengths; the rest of the group still commits.
#[tokio::test]
async fn unencodable_record_is_rejected_without_affecting_others() {
    let dir = TempDir::new().unwrap();
    let storage = small_segments(dir.path(), 1 << 20);
    let s = stream("enc");
    storage.create_stream(&s, 0, 0).await.unwrap();
    let mut bad = plain(0);
    bad.subject = "x".repeat(70_000);
    let err = storage.append(&s, &bad).await.unwrap_err();
    assert!(matches!(err, StorageError::InvalidRecord(_)), "{err:?}");
    let (o, _) = storage.append(&s, &plain(0)).await.unwrap();
    assert_eq!(o, Offset(0));
    assert_eq!(read_all(&storage, &s, 0).await.len(), 1);
}
