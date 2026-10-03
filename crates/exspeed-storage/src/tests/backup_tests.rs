//! Online backup (`snapshot_stream`, `write_backup`) and offline restore.

use std::io::Cursor;
use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use exspeed_common::Offset;
use exspeed_streams::{Record, StorageEngine, StoredRecord, StreamConfig};
use tempfile::TempDir;

use super::util::*;
use crate::file::backup::{
    read_manifest, restore_backup, write_backup, BackupManifest, RestoreOptions, BACKUP_VERSION,
    MANIFEST_NAME,
};
use crate::file::FileStorage;

fn rich(i: u64) -> Record {
    Record {
        key: Some(Bytes::from(format!("key-{}", i % 7))),
        value: Bytes::from(format!(
            "{{\"i\":{i},\"pad\":\"{}\"}}",
            "x".repeat((i % 50) as usize)
        )),
        subject: format!("orders.{}", i % 3),
        headers: vec![
            ("trace".into(), format!("t-{i}")),
            ("x-idempotency-key".into(), format!("m-{i}")),
        ],
        timestamp_ns: Some(1_700_000_000_000_000_000 + i * 1_000),
    }
}

fn same(a: &StoredRecord, b: &StoredRecord) {
    assert_eq!(a.offset, b.offset);
    assert_eq!(a.timestamp, b.timestamp, "timestamp of {}", a.offset.0);
    assert_eq!(a.subject, b.subject);
    assert_eq!(a.key, b.key);
    assert_eq!(a.value, b.value);
    assert_eq!(a.headers, b.headers);
}

fn backup(storage: &FileStorage) -> (Vec<u8>, BackupManifest) {
    let mut buf = Vec::new();
    let m = write_backup(storage, "test", &mut buf).unwrap();
    (buf, m)
}

fn restore(archive: &[u8], dir: &Path) -> BackupManifest {
    let m = restore_backup(Cursor::new(archive), dir, RestoreOptions::default()).unwrap();
    for s in &m.streams {
        check_sidecars(&dir.join("streams").join(&s.name).join("partitions/0"));
    }
    m
}

/// Every segment but the last has a `.meta` and `.idx` that match it, so
/// startup opens it without a rebuild scan; the last one is header-only.
fn check_sidecars(dir: &Path) {
    use crate::file::segment::{idx_path, load_meta, meta_path, INDEX_ENTRY_LEN};
    let bases = crate::file::partition::list_segment_bases(dir).unwrap();
    let (last, sealed) = bases.split_last().unwrap();
    for &b in sealed {
        let meta = load_meta(&meta_path(dir, b)).expect("meta");
        let seg_len = std::fs::metadata(crate::file::segment::seg_path(dir, b))
            .unwrap()
            .len();
        assert_eq!(meta.len, seg_len, "segment {b}");
        let idx_len = std::fs::metadata(idx_path(dir, b)).unwrap().len();
        assert_eq!(idx_len, meta.index_entries * INDEX_ENTRY_LEN as u64);
        assert!(meta.records > 0);
    }
    let last_len = std::fs::metadata(crate::file::segment::seg_path(dir, *last))
        .unwrap()
        .len();
    assert_eq!(last_len, crate::file::segment::SEGMENT_HEADER_LEN);
}

#[tokio::test]
async fn backup_restore_roundtrip_many_segments() {
    let src = TempDir::new().unwrap();
    let storage = small_segments(src.path(), 2048);
    let s = stream("orders");
    storage.create_stream(&s, 0, 0).await.unwrap();
    for i in 0..500 {
        storage.append(&s, &rich(i)).await.unwrap();
    }
    let empty = stream("empty");
    storage.create_stream(&empty, 0, 0).await.unwrap();
    assert!(segment_count(&storage, "orders") > 5);
    let original = read_all(&storage, &s, 0).await;

    let (archive, manifest) = backup(&storage);
    assert_eq!(manifest.version, BACKUP_VERSION);
    let names: Vec<_> = manifest.streams.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["empty", "orders"]);
    let om = &manifest.streams[1];
    assert_eq!(
        (om.earliest_offset, om.next_offset, om.records),
        (0, 500, 500)
    );
    assert_eq!(read_manifest(Cursor::new(&archive)).unwrap(), manifest);

    let dst = TempDir::new().unwrap();
    let restored_manifest = restore(&archive, dst.path());
    assert_eq!(restored_manifest, manifest);

    let restored = small_segments(dst.path(), 2048);
    let got = read_all(&restored, &s, 0).await;
    assert_eq!(got.len(), original.len());
    for (a, b) in got.iter().zip(&original) {
        same(a, b);
    }
    assert_eq!(
        restored.stream_bounds(&empty).await.unwrap(),
        (Offset(0), Offset(0))
    );
    // Time index works on the restored copy.
    let ts = rich(321).timestamp_ns.unwrap();
    assert_eq!(restored.seek_by_time(&s, ts).await.unwrap(), Offset(321));
    // New appends continue at the next offset.
    let (o, _) = restored.append(&s, &rich(500)).await.unwrap();
    assert_eq!(o, Offset(500));
    let (o, _) = restored.append(&empty, &plain(0)).await.unwrap();
    assert_eq!(o, Offset(0));
}

#[tokio::test]
async fn snapshot_is_point_in_time_while_writes_continue() {
    let src = TempDir::new().unwrap();
    let storage = small_segments(src.path(), 4096);
    let s = stream("live");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 300).await;

    let snap = storage.snapshot_stream("live").unwrap();
    assert_eq!(snap.next_offset, 300);
    // Writes after the snapshot (including segment rolls) are not included.
    append_n(&storage, &s, 300, 300).await;

    let mut buf = Vec::new();
    {
        let mut tar = tar::Builder::new(&mut buf);
        for f in &snap.files {
            let mut h = tar::Header::new_gnu();
            h.set_size(f.len());
            h.set_mode(0o644);
            tar.append_data(&mut h, format!("streams/live/{}", f.path), f.reader())
                .unwrap();
        }
        tar.finish().unwrap();
    }
    snap.verify().unwrap();
    let dst = TempDir::new().unwrap();
    std::fs::create_dir_all(dst.path().join("streams")).unwrap();
    tar::Archive::new(Cursor::new(buf))
        .unpack(dst.path())
        .unwrap();
    let restored = small_segments(dst.path(), 4096);
    let got = read_all(&restored, &s, 0).await;
    assert_eq!(got.len(), 300);
    check_values(&got);
    assert_eq!(
        restored.append(&s, &plain(300)).await.unwrap().0,
        Offset(300)
    );
}

#[tokio::test]
async fn backup_under_concurrent_appends_is_a_consistent_prefix() {
    let src = TempDir::new().unwrap();
    let storage = small_segments(src.path(), 8192);
    let s = stream("busy");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 100).await;

    let writer = {
        let storage = storage.clone();
        let s = s.clone();
        tokio::spawn(async move {
            for i in 100..3000u64 {
                let (o, _) = storage.append(&s, &plain(i)).await.unwrap();
                assert_eq!(o, Offset(i));
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    let st = storage.clone();
    let (archive, manifest) = tokio::task::spawn_blocking(move || backup(&st))
        .await
        .unwrap();
    writer.await.unwrap();
    let next = manifest.streams[0].next_offset;
    assert!(next >= 100);

    let dst = TempDir::new().unwrap();
    restore(&archive, dst.path());
    let restored = small_segments(dst.path(), 8192);
    let got = read_all(&restored, &s, 0).await;
    assert_eq!(got.len() as u64, next);
    check_values(&got);
    assert_eq!(
        restored.append(&s, &plain(next)).await.unwrap().0,
        Offset(next)
    );
}

#[tokio::test]
async fn retention_after_snapshot_does_not_lose_included_segments() {
    let src = TempDir::new().unwrap();
    let storage = small_segments(src.path(), 1024);
    let s = stream("ret");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 200).await;
    let snap = storage.snapshot_stream("ret").unwrap();
    assert_eq!(snap.earliest_offset, 0);

    // Drop every sealed segment from disk.
    storage.trim_up_to(&s, Offset(199)).await.unwrap();
    assert!(storage.stream_bounds(&s).await.unwrap().0 .0 > 0);

    let mut total = 0;
    for f in &snap.files {
        let mut v = Vec::new();
        std::io::copy(&mut f.reader(), &mut v).unwrap();
        assert_eq!(v.len() as u64, f.len(), "{}", f.path);
        total += v.len();
    }
    assert!(total > 0);
    snap.verify().unwrap();
}

#[tokio::test]
async fn backup_of_compacted_stream() {
    let src = TempDir::new().unwrap();
    let storage = small_segments(src.path(), 2048);
    let s = stream("kv");
    let cfg = StreamConfig {
        compaction: true,
        ..StreamConfig::default()
    };
    storage.create_stream_with(&s, &cfg).await.unwrap();
    for r in 0..40u64 {
        for k in 0..5u64 {
            storage
                .append(&s, &keyed(&format!("k{k}"), &format!("v{r}")))
                .await
                .unwrap();
        }
    }
    let st = storage.clone();
    tokio::task::spawn_blocking(move || st.compact_stream("kv").unwrap())
        .await
        .unwrap();
    let original = read_all(&storage, &s, 0).await;
    assert!(original.len() < 200, "compaction removed records");

    let (archive, manifest) = backup(&storage);
    assert_eq!(manifest.streams[0].records, original.len() as u64);
    assert_eq!(manifest.streams[0].next_offset, 200);
    let dst = TempDir::new().unwrap();
    restore(&archive, dst.path());
    let restored = small_segments(dst.path(), 2048);
    let got = read_all(&restored, &s, 0).await;
    assert_eq!(got.len(), original.len());
    for (a, b) in got.iter().zip(&original) {
        same(a, b);
    }
    assert!(
        restored.stream_config(&s).await.unwrap().compaction,
        "stream.json is restored"
    );
}

#[tokio::test]
async fn replication_floor_cuts_inside_a_sealed_segment() {
    let src = TempDir::new().unwrap();
    let storage = small_segments(src.path(), 1024);
    let s = stream("floor");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 50).await;
    // Hold the visible high watermark at 37 while more is written.
    storage.set_replication_floor("floor", Some(37));
    for i in 50..120 {
        storage.append(&s, &plain(i)).await.unwrap();
    }
    assert_eq!(storage.stream_head_offset("floor"), Some(37));

    let (archive, manifest) = backup(&storage);
    assert_eq!(manifest.streams[0].next_offset, 37);
    let dst = TempDir::new().unwrap();
    restore(&archive, dst.path());
    let restored = small_segments(dst.path(), 1024);
    let got = read_all(&restored, &s, 0).await;
    assert_eq!(got.len(), 37);
    check_values(&got);
    assert_eq!(restored.append(&s, &plain(37)).await.unwrap().0, Offset(37));
}

#[tokio::test]
async fn truncation_during_backup_is_detected() {
    let src = TempDir::new().unwrap();
    let storage = small_segments(src.path(), 1024);
    let s = stream("trunc");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 100).await;
    let snap = storage.snapshot_stream("trunc").unwrap();
    storage.truncate_from(&s, Offset(10)).await.unwrap();
    assert!(snap.verify().is_err());
}

#[tokio::test]
async fn config_dirs_are_included_and_credentials_are_not() {
    let src = TempDir::new().unwrap();
    let storage = small_segments(src.path(), 1 << 20);
    let s = stream("a");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 3).await;
    let d = src.path();
    std::fs::create_dir_all(d.join("connectors.d")).unwrap();
    std::fs::write(d.join("connectors.d/pg.toml"), "name = 'pg'\n").unwrap();
    std::fs::create_dir_all(d.join("exql/queries")).unwrap();
    std::fs::write(d.join("exql/queries/q1.json"), "{}").unwrap();
    std::fs::write(d.join("exql/queries/q2.json.tmp"), "partial").unwrap();
    std::fs::write(d.join("credentials.toml"), "secret").unwrap();
    std::fs::create_dir_all(d.join("replication")).unwrap();
    std::fs::write(d.join("replication/cursor.json"), "{}").unwrap();

    let (archive, manifest) = backup(&storage);
    assert_eq!(manifest.config_dirs, ["connectors.d", "exql"]);
    let names: Vec<String> = tar::Archive::new(Cursor::new(&archive))
        .entries()
        .unwrap()
        .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names[0], MANIFEST_NAME);
    assert!(names.contains(&"connectors.d/pg.toml".to_string()));
    assert!(names.contains(&"exql/queries/q1.json".to_string()));
    assert!(names.contains(&"streams/a/stream.json".to_string()));
    for n in &names {
        assert!(!n.contains("credentials"), "{n}");
        assert!(!n.contains("replication"), "{n}");
        assert!(!n.ends_with(".tmp"), "{n}");
        assert!(!n.contains("dedup"), "{n}");
    }

    let dst = TempDir::new().unwrap();
    restore(&archive, dst.path());
    assert_eq!(
        std::fs::read_to_string(dst.path().join("connectors.d/pg.toml")).unwrap(),
        "name = 'pg'\n"
    );
    assert!(dst.path().join("exql/queries/q1.json").is_file());
}

#[tokio::test]
async fn restore_refuses_non_empty_dir_unless_forced() {
    let src = TempDir::new().unwrap();
    let storage = small_segments(src.path(), 1 << 20);
    let s = stream("a");
    storage.create_stream(&s, 0, 0).await.unwrap();
    append_n(&storage, &s, 0, 5).await;
    let (archive, _) = backup(&storage);

    let dst = TempDir::new().unwrap();
    // A stale stream and a credentials file in the target.
    std::fs::create_dir_all(dst.path().join("streams/stale/partitions/0")).unwrap();
    std::fs::write(dst.path().join("credentials.toml"), "keep me").unwrap();
    std::fs::write(dst.path().join(".exspeed.lock"), "").unwrap();
    // Cluster state of a former cluster node describes the old log.
    std::fs::create_dir_all(dst.path().join("cluster/epochs")).unwrap();
    std::fs::write(dst.path().join("cluster/epochs/stale.json"), "{}").unwrap();
    std::fs::write(dst.path().join("node_id"), "node-a\n").unwrap();
    let err =
        restore_backup(Cursor::new(&archive), dst.path(), RestoreOptions::default()).unwrap_err();
    assert!(err.to_string().contains("not empty"), "{err}");
    assert!(dst.path().join("streams/stale").is_dir(), "nothing touched");

    restore_backup(
        Cursor::new(&archive),
        dst.path(),
        RestoreOptions { force: true },
    )
    .unwrap();
    assert!(!dst.path().join("streams/stale").exists());
    assert!(
        !dst.path().join("cluster").exists(),
        "stale epoch histories removed"
    );
    assert_eq!(
        std::fs::read_to_string(dst.path().join("node_id")).unwrap(),
        "node-a\n",
        "the node keeps its identity"
    );
    assert_eq!(
        std::fs::read_to_string(dst.path().join("credentials.toml")).unwrap(),
        "keep me"
    );
    let restored = small_segments(dst.path(), 1 << 20);
    assert_eq!(restored.list_streams(), ["a"]);
    assert_eq!(read_all(&restored, &s, 0).await.len(), 5);
}

#[tokio::test]
async fn an_empty_lock_file_alone_is_an_empty_dir() {
    let src = TempDir::new().unwrap();
    let storage = small_segments(src.path(), 1 << 20);
    let (archive, _) = backup(&storage);
    let dst = TempDir::new().unwrap();
    std::fs::write(dst.path().join(".exspeed.lock"), "").unwrap();
    restore(&archive, dst.path());
}

fn tar_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut tar = tar::Builder::new(&mut buf);
        for (path, data) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_size(data.len() as u64);
            h.set_mode(0o644);
            tar.append_data(&mut h, path, *data).unwrap();
        }
        tar.finish().unwrap();
    }
    buf
}

#[test]
fn restore_rejects_bad_archives() {
    let manifest = |version: u32| {
        format!(
            r#"{{"format":"exspeed-backup","version":{version},"server_version":"x","created_at":"now","streams":[]}}"#
        )
    };
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (tar_of(&[("streams/a/stream.json", b"{}")]), "first entry"),
        (
            tar_of(&[(MANIFEST_NAME, manifest(99).as_bytes())]),
            "not supported",
        ),
        (
            tar_of(&[
                (MANIFEST_NAME, manifest(1).as_bytes()),
                ("credentials.toml", b"x"),
            ]),
            "unexpected path",
        ),
        (
            tar_of(&[
                (MANIFEST_NAME, manifest(1).as_bytes()),
                ("streams/ghost/stream.json", b"{}"),
                (
                    "streams/ghost/partitions/0/00000000000000000000.seg",
                    &crate::file::segment::header_bytes(0),
                ),
            ]),
            "do not match the manifest",
        ),
    ];
    for (archive, want) in cases {
        let dst = TempDir::new().unwrap();
        let err = restore_backup(Cursor::new(&archive), dst.path(), RestoreOptions::default())
            .unwrap_err();
        assert!(err.to_string().contains(want), "{err} (wanted {want})");
        // Nothing but the (removed) staging dir was created.
        assert_eq!(std::fs::read_dir(dst.path()).unwrap().count(), 0);
    }
}
