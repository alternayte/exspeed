//! Running out of disk on a real filesystem.
//!
//! Needs a small, otherwise empty filesystem in `EXSPEED_ENOSPC_DIR`
//! (CI mounts a 16 MiB tmpfs: `mount -t tmpfs -o size=16m tmpfs <dir>`).
//! Skipped when the variable is unset.

use std::path::PathBuf;

use bytes::Bytes;
use exspeed_common::Offset;
use exspeed_streams::{Record, StorageEngine, StorageError};

use super::util::{options, stream};
use crate::file::io_errors::is_storage_full;
use crate::file::FileStorage;

fn enospc_dir() -> Option<PathBuf> {
    std::env::var_os("EXSPEED_ENOSPC_DIR").map(PathBuf::from)
}

fn record(i: u64) -> Record {
    let mut value = format!("{i:08}-").into_bytes();
    value.resize(32 * 1024, b'x');
    Record {
        value: Bytes::from(value),
        subject: "fill".into(),
        ..Default::default()
    }
}

fn full(e: &StorageError) -> bool {
    match e {
        StorageError::Io(io) => is_storage_full(io),
        StorageError::PartitionFailed { reason, .. } => {
            reason.contains("No space") || reason.contains("space")
        }
        _ => false,
    }
}

#[tokio::test]
async fn disk_full_rejects_writes_cleanly_and_recovers() {
    let Some(root) = enospc_dir() else {
        eprintln!("skipping: EXSPEED_ENOSPC_DIR is not set");
        return;
    };
    let dir = root.join(format!("storage-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    // Ballast to delete later, freeing room for new writes.
    let ballast = root.join(format!("ballast-{}", std::process::id()));
    std::fs::write(&ballast, vec![0u8; 2 * 1024 * 1024]).unwrap();

    let s = stream("full");
    let storage = FileStorage::open_with_options(&dir, options(1024 * 1024)).unwrap();
    storage.create_stream(&s, 0, 0).await.unwrap();

    // Fill the disk. Every acknowledged record gets the next offset.
    let mut acked = Vec::new();
    let err = loop {
        let i = acked.len() as u64;
        assert!(i < 100_000, "the filesystem never filled up");
        match storage.append(&s, &record(i)).await {
            Ok((o, _)) => {
                assert_eq!(o.0, i, "offsets are dense");
                acked.push(o.0);
            }
            Err(e) => break e,
        }
    };
    assert!(full(&err), "expected a disk-full error, got {err:?}");
    assert!(!acked.is_empty());

    // Further writes keep failing cleanly; nothing acknowledged is lost.
    assert!(storage.append(&s, &record(9_999_999)).await.is_err());
    let (_, next) = storage.stream_bounds(&s).await.unwrap();
    assert_eq!(
        next.0,
        acked.len() as u64,
        "no offset was handed out for failed writes"
    );
    let back = storage.read(&s, Offset(0), acked.len() + 10).await.unwrap();
    assert_eq!(back.len(), acked.len());
    for (i, r) in back.iter().enumerate() {
        assert_eq!(r.offset.0, i as u64);
        assert!(r.value.starts_with(format!("{i:08}-").as_bytes()));
    }

    // Free space: writing resumes at the next offset (unless the partition
    // fenced itself, in which case a restart recovers it).
    std::fs::remove_file(&ballast).unwrap();
    let resumed = match storage.append(&s, &record(acked.len() as u64)).await {
        Ok((o, _)) => Some(o.0),
        Err(StorageError::PartitionFailed { .. }) => None,
        Err(e) => panic!("append after freeing space failed: {e:?}"),
    };
    if let Some(o) = resumed {
        assert_eq!(o, acked.len() as u64);
        acked.push(o);
    }
    let storage_for_close = std::sync::Arc::new(storage);
    let st = storage_for_close.clone();
    tokio::task::spawn_blocking(move || st.close())
        .await
        .unwrap();
    drop(storage_for_close);

    // Restart: every acknowledged record is intact and appends continue.
    let storage = FileStorage::open_with_options(&dir, options(1024 * 1024)).unwrap();
    let back = storage.read(&s, Offset(0), acked.len() + 10).await.unwrap();
    assert_eq!(back.len(), acked.len());
    let (o, _) = storage
        .append(&s, &record(acked.len() as u64))
        .await
        .unwrap();
    assert_eq!(o.0, acked.len() as u64);
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}
