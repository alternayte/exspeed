//! Simple throughput benchmark for the file storage engine.
//!
//! Run with:
//! ```text
//! cargo test -p exspeed-storage --lib -- --ignored --nocapture storage_throughput
//! ```
//!
//! Only the public `StorageEngine` API is used so the same benchmark can be
//! run against any engine revision.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{Record, StorageEngine};
use tempfile::TempDir;

use crate::file::FileStorage;

const VALUE_SIZE: usize = 256;
const BATCH_RECORDS: u64 = 200_000;
const BATCH_SIZE: usize = 100;
const SINGLE_TASKS: u64 = 64;
const SINGLE_PER_TASK: u64 = 500;

fn rec(i: u64) -> Record {
    let mut v = vec![b'x'; VALUE_SIZE];
    v[..8].copy_from_slice(&i.to_le_bytes());
    Record {
        key: Some(Bytes::from(format!("key-{}", i % 1000))),
        value: Bytes::from(v),
        subject: "bench.orders.created".into(),
        headers: vec![("content-type".into(), "application/json".into())],
        timestamp_ns: None,
    }
}

fn rate(n: u64, secs: f64) -> String {
    let mb = n as f64 * VALUE_SIZE as f64 / (1024.0 * 1024.0);
    format!(
        "{:>10.0} rec/s  {:>7.1} MiB/s  ({n} records in {secs:.2}s)",
        n as f64 / secs,
        mb / secs
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn storage_throughput() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(FileStorage::open(dir.path()).unwrap());

    // 1. append_batch (sync mode, one fsync per batch).
    let s: StreamName = "batch".try_into().unwrap();
    storage.create_stream(&s, 0, 0).await.unwrap();
    let t = Instant::now();
    let mut i = 0u64;
    while i < BATCH_RECORDS {
        let batch: Vec<Record> = (i..i + BATCH_SIZE as u64).map(rec).collect();
        storage.append_batch(&s, batch).await.unwrap();
        i += BATCH_SIZE as u64;
    }
    println!(
        "append_batch x{BATCH_SIZE} (sync):      {}",
        rate(BATCH_RECORDS, t.elapsed().as_secs_f64())
    );

    // 2. concurrent single-record appends (group commit, sync mode).
    let s2: StreamName = "single".try_into().unwrap();
    storage.create_stream(&s2, 0, 0).await.unwrap();
    let t = Instant::now();
    let mut handles = Vec::new();
    for task in 0..SINGLE_TASKS {
        let st = storage.clone();
        let s2 = s2.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..SINGLE_PER_TASK {
                st.append(&s2, &rec(task * SINGLE_PER_TASK + j))
                    .await
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    println!(
        "append x{SINGLE_TASKS} tasks (sync):        {}",
        rate(SINGLE_TASKS * SINGLE_PER_TASK, t.elapsed().as_secs_f64())
    );

    // 3. sequential read of the batch stream, 1000 records per call.
    let t = Instant::now();
    let mut from = 0u64;
    let mut n = 0u64;
    loop {
        let recs = storage.read(&s, Offset(from), 1000).await.unwrap();
        if recs.is_empty() {
            break;
        }
        n += recs.len() as u64;
        from = recs.last().unwrap().offset.0 + 1;
    }
    assert_eq!(n, BATCH_RECORDS);
    println!(
        "sequential read x1000:           {}",
        rate(n, t.elapsed().as_secs_f64())
    );

    // 4. random point reads (1 record each).
    let t = Instant::now();
    let points = 500u64;
    let mut x = 12345u64;
    for _ in 0..points {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let off = (x >> 33) % BATCH_RECORDS;
        let recs = storage.read(&s, Offset(off), 1).await.unwrap();
        assert_eq!(recs[0].offset.0, off);
    }
    println!(
        "random point read:               {}",
        rate(points, t.elapsed().as_secs_f64())
    );
}
