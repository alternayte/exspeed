use std::sync::Arc;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use exspeed_common::StreamName;
use exspeed_processing::runtime::bounded::execute_bounded;
use exspeed_storage::memory::MemoryStorage;
use exspeed_streams::{Record, StorageEngine};

const N_RECORDS: usize = 500_000;

fn build_storage(rt: &tokio::runtime::Runtime) -> Arc<dyn StorageEngine> {
    rt.block_on(async {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let stream = StreamName::try_from("bench").unwrap();
        storage.create_stream(&stream, 0, 0).await.unwrap();
        for i in 0..N_RECORDS {
            let payload = format!(
                r#"{{"i":{i},"status":"{s}","customer_id":"c{cid}","type":"{t}"}}"#,
                s = if i % 3 == 0 { "active" } else { "inactive" },
                cid = i % 100,
                t = if i % 2 == 0 { "A" } else { "B" },
            );
            let rec = Record {
                key: Some(Bytes::from(format!("k{i}").into_bytes())),
                subject: "bench.s".into(),
                value: Bytes::from(payload.into_bytes()),
                headers: vec![],
                timestamp_ns: None,
            };
            storage.append(&stream, &rec).await.unwrap();
        }
        storage
    })
}

fn bench_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let storage = build_storage(&rt);

    let mut group = c.benchmark_group("bounded_scan");
    group.sample_size(10);

    group.bench_function("select_star_limit_5", |b| {
        b.iter(|| {
            rt.block_on(async {
                execute_bounded("SELECT * FROM bench LIMIT 5", &storage)
                    .await
                    .unwrap();
            });
        });
    });

    group.bench_function("select_offset_limit_1000", |b| {
        b.iter(|| {
            rt.block_on(async {
                execute_bounded("SELECT offset FROM bench LIMIT 1000", &storage)
                    .await
                    .unwrap();
            });
        });
    });

    group.bench_function("filtered_limit_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                execute_bounded(
                    "SELECT payload->>'status' FROM bench WHERE payload->>'type' = 'A' LIMIT 100",
                    &storage,
                )
                .await
                .unwrap();
            });
        });
    });

    group.bench_function("full_scan", |b| {
        b.iter(|| {
            rt.block_on(async {
                execute_bounded("SELECT * FROM bench", &storage)
                    .await
                    .unwrap();
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
