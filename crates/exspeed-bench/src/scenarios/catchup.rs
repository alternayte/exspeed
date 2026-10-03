//! Catch-up throughput: drain a backlog that is already on disk, with
//! stateless reads (`Read`) and with a durable push consumer (deliver +
//! ack). Exspeed only (it measures the broker's read path, not a
//! producer).

use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use bytes::Bytes;
use exspeed_client::{ConsumerSpec, DeliverPolicy, PublishRecord};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;

use crate::driver::exspeed::ExspeedClient;
use crate::profile::Profile;
use crate::report::CatchupResult;

const STREAM: &str = "bench-catchup";
const PAYLOAD: usize = 1024;

pub async fn run(addr: &str, profile: &Profile) -> Result<CatchupResult> {
    let records = profile.catchup_records;
    let mut setup = ExspeedClient::connect(addr).await?;
    setup.ensure_stream(STREAM).await?;
    let client = setup.client;

    // Fill the backlog up to `records` (reusing what an earlier run left).
    let mut hwm = client
        .read(STREAM, 0, 1, Duration::ZERO, "")
        .await?
        .high_watermark;
    if hwm < records {
        let payload = Bytes::from(vec![b'x'; PAYLOAD]);
        let publisher = client
            .publisher()
            .max_batch_records(512)
            .max_in_flight(8192)
            .build();
        let mut pending = FuturesUnordered::new();
        for _ in hwm..records {
            let p = publisher.clone();
            let rec = PublishRecord::new("bench", payload.clone());
            pending.push(async move { p.publish(STREAM, rec).await });
            if pending.len() >= 4096 {
                pending.next().await.transpose()?;
            }
        }
        while let Some(r) = pending.next().await {
            r?;
        }
        publisher.close().await.ok();
        hwm = client
            .read(STREAM, 0, 1, Duration::ZERO, "")
            .await?
            .high_watermark;
    }
    let target = hwm;

    // Stateless reads, one request at a time.
    let start = Instant::now();
    let mut next = 0u64;
    let mut read_records = 0u64;
    let mut read_bytes = 0u64;
    while next < target {
        let r = client.read(STREAM, next, 1000, Duration::ZERO, "").await?;
        if r.next_offset <= next {
            bail!("read made no progress at offset {next}");
        }
        read_records += r.records.len() as u64;
        read_bytes += r.records.iter().map(|r| r.value.len() as u64).sum::<u64>();
        next = r.next_offset;
    }
    let read_secs = start.elapsed().as_secs_f64();

    // Durable push consumer from the first record, acking in batches.
    let name = format!(
        "bench-catchup-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    client
        .create_consumer(ConsumerSpec {
            deliver: DeliverPolicy::All,
            max_ack_pending: 100_000,
            ..ConsumerSpec::new(&name, STREAM)
        })
        .await?;
    let start = Instant::now();
    let mut sub = client.subscribe(&name, 8192).await?;
    let mut consumed = 0u64;
    let mut consumed_bytes = 0u64;
    let mut acks = Vec::with_capacity(1024);
    while consumed < target {
        let Some(msg) = sub.next_timeout(Duration::from_secs(10)).await else {
            bail!("consumer stalled after {consumed} of {target} records");
        };
        consumed += 1;
        consumed_bytes += msg.record.value.len() as u64;
        acks.push(msg.record.offset);
        if acks.len() >= 1024 {
            client.ack_nowait(&name, std::mem::take(&mut acks)).await?;
        }
    }
    let consume_secs = start.elapsed().as_secs_f64();
    if !acks.is_empty() {
        client.ack_nowait(&name, acks).await?;
    }
    drop(sub);
    client.delete_consumer(&name).await.ok();

    Ok(CatchupResult {
        payload_bytes: PAYLOAD,
        records: target,
        read_msg_per_sec: read_records as f64 / read_secs,
        read_mb_per_sec: read_bytes as f64 / read_secs / 1_000_000.0,
        consume_msg_per_sec: consumed as f64 / consume_secs,
        consume_mb_per_sec: consumed_bytes as f64 / consume_secs / 1_000_000.0,
    })
}
