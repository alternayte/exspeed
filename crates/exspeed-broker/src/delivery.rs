use std::sync::Arc;

use tokio::sync::mpsc;

use exspeed_common::{subject_matches, Metrics, Offset, StreamName};
use exspeed_streams::{StorageEngine, StorageError};

use crate::consumer_state::{DeliveryBatch, DeliveryRecord};
use crate::work_coordinator::WorkCoordinator;

// ---------------------------------------------------------------------------
// DeliveryConfig
// ---------------------------------------------------------------------------

pub struct DeliveryConfig {
    pub consumer_name: String,
    pub stream_name: String,
    pub subject_filter: String,
    pub start_offset: u64,
    pub group_name: Option<String>,
    pub subscriber_id: String,
    pub work_coordinator: Arc<dyn WorkCoordinator>,
    pub metrics: Arc<Metrics>,
}

/// Compute consume latency in seconds: `now - record_timestamp`, clamped to 0
/// if the record was written in the future (clock skew) or the UNIX_EPOCH
/// subtraction fails.
fn consume_latency_secs(record_timestamp_nanos: u64) -> f64 {
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    if now_nanos > record_timestamp_nanos {
        (now_nanos - record_timestamp_nanos) as f64 / 1_000_000_000.0
    } else {
        0.0
    }
}

// ---------------------------------------------------------------------------
// run_delivery — branches on grouped vs non-grouped + coordinator support
// ---------------------------------------------------------------------------

pub async fn run_delivery(
    config: DeliveryConfig,
    storage: Arc<dyn StorageEngine>,
    tx: mpsc::Sender<DeliveryBatch>,
) {
    let stream_name = match StreamName::try_from(config.stream_name.as_str()) {
        Ok(sn) => sn,
        Err(_) => return,
    };

    let batch_size: usize = 100;
    let ack_timeout_secs: u64 = 30;
    let max_ack_pending: usize = 1000;

    match config.group_name.clone() {
        Some(group) if config.work_coordinator.supports_coordination() => {
            run_grouped(
                group,
                config,
                stream_name,
                storage,
                tx,
                batch_size,
                ack_timeout_secs,
                max_ack_pending,
            )
            .await
        }
        _ => {
            // Non-grouped OR noop coordinator: use cursor-based loop.
            run_ungrouped(config, stream_name, storage, tx, batch_size).await
        }
    }
}

// ---------------------------------------------------------------------------
// Non-grouped delivery: cursor-based, one subscriber, existing semantics.
// ---------------------------------------------------------------------------

async fn run_ungrouped(
    config: DeliveryConfig,
    stream_name: StreamName,
    storage: Arc<dyn StorageEngine>,
    tx: mpsc::Sender<DeliveryBatch>,
    batch_size: usize,
) {
    let mut current_offset = config.start_offset;
    loop {
        let records = match storage
            .read(&stream_name, Offset(current_offset), batch_size)
            .await
        {
            Ok(recs) => recs,
            Err(StorageError::OffsetOutOfRange { requested, earliest }) => {
                tracing::warn!(
                    stream = %stream_name,
                    consumer = %config.consumer_name,
                    requested,
                    earliest,
                    "consumer is behind retention window; subscription ending — \
                     client must re-seek to earliest or a chosen offset"
                );
                break;
            }
            Err(_) => break,
        };

        if records.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue;
        }

        let mut batch: Vec<DeliveryRecord> = Vec::with_capacity(records.len());
        for record in records {
            if !config.subject_filter.is_empty()
                && !subject_matches(&record.subject, &config.subject_filter)
            {
                current_offset = record.offset.0 + 1;
                continue;
            }

            let delivery = DeliveryRecord {
                record: record.clone(),
                delivery_attempt: 1,
            };

            config.metrics.record_consume_latency(
                &config.stream_name,
                &config.consumer_name,
                consume_latency_secs(record.timestamp),
            );

            batch.push(delivery);
            current_offset = record.offset.0 + 1;
        }

        if !batch.is_empty() {
            if tx.send(DeliveryBatch { records: batch }).await.is_err() {
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Grouped delivery: coordinator-driven work distribution across subscribers.
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_grouped(
    group: String,
    config: DeliveryConfig,
    stream_name: StreamName,
    storage: Arc<dyn StorageEngine>,
    tx: mpsc::Sender<DeliveryBatch>,
    batch_size: usize,
    ack_timeout_secs: u64,
    max_ack_pending: usize,
) {
    let coord = Arc::clone(&config.work_coordinator);

    loop {
        // 1) Top up pending set if below max_ack_pending.
        let pending = coord.pending_count(&group).await.unwrap_or(0);
        if pending < max_ack_pending {
            let head = coord.delivery_head(&group).await.unwrap_or(0);
            let read_from = if pending == 0 && head == 0 {
                config.start_offset
            } else {
                head + 1
            };

            let records = match storage
                .read(&stream_name, Offset(read_from), batch_size)
                .await
            {
                Ok(recs) => recs,
                Err(StorageError::OffsetOutOfRange { requested, earliest }) => {
                    tracing::warn!(
                        stream = %stream_name,
                        group = %group,
                        consumer = %config.consumer_name,
                        requested,
                        earliest,
                        "group consumer behind retention window; delivery task ending"
                    );
                    return;
                }
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
            };

            // Filter by subject: only enqueue matching records.
            let enqueue_offsets: Vec<u64> = records
                .iter()
                .filter(|r| {
                    config.subject_filter.is_empty()
                        || subject_matches(&r.subject, &config.subject_filter)
                })
                .map(|r| r.offset.0)
                .collect();

            if !enqueue_offsets.is_empty() {
                if let Err(e) = coord.enqueue(&group, &enqueue_offsets).await {
                    tracing::warn!(group = group.as_str(), error = %e, "enqueue failed");
                }
            }

            // Advance delivery_head past filtered-out records so we don't
            // re-read them. Enqueue + immediate ack for the max offset read.
            if let Some(max_read) = records.iter().map(|r| r.offset.0).max() {
                if enqueue_offsets.is_empty() {
                    let _ = coord.enqueue(&group, &[max_read]).await;
                    let _ = coord.ack(&group, max_read).await;
                }
            }
        }

        // 2) Claim available work for this subscriber.
        let claims = match coord
            .claim_batch(&group, &config.subscriber_id, batch_size, ack_timeout_secs)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(group = group.as_str(), error = %e, "claim_batch failed");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
        };

        if claims.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue;
        }

        // 3) Fetch each claimed record from storage and deliver.
        for claim in claims {
            let records = match storage.read(&stream_name, Offset(claim.offset), 1).await {
                Ok(r) => r,
                Err(_) => continue,
            };
            let record = match records.into_iter().next() {
                Some(r) if r.offset.0 == claim.offset => r,
                _ => continue,
            };

            let record_timestamp = record.timestamp;
            let delivery = DeliveryRecord {
                record,
                delivery_attempt: claim.attempts as u16,
            };

            config.metrics.record_consume_latency(
                &config.stream_name,
                &config.consumer_name,
                consume_latency_secs(record_timestamp),
            );

            if tx.send(DeliveryBatch::single(delivery)).await.is_err() {
                // Subscriber gone — do not ack; let it expire + be redelivered.
                return;
            }
        }
    }
}
