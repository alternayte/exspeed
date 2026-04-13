use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, watch};

use exspeed_common::{subject_matches, Offset, StreamName};
use exspeed_streams::StorageEngine;

use crate::consumer_state::DeliveryRecord;

// ---------------------------------------------------------------------------
// DeliveryConfig
// ---------------------------------------------------------------------------

pub struct DeliveryConfig {
    pub consumer_name: String,
    pub stream_name: String,
    pub subject_filter: String,
    pub start_offset: u64,
    pub group_name: Option<String>,
}

// ---------------------------------------------------------------------------
// run_delivery — the main poll loop
// ---------------------------------------------------------------------------

pub async fn run_delivery(
    config: DeliveryConfig,
    storage: Arc<dyn StorageEngine>,
    tx: mpsc::Sender<DeliveryRecord>,
    mut cancel_rx: oneshot::Receiver<()>,
    group_members: Option<watch::Receiver<Vec<String>>>,
) {
    // Parse stream name; exit silently if invalid.
    let stream_name = match StreamName::try_from(config.stream_name.as_str()) {
        Ok(sn) => sn,
        Err(_) => return,
    };

    let mut current_offset = config.start_offset;
    let mut round_robin_counter: u64 = 0;
    let batch_size: usize = 100;

    loop {
        // (a) Check cancellation (non-blocking).
        if cancel_rx.try_recv().is_ok() {
            break;
        }

        // (b) Read a batch from storage via spawn_blocking (storage.read is sync).
        let storage_clone = Arc::clone(&storage);
        let sn = stream_name.clone();
        let offset = Offset(current_offset);
        let bs = batch_size;

        let read_result =
            tokio::task::spawn_blocking(move || storage_clone.read(&sn, offset, bs)).await;

        let records = match read_result {
            Ok(Ok(recs)) => recs,
            Ok(Err(_)) => break, // storage error — stop delivery
            Err(_) => break,     // join error — stop delivery
        };

        // (c) If empty, sleep and continue.
        if records.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue;
        }

        // (d) Process each record.
        for record in records {
            // Subject filtering.
            if !config.subject_filter.is_empty()
                && !subject_matches(&record.subject, &config.subject_filter)
            {
                current_offset = record.offset.0 + 1;
                continue;
            }

            // Group routing — skip records not assigned to us.
            if let Some(ref members_rx) = group_members {
                let members = members_rx.borrow();
                if !members.is_empty() {
                    let target =
                        route_to_member(&members, record.key.as_deref(), round_robin_counter);
                    if target != config.consumer_name {
                        current_offset = record.offset.0 + 1;
                        round_robin_counter += 1;
                        continue;
                    }
                }
            }

            // Build DeliveryRecord and send.
            let delivery = DeliveryRecord {
                record: record.clone(),
                delivery_attempt: 1,
            };

            if tx.send(delivery).await.is_err() {
                // Channel closed — consumer gone.
                return;
            }

            current_offset = record.offset.0 + 1;
            round_robin_counter += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// route_to_member — pick the group member for a record
// ---------------------------------------------------------------------------

fn route_to_member(members: &[String], key: Option<&[u8]>, round_robin: u64) -> String {
    let idx = match key {
        Some(k) => {
            let mut hasher = DefaultHasher::new();
            k.hash(&mut hasher);
            (hasher.finish() % members.len() as u64) as usize
        }
        None => (round_robin % members.len() as u64) as usize,
    };
    members[idx].clone()
}
