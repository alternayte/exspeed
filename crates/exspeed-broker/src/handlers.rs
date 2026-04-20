use exspeed_common::{subject_matches, Offset, StreamName};
use exspeed_protocol::messages::{
    AckRequest, BatchRecord, CreateConsumerRequest, CreateStreamRequest, DeleteConsumerRequest,
    FetchRequest, NackRequest, PublishRequest, RecordsBatch, SeekRequest, ServerMessage, StartFrom,
};
use exspeed_streams::record::Record;
use exspeed_streams::StorageError;

use crate::broker::Broker;
use crate::consumer_state::{ConsumerConfig, ConsumerGroup, ConsumerState};

pub async fn handle_create_stream(broker: &Broker, req: CreateStreamRequest) -> ServerMessage {
    let stream_name = match StreamName::try_from(req.stream_name) {
        Ok(n) => n,
        Err(e) => {
            return ServerMessage::Error {
                code: 400,
                message: format!("invalid stream name: {e}"),
            }
        }
    };

    match broker
        .storage
        .create_stream(&stream_name, req.max_age_secs, req.max_bytes)
        .await
    {
        Ok(()) => {
            // Load the stream config that was just persisted (uses defaults for dedup
            // fields since CreateStreamRequest doesn't carry dedup params yet).
            let stream_dir = broker
                .data_dir
                .join("streams")
                .join(stream_name.as_str());
            let cfg = exspeed_storage::file::stream_config::StreamConfig::load(&stream_dir)
                .unwrap_or_default();
            broker
                .broker_append
                .configure_stream(&stream_name, cfg.dedup_window_secs, cfg.dedup_max_entries)
                .await;

            // Fan out to replication followers (no-op if single-pod).
            broker.emit_replication_event(|| {
                use exspeed_protocol::messages::replicate::StreamCreatedEvent;
                crate::replication::ReplicationEvent::StreamCreated(StreamCreatedEvent {
                    name: stream_name.as_str().to_string(),
                    max_age_secs: req.max_age_secs,
                    max_bytes: req.max_bytes,
                })
            });
            ServerMessage::Ok
        }
        Err(e) => ServerMessage::Error {
            code: 409,
            message: format!("create_stream failed: {e}"),
        },
    }
}

pub async fn handle_publish(broker: &Broker, req: PublishRequest) -> ServerMessage {
    let stream_name = match StreamName::try_from(req.stream) {
        Ok(n) => n,
        Err(e) => {
            return ServerMessage::Error {
                code: 400,
                message: format!("invalid stream name: {e}"),
            }
        }
    };

    // Translate msg_id field → x-idempotency-key header.
    let mut headers = req.headers;
    if let Some(ref id) = req.msg_id {
        // Log at DEBUG when both explicit msg_id and x-idempotency-key header are
        // present but disagree, so operators can detect misconfigured callers.
        if let Some((_, existing)) = headers.iter().find(|(k, _)| k == "x-idempotency-key") {
            if existing != id {
                tracing::debug!(
                    stream = %stream_name,
                    "publish has both explicit msg_id and x-idempotency-key header; using explicit field"
                );
            }
        }
        headers.retain(|(k, _)| k != "x-idempotency-key");
        headers.push(("x-idempotency-key".to_string(), id.clone()));
    }

    let record = Record {
        key: req.key,
        value: req.value,
        subject: req.subject,
        headers,
        timestamp_ns: None,
    };

    let start = std::time::Instant::now();
    match broker.broker_append.append(&stream_name, &record).await {
        Ok(crate::broker_append::AppendResult::Written(offset, timestamp_ns)) => {
            let elapsed_secs = start.elapsed().as_secs_f64();
            broker
                .metrics
                .record_publish_latency(stream_name.as_str(), elapsed_secs);
            broker.metrics.record_publish(stream_name.as_str());

            // Fan out to replication followers. Only on `Written` — a
            // `Duplicate` means the record was persisted on an earlier
            // publish, which was itself replicated at that time.
            //
            // Use the leader-assigned timestamp (converted from ns to ms)
            // rather than a fresh wall-clock reading, so the follower's
            // time-index matches the leader's for seek_by_time + windowed
            // continuous queries.
            broker.emit_replication_event(|| {
                use exspeed_protocol::messages::replicate::{RecordsAppended, ReplicatedRecord};
                // Translate `x-idempotency-key` out of the headers into
                // the typed `msg_id` field. The follower's apply path
                // rehydrates it back under the same header name, so this
                // keeps the wire tight (no duplicated representation) and
                // lets the follower skip a per-record header dedup pass.
                let mut headers = record.headers.clone();
                let mut msg_id = None;
                headers.retain(|(k, v)| {
                    if k.eq_ignore_ascii_case("x-idempotency-key") {
                        msg_id = Some(v.clone());
                        false
                    } else {
                        true
                    }
                });
                crate::replication::ReplicationEvent::RecordsAppended(RecordsAppended {
                    stream: stream_name.as_str().to_string(),
                    base_offset: offset.0,
                    records: vec![ReplicatedRecord {
                        subject: record.subject.clone(),
                        payload: record.value.to_vec(),
                        headers,
                        timestamp_ms: timestamp_ns / 1_000_000,
                        msg_id,
                    }],
                })
            });

            ServerMessage::PublishOk {
                offset: offset.0,
                duplicate: false,
            }
        }
        Ok(crate::broker_append::AppendResult::Duplicate(offset)) => {
            let elapsed_secs = start.elapsed().as_secs_f64();
            broker
                .metrics
                .record_publish_latency(stream_name.as_str(), elapsed_secs);
            broker.metrics.record_publish(stream_name.as_str());
            ServerMessage::PublishOk {
                offset: offset.0,
                duplicate: true,
            }
        }
        Err(StorageError::KeyCollision { stored_offset }) => {
            ServerMessage::KeyCollision { stored_offset }
        }
        Err(StorageError::DedupMapFull { retry_after_secs }) => {
            ServerMessage::DedupMapFull { retry_after_secs }
        }
        Err(e) => ServerMessage::Error {
            code: 500,
            message: format!("append failed: {e}"),
        },
    }
}

pub async fn handle_fetch(broker: &Broker, req: FetchRequest) -> ServerMessage {
    let stream_name = match StreamName::try_from(req.stream) {
        Ok(n) => n,
        Err(e) => {
            return ServerMessage::Error {
                code: 400,
                message: format!("invalid stream name: {e}"),
            }
        }
    };

    let max_records = req.max_records as usize;
    let fetch_count = if req.subject_filter.is_empty() {
        max_records
    } else {
        max_records * 4
    };

    let stored = match broker
        .storage
        .read(&stream_name, Offset(req.offset), fetch_count)
        .await
    {
        Ok(records) => records,
        Err(e) => {
            return ServerMessage::Error {
                code: 404,
                message: format!("read failed: {e}"),
            }
        }
    };

    let records: Vec<BatchRecord> = stored
        .into_iter()
        .filter(|r| {
            req.subject_filter.is_empty() || subject_matches(&r.subject, &req.subject_filter)
        })
        .take(max_records)
        .map(|r| BatchRecord {
            offset: r.offset.0,
            timestamp: r.timestamp,
            subject: r.subject,
            key: r.key,
            value: r.value,
            headers: r.headers,
        })
        .collect();

    ServerMessage::RecordsBatch(RecordsBatch { records })
}

pub async fn handle_create_consumer(broker: &Broker, req: CreateConsumerRequest) -> ServerMessage {
    use exspeed_common::validate_resource_name;

    if let Err(e) = validate_resource_name(&req.name, "consumer name") {
        return ServerMessage::Error {
            code: 400,
            message: e.to_string(),
        };
    }
    // group can be empty (means "no group"); only validate length if non-empty
    if !req.group.is_empty() {
        if let Err(e) = validate_resource_name(&req.group, "group name") {
            return ServerMessage::Error {
                code: 400,
                message: e.to_string(),
            };
        }
    }
    if !req.subject_filter.is_empty() && req.subject_filter.len() > exspeed_common::MAX_NAME_LEN {
        return ServerMessage::Error {
            code: 400,
            message: format!(
                "subject filter length {} exceeds max {}",
                req.subject_filter.len(),
                exspeed_common::MAX_NAME_LEN
            ),
        };
    }

    // Validate stream name
    let stream_name = match StreamName::try_from(req.stream.clone()) {
        Ok(n) => n,
        Err(e) => {
            return ServerMessage::Error {
                code: 400,
                message: format!("invalid stream name: {e}"),
            }
        }
    };

    // Check stream exists by attempting a zero-count read
    match broker.storage.read(&stream_name, Offset(0), 0).await {
        Ok(_) => {}
        Err(StorageError::StreamNotFound(_)) => {
            return ServerMessage::Error {
                code: 404,
                message: format!("stream '{}' not found", req.stream),
            }
        }
        Err(e) => {
            return ServerMessage::Error {
                code: 500,
                message: format!("error checking stream: {e}"),
            }
        }
    }

    // Check consumer doesn't already exist
    {
        let consumers = broker.consumers.read().unwrap();
        if consumers.contains_key(&req.name) {
            return ServerMessage::Error {
                code: 409,
                message: format!("consumer '{}' already exists", req.name),
            };
        }
    }

    // Determine start offset
    let offset = match req.start_from {
        StartFrom::Earliest => 0,
        StartFrom::Latest => {
            // Read a large batch to find the last offset.
            // We scan for the highest offset available.
            match broker.storage.read(&stream_name, Offset(0), usize::MAX).await {
                Ok(records) => {
                    if let Some(last) = records.last() {
                        last.offset.0 + 1
                    } else {
                        0
                    }
                }
                Err(_) => 0,
            }
        }
        StartFrom::Offset => req.start_offset,
    };

    let config = ConsumerConfig {
        name: req.name.clone(),
        stream: req.stream.clone(),
        group: req.group.clone(),
        subject_filter: req.subject_filter.clone(),
        offset,
    };

    // Persist to disk
    if let Err(e) = broker.consumer_store.save(&config).await {
        return ServerMessage::Error {
            code: 500,
            message: format!("failed to persist consumer: {e}"),
        };
    }

    // Insert into in-memory state
    {
        let mut consumers = broker.consumers.write().unwrap();
        consumers.insert(
            req.name.clone(),
            ConsumerState {
                config,
                subscribers: std::collections::HashMap::new(),
            },
        );
    }

    // Add to group if specified
    if !req.group.is_empty() {
        let mut groups = broker.groups.write().unwrap();
        groups
            .entry(req.group.clone())
            .or_insert_with(|| ConsumerGroup::new(&req.group))
            .add_member(&req.name);
    }

    ServerMessage::Ok
}

pub async fn handle_delete_consumer(broker: &Broker, req: DeleteConsumerRequest) -> ServerMessage {
    // Check consumer exists and get its group
    let group_name = {
        let consumers = broker.consumers.read().unwrap();
        match consumers.get(&req.name) {
            Some(state) => {
                let g = state.config.group.clone();
                Some(g)
            }
            None => None,
        }
    };

    let group_name = match group_name {
        Some(g) => g,
        None => {
            return ServerMessage::Error {
                code: 404,
                message: format!("consumer '{}' not found", req.name),
            }
        }
    };

    // Remove from group if applicable
    if !group_name.is_empty() {
        let mut groups = broker.groups.write().unwrap();
        if let Some(group) = groups.get_mut(&group_name) {
            group.remove_member(&req.name);
            if group.is_empty() {
                groups.remove(&group_name);
            }
        }
    }

    // Remove from consumers map
    {
        let mut consumers = broker.consumers.write().unwrap();
        consumers.remove(&req.name);
    }

    // Delete persistence file (ignore errors if file doesn't exist)
    let _ = broker.consumer_store.delete(&req.name).await;

    ServerMessage::Ok
}

pub async fn handle_ack(broker: &Broker, req: AckRequest) -> ServerMessage {
    // Check if the consumer is grouped; also clone out the group name.
    let group_opt = {
        let consumers = broker.consumers.read().unwrap();
        match consumers.get(&req.consumer_name) {
            Some(state) => {
                if state.config.group.is_empty() {
                    None
                } else {
                    Some(state.config.group.clone())
                }
            }
            None => {
                return ServerMessage::Error {
                    code: 404,
                    message: format!("consumer '{}' not found", req.consumer_name),
                }
            }
        }
    };

    if let Some(group) = group_opt {
        // Grouped: route to coordinator.
        if let Err(e) = broker.work_coordinator.ack(&group, req.offset).await {
            return ServerMessage::Error {
                code: 500,
                message: format!("coordinator ack failed: {e}"),
            };
        }
        return ServerMessage::Ok;
    }

    // Non-grouped: existing behavior — bump local offset and persist.
    let config_to_save = {
        let mut consumers = broker.consumers.write().unwrap();
        match consumers.get_mut(&req.consumer_name) {
            Some(state) => {
                if req.offset > state.config.offset {
                    state.config.offset = req.offset;
                }
                state.config.clone()
            }
            None => {
                return ServerMessage::Error {
                    code: 404,
                    message: format!("consumer '{}' not found", req.consumer_name),
                }
            }
        }
    };

    if let Err(e) = broker.consumer_store.save(&config_to_save).await {
        return ServerMessage::Error {
            code: 500,
            message: format!("failed to persist consumer offset: {e}"),
        };
    }

    {
        let mut nack_attempts = broker.nack_attempts.write().unwrap();
        nack_attempts
            .retain(|(name, offset), _| !(name == &req.consumer_name && *offset <= req.offset));
    }

    ServerMessage::Ok
}

pub async fn handle_nack(broker: &Broker, req: NackRequest) -> ServerMessage {
    // Extract group + stream under lock.
    let (group_opt, stream) = {
        let consumers = broker.consumers.read().unwrap();
        match consumers.get(&req.consumer_name) {
            Some(state) => {
                let group = if state.config.group.is_empty() {
                    None
                } else {
                    Some(state.config.group.clone())
                };
                (group, state.config.stream.clone())
            }
            None => {
                return ServerMessage::Error {
                    code: 404,
                    message: format!("consumer '{}' not found", req.consumer_name),
                }
            }
        }
    };

    // Compute attempts count.
    let attempts: u16 = match group_opt.as_ref() {
        Some(group) => match broker.work_coordinator.nack(group, req.offset).await {
            Ok(a) => a as u16,
            Err(e) => {
                return ServerMessage::Error {
                    code: 500,
                    message: format!("coordinator nack failed: {e}"),
                };
            }
        },
        None => {
            let mut nack_attempts = broker.nack_attempts.write().unwrap();
            let entry = nack_attempts
                .entry((req.consumer_name.clone(), req.offset))
                .or_insert(0);
            *entry += 1;
            *entry
        }
    };

    if attempts >= broker.max_delivery_attempts {
        // Route to DLQ.
        let stream_name = match StreamName::try_from(stream.clone()) {
            Ok(n) => n,
            Err(e) => {
                return ServerMessage::Error {
                    code: 500,
                    message: format!("invalid stream name: {e}"),
                }
            }
        };

        let records = match broker.storage.read(&stream_name, Offset(req.offset), 1).await {
            Ok(r) => r,
            Err(e) => {
                return ServerMessage::Error {
                    code: 500,
                    message: format!("failed to read NACKed record: {e}"),
                }
            }
        };

        let record = match records.into_iter().next() {
            Some(r) => r,
            None => {
                return ServerMessage::Error {
                    code: 500,
                    message: format!("NACKed record at offset {} not found", req.offset),
                }
            }
        };

        let mut dlq_headers = record.headers.clone();
        dlq_headers.push(("x-exspeed-original-stream".to_string(), stream.clone()));
        dlq_headers.push((
            "x-exspeed-original-offset".to_string(),
            req.offset.to_string(),
        ));
        dlq_headers.push((
            "x-exspeed-original-subject".to_string(),
            record.subject.clone(),
        ));
        dlq_headers.push(("x-exspeed-failure-count".to_string(), attempts.to_string()));
        dlq_headers.push(("x-exspeed-consumer".to_string(), req.consumer_name.clone()));

        let dlq_stream_str = format!("{stream}-dlq");
        let dlq_stream_name = match StreamName::try_from(dlq_stream_str.clone()) {
            Ok(n) => n,
            Err(e) => {
                return ServerMessage::Error {
                    code: 500,
                    message: format!("invalid DLQ stream name: {e}"),
                }
            }
        };

        match broker.storage.create_stream(&dlq_stream_name, 0, 0).await {
            Ok(()) => {}
            Err(StorageError::StreamAlreadyExists(_)) => {}
            Err(e) => {
                return ServerMessage::Error {
                    code: 500,
                    message: format!("failed to create DLQ stream: {e}"),
                }
            }
        }

        let dlq_record = Record {
            key: record.key.clone(),
            value: record.value.clone(),
            subject: record.subject.clone(),
            headers: dlq_headers,
            timestamp_ns: None,
        };

        if let Err(e) = broker.storage.append(&dlq_stream_name, &dlq_record).await {
            return ServerMessage::Error {
                code: 500,
                message: format!("failed to publish to DLQ: {e}"),
            };
        }

        // Advance past this record in the respective offset tracking system.
        if let Some(group) = group_opt {
            // Grouped: ack in the coordinator to remove from pending.
            let _ = broker.work_coordinator.ack(&group, req.offset).await;
        } else {
            // Non-grouped: advance consumer offset in ConsumerStore.
            let config_to_save = {
                let mut consumers = broker.consumers.write().unwrap();
                consumers.get_mut(&req.consumer_name).map(|state| {
                    let new_offset = req.offset + 1;
                    if new_offset > state.config.offset {
                        state.config.offset = new_offset;
                    }
                    state.config.clone()
                })
            };
            if let Some(cfg) = config_to_save {
                let _ = broker.consumer_store.save(&cfg).await;
            }
            let mut nack_attempts = broker.nack_attempts.write().unwrap();
            nack_attempts.remove(&(req.consumer_name.clone(), req.offset));
        }
    }

    ServerMessage::Ok
}

pub async fn handle_seek(broker: &Broker, req: SeekRequest) -> ServerMessage {
    let consumer_name = req.consumer_name;

    // Extract the stream name from consumer config (lock released before .await)
    let stream_name = {
        let consumers = broker.consumers.read().unwrap();
        match consumers.get(&consumer_name) {
            Some(c) => match StreamName::try_from(c.config.stream.as_str()) {
                Ok(n) => n,
                Err(e) => {
                    return ServerMessage::Error {
                        code: 400,
                        message: e.to_string(),
                    }
                }
            },
            None => {
                return ServerMessage::Error {
                    code: 404,
                    message: format!("consumer not found: {consumer_name}"),
                }
            }
        }
    };

    match broker.storage.seek_by_time(&stream_name, req.timestamp).await {
        Ok(offset) => {
            // Re-acquire lock after .await to update consumer state; clone config out before awaiting the store.
            let config_to_save = {
                let mut consumers = broker.consumers.write().unwrap();
                consumers.get_mut(&consumer_name).map(|consumer| {
                    consumer.config.offset = offset.0;
                    consumer.config.clone()
                })
            };
            if let Some(config_to_save) = config_to_save {
                if let Err(e) = broker.consumer_store.save(&config_to_save).await {
                    return ServerMessage::Error {
                        code: 500,
                        message: format!("persist failed: {e}"),
                    };
                }
            }
            ServerMessage::PublishOk {
                offset: offset.0,
                duplicate: false,
            } // reuse PublishOk for offset response
        }
        Err(e) => ServerMessage::Error {
            code: 500,
            message: format!("seek failed: {e}"),
        },
    }
}
