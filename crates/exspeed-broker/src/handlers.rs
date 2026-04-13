use exspeed_common::{subject_matches, Offset, StreamName};
use exspeed_protocol::messages::{
    AckRequest, BatchRecord, CreateConsumerRequest, CreateStreamRequest, DeleteConsumerRequest,
    FetchRequest, NackRequest, PublishRequest, RecordsBatch, SeekRequest, ServerMessage, StartFrom,
};
use exspeed_streams::record::Record;
use exspeed_streams::StorageError;

use crate::broker::Broker;
use crate::consumer_state::{ConsumerConfig, ConsumerGroup, ConsumerState};
use crate::persistence;

pub fn handle_create_stream(broker: &Broker, req: CreateStreamRequest) -> ServerMessage {
    let stream_name = match StreamName::try_from(req.stream_name) {
        Ok(n) => n,
        Err(e) => {
            return ServerMessage::Error {
                code: 400,
                message: format!("invalid stream name: {e}"),
            }
        }
    };

    match broker.storage.create_stream(&stream_name) {
        Ok(()) => ServerMessage::Ok,
        Err(e) => ServerMessage::Error {
            code: 409,
            message: format!("create_stream failed: {e}"),
        },
    }
}

pub fn handle_publish(broker: &Broker, req: PublishRequest) -> ServerMessage {
    let stream_name = match StreamName::try_from(req.stream) {
        Ok(n) => n,
        Err(e) => {
            return ServerMessage::Error {
                code: 400,
                message: format!("invalid stream name: {e}"),
            }
        }
    };

    let record = Record {
        key: req.key,
        value: req.value,
        subject: req.subject,
        headers: req.headers,
    };

    match broker.storage.append(&stream_name, &record) {
        Ok(offset) => ServerMessage::PublishOk { offset: offset.0 },
        Err(e) => ServerMessage::Error {
            code: 404,
            message: format!("append failed: {e}"),
        },
    }
}

pub fn handle_fetch(broker: &Broker, req: FetchRequest) -> ServerMessage {
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

pub fn handle_create_consumer(broker: &Broker, req: CreateConsumerRequest) -> ServerMessage {
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
    match broker.storage.read(&stream_name, Offset(0), 0) {
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
            match broker.storage.read(&stream_name, Offset(0), usize::MAX) {
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
    if let Err(e) = persistence::save_consumer(&broker.data_dir, &config) {
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
                delivery_tx: None,
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

pub fn handle_delete_consumer(broker: &Broker, req: DeleteConsumerRequest) -> ServerMessage {
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
    let _ = persistence::delete_consumer(&broker.data_dir, &req.name);

    ServerMessage::Ok
}

pub fn handle_ack(broker: &Broker, req: AckRequest) -> ServerMessage {
    // Look up consumer and update offset
    {
        let mut consumers = broker.consumers.write().unwrap();
        match consumers.get_mut(&req.consumer_name) {
            Some(state) => {
                if req.offset > state.config.offset {
                    state.config.offset = req.offset;
                }
                // Persist updated config
                if let Err(e) = persistence::save_consumer(&broker.data_dir, &state.config) {
                    return ServerMessage::Error {
                        code: 500,
                        message: format!("failed to persist consumer offset: {e}"),
                    };
                }
            }
            None => {
                return ServerMessage::Error {
                    code: 404,
                    message: format!("consumer '{}' not found", req.consumer_name),
                }
            }
        }
    }

    // Clean up nack_attempts entries for this consumer where offset <= req.offset
    {
        let mut nack_attempts = broker.nack_attempts.write().unwrap();
        nack_attempts
            .retain(|(name, offset), _| !(name == &req.consumer_name && *offset <= req.offset));
    }

    ServerMessage::Ok
}

pub fn handle_nack(broker: &Broker, req: NackRequest) -> ServerMessage {
    // Look up consumer
    let (stream, _subject_filter) = {
        let consumers = broker.consumers.read().unwrap();
        match consumers.get(&req.consumer_name) {
            Some(state) => (
                state.config.stream.clone(),
                state.config.subject_filter.clone(),
            ),
            None => {
                return ServerMessage::Error {
                    code: 404,
                    message: format!("consumer '{}' not found", req.consumer_name),
                }
            }
        }
    };

    // Increment attempt count
    let attempts = {
        let mut nack_attempts = broker.nack_attempts.write().unwrap();
        let entry = nack_attempts
            .entry((req.consumer_name.clone(), req.offset))
            .or_insert(0);
        *entry += 1;
        *entry
    };

    if attempts >= broker.max_delivery_attempts {
        // Read the record at the NACKed offset from storage
        let stream_name = match StreamName::try_from(stream.clone()) {
            Ok(n) => n,
            Err(e) => {
                return ServerMessage::Error {
                    code: 500,
                    message: format!("invalid stream name: {e}"),
                }
            }
        };

        let records = match broker.storage.read(&stream_name, Offset(req.offset), 1) {
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

        // Build DLQ headers
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

        // Create/ensure DLQ stream exists
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

        // Ignore StreamAlreadyExists error
        match broker.storage.create_stream(&dlq_stream_name) {
            Ok(()) => {}
            Err(StorageError::StreamAlreadyExists(_)) => {}
            Err(e) => {
                return ServerMessage::Error {
                    code: 500,
                    message: format!("failed to create DLQ stream: {e}"),
                }
            }
        }

        // Publish record to DLQ
        let dlq_record = Record {
            key: record.key.clone(),
            value: record.value.clone(),
            subject: record.subject.clone(),
            headers: dlq_headers,
        };

        if let Err(e) = broker.storage.append(&dlq_stream_name, &dlq_record) {
            return ServerMessage::Error {
                code: 500,
                message: format!("failed to publish to DLQ: {e}"),
            };
        }

        // Advance consumer offset past this record
        {
            let mut consumers = broker.consumers.write().unwrap();
            if let Some(state) = consumers.get_mut(&req.consumer_name) {
                let new_offset = req.offset + 1;
                if new_offset > state.config.offset {
                    state.config.offset = new_offset;
                }
                let _ = persistence::save_consumer(&broker.data_dir, &state.config);
            }
        }

        // Remove from nack_attempts
        {
            let mut nack_attempts = broker.nack_attempts.write().unwrap();
            nack_attempts.remove(&(req.consumer_name.clone(), req.offset));
        }
    }

    ServerMessage::Ok
}

pub fn handle_seek(broker: &Broker, req: SeekRequest) -> ServerMessage {
    let consumer_name = req.consumer_name;
    let mut consumers = broker.consumers.write().unwrap();
    let consumer = match consumers.get_mut(&consumer_name) {
        Some(c) => c,
        None => {
            return ServerMessage::Error {
                code: 404,
                message: format!("consumer not found: {consumer_name}"),
            }
        }
    };

    let stream_name = match StreamName::try_from(consumer.config.stream.as_str()) {
        Ok(n) => n,
        Err(e) => {
            return ServerMessage::Error {
                code: 400,
                message: e.to_string(),
            }
        }
    };

    match broker.storage.seek_by_time(&stream_name, req.timestamp) {
        Ok(offset) => {
            consumer.config.offset = offset.0;
            // Persist
            if let Err(e) = persistence::save_consumer(&broker.data_dir, &consumer.config) {
                return ServerMessage::Error {
                    code: 500,
                    message: format!("persist failed: {e}"),
                };
            }
            ServerMessage::PublishOk { offset: offset.0 } // reuse PublishOk for offset response
        }
        Err(e) => ServerMessage::Error {
            code: 500,
            message: format!("seek failed: {e}"),
        },
    }
}
