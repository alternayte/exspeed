use std::sync::Arc;

use exspeed_common::{subject_matches, Offset, StreamName};
use exspeed_protocol::messages::{
    BatchRecord, CreateStreamRequest, FetchRequest, PublishRequest, RecordsBatch, ServerMessage,
};
use exspeed_streams::{record::Record, StorageEngine};

pub fn handle_create_stream(
    storage: &Arc<dyn StorageEngine>,
    req: CreateStreamRequest,
) -> ServerMessage {
    let stream_name = match StreamName::try_from(req.stream_name) {
        Ok(n) => n,
        Err(e) => {
            return ServerMessage::Error {
                code: 400,
                message: format!("invalid stream name: {e}"),
            }
        }
    };

    match storage.create_stream(&stream_name) {
        Ok(()) => ServerMessage::Ok,
        Err(e) => ServerMessage::Error {
            code: 409,
            message: format!("create_stream failed: {e}"),
        },
    }
}

pub fn handle_publish(storage: &Arc<dyn StorageEngine>, req: PublishRequest) -> ServerMessage {
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

    match storage.append(&stream_name, &record) {
        Ok(offset) => ServerMessage::PublishOk { offset: offset.0 },
        Err(e) => ServerMessage::Error {
            code: 404,
            message: format!("append failed: {e}"),
        },
    }
}

pub fn handle_fetch(storage: &Arc<dyn StorageEngine>, req: FetchRequest) -> ServerMessage {
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

    let stored = match storage.read(&stream_name, Offset(req.offset), fetch_count) {
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
