//! Stream capture: core messages published to a subject a stream captures
//! (`capture_subjects`) are also appended to that stream, so a NATS client
//! (or an Exspeed `CorePublish`) can write durable records.
//!
//! A captured message published with a reply subject gets an
//! acknowledgement there, in JetStream's `PubAck` shape:
//! `{"stream":"orders","seq":42}` (`seq` = offset + 1, plus
//! `"duplicate":true` for a deduplicated retry), or
//! `{"error":{"code":503,"description":"…"}}`. A `Nats-Msg-Id` header acts
//! as the record's `msg_id` for deduplication.
//!
//! Core subscribers still receive the message as usual.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use exspeed_common::{StreamName, SubjectFilter, INTERNAL_STREAM_PREFIX};
use exspeed_streams::{Record, StorageError};
use serde_json::json;
use tokio::sync::mpsc;
use tracing::warn;

use crate::broker_append::{AppendResult, IDEMPOTENCY_HEADER};
use crate::log::{Log, LogError};
use crate::pubsub::{CoreBus, CoreMessage};

/// The header NATS clients use for message deduplication.
pub const NATS_MSG_ID_HEADER: &str = "Nats-Msg-Id";

/// The capture table is rebuilt when stream metadata changes, and at least
/// this often (a follower that becomes leader may have applied replicated
/// stream changes without bumping its local counter).
const MAX_CACHE_AGE: Duration = Duration::from_secs(5);
/// Captured messages queued per connection.
const PIPELINE_QUEUE: usize = 1024;
/// Records appended in one batch at most.
const MAX_BATCH: usize = 512;

type Table = Vec<(StreamName, Vec<SubjectFilter>)>;

struct Cached {
    counter: u64,
    built: Instant,
    table: Arc<Table>,
}

/// Which stream (if any) captures a subject.
pub struct Capture {
    log: Arc<Log>,
    cache: RwLock<Option<Cached>>,
}

impl Capture {
    pub fn new(log: Arc<Log>) -> Arc<Self> {
        Arc::new(Self {
            log,
            cache: RwLock::new(None),
        })
    }

    /// The stream that captures `subject`.
    pub async fn target(&self, subject: &str) -> Option<StreamName> {
        let table = self.table().await;
        table
            .iter()
            .find(|(_, fs)| fs.iter().any(|f| f.matches(subject)))
            .map(|(s, _)| s.clone())
    }

    async fn table(&self) -> Arc<Table> {
        let counter = self.log.metadata_counter();
        if let Some(c) = self.cache.read().unwrap().as_ref() {
            if c.counter == counter && c.built.elapsed() < MAX_CACHE_AGE {
                return c.table.clone();
            }
        }
        let storage = self.log.storage();
        let mut table = Table::new();
        for stream in storage.list_streams().await.unwrap_or_default() {
            if stream.as_str().starts_with(INTERNAL_STREAM_PREFIX) {
                continue;
            }
            let Ok(cfg) = storage.stream_config(&stream).await else {
                continue;
            };
            let filters: Vec<SubjectFilter> = cfg
                .capture_subjects
                .iter()
                .filter_map(|f| SubjectFilter::parse(f).ok())
                .collect();
            if !filters.is_empty() {
                table.push((stream, filters));
            }
        }
        let table = Arc::new(table);
        *self.cache.write().unwrap() = Some(Cached {
            counter,
            built: Instant::now(),
            table: table.clone(),
        });
        table
    }

    /// A connection's capture pipeline: appends jobs in order (batching
    /// consecutive ones for the same stream) and publishes each job's
    /// acknowledgement to its reply subject. Ends when the sender drops,
    /// after the queued jobs.
    pub fn pipeline(log: Arc<Log>, bus: Arc<CoreBus>) -> mpsc::Sender<CaptureJob> {
        let (tx, rx) = mpsc::channel(PIPELINE_QUEUE);
        tokio::spawn(run_pipeline(log, bus, rx));
        tx
    }
}

/// One captured message.
#[derive(Debug)]
pub struct CaptureJob {
    pub stream: StreamName,
    pub record: Record,
    pub reply_to: Option<String>,
}

impl CaptureJob {
    /// The record for a core message: its subject, headers and value; a
    /// `Nats-Msg-Id` header becomes the record's `msg_id`.
    pub fn new(
        stream: StreamName,
        subject: &str,
        reply_to: Option<String>,
        mut headers: Vec<(String, String)>,
        value: Bytes,
    ) -> Self {
        let msg_id = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(NATS_MSG_ID_HEADER))
            .map(|(_, v)| v.clone());
        if let Some(id) = msg_id {
            headers.retain(|(k, _)| k != IDEMPOTENCY_HEADER);
            headers.push((IDEMPOTENCY_HEADER.to_string(), id));
        }
        Self {
            stream,
            record: Record {
                key: None,
                value,
                subject: subject.to_string(),
                headers,
                timestamp_ns: None,
            },
            reply_to,
        }
    }
}

async fn run_pipeline(log: Arc<Log>, bus: Arc<CoreBus>, mut rx: mpsc::Receiver<CaptureJob>) {
    let mut next: Option<CaptureJob> = None;
    loop {
        let first = match next.take() {
            Some(j) => j,
            None => match rx.recv().await {
                Some(j) => j,
                None => return,
            },
        };
        let mut group = vec![first];
        while group.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(j) if j.stream == group[0].stream => group.push(j),
                Ok(j) => {
                    next = Some(j);
                    break;
                }
                Err(_) => break,
            }
        }
        append_group(&log, &bus, group).await;
    }
}

async fn append_group(log: &Log, bus: &CoreBus, group: Vec<CaptureJob>) {
    let stream = group[0].stream.clone();
    let records: Vec<Record> = group.iter().map(|j| j.record.clone()).collect();
    match log.append_batch(&stream, records).await {
        Ok(results) => {
            for (job, r) in group.iter().zip(results) {
                ack(bus, job, Ok(r));
            }
        }
        // One record spoiled the batch before anything was written: append
        // one by one so only that one fails.
        Err(LogError::InvalidRecord(_))
        | Err(LogError::Storage(StorageError::KeyCollision { .. }))
            if group.len() > 1 =>
        {
            for job in group {
                let r = log.append(&job.stream, job.record.clone()).await;
                ack(bus, &job, r);
            }
        }
        Err(e) => {
            if group.iter().all(|j| j.reply_to.is_none()) {
                warn!(stream = %stream, error = %e, "dropped captured messages");
            }
            let (code, description) = error_code(&e);
            for job in &group {
                reply(
                    bus,
                    job,
                    json!({"error": {"code": code, "description": description}}),
                );
            }
        }
    }
}

fn ack(bus: &CoreBus, job: &CaptureJob, r: Result<AppendResult, LogError>) {
    let body = match r {
        Ok(AppendResult::Written(o, _)) => json!({"stream": job.stream.as_str(), "seq": o.0 + 1}),
        Ok(AppendResult::Duplicate(o)) => {
            json!({"stream": job.stream.as_str(), "seq": o.0 + 1, "duplicate": true})
        }
        Err(e) => {
            if job.reply_to.is_none() {
                warn!(stream = %job.stream, error = %e, "dropped a captured message");
            }
            let (code, description) = error_code(&e);
            json!({"error": {"code": code, "description": description}})
        }
    };
    reply(bus, job, body);
}

fn reply(bus: &CoreBus, job: &CaptureJob, body: serde_json::Value) {
    let Some(to) = &job.reply_to else { return };
    let _ = bus.publish(CoreMessage {
        subject: to.clone(),
        reply_to: None,
        headers: Vec::new(),
        value: Bytes::from(body.to_string()),
        origin: 0,
    });
}

fn error_code(e: &LogError) -> (u16, String) {
    let code = match e {
        LogError::NotLeader
        | LogError::DedupNotReady
        | LogError::NotEnoughReplicas { .. }
        | LogError::ReplicationTimeout => 503,
        LogError::InvalidRecord(_) | LogError::InvalidConfig(_) => 400,
        LogError::Storage(StorageError::StreamFull { .. }) => 429,
        LogError::Storage(StorageError::StreamNotFound(_)) => 404,
        LogError::Storage(StorageError::KeyCollision { .. }) => 409,
        LogError::Storage(_) => 500,
    };
    (code, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nats_msg_id_becomes_the_idempotency_key() {
        let s = StreamName::try_from("s").unwrap();
        let j = CaptureJob::new(
            s,
            "a.b",
            None,
            vec![
                ("nats-msg-id".into(), "m1".into()),
                ("x".into(), "y".into()),
            ],
            Bytes::from_static(b"v"),
        );
        assert!(j
            .record
            .headers
            .contains(&(IDEMPOTENCY_HEADER.to_string(), "m1".to_string())));
        assert!(j
            .record
            .headers
            .contains(&("x".to_string(), "y".to_string())));
        assert_eq!(j.record.subject, "a.b");
    }
}
