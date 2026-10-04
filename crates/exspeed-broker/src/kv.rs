//! Key-value buckets on top of streams.
//!
//! A bucket `B` is the stream `KV_B` with `max_msgs_per_subject` = the
//! bucket's history: each key is a subject, each put a record, and only the
//! newest `history` values of a key stay visible. Gets are O(1) through the
//! storage's per-subject index. A delete or purge appends a tombstone
//! (`exspeed-kv-op: DEL` / `PURGE`); a purge also hides the key's older
//! values from its history.
//!
//! A key's **revision** is its record's offset in the bucket's stream plus
//! one, so revision 0 always means "absent".
//!
//! **Compare-and-set.** A put or delete may name the revision it expects
//! the key to be at (0 = the key must not exist). Writes to a bucket
//! go through one lock per bucket and compare against the committed log
//! (including values still replicating), so concurrent compare-and-set
//! writers can't both win. Writes must use this API; a record published to
//! a bucket's stream directly bypasses the check.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use exspeed_common::msg_time::TTL_HEADER;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{ReadLimits, Record, StorageError, StoredRecord, StreamConfig};

use crate::broker_append::AppendResult;
use crate::log::{Log, LogError};

/// Header marking a tombstone: `DEL` or `PURGE`.
pub const KV_OP_HEADER: &str = "exspeed-kv-op";
/// Prefix of a bucket's stream name.
pub const BUCKET_STREAM_PREFIX: &str = "KV_";
/// Most values a bucket keeps per key.
pub const MAX_HISTORY: u64 = 64;

/// What a revision of a key holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KvOp {
    Put,
    Delete,
    Purge,
}

/// One revision of a key.
#[derive(Debug, Clone)]
pub struct KvEntry {
    pub key: String,
    pub value: Bytes,
    /// The record's offset in the bucket's stream plus one.
    pub revision: u64,
    /// Append time, nanoseconds since the Unix epoch.
    pub timestamp_ns: u64,
    pub op: KvOp,
    /// The record as stored (for returning it over the wire unchanged).
    pub record: StoredRecord,
}

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("invalid: {0}")]
    Invalid(String),
    #[error("bucket '{0}' not found")]
    BucketNotFound(String),
    #[error("stream '{0}' is not a KV bucket (it has no max_msgs_per_subject)")]
    NotABucket(String),
    #[error("key '{key}' is at revision {current}, not the expected {expected}")]
    WrongRevision {
        key: String,
        expected: u64,
        current: u64,
    },
    #[error(transparent)]
    Log(#[from] LogError),
}

impl From<StorageError> for KvError {
    fn from(e: StorageError) -> Self {
        KvError::Log(LogError::Storage(e))
    }
}

/// Settings of a new bucket.
#[derive(Debug, Clone, Copy)]
pub struct BucketConfig {
    /// Values kept per key (1..=64).
    pub history: u64,
    /// Every key expires this long after its last put (0 = never).
    pub ttl_ms: u64,
    /// Size limit of the bucket (0 = the server default).
    pub max_bytes: u64,
}

impl Default for BucketConfig {
    fn default() -> Self {
        Self {
            history: 1,
            ttl_ms: 0,
            max_bytes: 0,
        }
    }
}

/// The name of a bucket's stream.
pub fn bucket_stream(bucket: &str) -> Result<StreamName, KvError> {
    if bucket.is_empty()
        || !bucket
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(KvError::Invalid(format!(
            "bucket name '{bucket}' must be [A-Za-z0-9_-]+"
        )));
    }
    StreamName::try_from(format!("{BUCKET_STREAM_PREFIX}{bucket}").as_str())
        .map_err(|e| KvError::Invalid(e.to_string()))
}

/// Validate a key: dot-separated tokens of `[A-Za-z0-9_\-/=]`.
pub fn check_key(key: &str) -> Result<(), KvError> {
    let ok = !key.is_empty()
        && key.len() <= 1024
        && key.split('.').all(|t| {
            !t.is_empty()
                && t.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '/' | '='))
        });
    if ok {
        Ok(())
    } else {
        Err(KvError::Invalid(format!(
            "key '{key}' must be dot-separated tokens of [A-Za-z0-9_-/=]"
        )))
    }
}

fn op_of(r: &StoredRecord) -> KvOp {
    match r
        .headers
        .iter()
        .find(|(k, _)| k == KV_OP_HEADER)
        .map(|(_, v)| v.as_str())
    {
        Some("DEL") => KvOp::Delete,
        Some("PURGE") => KvOp::Purge,
        _ => KvOp::Put,
    }
}

fn entry(r: StoredRecord) -> KvEntry {
    KvEntry {
        key: r.subject.clone(),
        value: r.value.clone(),
        revision: r.offset.0 + 1,
        timestamp_ns: r.timestamp,
        op: op_of(&r),
        record: r,
    }
}

/// The KV API. Cheap to share.
pub struct Kv {
    log: Arc<Log>,
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl Kv {
    pub fn new(log: Arc<Log>) -> Arc<Self> {
        Arc::new(Self {
            log,
            locks: Mutex::new(HashMap::new()),
        })
    }

    fn lock_of(&self, stream: &StreamName) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .unwrap()
            .entry(stream.as_str().to_string())
            .or_default()
            .clone()
    }

    /// The stream config for a new bucket.
    pub fn bucket_stream_config(&self, cfg: &BucketConfig) -> Result<StreamConfig, KvError> {
        if cfg.history == 0 || cfg.history > MAX_HISTORY {
            return Err(KvError::Invalid(format!(
                "history must be between 1 and {MAX_HISTORY}"
            )));
        }
        let mut sc = self.log.default_stream_config();
        // Keys stay until deleted (or their TTL ends), however old.
        sc.max_age_secs = 100 * 365 * 24 * 3600;
        if cfg.max_bytes > 0 {
            sc.max_bytes = cfg.max_bytes;
        }
        sc.max_msgs_per_subject = cfg.history;
        sc.allow_msg_ttl = true;
        sc.msg_ttl_ms = cfg.ttl_ms;
        Ok(sc)
    }

    /// Create a bucket (idempotent for the same settings).
    pub async fn create_bucket(&self, bucket: &str, cfg: &BucketConfig) -> Result<(), KvError> {
        let stream = bucket_stream(bucket)?;
        let sc = self.bucket_stream_config(cfg)?;
        match self.log.create_stream(&stream, &sc).await {
            Ok(()) => Ok(()),
            Err(LogError::Storage(StorageError::StreamAlreadyExists(_))) => {
                let have = self.log.storage().stream_config(&stream).await?;
                if have.max_msgs_per_subject == cfg.history && have.msg_ttl_ms == cfg.ttl_ms {
                    Ok(())
                } else {
                    Err(KvError::Invalid(format!(
                        "bucket '{bucket}' exists with different settings"
                    )))
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// The bucket's stream, checked to be a bucket.
    pub async fn open(&self, bucket: &str) -> Result<StreamName, KvError> {
        let stream = bucket_stream(bucket)?;
        let cfg = match self.log.storage().stream_config(&stream).await {
            Ok(c) => c,
            Err(StorageError::StreamNotFound(_)) => {
                return Err(KvError::BucketNotFound(bucket.to_string()))
            }
            Err(e) => return Err(e.into()),
        };
        if cfg.max_msgs_per_subject == 0 {
            return Err(KvError::NotABucket(stream.as_str().to_string()));
        }
        Ok(stream)
    }

    async fn read_at(
        &self,
        stream: &StreamName,
        offset: u64,
    ) -> Result<Option<StoredRecord>, KvError> {
        let b = self
            .log
            .storage()
            .read_batch(
                stream,
                Offset(offset),
                ReadLimits {
                    max_records: 1,
                    max_bytes: 1,
                },
            )
            .await?;
        Ok(b.records
            .into_iter()
            .next()
            .filter(|r| r.offset.0 == offset))
    }

    /// The current value of `key` (`None` when absent or deleted), or the
    /// value at `revision` when it is still kept.
    pub async fn get(
        &self,
        bucket: &str,
        key: &str,
        revision: Option<u64>,
    ) -> Result<Option<KvEntry>, KvError> {
        check_key(key)?;
        let stream = self.open(bucket).await?;
        if let Some(r) = revision {
            if r == 0 {
                return Ok(None);
            }
            return Ok(self
                .read_at(&stream, r - 1)
                .await?
                .filter(|rec| rec.subject == key)
                .map(entry));
        }
        Ok(self
            .latest_record(&stream, key)
            .await?
            .map(entry)
            .filter(|e| e.op == KvOp::Put))
    }

    /// The newest visible record of `key`. A concurrent put can supersede
    /// the record found in the index before it is read; look again then.
    async fn latest_record(
        &self,
        stream: &StreamName,
        key: &str,
    ) -> Result<Option<StoredRecord>, KvError> {
        let storage = self.log.storage();
        let mut offset = match storage.latest_for_subject(stream, key) {
            Some(o) => o,
            None => return Ok(None),
        };
        for _ in 0..16 {
            if let Some(r) = self.read_at(stream, offset).await? {
                if r.subject == key {
                    return Ok(Some(r));
                }
            }
            match storage.latest_for_subject(stream, key) {
                Some(o) if o != offset => offset = o,
                // Still the newest but hidden: expired.
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    /// The revision `key` is at for compare-and-set: its newest record in
    /// the committed log, 0 when absent or deleted.
    async fn current_revision(&self, stream: &StreamName, key: &str) -> Result<u64, KvError> {
        let Some(o) = self.log.storage().latest_committed_for_subject(stream, key) else {
            return Ok(0);
        };
        let rec = self
            .log
            .storage()
            .read_batch_committed(
                stream,
                Offset(o),
                ReadLimits {
                    max_records: 1,
                    max_bytes: 1,
                },
            )
            .await?
            .records
            .into_iter()
            .next()
            .filter(|r| r.offset.0 == o);
        Ok(match rec {
            Some(r) if op_of(&r) == KvOp::Put => o + 1,
            _ => 0,
        })
    }

    async fn write(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        headers: Vec<(String, String)>,
        expected: Option<u64>,
    ) -> Result<u64, KvError> {
        check_key(key)?;
        let stream = self.open(bucket).await?;
        let lock = self.lock_of(&stream);
        let _g = lock.lock().await;
        if let Some(expected) = expected {
            let current = self.current_revision(&stream, key).await?;
            if current != expected {
                return Err(KvError::WrongRevision {
                    key: key.to_string(),
                    expected,
                    current,
                });
            }
        }
        let rec = Record {
            key: None,
            value,
            subject: key.to_string(),
            headers,
            timestamp_ns: None,
        };
        match self.log.append(&stream, rec).await? {
            AppendResult::Written(o, _) | AppendResult::Duplicate(o) => Ok(o.0 + 1),
        }
    }

    /// Set `key`. With `expected`, only when the key is at that revision (0 =
    /// absent). With `ttl_ms`, this value expires after that long. Returns
    /// the new revision.
    pub async fn put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        expected: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Result<u64, KvError> {
        let headers = match ttl_ms {
            Some(ms) if ms > 0 => vec![(TTL_HEADER.to_string(), format!("{ms}ms"))],
            _ => Vec::new(),
        };
        self.write(bucket, key, value, headers, expected).await
    }

    /// Delete `key` (a tombstone; `purge` also hides its older values).
    pub async fn delete(
        &self,
        bucket: &str,
        key: &str,
        purge: bool,
        expected: Option<u64>,
    ) -> Result<u64, KvError> {
        let op = if purge { "PURGE" } else { "DEL" };
        self.write(
            bucket,
            key,
            Bytes::new(),
            vec![(KV_OP_HEADER.to_string(), op.to_string())],
            expected,
        )
        .await
    }

    /// The keys that currently have a value, optionally only those matching
    /// a subject filter, sorted.
    pub async fn keys(&self, bucket: &str, filter: &str) -> Result<Vec<String>, KvError> {
        let stream = self.open(bucket).await?;
        let filter = exspeed_common::SubjectFilter::parse(filter).map_err(KvError::Invalid)?;
        let mut out = Vec::new();
        for (key, _) in self.log.storage().subjects_latest(&stream) {
            if !filter.matches(&key) {
                continue;
            }
            // Hidden (expired) or a tombstone: not a live key.
            if let Some(r) = self.latest_record(&stream, &key).await? {
                if op_of(&r) == KvOp::Put {
                    out.push(key);
                }
            }
        }
        Ok(out)
    }

    /// The kept revisions of `key`, oldest first, from its last purge on.
    pub async fn history(&self, bucket: &str, key: &str) -> Result<Vec<KvEntry>, KvError> {
        check_key(key)?;
        let stream = self.open(bucket).await?;
        let storage = self.log.storage();
        let mut out = Vec::new();
        let mut from = storage.stream_bounds(&stream).await?.0 .0;
        loop {
            let b = storage
                .read_batch(
                    &stream,
                    Offset(from),
                    ReadLimits {
                        max_records: 1000,
                        max_bytes: 8 << 20,
                    },
                )
                .await?;
            for r in b.records {
                if r.subject == key {
                    let e = entry(r);
                    if e.op == KvOp::Purge {
                        out.clear();
                    }
                    out.push(e);
                }
            }
            if b.next_offset.0 >= b.high_watermark.0 || b.next_offset.0 <= from {
                break;
            }
            from = b.next_offset.0;
        }
        Ok(out)
    }
}
