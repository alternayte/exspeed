//! Key-value buckets: [`Client::kv`] returns a handle to one bucket.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use bytes::Bytes;
use exspeed_protocol::client::{Request, Response, WireRecord};

use crate::{unexpected, Client, Error, Result};

/// Header marking a tombstone (`DEL` or `PURGE`).
pub const KV_OP_HEADER: &str = "exspeed-kv-op";

/// What a revision holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvOp {
    Put,
    Delete,
    Purge,
}

/// One revision of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvEntry {
    pub key: String,
    pub value: Bytes,
    /// The record's offset in the bucket's stream plus one (0 = absent).
    pub revision: u64,
    pub timestamp_ns: u64,
    pub op: KvOp,
}

impl KvEntry {
    fn from_record(r: WireRecord) -> Self {
        let op = match r
            .headers
            .iter()
            .find(|(k, _)| k == KV_OP_HEADER)
            .map(|(_, v)| v.as_str())
        {
            Some("DEL") => KvOp::Delete,
            Some("PURGE") => KvOp::Purge,
            _ => KvOp::Put,
        };
        Self {
            key: r.subject,
            value: r.value,
            revision: r.offset + 1,
            timestamp_ns: r.timestamp_ns,
            op,
        }
    }
}

/// Settings of a new bucket.
#[derive(Debug, Clone, Copy, Default)]
pub struct BucketOptions {
    /// Values kept per key (1..=64; 0 = 1).
    pub history: u64,
    /// Keys expire this long after their last put.
    pub ttl: Option<Duration>,
    /// Size limit (0 = server default).
    pub max_bytes: u64,
}

/// A key-value bucket.
#[derive(Clone)]
pub struct Kv {
    client: Client,
    bucket: String,
}

impl Client {
    /// A handle to the key-value bucket `bucket` (see [`Kv::create`]).
    pub fn kv(&self, bucket: &str) -> Kv {
        Kv {
            client: self.clone(),
            bucket: bucket.to_string(),
        }
    }
}

impl Kv {
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The bucket's stream.
    pub fn stream(&self) -> String {
        format!("KV_{}", self.bucket)
    }

    /// Create the bucket (idempotent for the same settings).
    pub async fn create(&self, opts: BucketOptions) -> Result<()> {
        self.client
            .request_ok(Request::KvCreateBucket {
                bucket: self.bucket.clone(),
                history: opts.history,
                ttl_ms: opts.ttl.map_or(0, |t| t.as_millis() as u64),
                max_bytes: opts.max_bytes,
            })
            .await
    }

    /// Delete the bucket and every key in it.
    pub async fn destroy(&self) -> Result<()> {
        self.client.delete_stream(&self.stream()).await
    }

    async fn write(&self, req: Request) -> Result<u64> {
        match self.client.request(req).await? {
            Response::PublishOk { offset, .. } => Ok(offset),
            other => Err(unexpected(other)),
        }
    }

    /// Set `key`; returns the new revision.
    pub async fn put(&self, key: &str, value: impl Into<Bytes>) -> Result<u64> {
        self.put_with(key, value, None, None).await
    }

    /// Set `key` only if it doesn't exist (or was deleted). Fails with code
    /// 409 otherwise.
    pub async fn create_key(&self, key: &str, value: impl Into<Bytes>) -> Result<u64> {
        self.put_with(key, value, Some(0), None).await
    }

    /// Set `key` only if it is at `revision` (compare-and-set). Fails with
    /// code 409 otherwise; the error detail has `current_revision`.
    pub async fn update(&self, key: &str, value: impl Into<Bytes>, revision: u64) -> Result<u64> {
        self.put_with(key, value, Some(revision), None).await
    }

    /// Set `key` with an optional expected revision and TTL.
    pub async fn put_with(
        &self,
        key: &str,
        value: impl Into<Bytes>,
        expected_revision: Option<u64>,
        ttl: Option<Duration>,
    ) -> Result<u64> {
        self.write(Request::KvPut {
            bucket: self.bucket.clone(),
            key: key.to_string(),
            value: value.into(),
            expected_revision,
            ttl_ms: ttl.map(|t| t.as_millis().max(1) as u64),
        })
        .await
    }

    /// The current value of `key`, `None` when absent or deleted.
    pub async fn get(&self, key: &str) -> Result<Option<KvEntry>> {
        self.get_revision_inner(key, None).await
    }

    /// `key` at `revision`, while the bucket still keeps it.
    pub async fn get_revision(&self, key: &str, revision: u64) -> Result<Option<KvEntry>> {
        self.get_revision_inner(key, Some(revision)).await
    }

    async fn get_revision_inner(
        &self,
        key: &str,
        revision: Option<u64>,
    ) -> Result<Option<KvEntry>> {
        let r = self
            .client
            .request(Request::KvGet {
                bucket: self.bucket.clone(),
                key: key.to_string(),
                revision,
            })
            .await;
        match r {
            Ok(Response::Messages { records }) => {
                Ok(records.into_iter().next().map(KvEntry::from_record))
            }
            Ok(other) => Err(unexpected(other)),
            Err(e) if e.code() == Some(exspeed_protocol::client::code::NOT_FOUND) => match e {
                Error::Server { ref message, .. } if message.starts_with("key ") => Ok(None),
                _ => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    /// Delete `key` (its history stays until it ages out).
    pub async fn delete(&self, key: &str) -> Result<u64> {
        self.delete_inner(key, false, None).await
    }

    /// Delete `key` and hide its older values.
    pub async fn purge(&self, key: &str) -> Result<u64> {
        self.delete_inner(key, true, None).await
    }

    async fn delete_inner(&self, key: &str, purge: bool, expected: Option<u64>) -> Result<u64> {
        self.write(Request::KvDelete {
            bucket: self.bucket.clone(),
            key: key.to_string(),
            purge,
            expected_revision: expected,
        })
        .await
    }

    /// Keys that have a value, matching `filter` ("" = all), sorted.
    pub async fn keys(&self, filter: &str) -> Result<Vec<String>> {
        self.client
            .request_json(Request::KvKeys {
                bucket: self.bucket.clone(),
                filter: filter.to_string(),
            })
            .await
    }

    /// Kept revisions of `key`, oldest first (deletes included).
    pub async fn history(&self, key: &str) -> Result<Vec<KvEntry>> {
        match self
            .client
            .request(Request::KvHistory {
                bucket: self.bucket.clone(),
                key: key.to_string(),
            })
            .await?
        {
            Response::Messages { records } => {
                Ok(records.into_iter().map(KvEntry::from_record).collect())
            }
            other => Err(unexpected(other)),
        }
    }

    /// Watch keys matching `filter` ("" = all): first the current value of
    /// every matching key (deleted keys left out), then every change as it
    /// happens.
    pub fn watch(&self, filter: &str) -> KvWatch {
        KvWatch {
            client: self.client.clone(),
            stream: self.stream(),
            filter: filter.to_string(),
            next: 0,
            initial_end: None,
            pending: VecDeque::new(),
        }
    }
}

/// Changes to a bucket's keys, from [`Kv::watch`].
pub struct KvWatch {
    client: Client,
    stream: String,
    filter: String,
    next: u64,
    /// End of the initial snapshot; `None` until it is known, `Some(0)` once
    /// the snapshot was delivered.
    initial_end: Option<u64>,
    pending: VecDeque<KvEntry>,
}

impl KvWatch {
    /// The next entry, waiting for changes as long as it takes.
    pub async fn next(&mut self) -> Result<KvEntry> {
        loop {
            if let Some(e) = self.pending.pop_front() {
                return Ok(e);
            }
            if self.initial_end != Some(0) {
                self.load_snapshot().await?;
                continue;
            }
            let r = self
                .client
                .read(
                    &self.stream,
                    self.next,
                    1000,
                    Duration::from_secs(10),
                    &self.filter,
                )
                .await?;
            self.next = r.next_offset.max(self.next);
            self.pending
                .extend(r.records.into_iter().map(KvEntry::from_record));
        }
    }

    /// Like [`next`](Self::next), giving up after `timeout`.
    pub async fn next_timeout(&mut self, timeout: Duration) -> Option<Result<KvEntry>> {
        tokio::time::timeout(timeout, self.next()).await.ok()
    }

    async fn load_snapshot(&mut self) -> Result<()> {
        let mut latest: BTreeMap<String, KvEntry> = BTreeMap::new();
        loop {
            let r = self
                .client
                .read(&self.stream, self.next, 1000, Duration::ZERO, &self.filter)
                .await?;
            let end = *self.initial_end.get_or_insert(r.high_watermark);
            for rec in r.records {
                let e = KvEntry::from_record(rec);
                latest.insert(e.key.clone(), e);
            }
            self.next = r.next_offset.max(self.next);
            if self.next >= end {
                break;
            }
        }
        let mut snapshot: Vec<KvEntry> =
            latest.into_values().filter(|e| e.op == KvOp::Put).collect();
        snapshot.sort_by_key(|e| e.revision);
        self.pending.extend(snapshot);
        self.initial_end = Some(0);
        Ok(())
    }
}
