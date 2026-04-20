use bytes::Bytes;
use exspeed_common::Offset;

#[derive(Debug, Clone, Default)]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub subject: String,
    pub headers: Vec<(String, String)>,
    /// Optional timestamp override in nanoseconds. When `Some`, the storage
    /// engine persists this exact value rather than minting a fresh one at
    /// append time. Used exclusively by the replication client to preserve
    /// the leader's timestamp so `seek_by_time` stays consistent across the
    /// cluster. All other callers (SDK publish path, connectors, SQL INSERT,
    /// tests) leave it `None`.
    pub timestamp_ns: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StoredRecord {
    pub offset: Offset,
    pub timestamp: u64,
    pub subject: String,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, String)>,
}
