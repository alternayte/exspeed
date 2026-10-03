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

impl StoredRecord {
    /// Size of this record in its wire (= stored) encoding, see
    /// [`exspeed_common::record_format`]. Read byte budgets count this, so
    /// headers and per-record framing are included, not just the value.
    pub fn wire_size(&self) -> usize {
        exspeed_common::record_format::MIN_RECORD_LEN
            + self.subject.len()
            + self.key.as_ref().map_or(0, |k| 4 + k.len())
            + self.value.len()
            + self
                .headers
                .iter()
                .map(|(k, v)| 4 + k.len() + v.len())
                .sum::<usize>()
    }
}
