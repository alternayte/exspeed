use bytes::Bytes;
use exspeed_common::Offset;

#[derive(Debug, Clone)]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub subject: String,
    pub headers: Vec<(String, String)>,
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
