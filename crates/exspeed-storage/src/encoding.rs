//! On-disk record framing.
//!
//! Segments store each record in exactly the encoding the client protocol
//! uses for a `WireRecord` (see [`exspeed_common::record_format`] for the
//! byte layout): a `u32` length, a CRC32C over everything after the
//! `delivery_count` field, a `delivery_count` that is always 0 on disk, then
//! offset, timestamp (ns), subject, key, value and headers. Reads that
//! serve clients copy these bytes straight into response frames; this
//! module covers the paths that need decoded records (ExQL, connectors,
//! replication, compaction).

use bytes::Bytes;
use exspeed_common::record_format::{self, Fields};
use exspeed_common::Offset;
use exspeed_streams::{StorageError, StoredRecord};

/// Bytes needed to read a record's length field.
pub const LEN_FIELD: usize = 4;

/// Largest record the engine writes or accepts.
pub use record_format::MAX_RECORD_LEN as MAX_FRAME_LEN;
/// Smallest valid record.
pub use record_format::MIN_RECORD_LEN as MIN_FRAME_LEN;

/// A record that can't be represented in the on-disk format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeError(pub String);

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "record cannot be encoded: {}", self.0)
    }
}

impl std::error::Error for EncodeError {}

impl From<EncodeError> for StorageError {
    fn from(e: EncodeError) -> Self {
        StorageError::InvalidRecord(e.0)
    }
}

/// The fields of one record, borrowed.
#[derive(Debug, Clone, Copy)]
pub struct RecordRef<'a> {
    pub offset: u64,
    pub timestamp: u64,
    pub subject: &'a str,
    pub key: Option<&'a [u8]>,
    pub value: &'a [u8],
    pub headers: &'a [(String, String)],
}

impl<'a> RecordRef<'a> {
    pub fn from_stored(r: &'a StoredRecord) -> Self {
        Self {
            offset: r.offset.0,
            timestamp: r.timestamp,
            subject: &r.subject,
            key: r.key.as_deref(),
            value: &r.value,
            headers: &r.headers,
        }
    }
}

/// Append one complete record for `r` to `dst`. Returns its size. On error
/// `dst` is left exactly as it was.
pub fn encode_frame(dst: &mut Vec<u8>, r: RecordRef<'_>) -> Result<usize, EncodeError> {
    record_format::encode(
        dst,
        &Fields {
            offset: r.offset,
            timestamp_ns: r.timestamp,
            delivery_count: 0,
            subject: r.subject,
            key: r.key,
            value: r.value,
            headers: r.headers,
        },
    )
    .map_err(|e| EncodeError(e.0))
}

/// Total size of the record whose first bytes are `head` (at least
/// [`LEN_FIELD`] bytes), or an error when its length is out of bounds.
pub fn frame_size(head: &[u8]) -> Result<usize, String> {
    record_format::record_size(head).map_err(|e| e.0)
}

/// Verify the CRC of one complete record.
pub fn check_crc(raw: &[u8]) -> Result<(), String> {
    record_format::check_crc(raw).map_err(|e| e.0)
}

/// The offset and timestamp of a complete record.
pub fn frame_offset_ts(raw: &[u8]) -> (u64, u64) {
    (record_format::offset(raw), record_format::timestamp_ns(raw))
}

/// The key and the value length of a record, without decoding the rest.
pub fn frame_key_value_len(raw: &[u8]) -> Result<(Option<&[u8]>, usize), String> {
    record_format::key_and_value_len(raw).map_err(|e| e.0)
}

/// Decode one complete record. Key and value are zero-copy slices of `raw`.
pub fn decode_frame(raw: &Bytes) -> Result<StoredRecord, String> {
    let l = record_format::layout(raw).map_err(|e| e.0)?;
    Ok(StoredRecord {
        offset: Offset(l.offset),
        timestamp: l.timestamp_ns,
        subject: l.subject(raw).to_owned(),
        key: l.key.clone().map(|r| raw.slice(r)),
        value: raw.slice(l.value.clone()),
        headers: l.headers(raw),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec<'a>(headers: &'a [(String, String)], key: Option<&'a [u8]>) -> RecordRef<'a> {
        RecordRef {
            offset: 42,
            timestamp: 1_700_000_000,
            subject: "orders.created",
            key,
            value: b"hello world",
            headers,
        }
    }

    fn roundtrip(r: RecordRef<'_>) -> StoredRecord {
        let mut buf = Vec::new();
        let n = encode_frame(&mut buf, r).unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(frame_size(&buf[..LEN_FIELD]).unwrap(), buf.len());
        check_crc(&buf).unwrap();
        decode_frame(&Bytes::from(buf)).unwrap()
    }

    #[test]
    fn roundtrip_with_key_and_headers() {
        let headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("trace-id".to_string(), "abc123".to_string()),
        ];
        let s = roundtrip(rec(&headers, Some(b"my-key")));
        assert_eq!(s.offset, Offset(42));
        assert_eq!(s.timestamp, 1_700_000_000);
        assert_eq!(s.subject, "orders.created");
        assert_eq!(s.key.as_deref(), Some(&b"my-key"[..]));
        assert_eq!(&s.value[..], b"hello world");
        assert_eq!(s.headers, headers);
    }

    #[test]
    fn roundtrip_without_key() {
        let s = roundtrip(rec(&[], None));
        assert!(s.key.is_none());
        assert!(s.headers.is_empty());
    }

    #[test]
    fn crc_detects_corruption() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, rec(&[], None)).unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        assert!(check_crc(&buf).is_err());
    }

    #[test]
    fn oversized_fields_are_rejected_not_truncated() {
        let mut buf = vec![1, 2, 3];
        let subject = "a".repeat(70_000);
        let r = RecordRef {
            subject: &subject,
            ..rec(&[], None)
        };
        assert!(encode_frame(&mut buf, r).is_err());
        assert_eq!(buf, vec![1, 2, 3], "dst must be untouched on error");
    }

    #[test]
    fn corrupt_lengths_never_overallocate() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, rec(&[], Some(b"k"))).unwrap();
        // Claim a 4 GiB key.
        let key_len_at = record_format::SUBJECT_AT + 2 + "orders.created".len() + 1;
        buf[key_len_at..key_len_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_frame(&Bytes::from(buf)).is_err());
        assert!(frame_size(&u32::MAX.to_le_bytes()).is_err());
        assert!(frame_size(&[0u8; 4]).is_err());
    }
}
