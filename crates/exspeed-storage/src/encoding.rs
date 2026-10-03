//! On-disk record framing.
//!
//! Every record is stored as one CRC32C-protected frame:
//!
//! ```text
//! frame   := len u32 LE  (= 4 + payload.len())
//!            crc u32 LE  (CRC32C of payload)
//!            payload
//! payload := offset      u64
//!            timestamp   u64            (nanoseconds since the epoch)
//!            subject_len u16, subject
//!            flags       u8             (bit 0: has_key)
//!            [key_len    u32, key]      (only when has_key)
//!            value_len   u32, value
//!            header_cnt  u16
//!            header_cnt × (k_len u16, k, v_len u16, v)
//! ```
//!
//! Encoders validate every length against its field width and return an
//! error instead of truncating. Decoders bounds-check every length against
//! the bytes actually present, so a corrupt length can never trigger a
//! huge allocation.

use bytes::{BufMut, Bytes};
use exspeed_common::Offset;
use exspeed_streams::{StorageError, StoredRecord};

/// Size of the `len` + `crc` frame header.
pub const FRAME_HEADER_LEN: usize = 8;

/// Smallest possible payload: offset + timestamp + subject_len + flags +
/// value_len + header_cnt.
pub const MIN_PAYLOAD_LEN: usize = 8 + 8 + 2 + 1 + 4 + 2;

/// Largest frame (`len` field value) the engine writes or accepts. Values
/// are limited to 8 MiB by the write path, so anything above this is
/// corruption.
pub const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

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

fn check_len(what: &str, len: usize, max: usize) -> Result<(), EncodeError> {
    if len > max {
        Err(EncodeError(format!(
            "{what} is {len} bytes; the limit is {max}"
        )))
    } else {
        Ok(())
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

/// Append one complete frame for `r` to `dst`. Returns the frame size. On
/// error `dst` is left exactly as it was.
pub fn encode_frame(dst: &mut Vec<u8>, r: RecordRef<'_>) -> Result<usize, EncodeError> {
    check_len("subject", r.subject.len(), u16::MAX as usize)?;
    if let Some(k) = r.key {
        check_len("key", k.len(), u32::MAX as usize)?;
    }
    check_len("value", r.value.len(), u32::MAX as usize)?;
    if r.headers.len() > u16::MAX as usize {
        return Err(EncodeError(format!(
            "{} headers; the limit is {}",
            r.headers.len(),
            u16::MAX
        )));
    }
    let mut payload_len = MIN_PAYLOAD_LEN + r.subject.len() + r.value.len();
    if let Some(k) = r.key {
        payload_len += 4 + k.len();
    }
    for (k, v) in r.headers {
        check_len("header key", k.len(), u16::MAX as usize)?;
        check_len("header value", v.len(), u16::MAX as usize)?;
        payload_len += 4 + k.len() + v.len();
    }
    let frame_len = 4 + payload_len;
    check_len("encoded record", frame_len, MAX_FRAME_LEN)?;

    let start = dst.len();
    dst.reserve(4 + frame_len);
    dst.put_u32_le(frame_len as u32);
    dst.put_u32_le(0); // CRC placeholder
    let payload_start = dst.len();
    dst.put_u64_le(r.offset);
    dst.put_u64_le(r.timestamp);
    dst.put_u16_le(r.subject.len() as u16);
    dst.put_slice(r.subject.as_bytes());
    dst.put_u8(u8::from(r.key.is_some()));
    if let Some(k) = r.key {
        dst.put_u32_le(k.len() as u32);
        dst.put_slice(k);
    }
    dst.put_u32_le(r.value.len() as u32);
    dst.put_slice(r.value);
    dst.put_u16_le(r.headers.len() as u16);
    for (k, v) in r.headers {
        dst.put_u16_le(k.len() as u16);
        dst.put_slice(k.as_bytes());
        dst.put_u16_le(v.len() as u16);
        dst.put_slice(v.as_bytes());
    }
    debug_assert_eq!(dst.len() - payload_start, payload_len);
    let crc = crc32c::crc32c(&dst[payload_start..]);
    dst[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
    Ok(dst.len() - start)
}

/// Parse a frame header. Returns the payload length, or an error when the
/// `len` field is out of bounds.
pub fn frame_payload_len(header: &[u8]) -> Result<usize, String> {
    let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    if !(4 + MIN_PAYLOAD_LEN..=MAX_FRAME_LEN).contains(&len) {
        return Err(format!("frame length {len} out of bounds"));
    }
    Ok(len - 4)
}

/// Verify the CRC of a frame whose header is `header` and payload `payload`.
pub fn check_crc(header: &[u8], payload: &[u8]) -> Result<(), String> {
    let stored = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let computed = crc32c::crc32c(payload);
    if stored != computed {
        return Err(format!(
            "CRC mismatch: stored {stored:#010x}, computed {computed:#010x}"
        ));
    }
    Ok(())
}

/// The offset and timestamp at the start of a payload.
pub fn payload_offset_ts(payload: &[u8]) -> (u64, u64) {
    (
        u64::from_le_bytes(payload[0..8].try_into().unwrap()),
        u64::from_le_bytes(payload[8..16].try_into().unwrap()),
    )
}

/// The key and the value length of a payload, without decoding the rest.
pub fn payload_key_value_len(payload: &[u8]) -> Result<(Option<&[u8]>, usize), String> {
    let mut c = Cursor {
        buf: payload,
        pos: 16,
    };
    let subject_len = c.u16()?;
    c.take(subject_len)?;
    let flags = c.u8()?;
    let key = if flags & 1 != 0 {
        let n = c.u32()?;
        Some(&payload[c.take(n)?])
    } else {
        None
    };
    let value_len = c.u32()?;
    Ok((key, value_len))
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<std::ops::Range<usize>, String> {
        if self.buf.len() - self.pos < n {
            return Err(format!(
                "truncated record: need {n} bytes at {}, have {}",
                self.pos,
                self.buf.len() - self.pos
            ));
        }
        let r = self.pos..self.pos + n;
        self.pos += n;
        Ok(r)
    }
    fn u8(&mut self) -> Result<u8, String> {
        let r = self.take(1)?;
        Ok(self.buf[r.start])
    }
    fn u16(&mut self) -> Result<usize, String> {
        let r = self.take(2)?;
        Ok(u16::from_le_bytes(self.buf[r].try_into().unwrap()) as usize)
    }
    fn u32(&mut self) -> Result<usize, String> {
        let r = self.take(4)?;
        Ok(u32::from_le_bytes(self.buf[r].try_into().unwrap()) as usize)
    }
    fn u64(&mut self) -> Result<u64, String> {
        let r = self.take(8)?;
        Ok(u64::from_le_bytes(self.buf[r].try_into().unwrap()))
    }
    fn str(&mut self, n: usize, what: &str) -> Result<String, String> {
        let r = self.take(n)?;
        std::str::from_utf8(&self.buf[r])
            .map(str::to_owned)
            .map_err(|e| format!("invalid {what} UTF-8: {e}"))
    }
}

/// Decode a payload (the bytes after the frame header). Key and value are
/// zero-copy slices of `payload`.
pub fn decode_payload(payload: &Bytes) -> Result<StoredRecord, String> {
    let mut c = Cursor {
        buf: payload,
        pos: 0,
    };
    let offset = c.u64()?;
    let timestamp = c.u64()?;
    let subject_len = c.u16()?;
    let subject = c.str(subject_len, "subject")?;
    let flags = c.u8()?;
    let key = if flags & 1 != 0 {
        let n = c.u32()?;
        Some(payload.slice(c.take(n)?))
    } else {
        None
    };
    let value_len = c.u32()?;
    let value = payload.slice(c.take(value_len)?);
    let header_cnt = c.u16()?;
    // Each header needs at least 4 bytes; never pre-allocate more than the
    // remaining bytes could hold.
    let mut headers = Vec::with_capacity(header_cnt.min((payload.len() - c.pos) / 4));
    for _ in 0..header_cnt {
        let kl = c.u16()?;
        let k = c.str(kl, "header key")?;
        let vl = c.u16()?;
        let v = c.str(vl, "header value")?;
        headers.push((k, v));
    }
    if c.pos != payload.len() {
        return Err(format!(
            "{} trailing bytes after record",
            payload.len() - c.pos
        ));
    }
    Ok(StoredRecord {
        offset: Offset(offset),
        timestamp,
        subject,
        key,
        value,
        headers,
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
        let plen = frame_payload_len(&buf[..8]).unwrap();
        assert_eq!(plen + 8, buf.len());
        check_crc(&buf[..8], &buf[8..]).unwrap();
        decode_payload(&Bytes::copy_from_slice(&buf[8..])).unwrap()
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
        assert!(check_crc(&buf[..8], &buf[8..]).is_err());
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

        let headers = vec![("k".to_string(), "v".repeat(70_000))];
        assert!(encode_frame(&mut buf, rec(&headers, None)).is_err());

        let many: Vec<(String, String)> = (0..70_000)
            .map(|_| (String::new(), String::new()))
            .collect();
        assert!(encode_frame(&mut buf, rec(&many, None)).is_err());
    }

    #[test]
    fn corrupt_lengths_never_overallocate() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, rec(&[], Some(b"k"))).unwrap();
        // Claim a 4 GiB key.
        let mut payload = buf[8..].to_vec();
        let key_len_at = 8 + 8 + 2 + "orders.created".len() + 1;
        payload[key_len_at..key_len_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_payload(&Bytes::from(payload)).is_err());
        // Frame length out of bounds.
        assert!(frame_payload_len(&u32::MAX.to_le_bytes().repeat(2)).is_err());
        assert!(frame_payload_len(&[0u8; 8]).is_err());
    }
}
