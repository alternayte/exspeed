// Built in Task 2

use bytes::{Buf, BufMut, Bytes};
use exspeed_common::Offset;
use exspeed_streams::record::{Record, StoredRecord};

/// Encode a record into `dst`.
///
/// Wire layout (all integers little-endian):
///   offset      u64
///   timestamp   u64
///   subject_len u16
///   subject     [u8; subject_len]
///   flags       u8   (bit 0: has_key)
///   [if has_key]
///     key_len   u32
///     key       [u8; key_len]
///   value_len   u32
///   value       [u8; value_len]
///   header_cnt  u16
///   [for each header]
///     k_len     u16
///     k         [u8; k_len]
///     v_len     u16
///     v         [u8; v_len]
pub fn encode_record(offset: Offset, timestamp: u64, record: &Record, dst: &mut Vec<u8>) {
    dst.put_u64_le(offset.0);
    dst.put_u64_le(timestamp);

    let subject_bytes = record.subject.as_bytes();
    dst.put_u16_le(subject_bytes.len() as u16);
    dst.put_slice(subject_bytes);

    let has_key: u8 = if record.key.is_some() { 1 } else { 0 };
    dst.put_u8(has_key);

    if let Some(key) = &record.key {
        dst.put_u32_le(key.len() as u32);
        dst.put_slice(key);
    }

    dst.put_u32_le(record.value.len() as u32);
    dst.put_slice(&record.value);

    dst.put_u16_le(record.headers.len() as u16);
    for (k, v) in &record.headers {
        let kb = k.as_bytes();
        let vb = v.as_bytes();
        dst.put_u16_le(kb.len() as u16);
        dst.put_slice(kb);
        dst.put_u16_le(vb.len() as u16);
        dst.put_slice(vb);
    }
}

/// Decode a record from `src`. Returns `(StoredRecord, bytes_consumed)`.
pub fn decode_record(src: &[u8]) -> Result<(StoredRecord, usize), String> {
    let mut cur = src;

    macro_rules! need {
        ($n:expr) => {
            if cur.remaining() < $n {
                return Err(format!(
                    "truncated record: need {} bytes, have {}",
                    $n,
                    cur.remaining()
                ));
            }
        };
    }

    need!(8);
    let offset = Offset(cur.get_u64_le());

    need!(8);
    let timestamp = cur.get_u64_le();

    need!(2);
    let subject_len = cur.get_u16_le() as usize;
    need!(subject_len);
    let subject_bytes = &cur[..subject_len];
    let subject = std::str::from_utf8(subject_bytes)
        .map_err(|e| format!("invalid subject UTF-8: {}", e))?
        .to_string();
    cur.advance(subject_len);

    need!(1);
    let flags = cur.get_u8();
    let has_key = (flags & 1) != 0;

    let key = if has_key {
        need!(4);
        let key_len = cur.get_u32_le() as usize;
        need!(key_len);
        let key_bytes = Bytes::copy_from_slice(&cur[..key_len]);
        cur.advance(key_len);
        Some(key_bytes)
    } else {
        None
    };

    need!(4);
    let value_len = cur.get_u32_le() as usize;
    need!(value_len);
    let value = Bytes::copy_from_slice(&cur[..value_len]);
    cur.advance(value_len);

    need!(2);
    let header_cnt = cur.get_u16_le() as usize;
    let mut headers = Vec::with_capacity(header_cnt);
    for _ in 0..header_cnt {
        need!(2);
        let k_len = cur.get_u16_le() as usize;
        need!(k_len);
        let k = std::str::from_utf8(&cur[..k_len])
            .map_err(|e| format!("invalid header key UTF-8: {}", e))?
            .to_string();
        cur.advance(k_len);

        need!(2);
        let v_len = cur.get_u16_le() as usize;
        need!(v_len);
        let v = std::str::from_utf8(&cur[..v_len])
            .map_err(|e| format!("invalid header value UTF-8: {}", e))?
            .to_string();
        cur.advance(v_len);

        headers.push((k, v));
    }

    let bytes_consumed = src.len() - cur.remaining();
    Ok((
        StoredRecord {
            offset,
            timestamp,
            subject,
            key,
            value,
            headers,
        },
        bytes_consumed,
    ))
}

/// Wrap `record_bytes` with a length-prefixed CRC32C frame:
///   length  u32 LE  (= 4 [CRC] + record_bytes.len())
///   crc     u32 LE  (CRC32C of record_bytes)
///   record_bytes
pub fn wrap_with_crc(record_bytes: &[u8]) -> Vec<u8> {
    let crc = crc32c::crc32c(record_bytes);
    let length = (4u32 + record_bytes.len() as u32).to_le_bytes();
    let crc_bytes = crc.to_le_bytes();

    let mut out = Vec::with_capacity(4 + 4 + record_bytes.len());
    out.extend_from_slice(&length);
    out.extend_from_slice(&crc_bytes);
    out.extend_from_slice(record_bytes);
    out
}

/// Validate and unwrap a CRC frame. `data` is the bytes **after** the length
/// field, i.e. `[crc (4 bytes)] ++ [record bytes]`.
///
/// Returns a slice of the record bytes on success.
pub fn unwrap_crc(data: &[u8]) -> Result<&[u8], String> {
    if data.len() < 4 {
        return Err(format!(
            "CRC frame too short: need at least 4 bytes, got {}",
            data.len()
        ));
    }
    let stored_crc = u32::from_le_bytes(data[..4].try_into().unwrap());
    let record_bytes = &data[4..];
    let computed = crc32c::crc32c(record_bytes);
    if stored_crc != computed {
        return Err(format!(
            "CRC mismatch: stored {:#010x}, computed {:#010x}",
            stored_crc, computed
        ));
    }
    Ok(record_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use exspeed_streams::record::Record;

    fn make_record_with_key() -> Record {
        Record {
            key: Some(Bytes::from_static(b"my-key")),
            value: Bytes::from_static(b"hello world"),
            subject: "orders.created".to_string(),
            headers: vec![
                ("content-type".to_string(), "application/json".to_string()),
                ("trace-id".to_string(), "abc123".to_string()),
            ],
        }
    }

    fn make_record_no_key() -> Record {
        Record {
            key: None,
            value: Bytes::from_static(b"no key here"),
            subject: "events.misc".to_string(),
            headers: vec![],
        }
    }

    #[test]
    fn encode_decode_roundtrip_with_key() {
        let record = make_record_with_key();
        let offset = Offset(42);
        let timestamp = 1_700_000_000u64;

        let mut buf = Vec::new();
        encode_record(offset, timestamp, &record, &mut buf);

        let (stored, consumed) = decode_record(&buf).expect("decode should succeed");
        assert_eq!(consumed, buf.len());
        assert_eq!(stored.offset, offset);
        assert_eq!(stored.timestamp, timestamp);
        assert_eq!(stored.subject, record.subject);
        assert_eq!(stored.key, record.key);
        assert_eq!(stored.value, record.value);
        assert_eq!(stored.headers, record.headers);
    }

    #[test]
    fn encode_decode_roundtrip_no_key() {
        let record = make_record_no_key();
        let offset = Offset(0);
        let timestamp = 999u64;

        let mut buf = Vec::new();
        encode_record(offset, timestamp, &record, &mut buf);

        let (stored, consumed) = decode_record(&buf).expect("decode should succeed");
        assert_eq!(consumed, buf.len());
        assert_eq!(stored.offset, offset);
        assert_eq!(stored.timestamp, timestamp);
        assert_eq!(stored.subject, record.subject);
        assert!(stored.key.is_none());
        assert_eq!(stored.value, record.value);
        assert!(stored.headers.is_empty());
    }

    #[test]
    fn crc_wrap_unwrap_roundtrip() {
        let record = make_record_with_key();
        let mut buf = Vec::new();
        encode_record(Offset(1), 12345, &record, &mut buf);

        let framed = wrap_with_crc(&buf);
        // framed = [length u32][crc u32][record bytes]
        // unwrap_crc receives the portion after the length field
        let after_length = &framed[4..];
        let record_bytes = unwrap_crc(after_length).expect("CRC should be valid");
        assert_eq!(record_bytes, buf.as_slice());
    }

    #[test]
    fn crc_detects_corruption() {
        let record = make_record_no_key();
        let mut buf = Vec::new();
        encode_record(Offset(7), 54321, &record, &mut buf);

        let mut framed = wrap_with_crc(&buf);
        // Flip a byte in the record portion (after the 4-byte length + 4-byte CRC)
        let corrupt_idx = framed.len() - 1;
        framed[corrupt_idx] ^= 0xFF;

        let after_length = &framed[4..];
        let result = unwrap_crc(after_length);
        assert!(result.is_err(), "expected CRC error but got Ok");
    }
}
