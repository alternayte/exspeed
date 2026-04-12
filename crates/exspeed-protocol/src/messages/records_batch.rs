use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// Flag bit: record includes a key field.
const FLAG_HAS_KEY: u8 = 0x01;

/// A single record within a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRecord {
    pub offset: u64,
    pub timestamp: u64,
    pub subject: String,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, String)>,
}

/// RECORDS_BATCH server response payload.
///
/// Wire format:
/// ```text
/// record_count(u32 LE)
/// + each record: offset(u64 LE) + timestamp(u64 LE) + subject(u16+utf8)
///   + flags(u8, bit0=has_key) + [key(u32+bytes)] + value(u32+bytes)
///   + header_count(u16) + headers(u16+utf8 key, u16+utf8 val each)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordsBatch {
    pub records: Vec<BatchRecord>,
}

impl RecordsBatch {
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_u32_le(self.records.len() as u32);

        for record in &self.records {
            // offset + timestamp
            dst.put_u64_le(record.offset);
            dst.put_u64_le(record.timestamp);

            // subject
            let subject_bytes = record.subject.as_bytes();
            dst.put_u16_le(subject_bytes.len() as u16);
            dst.extend_from_slice(subject_bytes);

            // flags
            let flags = if record.key.is_some() {
                FLAG_HAS_KEY
            } else {
                0
            };
            dst.put_u8(flags);

            // key (optional)
            if let Some(ref key) = record.key {
                dst.put_u32_le(key.len() as u32);
                dst.extend_from_slice(key);
            }

            // value
            dst.put_u32_le(record.value.len() as u32);
            dst.extend_from_slice(&record.value);

            // headers
            dst.put_u16_le(record.headers.len() as u16);
            for (k, v) in &record.headers {
                let kb = k.as_bytes();
                dst.put_u16_le(kb.len() as u16);
                dst.extend_from_slice(kb);
                let vb = v.as_bytes();
                dst.put_u16_le(vb.len() as u16);
                dst.extend_from_slice(vb);
            }
        }
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 4 {
            return Err(ProtocolError::Decode(
                "RECORDS_BATCH payload too short".into(),
            ));
        }

        let record_count = src.get_u32_le() as usize;
        let mut records = Vec::with_capacity(record_count);

        for i in 0..record_count {
            // offset + timestamp = 16 bytes
            if src.remaining() < 16 {
                return Err(ProtocolError::Decode(format!(
                    "RECORDS_BATCH truncated at record {i} offset/timestamp"
                )));
            }
            let offset = src.get_u64_le();
            let timestamp = src.get_u64_le();

            // subject
            if src.remaining() < 2 {
                return Err(ProtocolError::Decode(format!(
                    "RECORDS_BATCH truncated at record {i} subject length"
                )));
            }
            let subject_len = src.get_u16_le() as usize;
            if src.remaining() < subject_len {
                return Err(ProtocolError::Decode(format!(
                    "RECORDS_BATCH truncated at record {i} subject"
                )));
            }
            let subject = String::from_utf8(src.split_to(subject_len).to_vec()).map_err(|e| {
                ProtocolError::Decode(format!("invalid subject UTF-8 in record {i}: {e}"))
            })?;

            // flags
            if src.remaining() < 1 {
                return Err(ProtocolError::Decode(format!(
                    "RECORDS_BATCH truncated at record {i} flags"
                )));
            }
            let flags = src.get_u8();

            // key (optional)
            let key = if flags & FLAG_HAS_KEY != 0 {
                if src.remaining() < 4 {
                    return Err(ProtocolError::Decode(format!(
                        "RECORDS_BATCH truncated at record {i} key length"
                    )));
                }
                let key_len = src.get_u32_le() as usize;
                if src.remaining() < key_len {
                    return Err(ProtocolError::Decode(format!(
                        "RECORDS_BATCH truncated at record {i} key"
                    )));
                }
                Some(src.split_to(key_len))
            } else {
                None
            };

            // value
            if src.remaining() < 4 {
                return Err(ProtocolError::Decode(format!(
                    "RECORDS_BATCH truncated at record {i} value length"
                )));
            }
            let value_len = src.get_u32_le() as usize;
            if src.remaining() < value_len {
                return Err(ProtocolError::Decode(format!(
                    "RECORDS_BATCH truncated at record {i} value"
                )));
            }
            let value = src.split_to(value_len);

            // headers
            if src.remaining() < 2 {
                return Err(ProtocolError::Decode(format!(
                    "RECORDS_BATCH truncated at record {i} header_count"
                )));
            }
            let header_count = src.get_u16_le() as usize;
            let mut headers = Vec::with_capacity(header_count);
            for j in 0..header_count {
                if src.remaining() < 2 {
                    return Err(ProtocolError::Decode(format!(
                        "RECORDS_BATCH truncated at record {i} header {j} key length"
                    )));
                }
                let hk_len = src.get_u16_le() as usize;
                if src.remaining() < hk_len {
                    return Err(ProtocolError::Decode(format!(
                        "RECORDS_BATCH truncated at record {i} header {j} key"
                    )));
                }
                let hk = String::from_utf8(src.split_to(hk_len).to_vec()).map_err(|e| {
                    ProtocolError::Decode(format!(
                        "invalid header key UTF-8 in record {i} header {j}: {e}"
                    ))
                })?;

                if src.remaining() < 2 {
                    return Err(ProtocolError::Decode(format!(
                        "RECORDS_BATCH truncated at record {i} header {j} value length"
                    )));
                }
                let hv_len = src.get_u16_le() as usize;
                if src.remaining() < hv_len {
                    return Err(ProtocolError::Decode(format!(
                        "RECORDS_BATCH truncated at record {i} header {j} value"
                    )));
                }
                let hv = String::from_utf8(src.split_to(hv_len).to_vec()).map_err(|e| {
                    ProtocolError::Decode(format!(
                        "invalid header value UTF-8 in record {i} header {j}: {e}"
                    ))
                })?;

                headers.push((hk, hv));
            }

            records.push(BatchRecord {
                offset,
                timestamp,
                subject,
                key,
                value,
                headers,
            });
        }

        Ok(RecordsBatch { records })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_batch_roundtrip_two_records() {
        let batch = RecordsBatch {
            records: vec![
                BatchRecord {
                    offset: 0,
                    timestamp: 1_700_000_000,
                    subject: "orders.created".into(),
                    key: Some(Bytes::from_static(b"order-1")),
                    value: Bytes::from_static(b"{\"id\":1}"),
                    headers: vec![("trace".into(), "t1".into())],
                },
                BatchRecord {
                    offset: 1,
                    timestamp: 1_700_000_001,
                    subject: "orders.updated".into(),
                    key: None,
                    value: Bytes::from_static(b"{\"id\":1,\"status\":\"paid\"}"),
                    headers: vec![],
                },
            ],
        };

        let mut buf = BytesMut::new();
        batch.encode(&mut buf);

        let decoded = RecordsBatch::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.records.len(), 2);

        let r0 = &decoded.records[0];
        assert_eq!(r0.offset, 0);
        assert_eq!(r0.timestamp, 1_700_000_000);
        assert_eq!(r0.subject, "orders.created");
        assert_eq!(r0.key, Some(Bytes::from_static(b"order-1")));
        assert_eq!(r0.value, Bytes::from_static(b"{\"id\":1}"));
        assert_eq!(r0.headers, vec![("trace".into(), "t1".into())]);

        let r1 = &decoded.records[1];
        assert_eq!(r1.offset, 1);
        assert_eq!(r1.timestamp, 1_700_000_001);
        assert_eq!(r1.subject, "orders.updated");
        assert!(r1.key.is_none());
        assert_eq!(
            r1.value,
            Bytes::from_static(b"{\"id\":1,\"status\":\"paid\"}")
        );
        assert!(r1.headers.is_empty());
    }

    #[test]
    fn records_batch_roundtrip_empty() {
        let batch = RecordsBatch {
            records: vec![],
        };

        let mut buf = BytesMut::new();
        batch.encode(&mut buf);

        let decoded = RecordsBatch::decode(buf.freeze()).unwrap();
        assert!(decoded.records.is_empty());
    }
}
