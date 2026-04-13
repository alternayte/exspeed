use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// Flag bit: record includes a key field.
const FLAG_HAS_KEY: u8 = 0x01;

/// RECORD delivery payload (0x82, server -> client push).
///
/// Wire format:
/// ```text
/// consumer_name(u16+utf8) + offset(u64 LE) + timestamp(u64 LE)
/// + subject(u16+utf8) + delivery_attempt(u16 LE) + flags(u8, bit0=has_key)
/// + [key(u32+bytes)] + value(u32+bytes)
/// + header_count(u16) + headers(u16+utf8 key, u16+utf8 val each)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordDelivery {
    pub consumer_name: String,
    pub offset: u64,
    pub timestamp: u64,
    pub subject: String,
    pub delivery_attempt: u16,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, String)>,
}

impl RecordDelivery {
    pub fn encode(&self, dst: &mut BytesMut) {
        // consumer_name
        let name_bytes = self.consumer_name.as_bytes();
        dst.put_u16_le(name_bytes.len() as u16);
        dst.extend_from_slice(name_bytes);

        // offset + timestamp
        dst.put_u64_le(self.offset);
        dst.put_u64_le(self.timestamp);

        // subject
        let subject_bytes = self.subject.as_bytes();
        dst.put_u16_le(subject_bytes.len() as u16);
        dst.extend_from_slice(subject_bytes);

        // delivery_attempt
        dst.put_u16_le(self.delivery_attempt);

        // flags
        let flags = if self.key.is_some() { FLAG_HAS_KEY } else { 0 };
        dst.put_u8(flags);

        // key (optional)
        if let Some(ref key) = self.key {
            dst.put_u32_le(key.len() as u32);
            dst.extend_from_slice(key);
        }

        // value
        dst.put_u32_le(self.value.len() as u32);
        dst.extend_from_slice(&self.value);

        // headers
        dst.put_u16_le(self.headers.len() as u16);
        for (k, v) in &self.headers {
            let kb = k.as_bytes();
            dst.put_u16_le(kb.len() as u16);
            dst.extend_from_slice(kb);
            let vb = v.as_bytes();
            dst.put_u16_le(vb.len() as u16);
            dst.extend_from_slice(vb);
        }
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        // consumer_name
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode("RECORD payload too short".into()));
        }
        let name_len = src.get_u16_le() as usize;
        if src.remaining() < name_len {
            return Err(ProtocolError::Decode(
                "RECORD truncated at consumer_name".into(),
            ));
        }
        let consumer_name = String::from_utf8(src.split_to(name_len).to_vec()).map_err(|e| {
            ProtocolError::Decode(format!("RECORD invalid consumer_name UTF-8: {e}"))
        })?;

        // offset + timestamp = 16 bytes
        if src.remaining() < 16 {
            return Err(ProtocolError::Decode(
                "RECORD truncated before offset/timestamp".into(),
            ));
        }
        let offset = src.get_u64_le();
        let timestamp = src.get_u64_le();

        // subject
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode(
                "RECORD truncated before subject".into(),
            ));
        }
        let subject_len = src.get_u16_le() as usize;
        if src.remaining() < subject_len {
            return Err(ProtocolError::Decode(
                "RECORD truncated at subject".into(),
            ));
        }
        let subject = String::from_utf8(src.split_to(subject_len).to_vec())
            .map_err(|e| ProtocolError::Decode(format!("RECORD invalid subject UTF-8: {e}")))?;

        // delivery_attempt + flags = 3 bytes
        if src.remaining() < 3 {
            return Err(ProtocolError::Decode(
                "RECORD truncated before delivery_attempt/flags".into(),
            ));
        }
        let delivery_attempt = src.get_u16_le();
        let flags = src.get_u8();

        // key (optional)
        let key = if flags & FLAG_HAS_KEY != 0 {
            if src.remaining() < 4 {
                return Err(ProtocolError::Decode(
                    "RECORD truncated before key length".into(),
                ));
            }
            let key_len = src.get_u32_le() as usize;
            if src.remaining() < key_len {
                return Err(ProtocolError::Decode("RECORD truncated at key".into()));
            }
            Some(src.split_to(key_len))
        } else {
            None
        };

        // value
        if src.remaining() < 4 {
            return Err(ProtocolError::Decode(
                "RECORD truncated before value length".into(),
            ));
        }
        let value_len = src.get_u32_le() as usize;
        if src.remaining() < value_len {
            return Err(ProtocolError::Decode("RECORD truncated at value".into()));
        }
        let value = src.split_to(value_len);

        // headers
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode(
                "RECORD truncated before header_count".into(),
            ));
        }
        let header_count = src.get_u16_le() as usize;
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            if src.remaining() < 2 {
                return Err(ProtocolError::Decode(
                    "RECORD truncated at header key length".into(),
                ));
            }
            let hk_len = src.get_u16_le() as usize;
            if src.remaining() < hk_len {
                return Err(ProtocolError::Decode(
                    "RECORD truncated at header key".into(),
                ));
            }
            let hk = String::from_utf8(src.split_to(hk_len).to_vec()).map_err(|e| {
                ProtocolError::Decode(format!("RECORD invalid header key UTF-8: {e}"))
            })?;

            if src.remaining() < 2 {
                return Err(ProtocolError::Decode(
                    "RECORD truncated at header value length".into(),
                ));
            }
            let hv_len = src.get_u16_le() as usize;
            if src.remaining() < hv_len {
                return Err(ProtocolError::Decode(
                    "RECORD truncated at header value".into(),
                ));
            }
            let hv = String::from_utf8(src.split_to(hv_len).to_vec()).map_err(|e| {
                ProtocolError::Decode(format!("RECORD invalid header value UTF-8: {e}"))
            })?;

            headers.push((hk, hv));
        }

        Ok(RecordDelivery {
            consumer_name,
            offset,
            timestamp,
            subject,
            delivery_attempt,
            key,
            value,
            headers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_delivery_roundtrip_with_key_and_headers() {
        let delivery = RecordDelivery {
            consumer_name: "my-consumer".into(),
            offset: 100,
            timestamp: 1_700_000_000,
            subject: "orders.created".into(),
            delivery_attempt: 3,
            key: Some(Bytes::from_static(b"order-123")),
            value: Bytes::from_static(b"{\"total\": 42.0}"),
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("trace-id".into(), "abc-123".into()),
            ],
        };
        let mut buf = BytesMut::new();
        delivery.encode(&mut buf);

        let decoded = RecordDelivery::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.consumer_name, "my-consumer");
        assert_eq!(decoded.offset, 100);
        assert_eq!(decoded.timestamp, 1_700_000_000);
        assert_eq!(decoded.subject, "orders.created");
        assert_eq!(decoded.delivery_attempt, 3);
        assert_eq!(decoded.key, Some(Bytes::from_static(b"order-123")));
        assert_eq!(decoded.value, Bytes::from_static(b"{\"total\": 42.0}"));
        assert_eq!(decoded.headers.len(), 2);
        assert_eq!(decoded.headers[0].0, "content-type");
        assert_eq!(decoded.headers[0].1, "application/json");
        assert_eq!(decoded.headers[1].0, "trace-id");
        assert_eq!(decoded.headers[1].1, "abc-123");
    }

    #[test]
    fn record_delivery_roundtrip_without_key() {
        let delivery = RecordDelivery {
            consumer_name: "simple".into(),
            offset: 0,
            timestamp: 12345,
            subject: "logs.info".into(),
            delivery_attempt: 1,
            key: None,
            value: Bytes::from_static(b"hello world"),
            headers: vec![],
        };
        let mut buf = BytesMut::new();
        delivery.encode(&mut buf);

        let decoded = RecordDelivery::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.consumer_name, "simple");
        assert_eq!(decoded.offset, 0);
        assert_eq!(decoded.timestamp, 12345);
        assert_eq!(decoded.subject, "logs.info");
        assert_eq!(decoded.delivery_attempt, 1);
        assert!(decoded.key.is_none());
        assert_eq!(decoded.value, Bytes::from_static(b"hello world"));
        assert!(decoded.headers.is_empty());
    }
}
