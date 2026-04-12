use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// Flag bit: payload includes a key field.
const FLAG_HAS_KEY: u8 = 0x01;

/// PUBLISH request payload.
///
/// Wire format:
/// ```text
/// stream(u16+utf8) + subject(u16+utf8) + flags(u8, bit0=has_key)
/// + [key(u32+bytes)] + value(u32+bytes)
/// + header_count(u16) + headers(u16+utf8 key, u16+utf8 val each)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRequest {
    pub stream: String,
    pub subject: String,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, String)>,
}

impl PublishRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        // stream
        let stream_bytes = self.stream.as_bytes();
        dst.put_u16_le(stream_bytes.len() as u16);
        dst.extend_from_slice(stream_bytes);

        // subject
        let subject_bytes = self.subject.as_bytes();
        dst.put_u16_le(subject_bytes.len() as u16);
        dst.extend_from_slice(subject_bytes);

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
        // stream
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode("PUBLISH payload too short".into()));
        }
        let stream_len = src.get_u16_le() as usize;
        if src.remaining() < stream_len {
            return Err(ProtocolError::Decode(
                "PUBLISH truncated at stream".into(),
            ));
        }
        let stream = String::from_utf8(src.split_to(stream_len).to_vec())
            .map_err(|e| ProtocolError::Decode(format!("invalid stream UTF-8: {e}")))?;

        // subject
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode(
                "PUBLISH truncated before subject".into(),
            ));
        }
        let subject_len = src.get_u16_le() as usize;
        if src.remaining() < subject_len {
            return Err(ProtocolError::Decode(
                "PUBLISH truncated at subject".into(),
            ));
        }
        let subject = String::from_utf8(src.split_to(subject_len).to_vec())
            .map_err(|e| ProtocolError::Decode(format!("invalid subject UTF-8: {e}")))?;

        // flags
        if src.remaining() < 1 {
            return Err(ProtocolError::Decode(
                "PUBLISH truncated before flags".into(),
            ));
        }
        let flags = src.get_u8();

        // key (optional)
        let key = if flags & FLAG_HAS_KEY != 0 {
            if src.remaining() < 4 {
                return Err(ProtocolError::Decode(
                    "PUBLISH truncated before key length".into(),
                ));
            }
            let key_len = src.get_u32_le() as usize;
            if src.remaining() < key_len {
                return Err(ProtocolError::Decode("PUBLISH truncated at key".into()));
            }
            Some(src.split_to(key_len))
        } else {
            None
        };

        // value
        if src.remaining() < 4 {
            return Err(ProtocolError::Decode(
                "PUBLISH truncated before value length".into(),
            ));
        }
        let value_len = src.get_u32_le() as usize;
        if src.remaining() < value_len {
            return Err(ProtocolError::Decode("PUBLISH truncated at value".into()));
        }
        let value = src.split_to(value_len);

        // headers
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode(
                "PUBLISH truncated before header_count".into(),
            ));
        }
        let header_count = src.get_u16_le() as usize;
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            if src.remaining() < 2 {
                return Err(ProtocolError::Decode(
                    "PUBLISH truncated at header key length".into(),
                ));
            }
            let hk_len = src.get_u16_le() as usize;
            if src.remaining() < hk_len {
                return Err(ProtocolError::Decode(
                    "PUBLISH truncated at header key".into(),
                ));
            }
            let hk = String::from_utf8(src.split_to(hk_len).to_vec())
                .map_err(|e| ProtocolError::Decode(format!("invalid header key UTF-8: {e}")))?;

            if src.remaining() < 2 {
                return Err(ProtocolError::Decode(
                    "PUBLISH truncated at header value length".into(),
                ));
            }
            let hv_len = src.get_u16_le() as usize;
            if src.remaining() < hv_len {
                return Err(ProtocolError::Decode(
                    "PUBLISH truncated at header value".into(),
                ));
            }
            let hv = String::from_utf8(src.split_to(hv_len).to_vec())
                .map_err(|e| ProtocolError::Decode(format!("invalid header value UTF-8: {e}")))?;

            headers.push((hk, hv));
        }

        Ok(PublishRequest {
            stream,
            subject,
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
    fn publish_roundtrip_with_key_and_headers() {
        let req = PublishRequest {
            stream: "orders".into(),
            subject: "orders.created".into(),
            key: Some(Bytes::from_static(b"order-123")),
            value: Bytes::from_static(b"{\"total\": 42.0}"),
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("trace-id".into(), "abc-123".into()),
            ],
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = PublishRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.stream, "orders");
        assert_eq!(decoded.subject, "orders.created");
        assert_eq!(decoded.key, Some(Bytes::from_static(b"order-123")));
        assert_eq!(decoded.value, Bytes::from_static(b"{\"total\": 42.0}"));
        assert_eq!(decoded.headers.len(), 2);
        assert_eq!(decoded.headers[0].0, "content-type");
        assert_eq!(decoded.headers[0].1, "application/json");
        assert_eq!(decoded.headers[1].0, "trace-id");
        assert_eq!(decoded.headers[1].1, "abc-123");
    }

    #[test]
    fn publish_roundtrip_without_key_or_headers() {
        let req = PublishRequest {
            stream: "logs".into(),
            subject: "logs.info".into(),
            key: None,
            value: Bytes::from_static(b"hello world"),
            headers: vec![],
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = PublishRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.stream, "logs");
        assert_eq!(decoded.subject, "logs.info");
        assert!(decoded.key.is_none());
        assert_eq!(decoded.value, Bytes::from_static(b"hello world"));
        assert!(decoded.headers.is_empty());
    }
}
