use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// Where a new consumer should start reading from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StartFrom {
    Earliest = 0,
    Latest = 1,
    Offset = 2,
}

impl TryFrom<u8> for StartFrom {
    type Error = ProtocolError;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            0 => Ok(StartFrom::Earliest),
            1 => Ok(StartFrom::Latest),
            2 => Ok(StartFrom::Offset),
            other => Err(ProtocolError::Decode(format!(
                "unknown StartFrom value: {other}"
            ))),
        }
    }
}

/// Decode a u16-length-prefixed UTF-8 string from the buffer.
fn decode_string(src: &mut Bytes, ctx: &str) -> Result<String, ProtocolError> {
    if src.remaining() < 2 {
        return Err(ProtocolError::Decode(format!(
            "{ctx} truncated before string length"
        )));
    }
    let len = src.get_u16_le() as usize;
    if src.remaining() < len {
        return Err(ProtocolError::Decode(format!(
            "{ctx} truncated at string data"
        )));
    }
    let raw = src.split_to(len);
    String::from_utf8(raw.to_vec())
        .map_err(|e| ProtocolError::Decode(format!("{ctx} invalid UTF-8: {e}")))
}

/// Encode a u16-length-prefixed UTF-8 string into the buffer.
fn encode_string(dst: &mut BytesMut, s: &str) {
    let b = s.as_bytes();
    dst.put_u16_le(b.len() as u16);
    dst.extend_from_slice(b);
}

/// CREATE_CONSUMER request payload (0x13).
///
/// Wire format:
/// ```text
/// name(u16+utf8) + stream(u16+utf8) + group(u16+utf8, empty=solo)
/// + subject_filter(u16+utf8, empty=all) + start_from(u8) + start_offset(u64 LE)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateConsumerRequest {
    pub name: String,
    pub stream: String,
    pub group: String,
    pub subject_filter: String,
    pub start_from: StartFrom,
    pub start_offset: u64,
}

impl CreateConsumerRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        encode_string(dst, &self.name);
        encode_string(dst, &self.stream);
        encode_string(dst, &self.group);
        encode_string(dst, &self.subject_filter);
        dst.put_u8(self.start_from as u8);
        dst.put_u64_le(self.start_offset);
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        let name = decode_string(&mut src, "CREATE_CONSUMER name")?;
        let stream = decode_string(&mut src, "CREATE_CONSUMER stream")?;
        let group = decode_string(&mut src, "CREATE_CONSUMER group")?;
        let subject_filter = decode_string(&mut src, "CREATE_CONSUMER subject_filter")?;

        if src.remaining() < 9 {
            return Err(ProtocolError::Decode(
                "CREATE_CONSUMER truncated at start_from/start_offset".into(),
            ));
        }
        let start_from = StartFrom::try_from(src.get_u8())?;
        let start_offset = src.get_u64_le();

        Ok(CreateConsumerRequest {
            name,
            stream,
            group,
            subject_filter,
            start_from,
            start_offset,
        })
    }
}

/// DELETE_CONSUMER request payload (0x14).
///
/// Wire format: name(u16+utf8)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerRequest {
    pub name: String,
}

impl DeleteConsumerRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        encode_string(dst, &self.name);
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        let name = decode_string(&mut src, "DELETE_CONSUMER")?;
        Ok(DeleteConsumerRequest { name })
    }
}

/// SUBSCRIBE request payload (0x03).
///
/// Wire format: consumer_name(u16+utf8)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeRequest {
    pub consumer_name: String,
}

impl SubscribeRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        encode_string(dst, &self.consumer_name);
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        let consumer_name = decode_string(&mut src, "SUBSCRIBE")?;
        Ok(SubscribeRequest { consumer_name })
    }
}

/// UNSUBSCRIBE request payload (0x04).
///
/// Wire format: consumer_name(u16+utf8)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeRequest {
    pub consumer_name: String,
}

impl UnsubscribeRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        encode_string(dst, &self.consumer_name);
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        let consumer_name = decode_string(&mut src, "UNSUBSCRIBE")?;
        Ok(UnsubscribeRequest { consumer_name })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_consumer_roundtrip_with_group() {
        let req = CreateConsumerRequest {
            name: "my-consumer".into(),
            stream: "orders".into(),
            group: "worker-pool".into(),
            subject_filter: "orders.created".into(),
            start_from: StartFrom::Offset,
            start_offset: 42,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = CreateConsumerRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.name, "my-consumer");
        assert_eq!(decoded.stream, "orders");
        assert_eq!(decoded.group, "worker-pool");
        assert_eq!(decoded.subject_filter, "orders.created");
        assert_eq!(decoded.start_from, StartFrom::Offset);
        assert_eq!(decoded.start_offset, 42);
    }

    #[test]
    fn create_consumer_roundtrip_solo() {
        let req = CreateConsumerRequest {
            name: "solo-consumer".into(),
            stream: "events".into(),
            group: String::new(),
            subject_filter: String::new(),
            start_from: StartFrom::Earliest,
            start_offset: 0,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = CreateConsumerRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.name, "solo-consumer");
        assert_eq!(decoded.stream, "events");
        assert_eq!(decoded.group, "");
        assert_eq!(decoded.subject_filter, "");
        assert_eq!(decoded.start_from, StartFrom::Earliest);
        assert_eq!(decoded.start_offset, 0);
    }

    #[test]
    fn create_consumer_roundtrip_latest() {
        let req = CreateConsumerRequest {
            name: "tail".into(),
            stream: "logs".into(),
            group: String::new(),
            subject_filter: String::new(),
            start_from: StartFrom::Latest,
            start_offset: 0,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = CreateConsumerRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.start_from, StartFrom::Latest);
    }

    #[test]
    fn delete_consumer_roundtrip() {
        let req = DeleteConsumerRequest {
            name: "old-consumer".into(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = DeleteConsumerRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.name, "old-consumer");
    }

    #[test]
    fn subscribe_roundtrip() {
        let req = SubscribeRequest {
            consumer_name: "my-consumer".into(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = SubscribeRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.consumer_name, "my-consumer");
    }

    #[test]
    fn unsubscribe_roundtrip() {
        let req = UnsubscribeRequest {
            consumer_name: "my-consumer".into(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = UnsubscribeRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.consumer_name, "my-consumer");
    }
}
