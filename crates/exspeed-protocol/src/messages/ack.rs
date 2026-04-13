use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// ACK request payload (0x05).
///
/// Wire format: consumer_name(u16+utf8) + offset(u64 LE)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckRequest {
    pub consumer_name: String,
    pub offset: u64,
}

impl AckRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        let name_bytes = self.consumer_name.as_bytes();
        dst.put_u16_le(name_bytes.len() as u16);
        dst.extend_from_slice(name_bytes);
        dst.put_u64_le(self.offset);
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode("ACK payload too short".into()));
        }
        let name_len = src.get_u16_le() as usize;
        if src.remaining() < name_len {
            return Err(ProtocolError::Decode(
                "ACK truncated at consumer_name".into(),
            ));
        }
        let consumer_name = String::from_utf8(src.split_to(name_len).to_vec())
            .map_err(|e| ProtocolError::Decode(format!("ACK invalid consumer_name UTF-8: {e}")))?;

        if src.remaining() < 8 {
            return Err(ProtocolError::Decode("ACK truncated before offset".into()));
        }
        let offset = src.get_u64_le();

        Ok(AckRequest {
            consumer_name,
            offset,
        })
    }
}

/// NACK request payload (0x06).
///
/// Wire format: consumer_name(u16+utf8) + offset(u64 LE)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NackRequest {
    pub consumer_name: String,
    pub offset: u64,
}

impl NackRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        let name_bytes = self.consumer_name.as_bytes();
        dst.put_u16_le(name_bytes.len() as u16);
        dst.extend_from_slice(name_bytes);
        dst.put_u64_le(self.offset);
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode("NACK payload too short".into()));
        }
        let name_len = src.get_u16_le() as usize;
        if src.remaining() < name_len {
            return Err(ProtocolError::Decode(
                "NACK truncated at consumer_name".into(),
            ));
        }
        let consumer_name = String::from_utf8(src.split_to(name_len).to_vec())
            .map_err(|e| ProtocolError::Decode(format!("NACK invalid consumer_name UTF-8: {e}")))?;

        if src.remaining() < 8 {
            return Err(ProtocolError::Decode("NACK truncated before offset".into()));
        }
        let offset = src.get_u64_le();

        Ok(NackRequest {
            consumer_name,
            offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_roundtrip() {
        let req = AckRequest {
            consumer_name: "my-consumer".into(),
            offset: 999,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = AckRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.consumer_name, "my-consumer");
        assert_eq!(decoded.offset, 999);
    }

    #[test]
    fn nack_roundtrip() {
        let req = NackRequest {
            consumer_name: "retry-consumer".into(),
            offset: 42,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = NackRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.consumer_name, "retry-consumer");
        assert_eq!(decoded.offset, 42);
    }
}
