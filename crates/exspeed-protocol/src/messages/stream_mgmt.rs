use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// CREATE_STREAM request payload.
///
/// Payload: stream_name (u16 LE length + UTF-8 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamRequest {
    pub stream_name: String,
}

impl CreateStreamRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        let name_bytes = self.stream_name.as_bytes();
        dst.put_u16_le(name_bytes.len() as u16);
        dst.extend_from_slice(name_bytes);
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode(
                "CREATE_STREAM payload too short".into(),
            ));
        }

        let name_len = src.get_u16_le() as usize;

        if src.remaining() < name_len {
            return Err(ProtocolError::Decode(
                "CREATE_STREAM payload truncated at stream_name".into(),
            ));
        }

        let name_bytes = src.split_to(name_len);
        let stream_name = String::from_utf8(name_bytes.to_vec())
            .map_err(|e| ProtocolError::Decode(format!("invalid stream_name UTF-8: {e}")))?;

        Ok(CreateStreamRequest { stream_name })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_stream_roundtrip() {
        let req = CreateStreamRequest {
            stream_name: "orders.events".into(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = CreateStreamRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.stream_name, "orders.events");
    }
}
