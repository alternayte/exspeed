use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// CREATE_STREAM request payload.
///
/// Payload: stream_name (u16 LE length + UTF-8 bytes) + max_age_secs (u64 LE) + max_bytes (u64 LE).
/// The retention fields are optional on decode: if not enough bytes remain, they default to 0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamRequest {
    pub stream_name: String,
    pub max_age_secs: u64,
    pub max_bytes: u64,
}

impl CreateStreamRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        let name_bytes = self.stream_name.as_bytes();
        dst.put_u16_le(name_bytes.len() as u16);
        dst.extend_from_slice(name_bytes);
        dst.put_u64_le(self.max_age_secs);
        dst.put_u64_le(self.max_bytes);
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

        // Read retention fields if present, otherwise default to 0
        let max_age_secs = if src.remaining() >= 8 {
            src.get_u64_le()
        } else {
            0
        };

        let max_bytes = if src.remaining() >= 8 {
            src.get_u64_le()
        } else {
            0
        };

        Ok(CreateStreamRequest {
            stream_name,
            max_age_secs,
            max_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_stream_roundtrip() {
        let req = CreateStreamRequest {
            stream_name: "orders.events".into(),
            max_age_secs: 0,
            max_bytes: 0,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = CreateStreamRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.stream_name, "orders.events");
        assert_eq!(decoded.max_age_secs, 0);
        assert_eq!(decoded.max_bytes, 0);
    }

    #[test]
    fn create_stream_with_retention() {
        let req = CreateStreamRequest {
            stream_name: "logs".into(),
            max_age_secs: 3600,
            max_bytes: 1_000_000,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = CreateStreamRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.stream_name, "logs");
        assert_eq!(decoded.max_age_secs, 3600);
        assert_eq!(decoded.max_bytes, 1_000_000);
    }

    #[test]
    fn create_stream_backward_compat_no_retention_bytes() {
        // Simulate an old-format payload with only stream name (no retention fields)
        let mut buf = BytesMut::new();
        let name = "legacy-stream";
        buf.put_u16_le(name.len() as u16);
        buf.extend_from_slice(name.as_bytes());

        let decoded = CreateStreamRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.stream_name, "legacy-stream");
        assert_eq!(decoded.max_age_secs, 0);
        assert_eq!(decoded.max_bytes, 0);
    }
}
