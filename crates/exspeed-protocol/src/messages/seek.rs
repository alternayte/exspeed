use crate::error::ProtocolError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone)]
pub struct SeekRequest {
    pub consumer_name: String,
    pub timestamp: u64,
}

impl SeekRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        let nb = self.consumer_name.as_bytes();
        dst.put_u16_le(nb.len() as u16);
        dst.extend_from_slice(nb);
        dst.put_u64_le(self.timestamp);
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode("SEEK too short".into()));
        }
        let name_len = src.get_u16_le() as usize;
        if src.remaining() < name_len + 8 {
            return Err(ProtocolError::Decode("SEEK truncated".into()));
        }
        let consumer_name = String::from_utf8(src.split_to(name_len).to_vec())
            .map_err(|e| ProtocolError::Decode(format!("SEEK name UTF-8: {e}")))?;
        let timestamp = src.get_u64_le();
        Ok(Self {
            consumer_name,
            timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seek_roundtrip() {
        let req = SeekRequest {
            consumer_name: "my-consumer".into(),
            timestamp: 1_700_000_000_000_000_000,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let decoded = SeekRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.consumer_name, "my-consumer");
        assert_eq!(decoded.timestamp, 1_700_000_000_000_000_000);
    }
}
