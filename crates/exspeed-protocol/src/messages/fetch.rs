use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// FETCH request payload.
///
/// Wire format:
/// ```text
/// stream(u16+utf8) + offset(u64 LE) + max_records(u32 LE) + subject_filter(u16+utf8)
/// ```
///
/// An empty `subject_filter` means "fetch all subjects".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    pub stream: String,
    pub offset: u64,
    pub max_records: u32,
    pub subject_filter: String,
}

impl FetchRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        // stream
        let stream_bytes = self.stream.as_bytes();
        dst.put_u16_le(stream_bytes.len() as u16);
        dst.extend_from_slice(stream_bytes);

        // offset
        dst.put_u64_le(self.offset);

        // max_records
        dst.put_u32_le(self.max_records);

        // subject_filter
        let filter_bytes = self.subject_filter.as_bytes();
        dst.put_u16_le(filter_bytes.len() as u16);
        dst.extend_from_slice(filter_bytes);
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        // stream
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode("FETCH payload too short".into()));
        }
        let stream_len = src.get_u16_le() as usize;
        if src.remaining() < stream_len {
            return Err(ProtocolError::Decode("FETCH truncated at stream".into()));
        }
        let stream = String::from_utf8(src.split_to(stream_len).to_vec())
            .map_err(|e| ProtocolError::Decode(format!("invalid stream UTF-8: {e}")))?;

        // offset + max_records = 8 + 4 = 12 bytes
        if src.remaining() < 12 {
            return Err(ProtocolError::Decode(
                "FETCH truncated before offset/max_records".into(),
            ));
        }
        let offset = src.get_u64_le();
        let max_records = src.get_u32_le();

        // subject_filter
        if src.remaining() < 2 {
            return Err(ProtocolError::Decode(
                "FETCH truncated before subject_filter".into(),
            ));
        }
        let filter_len = src.get_u16_le() as usize;
        if src.remaining() < filter_len {
            return Err(ProtocolError::Decode(
                "FETCH truncated at subject_filter".into(),
            ));
        }
        let subject_filter = String::from_utf8(src.split_to(filter_len).to_vec())
            .map_err(|e| ProtocolError::Decode(format!("invalid subject_filter UTF-8: {e}")))?;

        Ok(FetchRequest {
            stream,
            offset,
            max_records,
            subject_filter,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_roundtrip_with_filter() {
        let req = FetchRequest {
            stream: "orders".into(),
            offset: 100,
            max_records: 50,
            subject_filter: "orders.created".into(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = FetchRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.stream, "orders");
        assert_eq!(decoded.offset, 100);
        assert_eq!(decoded.max_records, 50);
        assert_eq!(decoded.subject_filter, "orders.created");
    }

    #[test]
    fn fetch_roundtrip_without_filter() {
        let req = FetchRequest {
            stream: "events".into(),
            offset: 0,
            max_records: 1000,
            subject_filter: String::new(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = FetchRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.stream, "events");
        assert_eq!(decoded.offset, 0);
        assert_eq!(decoded.max_records, 1000);
        assert_eq!(decoded.subject_filter, "");
    }
}
