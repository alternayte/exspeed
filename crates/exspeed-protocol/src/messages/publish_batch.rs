use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::ProtocolError;

const FLAG_HAS_KEY: u8 = 0x01;
const FLAG_HAS_MSG_ID: u8 = 0x02;
const MAX_MSG_ID_BYTES: usize = 256;

/// A single record inside a PublishBatch request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatchRecord {
    pub subject: String,
    pub key: Option<Bytes>,
    pub msg_id: Option<String>,
    pub value: Bytes,
    pub headers: Vec<(String, String)>,
}

/// PUBLISH_BATCH request payload. All records target one stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatchRequest {
    pub stream: String,
    pub records: Vec<PublishBatchRecord>,
}

/// Per-record outcome in the PUBLISH_BATCH_OK response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchResult {
    Written { offset: u64 },
    Duplicate { offset: u64, duplicate_of: u64 },
    Error { code: u16, message: String },
}

/// PUBLISH_BATCH_OK response payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatchOkResponse {
    pub results: Vec<BatchResult>,
}

impl PublishBatchRequest {
    pub fn encode(&self, dst: &mut BytesMut) {
        let s = self.stream.as_bytes();
        dst.put_u16_le(s.len() as u16);
        dst.extend_from_slice(s);
        dst.put_u16_le(self.records.len() as u16);
        for r in &self.records {
            let sub = r.subject.as_bytes();
            dst.put_u16_le(sub.len() as u16);
            dst.extend_from_slice(sub);

            let mut flags: u8 = 0;
            if r.key.is_some() { flags |= FLAG_HAS_KEY; }
            if r.msg_id.is_some() { flags |= FLAG_HAS_MSG_ID; }
            dst.put_u8(flags);

            if let Some(ref k) = r.key {
                dst.put_u32_le(k.len() as u32);
                dst.extend_from_slice(k);
            }
            if let Some(ref id) = r.msg_id {
                let b = id.as_bytes();
                dst.put_u16_le(b.len() as u16);
                dst.extend_from_slice(b);
            }
            dst.put_u32_le(r.value.len() as u32);
            dst.extend_from_slice(&r.value);
            dst.put_u16_le(r.headers.len() as u16);
            for (hk, hv) in &r.headers {
                let kb = hk.as_bytes();
                dst.put_u16_le(kb.len() as u16);
                dst.extend_from_slice(kb);
                let vb = hv.as_bytes();
                dst.put_u16_le(vb.len() as u16);
                dst.extend_from_slice(vb);
            }
        }
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 4 {
            return Err(ProtocolError::Decode("PublishBatchRequest too short".into()));
        }
        let stream_len = src.get_u16_le() as usize;
        if src.remaining() < stream_len {
            return Err(ProtocolError::Decode("PublishBatchRequest truncated at stream".into()));
        }
        let stream = String::from_utf8(src.split_to(stream_len).to_vec())
            .map_err(|e| ProtocolError::Decode(format!("invalid stream UTF-8: {e}")))?;

        if src.remaining() < 2 {
            return Err(ProtocolError::Decode("PublishBatchRequest missing record_count".into()));
        }
        let record_count = src.get_u16_le() as usize;
        if record_count == 0 {
            return Err(ProtocolError::Decode("PublishBatchRequest has empty batch".into()));
        }

        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            if src.remaining() < 2 { return Err(ProtocolError::Decode("record missing subject_len".into())); }
            let sub_len = src.get_u16_le() as usize;
            if src.remaining() < sub_len { return Err(ProtocolError::Decode("record truncated at subject".into())); }
            let subject = String::from_utf8(src.split_to(sub_len).to_vec())
                .map_err(|e| ProtocolError::Decode(format!("invalid subject: {e}")))?;

            if src.remaining() < 1 { return Err(ProtocolError::Decode("record missing flags".into())); }
            let flags = src.get_u8();

            let key = if flags & FLAG_HAS_KEY != 0 {
                if src.remaining() < 4 { return Err(ProtocolError::Decode("record missing key_len".into())); }
                let kl = src.get_u32_le() as usize;
                if src.remaining() < kl { return Err(ProtocolError::Decode("record truncated at key".into())); }
                Some(src.split_to(kl))
            } else { None };

            let msg_id = if flags & FLAG_HAS_MSG_ID != 0 {
                if src.remaining() < 2 { return Err(ProtocolError::Decode("record missing msg_id_len".into())); }
                let ml = src.get_u16_le() as usize;
                if ml > MAX_MSG_ID_BYTES { return Err(ProtocolError::Decode(format!("msg_id {ml} > {MAX_MSG_ID_BYTES}"))); }
                if src.remaining() < ml { return Err(ProtocolError::Decode("record truncated at msg_id".into())); }
                Some(String::from_utf8(src.split_to(ml).to_vec())
                    .map_err(|e| ProtocolError::Decode(format!("invalid msg_id: {e}")))?)
            } else { None };

            if src.remaining() < 4 { return Err(ProtocolError::Decode("record missing value_len".into())); }
            let vl = src.get_u32_le() as usize;
            if src.remaining() < vl { return Err(ProtocolError::Decode("record truncated at value".into())); }
            let value = src.split_to(vl);

            if src.remaining() < 2 { return Err(ProtocolError::Decode("record missing header_count".into())); }
            let hc = src.get_u16_le() as usize;
            let mut headers = Vec::with_capacity(hc);
            for _ in 0..hc {
                if src.remaining() < 2 { return Err(ProtocolError::Decode("header missing key_len".into())); }
                let hkl = src.get_u16_le() as usize;
                if src.remaining() < hkl { return Err(ProtocolError::Decode("header truncated at key".into())); }
                let hk = String::from_utf8(src.split_to(hkl).to_vec())
                    .map_err(|e| ProtocolError::Decode(format!("invalid header key: {e}")))?;
                if src.remaining() < 2 { return Err(ProtocolError::Decode("header missing val_len".into())); }
                let hvl = src.get_u16_le() as usize;
                if src.remaining() < hvl { return Err(ProtocolError::Decode("header truncated at val".into())); }
                let hv = String::from_utf8(src.split_to(hvl).to_vec())
                    .map_err(|e| ProtocolError::Decode(format!("invalid header val: {e}")))?;
                headers.push((hk, hv));
            }
            records.push(PublishBatchRecord { subject, key, msg_id, value, headers });
        }
        Ok(PublishBatchRequest { stream, records })
    }
}

impl PublishBatchOkResponse {
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_u16_le(self.results.len() as u16);
        for r in &self.results {
            match r {
                BatchResult::Written { offset } => {
                    dst.put_u8(0);
                    dst.put_u64_le(*offset);
                }
                BatchResult::Duplicate { offset, duplicate_of } => {
                    dst.put_u8(1);
                    dst.put_u64_le(*offset);
                    dst.put_u64_le(*duplicate_of);
                }
                BatchResult::Error { code, message } => {
                    dst.put_u8(2);
                    dst.put_u16_le(*code);
                    let m = message.as_bytes();
                    dst.put_u16_le(m.len() as u16);
                    dst.extend_from_slice(m);
                }
            }
        }
    }

    pub fn decode(mut src: Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 2 { return Err(ProtocolError::Decode("PublishBatchOk missing count".into())); }
        let count = src.get_u16_le() as usize;
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            if src.remaining() < 1 { return Err(ProtocolError::Decode("result missing status".into())); }
            let status = src.get_u8();
            match status {
                0 => {
                    if src.remaining() < 8 { return Err(ProtocolError::Decode("Written truncated".into())); }
                    results.push(BatchResult::Written { offset: src.get_u64_le() });
                }
                1 => {
                    if src.remaining() < 16 { return Err(ProtocolError::Decode("Duplicate truncated".into())); }
                    let offset = src.get_u64_le();
                    let duplicate_of = src.get_u64_le();
                    results.push(BatchResult::Duplicate { offset, duplicate_of });
                }
                2 => {
                    if src.remaining() < 4 { return Err(ProtocolError::Decode("Error missing code/len".into())); }
                    let code = src.get_u16_le();
                    let ml = src.get_u16_le() as usize;
                    if src.remaining() < ml { return Err(ProtocolError::Decode("Error message truncated".into())); }
                    let message = String::from_utf8(src.split_to(ml).to_vec())
                        .map_err(|e| ProtocolError::Decode(format!("invalid error msg: {e}")))?;
                    results.push(BatchResult::Error { code, message });
                }
                other => return Err(ProtocolError::Decode(format!("unknown batch result status: {other}"))),
            }
        }
        Ok(PublishBatchOkResponse { results })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_batch_request_roundtrip() {
        let req = PublishBatchRequest {
            stream: "orders".into(),
            records: vec![
                PublishBatchRecord {
                    subject: "orders.placed".into(),
                    key: None,
                    msg_id: None,
                    value: Bytes::from_static(b"{\"a\":1}"),
                    headers: vec![],
                },
                PublishBatchRecord {
                    subject: "orders.placed".into(),
                    key: Some(Bytes::from_static(b"ord-123")),
                    msg_id: Some("m-1".into()),
                    value: Bytes::from_static(b"{\"b\":2}"),
                    headers: vec![("trace-id".into(), "abc".into())],
                },
            ],
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let decoded = PublishBatchRequest::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.stream, "orders");
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].subject, "orders.placed");
        assert_eq!(decoded.records[0].key, None);
        assert_eq!(decoded.records[1].key, Some(Bytes::from_static(b"ord-123")));
        assert_eq!(decoded.records[1].msg_id, Some("m-1".into()));
        assert_eq!(decoded.records[1].headers[0].0, "trace-id");
    }

    #[test]
    fn publish_batch_ok_mixed_results_roundtrip() {
        let resp = PublishBatchOkResponse {
            results: vec![
                BatchResult::Written { offset: 100 },
                BatchResult::Duplicate { offset: 50, duplicate_of: 42 },
                BatchResult::Error { code: 0x1001, message: "collision".into() },
            ],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf);
        let decoded = PublishBatchOkResponse::decode(buf.freeze()).unwrap();
        assert_eq!(decoded.results.len(), 3);
        match &decoded.results[0] { BatchResult::Written { offset } => assert_eq!(*offset, 100), _ => panic!("wrong variant") }
        match &decoded.results[1] { BatchResult::Duplicate { offset, duplicate_of } => { assert_eq!(*offset, 50); assert_eq!(*duplicate_of, 42); }, _ => panic!() }
        match &decoded.results[2] { BatchResult::Error { code, message } => { assert_eq!(*code, 0x1001); assert_eq!(message, "collision"); }, _ => panic!() }
    }

    #[test]
    fn publish_batch_empty_fails_decode() {
        let mut buf = BytesMut::new();
        let req = PublishBatchRequest { stream: "s".into(), records: vec![] };
        req.encode(&mut buf);
        let err = PublishBatchRequest::decode(buf.freeze()).unwrap_err();
        assert!(err.to_string().contains("empty batch"));
    }
}
