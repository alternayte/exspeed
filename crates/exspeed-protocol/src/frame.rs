use bytes::{Buf, BufMut, Bytes, BytesMut};
use exspeed_common::{FRAME_HEADER_SIZE, MAX_PAYLOAD_SIZE, PROTOCOL_VERSION};

use crate::error::ProtocolError;
use crate::opcodes::OpCode;

/// A parsed wire protocol frame.
///
/// ```text
/// ┌──────────┬──────────┬──────────────┬────────────┬─────────┐
/// │ Version  │ OpCode   │ CorrelID(4)  │ Length(4)  │ Payload │
/// │ (1 byte) │ (1 byte) │ uint32 LE    │ uint32 LE  │ (var)   │
/// └──────────┴──────────┴──────────────┴────────────┴─────────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub opcode: OpCode,
    pub correlation_id: u32,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(opcode: OpCode, correlation_id: u32, payload: Bytes) -> Self {
        Self {
            opcode,
            correlation_id,
            payload,
        }
    }

    pub fn empty(opcode: OpCode, correlation_id: u32) -> Self {
        Self::new(opcode, correlation_id, Bytes::new())
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        dst.reserve(FRAME_HEADER_SIZE + self.payload.len());
        dst.put_u8(PROTOCOL_VERSION);
        dst.put_u8(self.opcode.as_u8());
        dst.put_u32_le(self.correlation_id);
        dst.put_u32_le(self.payload.len() as u32);
        dst.extend_from_slice(&self.payload);
    }

    pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, ProtocolError> {
        if src.len() < FRAME_HEADER_SIZE {
            return Ok(None);
        }

        let version = src[0];
        let opcode_byte = src[1];
        let correl_id = u32::from_le_bytes([src[2], src[3], src[4], src[5]]);
        let payload_len = u32::from_le_bytes([src[6], src[7], src[8], src[9]]);

        if version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(version));
        }

        let opcode = OpCode::try_from(opcode_byte)?;

        if payload_len > MAX_PAYLOAD_SIZE {
            return Err(ProtocolError::PayloadTooLarge {
                size: payload_len,
                max: MAX_PAYLOAD_SIZE,
            });
        }

        let total_len = FRAME_HEADER_SIZE + payload_len as usize;

        if src.len() < total_len {
            return Ok(None);
        }

        src.advance(FRAME_HEADER_SIZE);
        let payload = src.split_to(payload_len as usize).freeze();

        Ok(Some(Frame {
            opcode,
            correlation_id: correl_id,
            payload,
        }))
    }
}

/// An outbound frame whose payload is a small `head` followed by `body`
/// chunks written back to back. Record-carrying responses use it so record
/// bytes read from a segment go to the socket without being copied into a
/// contiguous payload first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutFrame {
    pub opcode: OpCode,
    pub correlation_id: u32,
    pub head: Bytes,
    pub body: Vec<Bytes>,
}

impl OutFrame {
    pub fn new(opcode: OpCode, correlation_id: u32, head: Bytes, body: Vec<Bytes>) -> Self {
        Self {
            opcode,
            correlation_id,
            head,
            body,
        }
    }

    /// Payload length (head + body).
    pub fn payload_len(&self) -> usize {
        self.head.len() + self.body.iter().map(Bytes::len).sum::<usize>()
    }

    /// The 10-byte frame header.
    pub fn header(&self) -> [u8; FRAME_HEADER_SIZE] {
        let mut h = [0u8; FRAME_HEADER_SIZE];
        h[0] = PROTOCOL_VERSION;
        h[1] = self.opcode.as_u8();
        h[2..6].copy_from_slice(&self.correlation_id.to_le_bytes());
        h[6..10].copy_from_slice(&(self.payload_len() as u32).to_le_bytes());
        h
    }

    /// Append the complete frame to `dst`.
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.reserve(FRAME_HEADER_SIZE + self.payload_len());
        dst.extend_from_slice(&self.header());
        dst.extend_from_slice(&self.head);
        for b in &self.body {
            dst.extend_from_slice(b);
        }
    }

    /// Collapse into a [`Frame`] with a contiguous payload (copies).
    pub fn into_frame(self) -> Frame {
        if self.body.is_empty() {
            return Frame::new(self.opcode, self.correlation_id, self.head);
        }
        let mut p = BytesMut::with_capacity(self.payload_len());
        p.extend_from_slice(&self.head);
        for b in &self.body {
            p.extend_from_slice(b);
        }
        Frame::new(self.opcode, self.correlation_id, p.freeze())
    }
}

impl From<Frame> for OutFrame {
    fn from(f: Frame) -> Self {
        Self::new(f.opcode, f.correlation_id, f.payload, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let frame = Frame::new(OpCode::Ping, 42, Bytes::from_static(b"hello"));
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        assert_eq!(buf.len(), FRAME_HEADER_SIZE + 5);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode, OpCode::Ping);
        assert_eq!(decoded.correlation_id, 42);
        assert_eq!(decoded.payload, Bytes::from_static(b"hello"));
    }

    #[test]
    fn encode_decode_empty_payload() {
        let frame = Frame::empty(OpCode::Pong, 0);
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        assert_eq!(buf.len(), FRAME_HEADER_SIZE);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode, OpCode::Pong);
        assert_eq!(decoded.correlation_id, 0);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn decode_incomplete_header_returns_none() {
        let mut buf = BytesMut::from(&[0x01, 0xF0][..]);
        let result = Frame::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn decode_incomplete_payload_returns_none() {
        let mut buf = BytesMut::new();
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u8(OpCode::Ping.as_u8());
        buf.put_u32_le(1);
        buf.put_u32_le(100);
        buf.extend_from_slice(b"short");

        let result = Frame::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn decode_bad_version_returns_error() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xFF);
        buf.put_u8(OpCode::Ping.as_u8());
        buf.put_u32_le(0);
        buf.put_u32_le(0);

        let result = Frame::decode(&mut buf);
        assert!(matches!(
            result,
            Err(ProtocolError::UnsupportedVersion(0xFF))
        ));
    }

    #[test]
    fn decode_bad_opcode_returns_error() {
        let mut buf = BytesMut::new();
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u8(0xFF);
        buf.put_u32_le(0);
        buf.put_u32_le(0);

        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Err(ProtocolError::UnknownOpCode(0xFF))));
    }

    #[test]
    fn decode_oversized_payload_returns_error() {
        let mut buf = BytesMut::new();
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u8(OpCode::Ping.as_u8());
        buf.put_u32_le(0);
        buf.put_u32_le(MAX_PAYLOAD_SIZE + 1);

        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Err(ProtocolError::PayloadTooLarge { .. })));
    }

    #[test]
    fn out_frame_encodes_like_a_contiguous_frame() {
        let out = OutFrame::new(
            OpCode::Messages,
            9,
            Bytes::from_static(b"ab"),
            vec![Bytes::from_static(b"cd"), Bytes::from_static(b"e")],
        );
        let mut a = BytesMut::new();
        out.encode(&mut a);
        let mut b = BytesMut::new();
        out.clone().into_frame().encode(&mut b);
        assert_eq!(a, b);
        assert_eq!(&a[..FRAME_HEADER_SIZE], &out.header()[..]);
        let decoded = Frame::decode(&mut a).unwrap().unwrap();
        assert_eq!(decoded.payload, Bytes::from_static(b"abcde"));
    }

    #[test]
    fn multiple_frames_in_buffer() {
        let f1 = Frame::empty(OpCode::Ping, 1);
        let f2 = Frame::empty(OpCode::Pong, 2);

        let mut buf = BytesMut::new();
        f1.encode(&mut buf);
        f2.encode(&mut buf);

        let decoded1 = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded1.correlation_id, 1);

        let decoded2 = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded2.correlation_id, 2);

        assert!(buf.is_empty());
    }
}
