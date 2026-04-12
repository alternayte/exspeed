use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::ProtocolError;
use crate::frame::Frame;

/// Tokio codec for the Exspeed wire protocol.
pub struct ExspeedCodec;

impl ExspeedCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ExspeedCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for ExspeedCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Frame::decode(src)
    }
}

impl Encoder<Frame> for ExspeedCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::opcodes::OpCode;
    use bytes::Bytes;

    #[test]
    fn codec_decode() {
        let mut codec = ExspeedCodec::new();
        let frame = Frame::new(OpCode::Ping, 7, Bytes::from_static(b"test"));
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.opcode, OpCode::Ping);
        assert_eq!(decoded.correlation_id, 7);
        assert_eq!(decoded.payload, Bytes::from_static(b"test"));
    }

    #[test]
    fn codec_encode() {
        let mut codec = ExspeedCodec::new();
        let frame = Frame::empty(OpCode::Pong, 0);
        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();
        assert_eq!(buf.len(), exspeed_common::FRAME_HEADER_SIZE);
    }
}
