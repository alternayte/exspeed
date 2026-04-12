pub mod connect;
pub mod ping;

pub use connect::{AuthType, ConnectRequest};
pub use ping::{Ping, Pong};

use bytes::Bytes;

use crate::error::ProtocolError;
use crate::frame::Frame;
use crate::opcodes::OpCode;

/// A decoded client -> server message.
#[derive(Debug)]
pub enum ClientMessage {
    Connect(ConnectRequest),
    Ping,
}

impl ClientMessage {
    pub fn from_frame(frame: Frame) -> Result<Self, ProtocolError> {
        match frame.opcode {
            OpCode::Connect => {
                let req = ConnectRequest::decode(frame.payload)?;
                Ok(ClientMessage::Connect(req))
            }
            OpCode::Ping => Ok(ClientMessage::Ping),
            other => Err(ProtocolError::Decode(format!(
                "unhandled client opcode: {:?}",
                other
            ))),
        }
    }
}

/// A server -> client response.
#[derive(Debug)]
pub enum ServerMessage {
    Ok,
    Error { code: u16, message: String },
    Pong,
}

impl ServerMessage {
    pub fn into_frame(self, correlation_id: u32) -> Frame {
        match self {
            ServerMessage::Ok => Frame::empty(OpCode::Ok, correlation_id),
            ServerMessage::Pong => Frame::empty(OpCode::Pong, correlation_id),
            ServerMessage::Error { code, message } => {
                let mut payload = Vec::with_capacity(2 + message.len());
                payload.extend_from_slice(&code.to_le_bytes());
                payload.extend_from_slice(message.as_bytes());
                Frame::new(OpCode::Error, correlation_id, Bytes::from(payload))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn ping_frame_to_client_message() {
        let frame = Frame::empty(OpCode::Ping, 5);
        let msg = ClientMessage::from_frame(frame).unwrap();
        assert!(matches!(msg, ClientMessage::Ping));
    }

    #[test]
    fn connect_frame_to_client_message() {
        let req = ConnectRequest {
            client_id: "test".into(),
            auth_type: AuthType::None,
            auth_payload: Bytes::new(),
        };
        let mut payload = BytesMut::new();
        req.encode(&mut payload);

        let frame = Frame::new(OpCode::Connect, 1, payload.freeze());
        let msg = ClientMessage::from_frame(frame).unwrap();
        assert!(matches!(msg, ClientMessage::Connect(_)));
    }

    #[test]
    fn server_ok_to_frame() {
        let frame = ServerMessage::Ok.into_frame(42);
        assert_eq!(frame.opcode, OpCode::Ok);
        assert_eq!(frame.correlation_id, 42);
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn server_error_to_frame() {
        let frame = ServerMessage::Error {
            code: 404,
            message: "stream not found".into(),
        }.into_frame(1);
        assert_eq!(frame.opcode, OpCode::Error);
        assert!(frame.payload.len() > 2);
    }

    #[test]
    fn pong_to_frame() {
        let frame = ServerMessage::Pong.into_frame(99);
        assert_eq!(frame.opcode, OpCode::Pong);
        assert_eq!(frame.correlation_id, 99);
    }
}
