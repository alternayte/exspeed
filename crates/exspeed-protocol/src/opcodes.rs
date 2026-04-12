use crate::error::ProtocolError;

/// Wire protocol operation codes.
/// Client -> Server opcodes are 0x01-0x7F.
/// Server -> Client opcodes are 0x80-0xFF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpCode {
    // Client -> Server
    Connect = 0x01,
    Publish = 0x02,
    Subscribe = 0x03,
    Unsubscribe = 0x04,
    Ack = 0x05,
    Nack = 0x06,
    Fetch = 0x07,
    Seek = 0x08,
    CreateStream = 0x10,
    DeleteStream = 0x11,
    StreamInfo = 0x12,
    CreateConsumer = 0x13,
    DeleteConsumer = 0x14,
    Query = 0x20,
    QueryCancel = 0x21,
    Ping = 0xF0,

    // Server -> Client
    Ok = 0x80,
    Error = 0x81,
    Record = 0x82,
    RecordsBatch = 0x83,
    StreamInfoResp = 0x84,
    QueryResult = 0x85,
    Rebalance = 0x86,
    Drain = 0x87,
    Pong = 0xF1,
}

impl OpCode {
    pub fn is_client_opcode(self) -> bool {
        matches!(
            self,
            OpCode::Connect
                | OpCode::Publish
                | OpCode::Subscribe
                | OpCode::Unsubscribe
                | OpCode::Ack
                | OpCode::Nack
                | OpCode::Fetch
                | OpCode::Seek
                | OpCode::CreateStream
                | OpCode::DeleteStream
                | OpCode::StreamInfo
                | OpCode::CreateConsumer
                | OpCode::DeleteConsumer
                | OpCode::Query
                | OpCode::QueryCancel
                | OpCode::Ping
        )
    }

    pub fn is_server_opcode(self) -> bool {
        matches!(
            self,
            OpCode::Ok
                | OpCode::Error
                | OpCode::Record
                | OpCode::RecordsBatch
                | OpCode::StreamInfoResp
                | OpCode::QueryResult
                | OpCode::Rebalance
                | OpCode::Drain
                | OpCode::Pong
        )
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for OpCode {
    type Error = ProtocolError;

    fn try_from(byte: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        match byte {
            0x01 => Ok(OpCode::Connect),
            0x02 => Ok(OpCode::Publish),
            0x03 => Ok(OpCode::Subscribe),
            0x04 => Ok(OpCode::Unsubscribe),
            0x05 => Ok(OpCode::Ack),
            0x06 => Ok(OpCode::Nack),
            0x07 => Ok(OpCode::Fetch),
            0x08 => Ok(OpCode::Seek),
            0x10 => Ok(OpCode::CreateStream),
            0x11 => Ok(OpCode::DeleteStream),
            0x12 => Ok(OpCode::StreamInfo),
            0x13 => Ok(OpCode::CreateConsumer),
            0x14 => Ok(OpCode::DeleteConsumer),
            0x20 => Ok(OpCode::Query),
            0x21 => Ok(OpCode::QueryCancel),
            0xF0 => Ok(OpCode::Ping),
            0x80 => Ok(OpCode::Ok),
            0x81 => Ok(OpCode::Error),
            0x82 => Ok(OpCode::Record),
            0x83 => Ok(OpCode::RecordsBatch),
            0x84 => Ok(OpCode::StreamInfoResp),
            0x85 => Ok(OpCode::QueryResult),
            0x86 => Ok(OpCode::Rebalance),
            0x87 => Ok(OpCode::Drain),
            0xF1 => Ok(OpCode::Pong),
            other => Err(ProtocolError::UnknownOpCode(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opcode_roundtrip() {
        let codes = [
            OpCode::Connect,
            OpCode::Publish,
            OpCode::Ping,
            OpCode::Ok,
            OpCode::Error,
            OpCode::Pong,
            OpCode::Rebalance,
            OpCode::Drain,
        ];
        for code in codes {
            let byte = code.as_u8();
            let back = OpCode::try_from(byte).unwrap();
            assert_eq!(code, back);
        }
    }

    #[test]
    fn unknown_opcode_rejected() {
        let result = OpCode::try_from(0xFF);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("0xff"));
    }

    #[test]
    fn client_server_classification() {
        assert!(OpCode::Connect.is_client_opcode());
        assert!(OpCode::Ping.is_client_opcode());
        assert!(OpCode::Ok.is_server_opcode());
        assert!(OpCode::Pong.is_server_opcode());
    }
}
