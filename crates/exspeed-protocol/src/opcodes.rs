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
    RebalanceAck = 0x09,
    PublishBatch = 0x0A,
    CreateStream = 0x10,
    DeleteStream = 0x11,
    StreamInfo = 0x12,
    CreateConsumer = 0x13,
    DeleteConsumer = 0x14,
    Query = 0x20,
    QueryCancel = 0x21,

    // Cluster / replication — Follower -> Leader (0x30-0x3F)
    ReplicateResume = 0x30,
    ReplicationHeartbeat = 0x31, // classified as both client and server

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
    PublishOk = 0x88,
    ConnectOk = 0x89,
    PublishBatchOk = 0x8A,

    // Cluster / replication — Leader -> Follower (0xA0-0xAF)
    ClusterManifest = 0xA0,
    RecordsAppended = 0xA1,
    StreamCreatedEvent = 0xA2,
    StreamDeletedEvent = 0xA3,
    RetentionUpdatedEvent = 0xA4,
    RetentionTrimmedEvent = 0xA5,
    StreamReseedEvent = 0xA6,

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
                | OpCode::RebalanceAck
                | OpCode::PublishBatch
                | OpCode::CreateStream
                | OpCode::DeleteStream
                | OpCode::StreamInfo
                | OpCode::CreateConsumer
                | OpCode::DeleteConsumer
                | OpCode::Query
                | OpCode::QueryCancel
                | OpCode::Ping
                | OpCode::ReplicateResume
                | OpCode::ReplicationHeartbeat
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
                | OpCode::PublishOk
                | OpCode::ConnectOk
                | OpCode::PublishBatchOk
                | OpCode::Pong
                | OpCode::ReplicationHeartbeat
                | OpCode::ClusterManifest
                | OpCode::RecordsAppended
                | OpCode::StreamCreatedEvent
                | OpCode::StreamDeletedEvent
                | OpCode::RetentionUpdatedEvent
                | OpCode::RetentionTrimmedEvent
                | OpCode::StreamReseedEvent
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
            0x09 => Ok(OpCode::RebalanceAck),
            0x0A => Ok(OpCode::PublishBatch),
            0x10 => Ok(OpCode::CreateStream),
            0x11 => Ok(OpCode::DeleteStream),
            0x12 => Ok(OpCode::StreamInfo),
            0x13 => Ok(OpCode::CreateConsumer),
            0x14 => Ok(OpCode::DeleteConsumer),
            0x20 => Ok(OpCode::Query),
            0x21 => Ok(OpCode::QueryCancel),
            0x30 => Ok(OpCode::ReplicateResume),
            0x31 => Ok(OpCode::ReplicationHeartbeat),
            0xF0 => Ok(OpCode::Ping),
            0x80 => Ok(OpCode::Ok),
            0x81 => Ok(OpCode::Error),
            0x82 => Ok(OpCode::Record),
            0x83 => Ok(OpCode::RecordsBatch),
            0x84 => Ok(OpCode::StreamInfoResp),
            0x85 => Ok(OpCode::QueryResult),
            0x86 => Ok(OpCode::Rebalance),
            0x87 => Ok(OpCode::Drain),
            0x88 => Ok(OpCode::PublishOk),
            0x89 => Ok(OpCode::ConnectOk),
            0x8A => Ok(OpCode::PublishBatchOk),
            0xA0 => Ok(OpCode::ClusterManifest),
            0xA1 => Ok(OpCode::RecordsAppended),
            0xA2 => Ok(OpCode::StreamCreatedEvent),
            0xA3 => Ok(OpCode::StreamDeletedEvent),
            0xA4 => Ok(OpCode::RetentionUpdatedEvent),
            0xA5 => Ok(OpCode::RetentionTrimmedEvent),
            0xA6 => Ok(OpCode::StreamReseedEvent),
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

    #[test]
    fn replication_opcodes_roundtrip() {
        let codes = [
            OpCode::ReplicateResume,
            OpCode::ReplicationHeartbeat, // bidirectional — classified as client opcode for ergonomics
            OpCode::ClusterManifest,
            OpCode::RecordsAppended,
            OpCode::StreamCreatedEvent,
            OpCode::StreamDeletedEvent,
            OpCode::RetentionUpdatedEvent,
            OpCode::RetentionTrimmedEvent,
            OpCode::StreamReseedEvent,
        ];
        for code in codes {
            let byte = code.as_u8();
            let back = OpCode::try_from(byte).unwrap();
            assert_eq!(code, back);
        }
    }

    #[test]
    fn replication_client_server_classification() {
        // Follower -> Leader
        assert!(OpCode::ReplicateResume.is_client_opcode());
        // Bidirectional — classified as both; test each discriminator returns true.
        assert!(OpCode::ReplicationHeartbeat.is_client_opcode());
        assert!(OpCode::ReplicationHeartbeat.is_server_opcode());
        // Leader -> Follower
        assert!(OpCode::ClusterManifest.is_server_opcode());
        assert!(OpCode::RecordsAppended.is_server_opcode());
        assert!(OpCode::StreamCreatedEvent.is_server_opcode());
        assert!(OpCode::StreamReseedEvent.is_server_opcode());
    }
}
