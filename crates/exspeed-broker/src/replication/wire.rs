//! Serialize/deserialize replication events to/from the bytes carried
//! inside an existing protocol frame.
//!
//! The outer frame header (version / opcode / correlation id / payload
//! length) is handled by `exspeed-protocol::codec`; this module only
//! concerns itself with the payload-body encoding.

use exspeed_protocol::messages::replicate::{
    RecordsAppended, RetentionTrimmedEvent, RetentionUpdatedEvent, StreamCreatedEvent,
    StreamDeletedEvent,
};
use exspeed_protocol::opcodes::OpCode;

use crate::replication::errors::ReplicationError;
use crate::replication::ReplicationEvent;

pub struct EncodedFrame {
    pub opcode: OpCode,
    pub bytes: Vec<u8>,
}

pub fn encode_event(event: &ReplicationEvent) -> Result<EncodedFrame, ReplicationError> {
    let (opcode, bytes) = match event {
        ReplicationEvent::StreamCreated(v) => {
            (OpCode::StreamCreatedEvent, bincode::serialize(v)?)
        }
        ReplicationEvent::StreamDeleted(v) => {
            (OpCode::StreamDeletedEvent, bincode::serialize(v)?)
        }
        ReplicationEvent::RetentionUpdated(v) => {
            (OpCode::RetentionUpdatedEvent, bincode::serialize(v)?)
        }
        ReplicationEvent::RetentionTrimmed(v) => {
            (OpCode::RetentionTrimmedEvent, bincode::serialize(v)?)
        }
        ReplicationEvent::RecordsAppended(v) => (OpCode::RecordsAppended, bincode::serialize(v)?),
    };
    Ok(EncodedFrame { opcode, bytes })
}

pub fn decode_frame(opcode: OpCode, bytes: &[u8]) -> Result<ReplicationEvent, ReplicationError> {
    match opcode {
        OpCode::StreamCreatedEvent => Ok(ReplicationEvent::StreamCreated(
            bincode::deserialize::<StreamCreatedEvent>(bytes)?,
        )),
        OpCode::StreamDeletedEvent => Ok(ReplicationEvent::StreamDeleted(
            bincode::deserialize::<StreamDeletedEvent>(bytes)?,
        )),
        OpCode::RetentionUpdatedEvent => Ok(ReplicationEvent::RetentionUpdated(
            bincode::deserialize::<RetentionUpdatedEvent>(bytes)?,
        )),
        OpCode::RetentionTrimmedEvent => Ok(ReplicationEvent::RetentionTrimmed(
            bincode::deserialize::<RetentionTrimmedEvent>(bytes)?,
        )),
        OpCode::RecordsAppended => Ok(ReplicationEvent::RecordsAppended(
            bincode::deserialize::<RecordsAppended>(bytes)?,
        )),
        // StreamReseed is leader→follower but not a ReplicationEvent on the leader side —
        // it's a handshake-time signal. Still allow decoding for clients / tests.
        OpCode::StreamReseedEvent => Err(ReplicationError::Protocol(
            "StreamReseed is handled out-of-band; see handshake path".to_string(),
        )),
        other => Err(ReplicationError::Protocol(format!(
            "opcode {:?} is not a replication payload",
            other
        ))),
    }
}
