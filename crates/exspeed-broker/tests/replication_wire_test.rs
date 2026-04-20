use exspeed_broker::replication::wire::{decode_frame, encode_event, EncodedFrame};
use exspeed_broker::replication::ReplicationEvent;
use exspeed_protocol::messages::replicate::{
    RecordsAppended, ReplicatedRecord, RetentionTrimmedEvent, StreamCreatedEvent,
};
use exspeed_protocol::opcodes::OpCode;

#[test]
fn encode_decode_records_appended_roundtrip() {
    let event = ReplicationEvent::RecordsAppended(RecordsAppended {
        stream: "orders".into(),
        base_offset: 100,
        records: vec![ReplicatedRecord {
            subject: "s".into(),
            payload: vec![1, 2, 3],
            headers: vec![],
            timestamp_ms: 1234,
            msg_id: Some("m".into()),
        }],
    });

    let EncodedFrame { opcode, bytes } = encode_event(&event).unwrap();
    assert_eq!(opcode, OpCode::RecordsAppended);
    let decoded = decode_frame(opcode, &bytes).unwrap();
    match decoded {
        ReplicationEvent::RecordsAppended(r) => {
            assert_eq!(r.stream, "orders");
            assert_eq!(r.records.len(), 1);
            assert_eq!(r.records[0].msg_id.as_deref(), Some("m"));
        }
        _ => panic!("wrong event kind"),
    }
}

#[test]
fn encode_decode_stream_created_roundtrip() {
    let event = ReplicationEvent::StreamCreated(StreamCreatedEvent {
        name: "orders".into(),
        max_age_secs: 3600,
        max_bytes: 1_000_000,
    });
    let EncodedFrame { opcode, bytes } = encode_event(&event).unwrap();
    assert_eq!(opcode, OpCode::StreamCreatedEvent);
    let decoded = decode_frame(opcode, &bytes).unwrap();
    match decoded {
        ReplicationEvent::StreamCreated(s) => assert_eq!(s.name, "orders"),
        _ => panic!("wrong event kind"),
    }
}

#[test]
fn encode_decode_retention_trimmed_roundtrip() {
    let event = ReplicationEvent::RetentionTrimmed(RetentionTrimmedEvent {
        stream: "orders".into(),
        new_earliest_offset: 500,
    });
    let EncodedFrame { opcode, bytes } = encode_event(&event).unwrap();
    assert_eq!(opcode, OpCode::RetentionTrimmedEvent);
    let decoded = decode_frame(opcode, &bytes).unwrap();
    match decoded {
        ReplicationEvent::RetentionTrimmed(t) => assert_eq!(t.new_earliest_offset, 500),
        _ => panic!("wrong event kind"),
    }
}

#[test]
fn decode_frame_with_wrong_opcode_errors() {
    let res = decode_frame(OpCode::Ping, &[0xAB, 0xCD]);
    assert!(res.is_err(), "Ping is not a replication opcode");
}

#[test]
fn decode_garbage_payload_errors() {
    let res = decode_frame(OpCode::StreamCreatedEvent, &[0xFF; 3]);
    assert!(res.is_err(), "bincode should fail on garbage payload");
}
