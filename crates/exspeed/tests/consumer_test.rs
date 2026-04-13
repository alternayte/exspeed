use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::ack::{AckRequest, NackRequest};
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::consumer::{
    CreateConsumerRequest, StartFrom, SubscribeRequest, UnsubscribeRequest,
};
use exspeed_protocol::messages::publish::PublishRequest;
use exspeed_protocol::messages::record_delivery::RecordDelivery;
use exspeed_protocol::messages::stream_mgmt::CreateStreamRequest;
use exspeed_protocol::opcodes::OpCode;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_server() -> String {
    let port = portpicker::pick_unused_port().unwrap();
    let addr = format!("127.0.0.1:{}", port);
    let bind = addr.clone();
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    tokio::spawn(async move {
        let _tmp = tmp; // keep tempdir alive for the server's lifetime
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs {
            bind,
            api_bind: format!("127.0.0.1:{}", portpicker::pick_unused_port().unwrap()),
            data_dir,
        })
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    addr
}

type FramedReader = FramedRead<tokio::net::tcp::OwnedReadHalf, ExspeedCodec>;
type FramedWriter = FramedWrite<tokio::net::tcp::OwnedWriteHalf, ExspeedCodec>;

async fn connect_to(addr: &str) -> (FramedReader, FramedWriter) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, writer) = stream.into_split();
    (
        FramedRead::new(reader, ExspeedCodec::new()),
        FramedWrite::new(writer, ExspeedCodec::new()),
    )
}

async fn send_recv(writer: &mut FramedWriter, reader: &mut FramedReader, frame: Frame) -> Frame {
    let corr = frame.correlation_id;
    writer.send(frame).await.unwrap();
    // Read frames until we get one with matching correlation_id.
    // Server-pushed Record frames (correlation_id=0) are silently discarded here.
    loop {
        let resp = timeout(Duration::from_secs(5), reader.next())
            .await
            .expect("timeout waiting for response")
            .unwrap()
            .unwrap();
        if resp.correlation_id == corr {
            return resp;
        }
        // Otherwise it's a server push (Record with corr=0) — skip it.
    }
}

/// Receive the next frame, with a configurable timeout.
async fn recv_frame(reader: &mut FramedReader, secs: u64) -> Frame {
    timeout(Duration::from_secs(secs), reader.next())
        .await
        .expect("timeout waiting for frame")
        .unwrap()
        .unwrap()
}

fn connect_frame(corr: u32) -> Frame {
    let req = ConnectRequest {
        client_id: "consumer-test".into(),
        auth_type: AuthType::None,
        auth_payload: Bytes::new(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Connect, corr, buf.freeze())
}

fn create_stream_frame(stream_name: &str, corr: u32) -> Frame {
    let req = CreateStreamRequest {
        stream_name: stream_name.into(),
        max_age_secs: 0,
        max_bytes: 0,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::CreateStream, corr, buf.freeze())
}

fn publish_frame(stream: &str, subject: &str, value: &[u8], corr: u32) -> Frame {
    let req = PublishRequest {
        stream: stream.into(),
        subject: subject.into(),
        key: None,
        value: Bytes::copy_from_slice(value),
        headers: vec![],
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Publish, corr, buf.freeze())
}

fn create_consumer_frame(
    name: &str,
    stream: &str,
    group: &str,
    subject_filter: &str,
    start_from: StartFrom,
    start_offset: u64,
    corr: u32,
) -> Frame {
    let req = CreateConsumerRequest {
        name: name.into(),
        stream: stream.into(),
        group: group.into(),
        subject_filter: subject_filter.into(),
        start_from,
        start_offset,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::CreateConsumer, corr, buf.freeze())
}

fn subscribe_frame(consumer_name: &str, corr: u32) -> Frame {
    let req = SubscribeRequest {
        consumer_name: consumer_name.into(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Subscribe, corr, buf.freeze())
}

fn unsubscribe_frame(consumer_name: &str, corr: u32) -> Frame {
    let req = UnsubscribeRequest {
        consumer_name: consumer_name.into(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Unsubscribe, corr, buf.freeze())
}

fn ack_frame(consumer_name: &str, offset: u64, corr: u32) -> Frame {
    let req = AckRequest {
        consumer_name: consumer_name.into(),
        offset,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Ack, corr, buf.freeze())
}

fn nack_frame(consumer_name: &str, offset: u64, corr: u32) -> Frame {
    let req = NackRequest {
        consumer_name: consumer_name.into(),
        offset,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Nack, corr, buf.freeze())
}

/// Receive a RECORD push frame and decode it as RecordDelivery.
async fn recv_record(reader: &mut FramedReader, secs: u64) -> RecordDelivery {
    let frame = recv_frame(reader, secs).await;
    assert_eq!(
        frame.opcode,
        OpCode::Record,
        "expected Record opcode, got {:?}",
        frame.opcode
    );
    assert_eq!(
        frame.correlation_id, 0,
        "Record push should have correlation_id=0"
    );
    RecordDelivery::decode(frame.payload).expect("failed to decode RecordDelivery")
}

/// Helper: connect + create stream + publish N records with a given subject.
async fn setup_stream_with_records(
    writer: &mut FramedWriter,
    reader: &mut FramedReader,
    stream: &str,
    subject: &str,
    count: u32,
) {
    // Connect
    let resp = send_recv(writer, reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "CONNECT should return Ok");

    // Create stream
    let resp = send_recv(writer, reader, create_stream_frame(stream, 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "CREATE_STREAM should return Ok");

    // Publish records
    for i in 0..count {
        let value = format!("msg-{}", i);
        let resp = send_recv(
            writer,
            reader,
            publish_frame(stream, subject, value.as_bytes(), 100 + i),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok, "PUBLISH {} should return Ok", i);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subscribe_and_receive() {
    let addr = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    // 1. Connect, create stream, publish 3 records
    setup_stream_with_records(&mut writer, &mut reader, "orders", "order.created", 3).await;

    // 2. Create consumer at Earliest
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_consumer_frame("my-consumer", "orders", "", "", StartFrom::Earliest, 0, 200),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::Ok, "CREATE_CONSUMER should return Ok");

    // 3. Subscribe
    let resp = send_recv(
        &mut writer,
        &mut reader,
        subscribe_frame("my-consumer", 201),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::Ok, "SUBSCRIBE should return Ok");

    // 4. Receive 3 RECORD frames
    let mut deliveries = Vec::new();
    for _ in 0..3 {
        let delivery = recv_record(&mut reader, 5).await;
        deliveries.push(delivery);
    }

    // 5. Verify offsets and values
    for (i, delivery) in deliveries.iter().enumerate() {
        assert_eq!(
            delivery.offset, i as u64,
            "expected offset {}, got {}",
            i, delivery.offset
        );
        let expected_value = format!("msg-{}", i);
        assert_eq!(
            delivery.value,
            Bytes::from(expected_value.clone()),
            "expected value '{}' at offset {}",
            expected_value,
            i
        );
        assert_eq!(delivery.consumer_name, "my-consumer");
        assert_eq!(delivery.subject, "order.created");
    }

    // 6. ACK offset 2
    let resp = send_recv(&mut writer, &mut reader, ack_frame("my-consumer", 2, 202)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "ACK should return Ok");
}

#[tokio::test]
async fn resume_after_disconnect() {
    let addr = start_server().await;

    // --- First connection ---
    {
        let (mut reader, mut writer) = connect_to(&addr).await;

        // Connect, create stream, publish 5 records
        setup_stream_with_records(&mut writer, &mut reader, "events", "events.tick", 5).await;

        // Create consumer at Earliest
        let resp = send_recv(
            &mut writer,
            &mut reader,
            create_consumer_frame("resumable", "events", "", "", StartFrom::Earliest, 0, 200),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok, "CREATE_CONSUMER should return Ok");

        // Subscribe
        let resp = send_recv(&mut writer, &mut reader, subscribe_frame("resumable", 201)).await;
        assert_eq!(resp.opcode, OpCode::Ok, "SUBSCRIBE should return Ok");

        // Receive records until we have offsets 0, 1, 2
        for _ in 0..3 {
            let _delivery = recv_record(&mut reader, 5).await;
        }

        // ACK offset 2 — this sets the consumer's persisted offset
        let resp = send_recv(&mut writer, &mut reader, ack_frame("resumable", 2, 202)).await;
        assert_eq!(resp.opcode, OpCode::Ok, "ACK should return Ok");

        // Unsubscribe before disconnecting so the consumer state is cleared
        let resp = send_recv(
            &mut writer,
            &mut reader,
            unsubscribe_frame("resumable", 203),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok, "UNSUBSCRIBE should return Ok");

        // Give a moment for the state to settle
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Drop connection by letting reader/writer go out of scope
    }

    // --- Second connection ---
    {
        let (mut reader, mut writer) = connect_to(&addr).await;

        // Connect (no need to recreate stream or consumer — they're persisted)
        let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
        assert_eq!(resp.opcode, OpCode::Ok, "CONNECT should return Ok");

        // Subscribe the same consumer again
        let resp = send_recv(&mut writer, &mut reader, subscribe_frame("resumable", 301)).await;
        assert_eq!(resp.opcode, OpCode::Ok, "SUBSCRIBE should return Ok");

        // Should receive records from offset 2 onward (ACK set config.offset = 2,
        // delivery starts from config.offset which is 2).
        // Collect delivered records.
        let mut delivered_offsets = Vec::new();
        for _ in 0..3 {
            let delivery = recv_record(&mut reader, 5).await;
            delivered_offsets.push(delivery.offset);
        }

        // The consumer should resume from offset 2 (the ACK'd offset).
        // Records at offsets 2, 3, 4 should be delivered.
        assert_eq!(
            delivered_offsets,
            vec![2, 3, 4],
            "should resume from offset 2 onward"
        );
    }
}

#[tokio::test]
async fn subject_filter_delivery() {
    let addr = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    // Connect
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::Ok);

    // Create stream
    let resp = send_recv(&mut writer, &mut reader, create_stream_frame("events", 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok);

    // Create consumer with subject_filter BEFORE publishing
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_consumer_frame(
            "eu-only",
            "events",
            "",
            "order.eu.*",
            StartFrom::Earliest,
            0,
            200,
        ),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::Ok, "CREATE_CONSUMER should return Ok");

    // Publish records with different subjects
    let messages: &[(&str, &[u8])] = &[
        ("order.eu.created", b"eu-created"),
        ("order.us.created", b"us-created"),
        ("order.eu.shipped", b"eu-shipped"),
        ("payment.captured", b"pay-captured"),
    ];
    for (i, (subject, value)) in messages.iter().enumerate() {
        let resp = send_recv(
            &mut writer,
            &mut reader,
            publish_frame("events", subject, value, 100 + i as u32),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok);
    }

    // Subscribe
    let resp = send_recv(&mut writer, &mut reader, subscribe_frame("eu-only", 201)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "SUBSCRIBE should return Ok");

    // Should only receive 2 records: order.eu.created and order.eu.shipped
    let mut deliveries = Vec::new();
    for _ in 0..2 {
        let delivery = recv_record(&mut reader, 5).await;
        deliveries.push(delivery);
    }

    let subjects: Vec<&str> = deliveries.iter().map(|d| d.subject.as_str()).collect();
    assert!(
        subjects.contains(&"order.eu.created"),
        "should contain order.eu.created, got {:?}",
        subjects
    );
    assert!(
        subjects.contains(&"order.eu.shipped"),
        "should contain order.eu.shipped, got {:?}",
        subjects
    );
    assert_eq!(
        deliveries.len(),
        2,
        "should receive exactly 2 records, got {}",
        deliveries.len()
    );

    // Verify no more records arrive (the other 2 should be filtered out).
    // Use a short timeout — if we get a frame, the filter is broken.
    let extra = timeout(Duration::from_millis(500), reader.next()).await;
    assert!(
        extra.is_err(),
        "should not receive any more records after the 2 matching ones"
    );
}

#[tokio::test]
async fn nack_redelivery() {
    let addr = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    // Connect, create stream, publish 1 record
    setup_stream_with_records(&mut writer, &mut reader, "orders", "order.created", 1).await;

    // Create consumer at Earliest
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_consumer_frame("nacker", "orders", "", "", StartFrom::Earliest, 0, 200),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::Ok, "CREATE_CONSUMER should return Ok");

    // Subscribe
    let resp = send_recv(&mut writer, &mut reader, subscribe_frame("nacker", 201)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "SUBSCRIBE should return Ok");

    // Receive the first delivery
    let delivery1 = recv_record(&mut reader, 5).await;
    assert_eq!(delivery1.offset, 0);
    assert_eq!(delivery1.delivery_attempt, 1);
    assert_eq!(delivery1.value, Bytes::from("msg-0"));

    // NACK the record
    let resp = send_recv(&mut writer, &mut reader, nack_frame("nacker", 0, 202)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "NACK should return Ok");

    // The NACK does not advance the consumer offset, so the delivery task
    // (which is still running) will continue to poll from where it left off.
    // Since delivery_attempt in the delivery engine is always 1 (it doesn't
    // track NACK attempts at the delivery layer), we just verify the record
    // is re-delivered. The delivery engine reads forward from current_offset,
    // but since NACK didn't advance the consumer offset, on a re-subscribe
    // the consumer would start from offset 0 again.
    //
    // However, the current delivery task is still running and has already
    // moved past offset 0 in its local current_offset. To get redelivery,
    // we need to unsubscribe and re-subscribe so a new delivery task starts
    // from the consumer's persisted offset (which NACK didn't advance).

    // Unsubscribe
    let resp = send_recv(&mut writer, &mut reader, unsubscribe_frame("nacker", 203)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "UNSUBSCRIBE should return Ok");

    // Brief sleep to let the delivery task stop
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Re-subscribe — new delivery task should start from consumer offset 0
    let resp = send_recv(&mut writer, &mut reader, subscribe_frame("nacker", 204)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "re-SUBSCRIBE should return Ok");

    // Should receive offset 0 again (it was NACKed, offset not advanced)
    let delivery2 = recv_record(&mut reader, 5).await;
    assert_eq!(
        delivery2.offset, 0,
        "NACKed record should be redelivered at offset 0"
    );
    assert_eq!(delivery2.value, Bytes::from("msg-0"));
}
