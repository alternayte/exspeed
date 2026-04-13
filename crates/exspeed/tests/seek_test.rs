use bytes::{Buf, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::consumer::{
    CreateConsumerRequest, StartFrom, SubscribeRequest, UnsubscribeRequest,
};
use exspeed_protocol::messages::fetch::FetchRequest;
use exspeed_protocol::messages::publish::PublishRequest;
use exspeed_protocol::messages::record_delivery::RecordDelivery;
use exspeed_protocol::messages::records_batch::RecordsBatch;
use exspeed_protocol::messages::seek::SeekRequest;
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
        exspeed::cli::server::run(exspeed::cli::server::ServerArgs { bind, api_bind: format!("127.0.0.1:{}", portpicker::pick_unused_port().unwrap()), data_dir })
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
        // Otherwise it's a server push (Record with corr=0) -- skip it.
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

fn connect_frame(corr: u32) -> Frame {
    let req = ConnectRequest {
        client_id: "seek-test".into(),
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

fn create_stream_with_retention_frame(
    stream_name: &str,
    max_age_secs: u64,
    max_bytes: u64,
    corr: u32,
) -> Frame {
    let req = CreateStreamRequest {
        stream_name: stream_name.into(),
        max_age_secs,
        max_bytes,
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

fn fetch_frame(
    stream: &str,
    offset: u64,
    max_records: u32,
    subject_filter: &str,
    corr: u32,
) -> Frame {
    let req = FetchRequest {
        stream: stream.into(),
        offset,
        max_records,
        subject_filter: subject_filter.into(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Fetch, corr, buf.freeze())
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

fn seek_frame(consumer_name: &str, timestamp: u64, corr: u32) -> Frame {
    let req = SeekRequest {
        consumer_name: consumer_name.into(),
        timestamp,
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Seek, corr, buf.freeze())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn seek_repositions_consumer() {
    let addr = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    // 1. Connect
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "CONNECT should return Ok");

    // 2. Create stream "events"
    let resp = send_recv(&mut writer, &mut reader, create_stream_frame("events", 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "CREATE_STREAM should return Ok");

    // 3. Publish 5 records (small sleep between publishes so timestamps differ)
    for i in 0u32..5 {
        let value = format!("msg-{}", i);
        let resp = send_recv(
            &mut writer,
            &mut reader,
            publish_frame("events", "evt.data", value.as_bytes(), 10 + i),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok, "PUBLISH {} should return Ok", i);
        // Brief sleep to ensure distinct timestamps from the server
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // 4. Create consumer "seeker" at Earliest
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_consumer_frame("seeker", "events", "", "", StartFrom::Earliest, 0, 200),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::Ok, "CREATE_CONSUMER should return Ok");

    // 5. Subscribe "seeker" -- receive all 5 records, note their timestamps
    let resp = send_recv(&mut writer, &mut reader, subscribe_frame("seeker", 201)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "SUBSCRIBE should return Ok");

    let mut deliveries = Vec::new();
    for _ in 0..5 {
        let delivery = recv_record(&mut reader, 5).await;
        deliveries.push(delivery);
    }

    // Verify we got all 5, offsets 0..4
    for (i, d) in deliveries.iter().enumerate() {
        assert_eq!(d.offset, i as u64, "expected offset {}", i);
    }

    // Note the timestamp of record 3 (offset 2, index 2)
    let seek_timestamp = deliveries[2].timestamp;
    assert!(seek_timestamp > 0, "timestamp should be non-zero");

    // 6. Unsubscribe
    let resp = send_recv(&mut writer, &mut reader, unsubscribe_frame("seeker", 202)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "UNSUBSCRIBE should return Ok");

    // Brief sleep to let the delivery task stop
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 7. SEEK with the timestamp from record at offset 2
    let resp = send_recv(
        &mut writer,
        &mut reader,
        seek_frame("seeker", seek_timestamp, 203),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::Ok, "SEEK should return Ok");

    // The payload contains the new offset (u64 LE)
    let mut payload = resp.payload;
    assert!(
        payload.remaining() >= 8,
        "SEEK response should contain offset"
    );
    let seek_offset = payload.get_u64_le();
    // The seek offset should be at or before offset 2 (due to sparse index rounding)
    assert!(
        seek_offset <= 2,
        "SEEK offset should be at or before record 2, got {}",
        seek_offset
    );

    // 8. Subscribe again -- should resume from approximately record at offset 2
    let resp = send_recv(&mut writer, &mut reader, subscribe_frame("seeker", 204)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "re-SUBSCRIBE should return Ok");

    // Receive records and verify they start from at or before offset 2
    let first_delivery = recv_record(&mut reader, 5).await;
    assert!(
        first_delivery.offset <= 2,
        "after SEEK, first delivered record should be at or before offset 2, got {}",
        first_delivery.offset
    );

    // Collect remaining records up to offset 4
    let mut redelivered_offsets = vec![first_delivery.offset];
    let records_remaining = (4 - first_delivery.offset) as usize;
    for _ in 0..records_remaining {
        let delivery = recv_record(&mut reader, 5).await;
        redelivered_offsets.push(delivery.offset);
    }

    // The last offset should be 4
    assert_eq!(
        *redelivered_offsets.last().unwrap(),
        4,
        "should have received up to offset 4"
    );
}

#[tokio::test]
async fn seek_to_beginning_returns_offset_zero() {
    let addr = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    // Connect
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::Ok);

    // Create stream
    let resp = send_recv(&mut writer, &mut reader, create_stream_frame("logs", 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok);

    // Publish a few records
    for i in 0u32..3 {
        let value = format!("log-{}", i);
        let resp = send_recv(
            &mut writer,
            &mut reader,
            publish_frame("logs", "log.info", value.as_bytes(), 10 + i),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok);
    }

    // Create consumer
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_consumer_frame("log-reader", "logs", "", "", StartFrom::Earliest, 0, 200),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::Ok);

    // SEEK with timestamp=0 should return offset 0
    let resp = send_recv(&mut writer, &mut reader, seek_frame("log-reader", 0, 201)).await;
    assert_eq!(
        resp.opcode,
        OpCode::Ok,
        "SEEK to timestamp=0 should return Ok"
    );

    let mut payload = resp.payload;
    assert!(payload.remaining() >= 8);
    let offset = payload.get_u64_le();
    assert_eq!(offset, 0, "SEEK to timestamp=0 should return offset 0");
}

#[tokio::test]
async fn create_stream_with_retention() {
    let addr = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    // 1. Connect
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "CONNECT should return Ok");

    // 2. Create stream "short-lived" with max_age_secs=1, max_bytes=0
    let resp = send_recv(
        &mut writer,
        &mut reader,
        create_stream_with_retention_frame("short-lived", 1, 0, 2),
    )
    .await;
    assert_eq!(
        resp.opcode,
        OpCode::Ok,
        "CREATE_STREAM with retention should return Ok"
    );

    // 3. Publish a few records
    for i in 0u32..3 {
        let value = format!("ephemeral-{}", i);
        let resp = send_recv(
            &mut writer,
            &mut reader,
            publish_frame("short-lived", "data.tick", value.as_bytes(), 10 + i),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok, "PUBLISH {} should return Ok", i);
    }

    // 4. FETCH from offset 0 -- records should be there
    let resp = send_recv(
        &mut writer,
        &mut reader,
        fetch_frame("short-lived", 0, 100, "", 20),
    )
    .await;
    assert_eq!(
        resp.opcode,
        OpCode::RecordsBatch,
        "FETCH should return RecordsBatch"
    );
    assert_eq!(resp.correlation_id, 20);

    let batch = RecordsBatch::decode(resp.payload).unwrap();
    assert_eq!(
        batch.records.len(),
        3,
        "expected 3 records in short-lived stream"
    );

    for (i, record) in batch.records.iter().enumerate() {
        let expected_value = format!("ephemeral-{}", i);
        assert_eq!(record.value, Bytes::from(expected_value));
        assert_eq!(record.subject, "data.tick");
    }

    // We intentionally do not wait for the retention task to run (would take
    // too long for a test). The fact that the stream was created successfully
    // with retention config and records can be published/fetched validates the
    // wire protocol round-trip. Actual retention deletion is covered by unit
    // tests in the retention module.
}
