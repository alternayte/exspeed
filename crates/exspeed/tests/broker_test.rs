use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::fetch::FetchRequest;
use exspeed_protocol::messages::publish::PublishRequest;
use exspeed_protocol::messages::records_batch::RecordsBatch;
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
    writer.send(frame).await.unwrap();
    timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout waiting for response")
        .unwrap()
        .unwrap()
}

fn connect_frame(corr: u32) -> Frame {
    let req = ConnectRequest {
        client_id: "broker-test".into(),
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_and_fetch() {
    let addr = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    // 1. Connect
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "CONNECT should return Ok");
    assert_eq!(resp.correlation_id, 1);

    // 2. Create stream "orders"
    let resp = send_recv(&mut writer, &mut reader, create_stream_frame("orders", 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "CREATE_STREAM should return Ok");
    assert_eq!(resp.correlation_id, 2);

    // 3. Publish 3 records
    for i in 0u32..3 {
        let value = format!("msg-{}", i);
        let resp = send_recv(
            &mut writer,
            &mut reader,
            publish_frame("orders", "order.created", value.as_bytes(), 10 + i),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok, "PUBLISH {} should return Ok", i);
        assert_eq!(resp.correlation_id, 10 + i);
    }

    // 4. Fetch all from offset 0 (empty subject filter)
    let resp = send_recv(
        &mut writer,
        &mut reader,
        fetch_frame("orders", 0, 100, "", 20),
    )
    .await;
    assert_eq!(
        resp.opcode,
        OpCode::RecordsBatch,
        "FETCH should return RecordsBatch"
    );
    assert_eq!(resp.correlation_id, 20);

    // 5. Decode and verify
    let batch = RecordsBatch::decode(resp.payload).unwrap();
    assert_eq!(batch.records.len(), 3, "expected 3 records");

    for (i, record) in batch.records.iter().enumerate() {
        let expected_value = format!("msg-{}", i);
        assert_eq!(record.subject, "order.created");
        assert_eq!(record.value, Bytes::from(expected_value));
    }
}

#[tokio::test]
async fn fetch_with_subject_filter() {
    let addr = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    // Connect
    let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    assert_eq!(resp.opcode, OpCode::Ok);

    // Create stream "events"
    let resp = send_recv(&mut writer, &mut reader, create_stream_frame("events", 2)).await;
    assert_eq!(resp.opcode, OpCode::Ok);

    // Publish 4 records with different subjects
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
            publish_frame("events", subject, value, 10 + i as u32),
        )
        .await;
        assert_eq!(
            resp.opcode,
            OpCode::Ok,
            "PUBLISH '{}' should return Ok",
            subject
        );
    }

    // Fetch with filter "order.eu.*" -> expect 2 records
    let resp = send_recv(
        &mut writer,
        &mut reader,
        fetch_frame("events", 0, 100, "order.eu.*", 30),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::RecordsBatch);
    assert_eq!(resp.correlation_id, 30);

    let batch = RecordsBatch::decode(resp.payload).unwrap();
    assert_eq!(
        batch.records.len(),
        2,
        "order.eu.* should match 2 records, got: {:?}",
        batch.records.iter().map(|r| &r.subject).collect::<Vec<_>>()
    );
    let subjects: Vec<&str> = batch.records.iter().map(|r| r.subject.as_str()).collect();
    assert!(
        subjects.contains(&"order.eu.created"),
        "should contain order.eu.created"
    );
    assert!(
        subjects.contains(&"order.eu.shipped"),
        "should contain order.eu.shipped"
    );

    // Fetch with filter "order.>" -> expect 3 records (all order.* but not payment.*)
    let resp = send_recv(
        &mut writer,
        &mut reader,
        fetch_frame("events", 0, 100, "order.>", 31),
    )
    .await;
    assert_eq!(resp.opcode, OpCode::RecordsBatch);
    assert_eq!(resp.correlation_id, 31);

    let batch = RecordsBatch::decode(resp.payload).unwrap();
    assert_eq!(
        batch.records.len(),
        3,
        "order.> should match 3 records, got: {:?}",
        batch.records.iter().map(|r| &r.subject).collect::<Vec<_>>()
    );
    let subjects: Vec<&str> = batch.records.iter().map(|r| r.subject.as_str()).collect();
    assert!(subjects.contains(&"order.eu.created"));
    assert!(subjects.contains(&"order.us.created"));
    assert!(subjects.contains(&"order.eu.shipped"));
    assert!(
        !subjects.contains(&"payment.captured"),
        "payment.captured should NOT match order.>"
    );
}
