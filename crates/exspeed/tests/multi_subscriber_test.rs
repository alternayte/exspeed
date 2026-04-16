use bytes::{Bytes, BytesMut};
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
use exspeed_protocol::messages::stream_mgmt::CreateStreamRequest;
use exspeed_protocol::opcodes::OpCode;

// ---------------------------------------------------------------------------
// Helpers (mirroring consumer_test.rs style)
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
    // Server-pushed Record frames (correlation_id=0) are silently discarded.
    loop {
        let resp = timeout(Duration::from_secs(5), reader.next())
            .await
            .expect("timeout waiting for response")
            .unwrap()
            .unwrap();
        if resp.correlation_id == corr {
            return resp;
        }
    }
}

fn connect_frame(corr: u32) -> Frame {
    let req = ConnectRequest {
        client_id: "multi-sub-test".into(),
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

fn subscribe_frame_with_id(consumer_name: &str, subscriber_id: &str, corr: u32) -> Frame {
    let req = SubscribeRequest {
        consumer_name: consumer_name.into(),
        subscriber_id: subscriber_id.into(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Subscribe, corr, buf.freeze())
}

fn unsubscribe_frame_with_id(consumer_name: &str, subscriber_id: &str, corr: u32) -> Frame {
    let req = UnsubscribeRequest {
        consumer_name: consumer_name.into(),
        subscriber_id: subscriber_id.into(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    Frame::new(OpCode::Unsubscribe, corr, buf.freeze())
}

/// Decode an OpCode::Error frame payload into its message string.
/// Payload layout: [u16 code LE][message bytes].
fn decode_error_message(frame: &Frame) -> String {
    assert_eq!(frame.opcode, OpCode::Error, "expected Error opcode");
    assert!(
        frame.payload.len() >= 2,
        "Error payload must contain at least the 2-byte code"
    );
    String::from_utf8(frame.payload[2..].to_vec()).expect("Error message should be UTF-8")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_subscribers_can_bind_to_grouped_consumer() {
    let addr = start_server().await;

    // --- Setup on connection A: create stream + grouped consumer ---
    {
        let (mut reader, mut writer) = connect_to(&addr).await;

        let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
        assert_eq!(resp.opcode, OpCode::Ok, "CONNECT should return Ok");

        let resp = send_recv(&mut writer, &mut reader, create_stream_frame("events", 2)).await;
        assert_eq!(resp.opcode, OpCode::Ok, "CREATE_STREAM should return Ok");

        let resp = send_recv(
            &mut writer,
            &mut reader,
            create_consumer_frame(
                "worker",
                "events",
                "workers", // grouped
                "",
                StartFrom::Earliest,
                0,
                3,
            ),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok, "CREATE_CONSUMER should return Ok");

        // Connection A closes here when writer/reader are dropped.
    }

    // Give the server a moment to process the disconnect cleanup.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // --- Connection B: subscribe with subscriber_id="sub-a" ---
    let (mut reader_b, mut writer_b) = connect_to(&addr).await;
    let resp = send_recv(&mut writer_b, &mut reader_b, connect_frame(10)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "B: CONNECT should return Ok");

    let resp = send_recv(
        &mut writer_b,
        &mut reader_b,
        subscribe_frame_with_id("worker", "sub-a", 11),
    )
    .await;
    assert_eq!(
        resp.opcode,
        OpCode::Ok,
        "B: SUBSCRIBE with subscriber_id='sub-a' should return Ok, got {:?}",
        resp.opcode
    );

    // --- Connection C: subscribe with subscriber_id="sub-b" ---
    let (mut reader_c, mut writer_c) = connect_to(&addr).await;
    let resp = send_recv(&mut writer_c, &mut reader_c, connect_frame(20)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "C: CONNECT should return Ok");

    let resp = send_recv(
        &mut writer_c,
        &mut reader_c,
        subscribe_frame_with_id("worker", "sub-b", 21),
    )
    .await;
    assert_eq!(
        resp.opcode,
        OpCode::Ok,
        "C: second SUBSCRIBE to grouped consumer with different subscriber_id \
         should return Ok (not 'already subscribed'), got {:?} (payload len {})",
        resp.opcode,
        resp.payload.len()
    );

    // --- Unsubscribe on C with the explicit subscriber_id ---
    let resp = send_recv(
        &mut writer_c,
        &mut reader_c,
        unsubscribe_frame_with_id("worker", "sub-b", 22),
    )
    .await;
    assert_eq!(
        resp.opcode,
        OpCode::Ok,
        "C: UNSUBSCRIBE with subscriber_id='sub-b' should return Ok, got {:?}",
        resp.opcode
    );
}

#[tokio::test]
async fn non_grouped_consumer_rejects_second_subscriber() {
    let addr = start_server().await;

    // --- Setup: create stream + non-grouped consumer ---
    {
        let (mut reader, mut writer) = connect_to(&addr).await;

        let resp = send_recv(&mut writer, &mut reader, connect_frame(1)).await;
        assert_eq!(resp.opcode, OpCode::Ok, "CONNECT should return Ok");

        let resp = send_recv(&mut writer, &mut reader, create_stream_frame("events", 2)).await;
        assert_eq!(resp.opcode, OpCode::Ok, "CREATE_STREAM should return Ok");

        let resp = send_recv(
            &mut writer,
            &mut reader,
            create_consumer_frame(
                "solo",
                "events",
                "", // non-grouped
                "",
                StartFrom::Earliest,
                0,
                3,
            ),
        )
        .await;
        assert_eq!(resp.opcode, OpCode::Ok, "CREATE_CONSUMER should return Ok");
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // --- Connection A: subscribe with subscriber_id="sub-a" -> Ok ---
    let (mut reader_a, mut writer_a) = connect_to(&addr).await;
    let resp = send_recv(&mut writer_a, &mut reader_a, connect_frame(10)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "A: CONNECT should return Ok");

    let resp = send_recv(
        &mut writer_a,
        &mut reader_a,
        subscribe_frame_with_id("solo", "sub-a", 11),
    )
    .await;
    assert_eq!(
        resp.opcode,
        OpCode::Ok,
        "A: first SUBSCRIBE to non-grouped consumer should return Ok"
    );

    // --- Connection B: subscribe to same consumer with subscriber_id="sub-b" -> Error ---
    let (mut reader_b, mut writer_b) = connect_to(&addr).await;
    let resp = send_recv(&mut writer_b, &mut reader_b, connect_frame(20)).await;
    assert_eq!(resp.opcode, OpCode::Ok, "B: CONNECT should return Ok");

    let resp = send_recv(
        &mut writer_b,
        &mut reader_b,
        subscribe_frame_with_id("solo", "sub-b", 21),
    )
    .await;
    assert_eq!(
        resp.opcode,
        OpCode::Error,
        "B: second SUBSCRIBE to non-grouped consumer should return Error, got {:?}",
        resp.opcode
    );

    let msg = decode_error_message(&resp);
    assert!(
        msg.contains("already subscribed"),
        "error message should contain 'already subscribed', got: {:?}",
        msg
    );
}
