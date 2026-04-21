use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::publish_batch::{
    BatchResult, PublishBatchOkResponse, PublishBatchRecord, PublishBatchRequest,
};
use exspeed_protocol::messages::stream_mgmt::CreateStreamRequest;
use exspeed_protocol::opcodes::OpCode;

// ---------------------------------------------------------------------------
// Helpers (copied from broker_test.rs)
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
            auth_token: None,
            credentials_file: None,
            tls_cert: None,
            tls_key: None,
            storage_sync: exspeed::cli::server::StorageSyncArg::Sync,
            storage_flush_window_us: 500,
            storage_flush_threshold_records: 256,
            storage_flush_threshold_bytes: 1_048_576,
            storage_sync_interval_ms: 10,
            storage_sync_bytes: 4 * 1024 * 1024,
            delivery_buffer: 8192,
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
        client_id: "publish-batch-test".into(),
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_batch_ok_returns_sequential_offsets() {
    let addr = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    send_recv(&mut writer, &mut reader, create_stream_frame("orders", 2)).await;

    let req = PublishBatchRequest {
        stream: "orders".into(),
        records: (0..5)
            .map(|i| PublishBatchRecord {
                subject: "orders.placed".into(),
                key: None,
                msg_id: None,
                value: Bytes::from(format!("rec-{i}").into_bytes()),
                headers: vec![],
            })
            .collect(),
    };
    let mut buf = BytesMut::new();
    req.encode(&mut buf);
    let resp = send_recv(
        &mut writer,
        &mut reader,
        Frame::new(OpCode::PublishBatch, 3, buf.freeze()),
    )
    .await;

    assert_eq!(resp.opcode, OpCode::PublishBatchOk);
    let decoded = PublishBatchOkResponse::decode(resp.payload).unwrap();
    assert_eq!(decoded.results.len(), 5);
    for (i, r) in decoded.results.iter().enumerate() {
        match r {
            BatchResult::Written { offset } => assert_eq!(*offset, i as u64),
            other => panic!("expected Written, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn publish_batch_returns_duplicate_for_repeat_msg_id() {
    let addr = start_server().await;
    let (mut reader, mut writer) = connect_to(&addr).await;

    send_recv(&mut writer, &mut reader, connect_frame(1)).await;
    send_recv(&mut writer, &mut reader, create_stream_frame("dedup-stream", 2)).await;

    // First batch: msg_id = "m-1"
    let req1 = PublishBatchRequest {
        stream: "dedup-stream".into(),
        records: vec![PublishBatchRecord {
            subject: "s".into(),
            key: None,
            msg_id: Some("m-1".into()),
            value: Bytes::from_static(b"v"),
            headers: vec![],
        }],
    };
    let mut buf = BytesMut::new();
    req1.encode(&mut buf);
    send_recv(
        &mut writer,
        &mut reader,
        Frame::new(OpCode::PublishBatch, 3, buf.freeze()),
    )
    .await;

    // Second batch: same msg_id "m-1" AND a new "m-2" in the same batch.
    let req2 = PublishBatchRequest {
        stream: "dedup-stream".into(),
        records: vec![
            PublishBatchRecord {
                subject: "s".into(),
                key: None,
                msg_id: Some("m-1".into()),
                value: Bytes::from_static(b"v"),
                headers: vec![],
            },
            PublishBatchRecord {
                subject: "s".into(),
                key: None,
                msg_id: Some("m-2".into()),
                value: Bytes::from_static(b"v2"),
                headers: vec![],
            },
        ],
    };
    let mut buf2 = BytesMut::new();
    req2.encode(&mut buf2);
    let resp = send_recv(
        &mut writer,
        &mut reader,
        Frame::new(OpCode::PublishBatch, 4, buf2.freeze()),
    )
    .await;

    assert_eq!(resp.opcode, OpCode::PublishBatchOk);
    let decoded = PublishBatchOkResponse::decode(resp.payload).unwrap();
    assert_eq!(decoded.results.len(), 2);
    assert!(matches!(decoded.results[0], BatchResult::Duplicate { .. }));
    assert!(matches!(decoded.results[1], BatchResult::Written { offset: 1 }));
}
