//! Client protocol v2: handshake, framing errors, publishing, stream
//! management and stateless reads.

use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use exspeed_client::{code, PublishRecord, Request, Response, StreamSpec};
use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::opcodes::OpCode;

use crate::common::{create_stream, publish_n, TestServer};

async fn raw(
    addr: &str,
) -> (
    FramedRead<tokio::net::tcp::OwnedReadHalf, ExspeedCodec>,
    FramedWrite<tokio::net::tcp::OwnedWriteHalf, ExspeedCodec>,
) {
    let (r, w) = TcpStream::connect(addr).await.unwrap().into_split();
    (
        FramedRead::new(r, ExspeedCodec::new()),
        FramedWrite::new(w, ExspeedCodec::new()),
    )
}

async fn recv(r: &mut FramedRead<tokio::net::tcp::OwnedReadHalf, ExspeedCodec>) -> Option<Frame> {
    tokio::time::timeout(Duration::from_secs(5), r.next())
        .await
        .expect("timed out")
        .map(|f| f.unwrap())
}

#[tokio::test]
async fn connect_and_ping() {
    let server = TestServer::start().await;
    let client = server.client().await;
    assert!(!client.server_info().server_version.is_empty());
    assert_eq!(client.server_info().leader, None);
    client.ping().await.unwrap();
    let meta = client.metadata().await.unwrap();
    assert_eq!(meta["is_leader"], true);
}

#[tokio::test]
async fn first_frame_must_be_connect() {
    let server = TestServer::start().await;
    let (mut r, mut w) = raw(&server.addr).await;
    w.send(Request::Ping.into_frame(7)).await.unwrap();
    let f = recv(&mut r).await.expect("error frame");
    match Response::from_frame(&f).unwrap() {
        Response::Error { code: c, .. } => assert_eq!(c, code::UNAUTHORIZED),
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(f.correlation_id, 7);
    assert!(recv(&mut r).await.is_none(), "connection should close");
}

#[tokio::test]
async fn malformed_request_gets_400_and_connection_survives() {
    let server = TestServer::start().await;
    let (mut r, mut w) = raw(&server.addr).await;
    w.send(
        Request::Connect {
            client_id: "raw".into(),
            token: None,
        }
        .into_frame(1),
    )
    .await
    .unwrap();
    let f = recv(&mut r).await.unwrap();
    assert_eq!(f.opcode, OpCode::ConnectOk);

    w.send(Frame::new(
        OpCode::Publish,
        2,
        Bytes::from_static(b"garbage"),
    ))
    .await
    .unwrap();
    let f = recv(&mut r).await.unwrap();
    assert_eq!(f.correlation_id, 2);
    match Response::from_frame(&f).unwrap() {
        Response::Error { code: c, .. } => assert_eq!(c, code::BAD_REQUEST),
        other => panic!("unexpected {other:?}"),
    }

    w.send(Request::Ping.into_frame(3)).await.unwrap();
    let f = recv(&mut r).await.unwrap();
    assert_eq!(f.opcode, OpCode::Pong);
    assert_eq!(f.correlation_id, 3);
}

#[tokio::test]
async fn publish_then_read_with_filter() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "orders").await;
    for (subject, v) in [
        ("orders.placed", "a"),
        ("orders.shipped", "b"),
        ("orders.placed", "c"),
        ("orders.us.placed", "d"),
    ] {
        c.publish(
            "orders",
            PublishRecord::new(subject, v).key("k1").header("h", "1"),
        )
        .await
        .unwrap();
    }

    let all = c.read("orders", 0, 100, Duration::ZERO, "").await.unwrap();
    assert_eq!(all.records.len(), 4);
    assert_eq!(all.next_offset, 4);
    assert_eq!(all.high_watermark, 4);
    let first = &all.records[0];
    assert_eq!(first.offset, 0);
    assert_eq!(first.subject, "orders.placed");
    assert_eq!(first.key.as_deref(), Some(&b"k1"[..]));
    assert_eq!(first.value.as_ref(), b"a");
    assert!(first.headers.contains(&("h".to_string(), "1".to_string())));
    assert!(
        first.timestamp_ns > 1_600_000_000_000_000_000,
        "timestamp is in ns"
    );
    assert_eq!(first.timestamp_ms(), first.timestamp_ns / 1_000_000);

    let placed = c
        .read("orders", 0, 100, Duration::ZERO, "orders.placed")
        .await
        .unwrap();
    let values: Vec<_> = placed.records.iter().map(|r| r.value.clone()).collect();
    assert_eq!(values, vec![Bytes::from("a"), Bytes::from("c")]);

    let deep = c
        .read("orders", 0, 100, Duration::ZERO, "orders.>")
        .await
        .unwrap();
    assert_eq!(deep.records.len(), 4);

    // Paging with max_records resumes right after the last record.
    let page = c
        .read("orders", 0, 1, Duration::ZERO, "orders.placed")
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    let page2 = c
        .read(
            "orders",
            page.next_offset,
            1,
            Duration::ZERO,
            "orders.placed",
        )
        .await
        .unwrap();
    assert_eq!(page2.records[0].value.as_ref(), b"c");
}

#[tokio::test]
async fn read_long_polls_for_new_data() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "events").await;

    let reader = c.clone();
    let wait = tokio::spawn(async move {
        reader
            .read("events", 0, 10, Duration::from_secs(10), "")
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!wait.is_finished(), "read should be waiting");
    c.publish("events", PublishRecord::new("e", "x"))
        .await
        .unwrap();
    let res = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res.records.len(), 1);

    // Times out cleanly with no data.
    let start = std::time::Instant::now();
    let res = c
        .read("events", 1, 10, Duration::from_millis(300), "")
        .await
        .unwrap();
    assert!(res.records.is_empty());
    assert!(start.elapsed() >= Duration::from_millis(250));
}

#[tokio::test]
async fn publish_batch_returns_offsets_and_duplicates() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "batch").await;
    let acks = c
        .publish_batch(
            "batch",
            (0..5)
                .map(|i| PublishRecord::new("s", format!("v{i}")).msg_id(format!("m{i}")))
                .collect(),
        )
        .await
        .unwrap();
    let offsets: Vec<_> = acks.iter().map(|a| a.offset).collect();
    assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
    assert!(acks.iter().all(|a| !a.duplicate));

    let again = c
        .publish_batch(
            "batch",
            vec![
                PublishRecord::new("s", "v1").msg_id("m1"),
                PublishRecord::new("s", "new").msg_id("m9"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(again[0].offset, 1);
    assert!(again[0].duplicate);
    assert_eq!(again[1].offset, 5);
    assert!(!again[1].duplicate);
}

#[tokio::test]
async fn stream_management() {
    let server = TestServer::start().await;
    let c = server.client().await;

    let spec = StreamSpec {
        name: "s1".into(),
        max_age_secs: 3600,
        max_bytes: 1 << 30,
        ..Default::default()
    };
    c.create_stream(spec.clone()).await.unwrap();
    // Same settings: idempotent.
    c.create_stream(spec.clone()).await.unwrap();
    // Different settings: conflict.
    let err = c
        .create_stream(StreamSpec {
            max_age_secs: 60,
            ..spec.clone()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::CONFLICT));

    publish_n(&c, "s1", "x", 3).await;
    let info = c.stream_info("s1").await.unwrap();
    assert_eq!(info["name"], "s1");
    assert_eq!(info["next_offset"], 3);
    assert_eq!(info["config"]["max_age_secs"], 3600);

    c.update_stream(StreamSpec {
        max_age_secs: 7200,
        ..spec
    })
    .await
    .unwrap();
    let info = c.stream_info("s1").await.unwrap();
    assert_eq!(info["config"]["max_age_secs"], 7200);

    let names: Vec<String> = c
        .list_streams()
        .await
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"s1".to_string()));

    c.delete_stream("s1").await.unwrap();
    let err = c.stream_info("s1").await.unwrap_err();
    assert_eq!(err.code(), Some(code::NOT_FOUND));
    let err = c
        .publish("s1", PublishRecord::new("x", "y"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::NOT_FOUND));
}

#[tokio::test]
async fn internal_streams_are_protected() {
    let server = TestServer::start().await;
    let c = server.client().await;
    let err = c
        .create_stream(StreamSpec::named("__mine"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::FORBIDDEN));
    let err = c
        .publish("__consumers", PublishRecord::new("x", "y"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(code::FORBIDDEN));
    let err = c.delete_stream("__consumers").await.unwrap_err();
    assert_eq!(err.code(), Some(code::FORBIDDEN));
}

#[tokio::test]
async fn query_over_tcp() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "q").await;
    publish_n(&c, "q", "x", 3).await;
    let res = c.query(r#"SELECT * FROM "q""#).await.unwrap();
    assert_eq!(res["row_count"], 3);

    let err = c.query("SELEKT nonsense").await.unwrap_err();
    assert_eq!(err.code(), Some(code::BAD_REQUEST));
}

#[tokio::test]
async fn many_concurrent_requests_on_one_connection() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create_stream(&c, "conc").await;
    // A long-poll read must not block publishes on the same connection.
    let reader = c.clone();
    let wait = tokio::spawn(async move {
        reader
            .read("conc", 100, 10, Duration::from_secs(5), "")
            .await
            .unwrap()
    });
    let mut tasks = Vec::new();
    for i in 0..50 {
        let c = c.clone();
        tasks.push(tokio::spawn(async move {
            c.publish("conc", PublishRecord::new("s", format!("{i}")))
                .await
                .unwrap()
                .offset
        }));
    }
    let mut offsets = Vec::new();
    for t in tasks {
        offsets.push(t.await.unwrap());
    }
    offsets.sort();
    assert_eq!(offsets, (0..50).collect::<Vec<_>>());
    wait.abort();
}
