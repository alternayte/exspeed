//! Response frames stay under the 16 MiB frame limit even when records
//! carry large headers: Read, Pull and push `Deliver` batches budget every
//! record's full wire size (headers and framing included), so a batch is
//! split across responses instead of overflowing the frame and dropping the
//! connection (which made a pull redeliver the same batch forever).

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use exspeed_client::{ConsumerSpec, PublishRecord, Request, Response};

use crate::common::{create_stream, TestServer};

/// ~600 × 32 KB of headers ≈ 19 MB: more than one frame can carry.
const N: usize = 600;
const HEADER: usize = 32_000;

async fn fill(client: &exspeed_client::Client, stream: &str) {
    create_stream(client, stream).await;
    let hv = "h".repeat(HEADER);
    for chunk in 0..(N / 100) {
        let recs = (0..100)
            .map(|i| PublishRecord {
                subject: "s".into(),
                key: None,
                value: Bytes::from(format!("{}", chunk * 100 + i)),
                headers: vec![("big".into(), hv.clone())],
                msg_id: None,
            })
            .collect();
        client.publish_batch(stream, recs).await.unwrap();
    }
}

#[tokio::test]
async fn read_splits_large_header_batches_across_frames() {
    let server = TestServer::start().await;
    let client = server.client().await;
    fill(&client, "bigread").await;

    let mut from = 0;
    let mut seen = 0usize;
    let mut reads = 0;
    while seen < N {
        // Ask for far more than a frame holds; the server caps it.
        let resp = client
            .raw(Request::Read {
                stream: "bigread".into(),
                from,
                max_records: 10_000,
                max_bytes: u32::MAX,
                wait_ms: 0,
                filter: String::new(),
            })
            .await
            .expect("read must succeed, not overflow the frame");
        let Response::ReadResult {
            next_offset,
            records,
            ..
        } = resp
        else {
            panic!("unexpected response {resp:?}");
        };
        assert!(!records.is_empty());
        assert!(records.len() < N, "one frame carried every record");
        let bytes: usize = records.iter().map(|r| r.encoded_len().unwrap()).sum();
        assert!(
            bytes <= exspeed_common::MAX_RECORDS_BYTES_PER_FRAME,
            "{bytes}"
        );
        seen += records.len();
        from = next_offset;
        reads += 1;
    }
    assert_eq!(seen, N);
    assert!(reads >= 3, "expected several frames, got {reads}");
    assert!(!client.is_closed(), "connection must survive");
}

#[tokio::test]
async fn pull_splits_large_header_batches_and_makes_progress() {
    let server = TestServer::start().await;
    let client = server.client().await;
    fill(&client, "bigpull").await;
    client
        .create_consumer(ConsumerSpec::new("p", "bigpull"))
        .await
        .unwrap();

    let mut offsets = BTreeSet::new();
    let mut pulls = 0;
    while offsets.len() < N {
        // Ask for far more than a frame holds; the server caps it.
        let resp = client
            .raw(Request::Pull {
                consumer: "p".into(),
                max_messages: N as u32,
                max_bytes: u32::MAX,
                expires_ms: 2_000,
            })
            .await
            .expect("pull must succeed, not overflow the frame");
        let Response::Messages { records } = resp else {
            panic!("unexpected response {resp:?}");
        };
        assert!(
            !records.is_empty(),
            "pull stalled at {} records",
            offsets.len()
        );
        assert!(records.len() < N, "one frame carried every record");
        for r in &records {
            assert!(offsets.insert(r.offset), "offset {} redelivered", r.offset);
            assert_eq!(r.delivery_count, 1);
        }
        client
            .ack("p", records.iter().map(|r| r.offset).collect())
            .await
            .unwrap();
        pulls += 1;
        assert!(pulls < 50, "pull is not making progress");
    }
    assert_eq!(offsets.len(), N);
    assert!(pulls >= 3, "expected several frames, got {pulls}");
}

#[tokio::test]
async fn push_splits_large_header_batches_into_several_deliver_frames() {
    let server = TestServer::start().await;
    let client = server.client().await;
    fill(&client, "bigpush").await;
    client
        .create_consumer(ConsumerSpec::new("q", "bigpush"))
        .await
        .unwrap();
    // A window covering every record lets the actor batch them all at once.
    let mut sub = client.subscribe("q", N as u32).await.unwrap();
    let mut offsets = BTreeSet::new();
    while offsets.len() < N {
        let m = sub
            .next_timeout(Duration::from_secs(10))
            .await
            .unwrap_or_else(|| panic!("push stalled at {} records", offsets.len()));
        assert!(offsets.insert(m.record.offset));
        m.ack().await.unwrap();
    }
    assert!(!client.is_closed(), "connection must survive");
}

/// A pull waiter holding just under its 8 MiB budget must not take one more
/// ~8.2 MiB record (the largest the broker accepts): together they would
/// exceed the 16 MiB frame limit. The big record goes out in the next reply.
#[tokio::test]
async fn pull_answers_before_a_big_record_would_overflow_the_frame() {
    let server = TestServer::start().await;
    let client = server.client().await;
    create_stream(&client, "bigtail").await;
    for i in 0..16u32 {
        let mut value = vec![b'x'; 520_000];
        value[..4].copy_from_slice(&i.to_le_bytes());
        client
            .publish("bigtail", PublishRecord::new("s", value))
            .await
            .unwrap();
    }
    let big = PublishRecord::new("s", vec![b'y'; 8 * 1024 * 1024])
        .key(vec![b'k'; 64 * 1024])
        .header("h1", "a".repeat(30_000))
        .header("h2", "b".repeat(30_000));
    client.publish("bigtail", big).await.unwrap();
    client
        .create_consumer(ConsumerSpec::new("t", "bigtail"))
        .await
        .unwrap();

    let mut got = Vec::new();
    let mut frames = 0;
    while got.len() < 17 {
        let resp = client
            .raw(Request::Pull {
                consumer: "t".into(),
                max_messages: 100,
                max_bytes: u32::MAX,
                expires_ms: 2_000,
            })
            .await
            .expect("pull must succeed, not overflow the frame");
        let Response::Messages { records } = resp else {
            panic!("unexpected response {resp:?}");
        };
        assert!(!records.is_empty(), "pull stalled at {}", got.len());
        let bytes: usize = records.iter().map(|r| r.encoded_len().unwrap()).sum();
        assert!(
            bytes + 4 <= exspeed_common::MAX_PAYLOAD_SIZE as usize,
            "Messages payload of {bytes} bytes"
        );
        client
            .ack("t", records.iter().map(|r| r.offset).collect())
            .await
            .unwrap();
        got.extend(records.iter().map(|r| r.offset));
        frames += 1;
        assert!(frames < 20);
    }
    assert_eq!(got, (0..17).collect::<Vec<u64>>());
    assert!(frames >= 2, "the big record needed its own reply");
    assert!(!client.is_closed());
}
