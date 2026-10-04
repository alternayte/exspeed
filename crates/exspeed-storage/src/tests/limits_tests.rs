//! Message limits and lifetimes: record-exact trim (log start offset),
//! `max_msgs` with both discard policies, `max_msgs_per_subject` and TTLs.

use std::time::Duration;

use bytes::Bytes;
use exspeed_common::msg_time::TTL_HEADER;
use exspeed_common::{Offset, StreamName};
use exspeed_streams::{
    DiscardPolicy, ReadLimits, Record, StorageEngine, StorageError, StreamConfig,
};
use tempfile::TempDir;

use super::util::*;
use crate::file::FileStorage;

fn rec(subject: &str, value: &str) -> Record {
    Record {
        key: None,
        value: Bytes::copy_from_slice(value.as_bytes()),
        subject: subject.into(),
        headers: vec![],
        timestamp_ns: None,
    }
}

fn with_ttl(subject: &str, value: &str, ttl: &str) -> Record {
    let mut r = rec(subject, value);
    r.headers.push((TTL_HEADER.into(), ttl.into()));
    r
}

async fn values(s: &FileStorage, st: &StreamName) -> Vec<String> {
    read_all(s, st, 0)
        .await
        .iter()
        .map(|r| String::from_utf8_lossy(&r.value).into_owned())
        .collect()
}

async fn raw_offsets(s: &FileStorage, st: &StreamName) -> Vec<u64> {
    let mut out = Vec::new();
    let mut from = 0;
    loop {
        let b = s
            .read_raw(
                st,
                Offset(from),
                ReadLimits {
                    max_records: 1000,
                    max_bytes: 1 << 20,
                },
            )
            .await
            .unwrap();
        out.extend(b.records().map(|p| p.unwrap().offset));
        if b.next_offset.0 >= b.high_watermark.0 {
            return out;
        }
        from = b.next_offset.0;
    }
}

async fn create(s: &FileStorage, name: &str, cfg: StreamConfig) -> StreamName {
    let st = stream(name);
    s.create_stream_with(&st, &cfg).await.unwrap();
    st
}

#[tokio::test]
async fn trim_is_record_exact_and_survives_restart() {
    let dir = TempDir::new().unwrap();
    let st = stream("trim-exact");
    {
        let s = FileStorage::new(dir.path()).unwrap();
        s.create_stream(&st, 0, 0).await.unwrap();
        for i in 0..10 {
            s.append(&st, &rec("a", &i.to_string())).await.unwrap();
        }
        s.trim_up_to(&st, Offset(4)).await.unwrap();
        assert_eq!(s.stream_bounds(&st).await.unwrap(), (Offset(4), Offset(10)));
        assert_eq!(values(&s, &st).await, ["4", "5", "6", "7", "8", "9"]);
        s.close();
    }
    let s = FileStorage::open(dir.path()).unwrap();
    assert_eq!(s.stream_bounds(&st).await.unwrap(), (Offset(4), Offset(10)));
    let err = s.read(&st, Offset(1), 10).await.unwrap_err();
    assert!(matches!(
        err,
        StorageError::OffsetOutOfRange { earliest: 4, .. }
    ));
}

#[tokio::test]
async fn max_msgs_discard_old_drops_the_oldest() {
    let dir = TempDir::new().unwrap();
    let s = FileStorage::new(dir.path()).unwrap();
    let st = create(
        &s,
        "cap-old",
        StreamConfig {
            max_msgs: 3,
            ..StreamConfig::default()
        },
    )
    .await;
    for i in 0..5 {
        s.append(&st, &rec("a", &i.to_string())).await.unwrap();
    }
    assert_eq!(values(&s, &st).await, ["2", "3", "4"]);
    assert_eq!(s.stream_bounds(&st).await.unwrap(), (Offset(2), Offset(5)));
}

#[tokio::test]
async fn max_msgs_discard_new_rejects_until_there_is_room() {
    let dir = TempDir::new().unwrap();
    let s = FileStorage::new(dir.path()).unwrap();
    let st = create(
        &s,
        "cap-new",
        StreamConfig {
            max_msgs: 2,
            discard: DiscardPolicy::New,
            ..StreamConfig::default()
        },
    )
    .await;
    s.append(&st, &rec("a", "0")).await.unwrap();
    s.append(&st, &rec("a", "1")).await.unwrap();
    let err = s.append(&st, &rec("a", "2")).await.unwrap_err();
    assert!(matches!(err, StorageError::StreamFull { .. }), "{err}");
    // A batch that doesn't fit is rejected whole.
    s.trim_up_to(&st, Offset(1)).await.unwrap();
    let err = s
        .append_batch(&st, vec![rec("a", "x"), rec("a", "y")])
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::StreamFull { .. }));
    s.append(&st, &rec("a", "2")).await.unwrap();
    assert_eq!(values(&s, &st).await, ["1", "2"]);
}

#[tokio::test]
async fn max_msgs_per_subject_hides_older_records_everywhere() {
    let dir = TempDir::new().unwrap();
    let st = stream("per-subject");
    {
        let s = FileStorage::new(dir.path()).unwrap();
        s.create_stream_with(
            &st,
            &StreamConfig {
                max_msgs_per_subject: 1,
                ..StreamConfig::default()
            },
        )
        .await
        .unwrap();
        for (subj, v) in [("k.a", "a1"), ("k.b", "b1"), ("k.a", "a2"), ("k.a", "a3")] {
            s.append(&st, &rec(subj, v)).await.unwrap();
        }
        assert_eq!(values(&s, &st).await, ["b1", "a3"]);
        assert_eq!(raw_offsets(&s, &st).await, [1, 3]);
        assert_eq!(s.latest_for_subject(&st, "k.a"), Some(3));
        assert_eq!(s.latest_for_subject(&st, "k.zzz"), None);
        s.close();
    }
    // The index is rebuilt from the log on restart.
    let s = FileStorage::open(dir.path()).unwrap();
    assert_eq!(values(&s, &st).await, ["b1", "a3"]);
    assert_eq!(s.latest_for_subject(&st, "k.b"), Some(1));
}

#[tokio::test]
async fn enabling_a_per_subject_limit_applies_to_existing_records() {
    let dir = TempDir::new().unwrap();
    let s = FileStorage::new(dir.path()).unwrap();
    let st = create(&s, "per-subject-later", StreamConfig::default()).await;
    for v in ["1", "2", "3"] {
        s.append(&st, &rec("x", v)).await.unwrap();
    }
    let cfg = StreamConfig {
        max_msgs_per_subject: 2,
        ..StreamConfig::default()
    };
    s.update_stream_config(&st, &cfg).await.unwrap();
    // The rebuild runs on the writer thread; an append queues behind it.
    s.append(&st, &rec("y", "y")).await.unwrap();
    assert_eq!(values(&s, &st).await, ["2", "3", "y"]);
}

#[tokio::test]
async fn expired_records_are_hidden_from_readers() {
    let dir = TempDir::new().unwrap();
    let s = FileStorage::new(dir.path()).unwrap();
    let st = create(
        &s,
        "ttl",
        StreamConfig {
            allow_msg_ttl: true,
            ..StreamConfig::default()
        },
    )
    .await;
    s.append(&st, &with_ttl("a", "short", "50ms"))
        .await
        .unwrap();
    s.append(&st, &rec("a", "forever")).await.unwrap();
    s.append(&st, &with_ttl("a", "long", "1h")).await.unwrap();
    assert_eq!(values(&s, &st).await, ["short", "forever", "long"]);
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(values(&s, &st).await, ["forever", "long"]);
    assert_eq!(raw_offsets(&s, &st).await, [1, 2]);
    // A consumer that dead-letters expired records still sees them.
    let all = s
        .read_raw_including_expired(
            &st,
            Offset(0),
            ReadLimits {
                max_records: 10,
                max_bytes: 1 << 20,
            },
        )
        .await
        .unwrap();
    assert_eq!(all.count, 3);
}

#[tokio::test]
async fn stream_default_ttl_applies_without_headers() {
    let dir = TempDir::new().unwrap();
    let s = FileStorage::new(dir.path()).unwrap();
    let st = create(
        &s,
        "ttl-default",
        StreamConfig {
            msg_ttl_ms: 50,
            ..StreamConfig::default()
        },
    )
    .await;
    s.append(&st, &rec("a", "x")).await.unwrap();
    // A TTL header is ignored on a stream that doesn't allow it.
    s.append(&st, &with_ttl("a", "y", "1h")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(values(&s, &st).await.is_empty());
    assert_eq!(s.stream_bounds(&st).await.unwrap().1, Offset(2));
}

#[tokio::test]
async fn compaction_removes_expired_and_superseded_records() {
    let dir = TempDir::new().unwrap();
    let s = FileStorage::new(dir.path()).unwrap();
    let st = create(
        &s,
        "compact-limits",
        StreamConfig {
            allow_msg_ttl: true,
            max_msgs_per_subject: 1,
            ..StreamConfig::default()
        },
    )
    .await;
    s.set_stream_segment_max_bytes(st.as_str(), 1);
    s.append(&st, &with_ttl("t", "gone", "1ms")).await.unwrap();
    s.append(&st, &rec("k", "old")).await.unwrap();
    s.append(&st, &rec("k", "new")).await.unwrap();
    s.append(&st, &rec("z", "tail")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let stats = s.compact_stream(st.as_str()).unwrap();
    assert_eq!(stats.records_removed, 2, "{stats:?}");
    assert_eq!(values(&s, &st).await, ["new", "tail"]);
}
