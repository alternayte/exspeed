//! Online backup (`GET /api/v1/backup`, `exspeed backup`) and offline
//! restore (`exspeed restore`) against real servers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use exspeed::cli::backup::{BackupArgs, RestoreArgs};
use exspeed_client::{Client, ConsumerSpec, PublishRecord, StreamSpec};
use exspeed_protocol::client::WireRecord;
use exspeed_storage::file::backup::{read_manifest, BackupManifest};

use crate::common::TestServer;

async fn read_range(client: &Client, stream: &str, from: u64, to: u64) -> Vec<WireRecord> {
    let mut out = Vec::new();
    let mut next = from;
    while next < to {
        let r = client
            .read(stream, next, 500, Duration::ZERO, "")
            .await
            .expect("read");
        assert!(r.next_offset > next, "read made no progress at {next}");
        out.extend(r.records.into_iter().filter(|rec| rec.offset < to));
        next = r.next_offset;
    }
    out
}

fn assert_identical(a: &[WireRecord], b: &[WireRecord], stream: &str) {
    assert_eq!(a.len(), b.len(), "{stream}: record count");
    for (x, y) in a.iter().zip(b) {
        assert_eq!(x.offset, y.offset, "{stream}");
        assert_eq!(x.timestamp_ns, y.timestamp_ns, "{stream}@{}", x.offset);
        assert_eq!(x.subject, y.subject, "{stream}@{}", x.offset);
        assert_eq!(x.key, y.key, "{stream}@{}", x.offset);
        assert_eq!(x.value, y.value, "{stream}@{}", x.offset);
        assert_eq!(x.headers, y.headers, "{stream}@{}", x.offset);
    }
}

fn keyed_record(i: u64) -> PublishRecord {
    PublishRecord::new(
        format!("orders.{}", ["eu", "us", "apac"][(i % 3) as usize]),
        format!(r#"{{"order":{i},"total":{}}}"#, i * 7 % 1000),
    )
    .key(format!("customer-{}", i % 17))
    .header("trace-id", format!("trace-{i}"))
    .header("source", "backup-test")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_while_publishing_then_restore_into_new_server() {
    let src = TestServer::start().await;
    let client = src.client().await;
    for s in ["orders", "events", "empty"] {
        client.create_stream(StreamSpec::named(s)).await.unwrap();
    }
    for i in 0..300u64 {
        client.publish("orders", keyed_record(i)).await.unwrap();
        client
            .publish(
                "events",
                PublishRecord::new("events.tick", vec![(i % 251) as u8; 64]),
            )
            .await
            .unwrap();
    }
    client
        .create_consumer(ConsumerSpec::new("billing", "orders"))
        .await
        .unwrap();

    // Keep publishing to two streams while the backup runs.
    let stop = Arc::new(AtomicBool::new(false));
    let publisher = {
        let client = src.client().await;
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut i = 300u64;
            while !stop.load(Ordering::Relaxed) {
                client.publish("orders", keyed_record(i)).await.unwrap();
                client
                    .publish("events", PublishRecord::new("events.tick", vec![1u8; 64]))
                    .await
                    .unwrap();
                i += 1;
            }
            i
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    let work = tempfile::tempdir().unwrap();
    let archive = work.path().join("backup.tar");
    exspeed::cli::backup::backup(
        BackupArgs {
            url: Some(format!("http://{}", src.api_addr)),
            token: None,
            output: archive.clone(),
        },
        "http://unused",
    )
    .await
    .expect("backup");
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop.store(true, Ordering::Relaxed);
    let published = publisher.await.unwrap();

    let manifest: BackupManifest =
        read_manifest(std::fs::File::open(&archive).unwrap()).expect("manifest");
    let by_name = |n: &str| {
        manifest
            .streams
            .iter()
            .find(|s| s.name == n)
            .unwrap_or_else(|| panic!("{n} missing from manifest"))
            .clone()
    };
    let orders = by_name("orders");
    let events = by_name("events");
    assert_eq!((orders.earliest_offset, events.earliest_offset), (0, 0));
    assert!(orders.next_offset >= 300 && orders.next_offset <= published);
    assert!(events.next_offset >= 300);
    assert_eq!(by_name("empty").next_offset, 0);
    assert!(
        manifest.streams.iter().any(|s| s.name == "__consumers"),
        "internal streams are backed up"
    );

    // Expected contents: the source's records below each manifest offset.
    let mut expected = Vec::new();
    for s in &manifest.streams {
        expected.push((
            s.name.clone(),
            read_range(&client, &s.name, s.earliest_offset, s.next_offset).await,
        ));
    }

    let restored_dir = work.path().join("restored");
    exspeed::cli::backup::restore(RestoreArgs {
        input: archive.clone(),
        data_dir: restored_dir.clone(),
        force: false,
    })
    .await
    .expect("restore");
    // A second restore into the now non-empty dir is refused.
    let err = exspeed::cli::backup::restore(RestoreArgs {
        input: archive.clone(),
        data_dir: restored_dir.clone(),
        force: false,
    })
    .await
    .unwrap_err();
    assert!(format!("{err:#}").contains("not empty"), "{err:#}");

    let dst = TestServer::builder().data_dir(&restored_dir).start().await;
    let rclient = dst.client().await;
    for (name, want) in &expected {
        let s = manifest.streams.iter().find(|s| &s.name == name).unwrap();
        let got = read_range(&rclient, name, s.earliest_offset, s.next_offset).await;
        assert_identical(&got, want, name);
        let r = rclient
            .read(name, s.next_offset, 10, Duration::ZERO, "")
            .await
            .unwrap();
        // Internal streams may legitimately grow once the restored server
        // runs (consumer state); user streams must end exactly there.
        if !name.starts_with("__") {
            assert_eq!(
                r.high_watermark, s.next_offset,
                "{name}: nothing beyond the snapshot"
            );
            assert!(r.records.is_empty());
        }
    }
    assert_eq!(orders.records, orders.next_offset);
    assert_eq!(
        expected
            .iter()
            .find(|(n, _)| n == "orders")
            .unwrap()
            .1
            .len() as u64,
        orders.next_offset
    );

    // The restored server appends at the right next offset.
    let ack = rclient
        .publish("orders", keyed_record(999_999))
        .await
        .unwrap();
    assert_eq!(ack.offset, orders.next_offset);
    let ack = rclient
        .publish("events", PublishRecord::new("events.tick", "x"))
        .await
        .unwrap();
    assert_eq!(ack.offset, events.next_offset);
    let ack = rclient
        .publish("empty", PublishRecord::new("e", "first"))
        .await
        .unwrap();
    assert_eq!(ack.offset, 0);

    // Consumer state came along with `__consumers`.
    let consumers = rclient.list_consumers(Some("orders")).await.unwrap();
    assert!(
        consumers
            .iter()
            .any(|c| c["spec"]["name"] == serde_json::json!("billing")),
        "{consumers:?}"
    );
}

#[tokio::test]
async fn backup_requires_admin_when_auth_is_on() {
    let srv = TestServer::builder().auth_token("s3cret").start().await;
    let http = reqwest::Client::new();
    let url = srv.api_url("/api/v1/backup");
    let r = http.get(&url).send().await.unwrap();
    assert_eq!(r.status(), 401);
    let r = http.get(&url).bearer_auth("wrong").send().await.unwrap();
    assert_eq!(r.status(), 401);
    let r = http.get(&url).bearer_auth("s3cret").send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.headers()["content-type"].to_str().unwrap(),
        "application/x-tar"
    );
    let body = r.bytes().await.unwrap();
    let m = read_manifest(std::io::Cursor::new(&body)).unwrap();
    assert_eq!(m.server_version, env!("CARGO_PKG_VERSION"));

    // The CLI passes --token through.
    let work = tempfile::tempdir().unwrap();
    let out = work.path().join("b.tar");
    let err = exspeed::cli::backup::backup(
        BackupArgs {
            url: Some(format!("http://{}", srv.api_addr)),
            token: None,
            output: out.clone(),
        },
        "",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("401"), "{err}");
    assert!(!out.exists());
    exspeed::cli::backup::backup(
        BackupArgs {
            url: Some(format!("http://{}", srv.api_addr)),
            token: Some("s3cret".into()),
            output: out.clone(),
        },
        "",
    )
    .await
    .unwrap();
    assert!(out.exists());
}
