//! `s3` sink against a real S3-compatible store (MinIO in CI): crash and
//! resume.
//!
//! Set `EXSPEED_S3_ENDPOINT` (e.g. `http://127.0.0.1:9000`) and optionally
//! `EXSPEED_S3_ACCESS_KEY` / `EXSPEED_S3_SECRET_KEY` (default `minioadmin`),
//! then run with `--include-ignored`. The test creates its own bucket.
//! Without the endpoint the test skips, except under `CI=true`, where it
//! fails.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use exspeed_connectors::offset_store::OffsetStore;
use exspeed_connectors::ConnectorType::Sink;
use exspeed_connectors::Registry;

use crate::common::*;

struct S3Env {
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: Box<s3::Bucket>,
}

async fn s3_env(bucket: &str) -> Option<S3Env> {
    let endpoint = service_env("EXSPEED_S3_ENDPOINT")?;
    let access_key = std::env::var("EXSPEED_S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret_key = std::env::var("EXSPEED_S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    let region = s3::Region::Custom {
        region: "us-east-1".into(),
        endpoint: endpoint.clone(),
    };
    let creds = s3::creds::Credentials::new(Some(&access_key), Some(&secret_key), None, None, None)
        .unwrap();
    // A custom region otherwise sends `<LocationConstraint>us-east-1`, which
    // S3 itself and some S3-compatible stores reject.
    std::env::set_var("RUST_S3_SKIP_LOCATION_CONSTRAINT", "true");
    s3::Bucket::create_with_path_style(
        bucket,
        region.clone(),
        creds.clone(),
        s3::BucketConfiguration::default(),
    )
    .await
    .expect("create bucket on EXSPEED_S3_ENDPOINT");
    let bucket = s3::Bucket::new(bucket, region, creds)
        .unwrap()
        .with_path_style();
    Some(S3Env {
        endpoint,
        access_key,
        secret_key,
        bucket,
    })
}

impl S3Env {
    /// Every object under `prefix`: key → NDJSON lines.
    async fn objects(&self, prefix: &str) -> BTreeMap<String, Vec<Value>> {
        let mut out = BTreeMap::new();
        for page in self.bucket.list(prefix.to_string(), None).await.unwrap() {
            for o in page.contents {
                let data = self.bucket.get_object(&o.key).await.unwrap();
                let lines = String::from_utf8(data.as_slice().to_vec())
                    .unwrap()
                    .lines()
                    .map(|l| serde_json::from_str(l).unwrap())
                    .collect();
                out.insert(o.key, lines);
            }
        }
        out
    }

    async fn cleanup(&self) {
        if let Ok(pages) = self.bucket.list(String::new(), None).await {
            for page in pages {
                for o in page.contents {
                    let _ = self.bucket.delete_object(&o.key).await;
                }
            }
        }
        let _ = self.bucket.delete().await;
    }
}

/// Offsets per object, in key order.
fn offsets_by_object(objects: &BTreeMap<String, Vec<Value>>) -> Vec<(String, Vec<u64>)> {
    objects
        .iter()
        .map(|(k, lines)| {
            let file = k.rsplit('/').next().unwrap().to_string();
            (
                file,
                lines
                    .iter()
                    .map(|l| l["offset"].as_u64().unwrap())
                    .collect(),
            )
        })
        .collect()
}

fn part(first: u64) -> String {
    format!("part-{first:020}.ndjson")
}

/// A crash after an object is uploaded but before the offset commit: the
/// restarted sink rebuilds a buffer starting at the same record and
/// overwrites the same object. Every record ends up in the bucket exactly
/// once, and a graceful stop flushes the partial buffer.
#[tokio::test]
#[ignore = "needs an S3-compatible store (EXSPEED_S3_ENDPOINT)"]
async fn sink_crash_before_commit_overwrites_the_same_object() {
    let name = unique("s3");
    let bucket = name.replace('_', "-");
    let Some(s3e) = s3_env(&bucket).await else {
        return;
    };

    let env = Env::new();
    let vals: Vec<Value> = (0..25).map(|i| json!({"n": i})).collect();
    publish_json(&env, &name, &vals).await;

    let mut cfg = fast_config(&name, Sink, "s3", &name);
    cfg.batch_size = 5;
    // Flush on a full buffer (10 records); the timer only flushes the tail.
    cfg.flush_interval_ms = Some(1_000);
    cfg.settings = settings(json!({
        "bucket": bucket,
        "endpoint": s3e.endpoint,
        "path_style": true,
        "access_key": s3e.access_key,
        "secret_key": s3e.secret_key,
        "prefix": "archive/",
        "max_records": 10,
    }));
    let mem: Arc<dyn OffsetStore> = Arc::new(MemOffsets::default());
    // The first two commits "crash" after their object was uploaded.
    let crashing: Arc<dyn OffsetStore> = Arc::new(CrashingOffsets::new(mem.clone(), 2));
    let reg = Registry::builtin();
    let (h, state) = env.run(&reg, cfg.clone(), crashing);
    wait_committed(&mem, &name, 25).await;
    let restarts = state.snapshot().restart_count;
    h.stop(Duration::from_secs(15)).await;
    let objects = s3e.objects("archive/").await;

    // Records appended later are flushed by a graceful stop.
    let more: Vec<Value> = (25..28).map(|i| json!({"n": i})).collect();
    publish_json(&env, &name, &more).await;
    let mut cfg2 = cfg.clone();
    cfg2.flush_interval_ms = Some(600_000);
    let (h, state2) = env.run(&reg, cfg2, mem.clone());
    wait_running(&state2).await;
    // Let the sink read and buffer the tail (the flush timer is far off).
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    h.stop(Duration::from_secs(15)).await;
    let committed = mem.load_sink(&name).await.unwrap();
    let after_stop = s3e.objects("archive/").await;
    s3e.cleanup().await;

    assert!(restarts >= 2, "both crashes restarted the connector");
    assert_eq!(
        offsets_by_object(&objects),
        vec![
            (part(0), (0..10).collect()),
            (part(10), (10..20).collect()),
            (part(20), (20..25).collect()),
        ],
        "one object per buffer, keyed by its first offset, no duplicates"
    );
    for lines in objects.values() {
        for l in lines {
            let off = l["offset"].as_u64().unwrap();
            assert_eq!(l["value"], json!(format!("{{\"n\":{off}}}")));
        }
    }
    assert_eq!(committed, Some(28), "the graceful stop committed the tail");
    let all: Vec<u64> = offsets_by_object(&after_stop)
        .into_iter()
        .flat_map(|(_, o)| o)
        .collect();
    assert_eq!(all, (0..28).collect::<Vec<_>>(), "exactly once");
    for k in after_stop.keys() {
        assert!(
            k.starts_with(&format!("archive/{name}/")),
            "object key layout: {k}"
        );
    }
}
