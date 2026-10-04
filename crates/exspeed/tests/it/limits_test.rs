//! Message limits and lifetimes end to end: per-message and stream-wide
//! TTLs, delayed delivery (also across a restart), `max_msgs` with both
//! discard policies, `max_msgs_per_subject`, and dead-lettering expired
//! records.

use std::time::Duration;

use exspeed_client::{
    code, Client, ConsumerSpec, DiscardPolicy, PublishRecord, StreamLimits, StreamSpec,
};

use crate::common::{eventually, TestServer};

const WAIT: Duration = Duration::from_secs(5);

async fn create(c: &Client, name: &str, limits: StreamLimits) {
    c.create_stream(StreamSpec {
        limits,
        ..StreamSpec::named(name)
    })
    .await
    .unwrap();
}

fn rec(subject: &str, v: &str) -> PublishRecord {
    PublishRecord::new(subject, v.to_string())
}

fn texts(records: &[exspeed_client::WireRecord]) -> Vec<String> {
    records
        .iter()
        .map(|r| String::from_utf8_lossy(&r.value).into_owned())
        .collect()
}

async fn read_all(c: &Client, stream: &str) -> Vec<String> {
    let r = c.read(stream, 0, 1000, Duration::ZERO, "").await.unwrap();
    texts(&r.records)
}

#[tokio::test]
async fn expired_records_are_never_delivered_or_read() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create(
        &c,
        "jobs",
        StreamLimits {
            allow_msg_ttl: true,
            ..Default::default()
        },
    )
    .await;
    c.publish("jobs", rec("j", "short").ttl(Duration::from_millis(150)))
        .await
        .unwrap();
    c.publish("jobs", rec("j", "keep")).await.unwrap();
    c.create_consumer(ConsumerSpec::new("w", "jobs"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let got = c.pull("w", 10, Duration::from_millis(500)).await.unwrap();
    assert_eq!(texts(&got), ["keep"]);
    assert_eq!(read_all(&c, "jobs").await, ["keep"]);
}

#[tokio::test]
async fn time_headers_need_the_stream_to_allow_them() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create(&c, "plain", StreamLimits::default()).await;
    for r in [
        rec("a", "x").ttl(Duration::from_secs(1)),
        rec("a", "x").delay(Duration::from_secs(1)),
        rec("a", "x").header("exspeed-ttl", "soon"),
    ] {
        let err = c.publish("plain", r).await.unwrap_err();
        assert_eq!(err.code(), Some(code::BAD_REQUEST), "{err}");
    }
}

#[tokio::test]
async fn delayed_records_are_delivered_when_due() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create(
        &c,
        "later",
        StreamLimits {
            allow_delayed: true,
            ..Default::default()
        },
    )
    .await;
    c.create_consumer(ConsumerSpec::new("w", "later"))
        .await
        .unwrap();
    c.publish(
        "later",
        rec("a", "delayed").delay(Duration::from_millis(700)),
    )
    .await
    .unwrap();
    c.publish("later", rec("a", "now")).await.unwrap();

    let first = c.pull("w", 10, Duration::from_millis(300)).await.unwrap();
    assert_eq!(texts(&first), ["now"], "the later record goes first");
    c.ack("w", first.iter().map(|r| r.offset).collect())
        .await
        .unwrap();
    let info = c.consumer_info("w").await.unwrap();
    assert_eq!(info["num_delayed"], 1);
    assert_eq!(info["ack_floor"], 0, "the delayed record holds the floor");

    let due = eventually(WAIT, || async {
        let got = c.pull("w", 10, Duration::from_millis(200)).await.unwrap();
        (!got.is_empty()).then_some(got)
    })
    .await;
    assert_eq!(texts(&due), ["delayed"]);
    assert_eq!(
        due[0].delivery_count, 1,
        "a delayed record's first delivery"
    );
    // A stateless read sees it right away: the delay applies to consumers.
    assert_eq!(read_all(&c, "later").await, ["delayed", "now"]);
}

#[tokio::test]
async fn delayed_records_wait_across_a_restart() {
    let server = TestServer::start().await;
    {
        let c = server.client().await;
        create(
            &c,
            "later",
            StreamLimits {
                allow_delayed: true,
                ..Default::default()
            },
        )
        .await;
        c.create_consumer(ConsumerSpec::new("w", "later"))
            .await
            .unwrap();
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 2_000;
        c.publish("later", rec("a", "scheduled").deliver_at(at))
            .await
            .unwrap();
        assert!(c
            .pull("w", 10, Duration::from_millis(300))
            .await
            .unwrap()
            .is_empty());
        // Let the consumer persist its state.
        eventually(WAIT, || async {
            let i = c.consumer_info("w").await.unwrap();
            (i["num_delayed"] == 1).then_some(())
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let server = server.restart().await;
    let c = server.client().await;
    assert!(
        c.pull("w", 10, Duration::from_millis(200))
            .await
            .unwrap()
            .is_empty(),
        "still not due after the restart"
    );
    let got = eventually(WAIT, || async {
        let got = c.pull("w", 10, Duration::from_millis(200)).await.unwrap();
        (!got.is_empty()).then_some(got)
    })
    .await;
    assert_eq!(texts(&got), ["scheduled"]);
}

#[tokio::test]
async fn max_msgs_discard_new_rejects_with_429() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create(
        &c,
        "bounded",
        StreamLimits {
            max_msgs: 2,
            discard: DiscardPolicy::New,
            ..Default::default()
        },
    )
    .await;
    c.publish("bounded", rec("a", "1")).await.unwrap();
    c.publish("bounded", rec("a", "2")).await.unwrap();
    let err = c.publish("bounded", rec("a", "3")).await.unwrap_err();
    assert_eq!(err.code(), Some(code::TOO_MANY_REQUESTS), "{err}");
    assert_eq!(read_all(&c, "bounded").await, ["1", "2"]);
}

#[tokio::test]
async fn max_msgs_discard_old_keeps_the_newest() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create(
        &c,
        "ring",
        StreamLimits {
            max_msgs: 3,
            ..Default::default()
        },
    )
    .await;
    for i in 0..10 {
        c.publish("ring", rec("a", &i.to_string())).await.unwrap();
    }
    assert_eq!(read_all(&c, "ring").await, ["7", "8", "9"]);
    let info = c.stream_info("ring").await.unwrap();
    assert_eq!(info["earliest_offset"], 7);
    assert_eq!(info["records"], 3);
}

#[tokio::test]
async fn max_msgs_per_subject_keeps_the_last_value_of_each_subject() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create(
        &c,
        "last",
        StreamLimits {
            max_msgs_per_subject: 1,
            ..Default::default()
        },
    )
    .await;
    for (s, v) in [
        ("price.btc", "100"),
        ("price.eth", "5"),
        ("price.btc", "101"),
        ("price.btc", "102"),
    ] {
        c.publish("last", rec(s, v)).await.unwrap();
    }
    assert_eq!(read_all(&c, "last").await, ["5", "102"]);
    c.create_consumer(ConsumerSpec::new("w", "last"))
        .await
        .unwrap();
    let got = c.pull("w", 10, Duration::from_millis(300)).await.unwrap();
    assert_eq!(texts(&got), ["5", "102"]);
}

#[tokio::test]
async fn expired_records_can_be_dead_lettered() {
    let server = TestServer::start().await;
    let c = server.client().await;
    create(
        &c,
        "orders",
        StreamLimits {
            msg_ttl_ms: 150,
            ..Default::default()
        },
    )
    .await;
    let mut spec = ConsumerSpec::new("w", "orders");
    spec.dlq_stream = Some("orders-dlq".into());
    spec.dead_letter_expired = true;
    c.create_consumer(spec).await.unwrap();
    c.publish("orders", rec("o", "stale")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(c
        .pull("w", 10, Duration::from_millis(300))
        .await
        .unwrap()
        .is_empty());
    let dead = eventually(WAIT, || async {
        let r = c.read("orders-dlq", 0, 10, Duration::ZERO, "").await.ok()?;
        (!r.records.is_empty()).then_some(r.records)
    })
    .await;
    assert_eq!(texts(&dead), ["stale"]);
    let reason = dead[0]
        .headers
        .iter()
        .find(|(k, _)| k == "exspeed-dlq-reason")
        .map(|(_, v)| v.as_str());
    assert_eq!(reason, Some("expired"));
}

#[tokio::test]
async fn limits_over_http() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let r = http
        .post(server.api_url("/api/v1/streams"))
        .json(&serde_json::json!({
            "name": "q",
            "max_msgs": 100,
            "discard": "new",
            "max_msgs_per_subject": 2,
            "allow_msg_ttl": true,
            "msg_ttl_ms": 60000,
            "allow_delayed": true,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201, "{}", r.text().await.unwrap());
    let info: serde_json::Value = http
        .get(server.api_url("/api/v1/streams/q"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(info["max_msgs"], 100);
    assert_eq!(info["discard"], "new");
    assert_eq!(info["max_msgs_per_subject"], 2);
    assert_eq!(info["allow_msg_ttl"], true);
    assert_eq!(info["msg_ttl_ms"], 60000);
    assert_eq!(info["allow_delayed"], true);
    assert_eq!(info["retention"], "limits");

    let r = http
        .patch(server.api_url("/api/v1/streams/q"))
        .json(&serde_json::json!({"max_msgs": 5, "discard": "old"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let info: serde_json::Value = r.json().await.unwrap();
    assert_eq!(info["max_msgs"], 5);
    assert_eq!(info["discard"], "old");

    let r = http
        .post(server.api_url("/api/v1/streams"))
        .json(&serde_json::json!({"name": "bad", "discard": "sometimes"}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_client_error());
}
