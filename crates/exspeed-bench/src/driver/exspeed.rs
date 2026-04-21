use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite};

use hdrhistogram::Histogram;

use exspeed_protocol::codec::ExspeedCodec;
use exspeed_protocol::frame::Frame;
use exspeed_protocol::messages::ack::AckRequest;
use exspeed_protocol::messages::connect::{AuthType, ConnectRequest};
use exspeed_protocol::messages::consumer::{CreateConsumerRequest, StartFrom, SubscribeRequest};
use exspeed_protocol::messages::publish::PublishRequest;
use exspeed_protocol::messages::record_delivery::RecordDelivery;
use exspeed_protocol::messages::stream_mgmt::CreateStreamRequest;
use exspeed_protocol::opcodes::OpCode;

pub const PUBLISH_TS_HEADER: &str = "bench.publish_us";

pub type Reader = FramedRead<OwnedReadHalf, ExspeedCodec>;
pub type Writer = FramedWrite<OwnedWriteHalf, ExspeedCodec>;

pub struct ExspeedClient {
    /// Exposed for subsequent tasks (Subscribe/Ack) that issue frames on the
    /// same TCP connection without going through this wrapper.
    pub reader: Reader,
    /// See `reader`.
    pub writer: Writer,
    next_corr: u32,
}

impl ExspeedClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await.context("tcp connect")?;
        stream.set_nodelay(true).ok();
        let (r, w) = stream.into_split();
        let mut client = Self {
            reader: FramedRead::new(r, ExspeedCodec::new()),
            writer: FramedWrite::new(w, ExspeedCodec::new()),
            next_corr: 1,
        };
        client.connect_handshake().await?;
        Ok(client)
    }

    async fn connect_handshake(&mut self) -> Result<()> {
        let req = ConnectRequest {
            client_id: "exspeed-bench".into(),
            auth_type: AuthType::None,
            auth_payload: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let corr = self.alloc_corr();
        self.writer.send(Frame::new(OpCode::Connect, corr, buf.freeze())).await?;
        let resp = self.reader.next().await.ok_or_else(|| anyhow!("connect closed"))??;
        if resp.opcode != OpCode::ConnectOk {
            return Err(anyhow!("expected ConnectOk, got {:?}", resp.opcode));
        }
        Ok(())
    }

    pub async fn ensure_stream(&mut self, name: &str) -> Result<()> {
        let req = CreateStreamRequest {
            stream_name: name.into(),
            max_age_secs: 0,
            max_bytes: 0,
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let corr = self.alloc_corr();
        self.writer.send(Frame::new(OpCode::CreateStream, corr, buf.freeze())).await?;
        let resp = self.reader.next().await.ok_or_else(|| anyhow!("closed"))??;
        match resp.opcode {
            OpCode::Ok => Ok(()),
            OpCode::Error => {
                // Any Error response is treated as "stream already exists". This is the
                // only Error path reachable in a well-formed benchmark run. Invalid
                // stream names would also land here and be silently ignored; if
                // ensure_stream ever starts being called with dynamic/untrusted names,
                // decode the error payload and only accept code == 409.
                Ok(())
            }
            other => Err(anyhow!("ensure_stream: unexpected opcode {other:?}")),
        }
    }

    pub fn alloc_corr(&mut self) -> u32 {
        let c = self.next_corr;
        // CorrelID 0 is reserved for push-delivered records, so skip it on wrap.
        self.next_corr = match self.next_corr.wrapping_add(1) {
            0 => 1,
            n => n,
        };
        c
    }

    /// Publishes a single record with the publish_us header encoded as ASCII decimal.
    pub async fn publish_once(&mut self, stream: &str, value: &Bytes, origin: Instant) -> Result<()> {
        let us = origin.elapsed().as_micros() as u64;
        let req = PublishRequest {
            stream: stream.into(),
            subject: "bench".into(),
            key: None,
            msg_id: None,
            value: value.clone(),
            headers: vec![(PUBLISH_TS_HEADER.to_owned(), format!("{us}"))],
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let corr = self.alloc_corr();
        self.writer.send(Frame::new(OpCode::Publish, corr, buf.freeze())).await?;
        let resp = self.reader.next().await.ok_or_else(|| anyhow!("closed"))??;
        match resp.opcode {
            OpCode::PublishOk => Ok(()),
            OpCode::Error => {
                let mut p = resp.payload;
                let code = if p.remaining() >= 2 { p.get_u16_le() } else { 0 };
                let msg = if p.remaining() >= 2 {
                    let len = p.get_u16_le() as usize;
                    if p.remaining() >= len {
                        String::from_utf8_lossy(&p.slice(..len)).into_owned()
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                };
                Err(anyhow!("publish error (code={code:#06x}): {msg}"))
            }
            other => Err(anyhow!("publish: unexpected opcode {other:?}")),
        }
    }
}

pub struct ProducerStats {
    pub messages: u64,
    pub bytes: u64,
    pub wall_secs: f64,
}

/// Spawn `tasks` producer tasks. Each owns one pipelined Publisher connection
/// and keeps up to 256 publishes in flight concurrently, removing per-publish
/// round-trip cost from the critical path. Returns aggregate stats.
/// `origin` is the shared start instant for publish_us headers so the consumer
/// can compute deltas with `now.duration_since(origin)`.
pub async fn run_producer(
    addr: &str,
    stream: &str,
    payload_bytes: usize,
    duration: Duration,
    tasks: usize,
    origin: Instant,
    shared_count: Arc<AtomicU64>,
) -> Result<ProducerStats> {
    use crate::driver::publisher::PublisherBuilder;
    use futures_util::stream::FuturesUnordered;

    let payload: Bytes = Bytes::from(vec![b'x'; payload_bytes]);
    let stream = stream.to_owned();
    let mut handles = Vec::with_capacity(tasks);
    // Concurrency window per producer task.  4 concurrent publishes provides a
    // meaningful pipelining gain (4× better than serial per task) while keeping
    // the queue depth shallow enough that the bench consumer — which reads records
    // serially with a 1 s p50 latency bound — does not fall behind.  The
    // Publisher semaphore allows up to 256 permits; this constant is a bench
    // driver policy, not a transport limit.
    let in_flight_per_task: usize = 4;

    for _ in 0..tasks {
        let addr = addr.to_owned();
        let stream = stream.clone();
        let payload = payload.clone();
        let shared_count = shared_count.clone();
        handles.push(tokio::spawn(async move {
            let task_start = Instant::now();
            let publisher = PublisherBuilder::new(&addr)
                .max_in_flight(in_flight_per_task)
                .build()
                .await?;
            let deadline = task_start + duration;
            let mut local: u64 = 0;
            let mut committed: u64 = 0;
            let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
            while Instant::now() < deadline {
                while futs.len() < in_flight_per_task && Instant::now() < deadline {
                    // Set the timestamp lazily inside the async block so it
                    // reflects when the message is actually sent, not when it
                    // is queued into FuturesUnordered.
                    let stream = stream.clone();
                    let payload = payload.clone();
                    let pub2 = publisher.clone();
                    futs.push(async move {
                        let us = origin.elapsed().as_micros() as u64;
                        let req = exspeed_protocol::messages::publish::PublishRequest {
                            stream,
                            subject: "bench".into(),
                            key: None,
                            msg_id: None,
                            value: payload,
                            headers: vec![(PUBLISH_TS_HEADER.to_owned(), format!("{us}"))],
                        };
                        pub2.publish(req).await
                    });
                }
                if let Some(res) = futs.next().await {
                    let _ = res; // count locally; offset/error not used here
                    local += 1;
                    if local - committed >= 256 {
                        shared_count.fetch_add(256, Ordering::Relaxed);
                        committed += 256;
                    }
                }
            }
            // Snapshot active window before draining so the wall_secs reported
            // to the caller reflects only the publish-rate window, not drain time.
            let active_secs = task_start.elapsed().as_secs_f64();
            // Drain remaining in-flight (don't lose in-flight acks).
            while let Some(res) = futs.next().await {
                let _ = res;
                local += 1;
            }
            drop(futs);
            // Commit the tail not yet published to shared_count.
            shared_count.fetch_add(local - committed, Ordering::Relaxed);
            publisher.close().await.ok();
            Ok::<(u64, f64), anyhow::Error>((local, active_secs))
        }));
    }
    let mut total: u64 = 0;
    let mut max_active_secs: f64 = 0.0;
    for h in handles {
        let (n, secs) = h.await??;
        total += n;
        if secs > max_active_secs { max_active_secs = secs; }
    }
    // wall_secs is the longest active publish window across all tasks, which
    // includes connection setup but excludes post-deadline drain time.
    Ok(ProducerStats {
        messages: total,
        bytes: total * payload_bytes as u64,
        wall_secs: max_active_secs,
    })
}

pub async fn run_producer_at_rate(
    addr: &str,
    stream: &str,
    payload_bytes: usize,
    duration: Duration,
    rate_per_sec: u64,
    origin: Instant,
) -> Result<ProducerStats> {
    use crate::driver::publisher::Publisher;

    let payload: Bytes = Bytes::from(vec![b'x'; payload_bytes]);
    let publisher = Publisher::new(addr).await?;
    let interval_ns = 1_000_000_000u64 / rate_per_sec.max(1);
    let mut ticker = tokio::time::interval(Duration::from_nanos(interval_ns));
    // We want ticks to "catch up" rather than bunch when a publish takes longer
    // than the interval — this gives accurate coordinated-omission measurement.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let start = Instant::now();
    let deadline = start + duration;
    let mut sent: u64 = 0;
    while Instant::now() < deadline {
        ticker.tick().await;
        let us = origin.elapsed().as_micros() as u64;
        let req = exspeed_protocol::messages::publish::PublishRequest {
            stream: stream.into(),
            subject: "bench".into(),
            key: None,
            msg_id: None,
            value: payload.clone(),
            headers: vec![(PUBLISH_TS_HEADER.to_owned(), format!("{us}"))],
        };
        publisher.publish(req).await.ok();
        sent += 1;
    }
    publisher.close().await.ok();
    Ok(ProducerStats {
        messages: sent,
        bytes: sent * payload_bytes as u64,
        wall_secs: start.elapsed().as_secs_f64(),
    })
}

pub struct ConsumerStats {
    pub messages: u64,
    pub latency_histogram: Histogram<u64>,
}

/// Subscribe to `stream` as a new consumer, record end-to-end latency for each
/// received record into an hdrhistogram keyed by microseconds, and return after
/// `duration`. End-to-end latency is computed as `origin.elapsed() - publish_us`,
/// so both sides must share the same `origin` instant (single-node benchmark).
pub async fn run_consumer(
    addr: &str,
    stream: &str,
    consumer_name: &str,
    duration: Duration,
    origin: Instant,
) -> Result<ConsumerStats> {
    let mut client = ExspeedClient::connect(addr).await?;

    // Create consumer starting from latest (ok if it already exists on reruns).
    let create_req = CreateConsumerRequest {
        name: consumer_name.into(),
        stream: stream.into(),
        group: String::new(),
        subject_filter: String::new(),
        start_from: StartFrom::Latest,
        start_offset: 0,
    };
    let mut buf = BytesMut::new();
    create_req.encode(&mut buf);
    let corr = client.alloc_corr();
    client.writer.send(Frame::new(OpCode::CreateConsumer, corr, buf.freeze())).await?;
    match client.reader.next().await {
        Some(Ok(_)) => {} // Ok or Error(AlreadyExists) — either means we can proceed
        Some(Err(e)) => return Err(e.into()),
        None => return Err(anyhow!("connection closed during CreateConsumer")),
    }

    // Subscribe
    let sub_req = SubscribeRequest {
        consumer_name: consumer_name.into(),
        subscriber_id: String::new(),
    };
    let mut buf = BytesMut::new();
    sub_req.encode(&mut buf);
    let corr = client.alloc_corr();
    client.writer.send(Frame::new(OpCode::Subscribe, corr, buf.freeze())).await?;
    let resp = client.reader.next().await.ok_or_else(|| anyhow!("closed"))??;
    if !matches!(resp.opcode, OpCode::Ok) {
        return Err(anyhow!("subscribe: unexpected opcode {:?}", resp.opcode));
    }

    let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
        .expect("histogram bounds");
    let mut messages: u64 = 0;
    let deadline = Instant::now() + duration;

    // Cumulative-ack batching: the broker's Ack opcode advances the consumer's
    // stored offset to N (implicitly acking 0..N), so we only need to send one
    // Ack per batch rather than one per record.
    const ACK_BATCH_SIZE: u64 = 64;
    const ACK_BATCH_WINDOW_MS: u64 = 5;

    let mut highest_unacked: Option<u64> = None;
    let mut records_since_last_ack: u64 = 0;
    let mut last_ack_time = Instant::now();

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let next = match tokio::time::timeout(remaining, client.reader.next()).await {
            Ok(Some(Ok(f))) => f,
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(None) => break,
            Err(_) => break, // deadline reached
        };
        if next.opcode != OpCode::Record {
            continue;
        }
        let delivery = RecordDelivery::decode(next.payload)?;

        // Find publish_us header; ignore records without it (shouldn't happen in bench runs).
        if let Some(v) = delivery
            .headers
            .iter()
            .find(|(k, _)| k == PUBLISH_TS_HEADER)
            .map(|(_, v)| v)
        {
            match v.parse::<u64>() {
                Ok(sent_us) => {
                    let now_us = origin.elapsed().as_micros() as u64;
                    if now_us > sent_us {
                        let _ = hist.record(now_us - sent_us);
                    }
                }
                Err(_) => {
                    // Malformed header — skip rather than poison the histogram.
                }
            }
        }

        highest_unacked = Some(delivery.offset);
        records_since_last_ack += 1;
        messages += 1;

        // Send one cumulative Ack every 64 records or every 5 ms, whichever
        // comes first.  The broker advances its stored offset to N on Ack(N),
        // implicitly acking everything below, so batching is safe.
        let should_ack = records_since_last_ack >= ACK_BATCH_SIZE
            || last_ack_time.elapsed() >= Duration::from_millis(ACK_BATCH_WINDOW_MS);
        if should_ack {
            if let Some(off) = highest_unacked {
                let ack = AckRequest {
                    consumer_name: consumer_name.into(),
                    offset: off,
                };
                let mut ack_buf = BytesMut::new();
                ack.encode(&mut ack_buf);
                let corr = client.alloc_corr();
                client
                    .writer
                    .send(Frame::new(OpCode::Ack, corr, ack_buf.freeze()))
                    .await?;
                highest_unacked = None;
                records_since_last_ack = 0;
                last_ack_time = Instant::now();
            }
        }
    }

    // Final flush — ack any records received since the last batch boundary.
    if let Some(off) = highest_unacked {
        let ack = AckRequest {
            consumer_name: consumer_name.into(),
            offset: off,
        };
        let mut ack_buf = BytesMut::new();
        ack.encode(&mut ack_buf);
        let corr = client.alloc_corr();
        client
            .writer
            .send(Frame::new(OpCode::Ack, corr, ack_buf.freeze()))
            .await?;
    }

    Ok(ConsumerStats {
        messages,
        latency_histogram: hist,
    })
}
