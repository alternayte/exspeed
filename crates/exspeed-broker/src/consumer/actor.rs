//! One task per active consumer. It owns the consumer's [`Core`] state and
//! does all I/O: reading the stream, handing records to push subscribers
//! (within their credit) and pull requests, redelivering on timeout or nack,
//! dead-lettering, and persisting snapshots.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use exspeed_common::{Metrics, Offset, StreamName, SubjectFilters};
use exspeed_protocol::client::{code, SeekTo, WireRecord};
use exspeed_streams::{ReadLimits, Record, StorageError, StoredRecord};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::core::{Core, Due};
use super::store::ConsumerStore;
use super::{ConsumerError, ConsumerInfo, SubEvent};
use crate::log::Log;

/// Persist at most this often while state changes (acks, deliveries).
const PERSIST_INTERVAL: Duration = Duration::from_millis(100);
/// Poll interval when the storage engine can't notify on appends.
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Upper bound on records read per pump step.
const READ_BATCH: usize = 256;

pub(crate) enum Cmd {
    Attach {
        sub_id: u32,
        credits: u32,
        tx: mpsc::UnboundedSender<SubEvent>,
        /// Confirms the subscription is live before the caller proceeds.
        ready: oneshot::Sender<()>,
    },
    Detach {
        sub_id: u32,
    },
    Credit {
        sub_id: u32,
        credits: u32,
    },
    Pull {
        max_messages: u32,
        max_bytes: u32,
        expires: Duration,
        reply: oneshot::Sender<Result<Vec<WireRecord>, ConsumerError>>,
    },
    Ack {
        offsets: Vec<u64>,
    },
    Nack {
        offset: u64,
        delay_ms: u32,
    },
    Term {
        offset: u64,
        reason: String,
    },
    InProgress {
        offsets: Vec<u64>,
    },
    Seek {
        to: SeekTo,
        reply: oneshot::Sender<Result<(), ConsumerError>>,
    },
    Info {
        reply: oneshot::Sender<ConsumerInfo>,
    },
    /// Stop and tombstone the consumer.
    Delete {
        reply: oneshot::Sender<Result<(), ConsumerError>>,
    },
}

struct Subscriber {
    sub_id: u32,
    credits: u32,
    tx: mpsc::UnboundedSender<SubEvent>,
}

struct PullWaiter {
    max_messages: usize,
    max_bytes: usize,
    deadline: Instant,
    records: Vec<WireRecord>,
    bytes: usize,
    reply: oneshot::Sender<Result<Vec<WireRecord>, ConsumerError>>,
}

impl PullWaiter {
    fn full(&self) -> bool {
        self.records.len() >= self.max_messages || self.bytes >= self.max_bytes
    }
}

pub(crate) struct Actor {
    core: Core,
    stream: StreamName,
    filters: SubjectFilters,
    log: Arc<Log>,
    store: Arc<ConsumerStore>,
    metrics: Arc<Metrics>,
    subs: Vec<Subscriber>,
    /// Which subscription each pushed, still-unacked record went to, so a
    /// subscriber that goes away (crash, disconnect) has its records
    /// redelivered to the others immediately instead of after `ack_wait`.
    owners: HashMap<u64, u32>,
    rr: usize,
    pulls: VecDeque<PullWaiter>,
    dirty: bool,
    last_persist: Instant,
    /// True when the last read reached the end of the log.
    caught_up: bool,
}

pub fn to_wire(r: &StoredRecord, delivery_count: u16) -> WireRecord {
    WireRecord {
        offset: r.offset.0,
        timestamp_ms: r.timestamp / 1_000_000,
        delivery_count,
        subject: r.subject.clone(),
        key: r.key.clone(),
        value: r.value.clone(),
        headers: r.headers.clone(),
    }
}

fn wire_size(r: &WireRecord) -> usize {
    r.value.len() + r.subject.len() + r.key.as_ref().map_or(0, |k| k.len()) + 32
}

impl Actor {
    pub(crate) fn new(
        core: Core,
        log: Arc<Log>,
        store: Arc<ConsumerStore>,
        metrics: Arc<Metrics>,
    ) -> Result<Self, ConsumerError> {
        let stream = StreamName::try_from(core.spec.stream.as_str())
            .map_err(|e| ConsumerError::Invalid(e.to_string()))?;
        let filters =
            SubjectFilters::parse(&core.spec.filter_subjects).map_err(ConsumerError::Invalid)?;
        Ok(Self {
            core,
            stream,
            filters,
            log,
            store,
            metrics,
            subs: Vec::new(),
            owners: HashMap::new(),
            rr: 0,
            pulls: VecDeque::new(),
            dirty: false,
            last_persist: Instant::now(),
            caught_up: false,
        })
    }

    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<Cmd>, token: CancellationToken) {
        let mut watch = self.log.storage().watch_appends(&self.stream);
        loop {
            self.pump().await;
            if self.dirty && self.last_persist.elapsed() >= PERSIST_INTERVAL {
                self.persist().await;
            }

            let now = Instant::now();
            let mut wake = self.core.next_wakeup();
            if let Some(p) = self.pulls.iter().map(|p| p.deadline).min() {
                wake = Some(wake.map_or(p, |w| w.min(p)));
            }
            if self.dirty {
                let p = self.last_persist + PERSIST_INTERVAL;
                wake = Some(wake.map_or(p, |w| w.min(p)));
            }
            // Waiting for new data only matters if someone can take it.
            let wants_data = self.has_takers() && self.core.capacity_for_new() > 0;
            if wants_data && self.caught_up && watch.is_none() {
                let p = now + POLL_INTERVAL;
                wake = Some(wake.map_or(p, |w| w.min(p)));
            }
            if wants_data && !self.caught_up {
                // More data is already available; loop straight back.
                wake = Some(now);
            }
            let sleep_until = wake.unwrap_or(now + Duration::from_secs(3600));

            tokio::select! {
                biased;
                _ = token.cancelled() => {
                    let msg = "consumer stopped (leadership change or shutdown)";
                    self.end_all(code::UNAVAILABLE, msg);
                    drain(&mut rx, code::UNAVAILABLE, msg);
                    if self.dirty { self.persist().await; }
                    return;
                }
                cmd = rx.recv() => match cmd {
                    Some(Cmd::Delete { reply }) => {
                        self.end_all(code::NOT_FOUND, "consumer deleted");
                        drain(&mut rx, code::NOT_FOUND, "consumer deleted");
                        let r = self.store.delete(&self.core.spec.name).await
                            .map_err(|e| ConsumerError::Storage(e.to_string()));
                        let _ = reply.send(r);
                        return;
                    }
                    Some(cmd) => self.handle(cmd).await,
                    None => {
                        if self.dirty { self.persist().await; }
                        return;
                    }
                },
                changed = async {
                    match watch.as_mut() {
                        Some(w) => w.changed().await.is_ok(),
                        None => std::future::pending().await,
                    }
                }, if wants_data && self.caught_up => {
                    if changed { self.caught_up = false; } else { watch = None; }
                }
                _ = tokio::time::sleep_until(sleep_until.into()) => {
                    self.caught_up = false;
                }
            }
        }
    }

    async fn handle(&mut self, cmd: Cmd) {
        let now = Instant::now();
        match cmd {
            Cmd::Attach {
                sub_id,
                credits,
                tx,
                ready,
            } => {
                self.subs.push(Subscriber {
                    sub_id,
                    credits,
                    tx,
                });
                let _ = ready.send(());
                self.caught_up = false;
            }
            Cmd::Detach { sub_id } => {
                self.subs.retain(|s| s.sub_id != sub_id);
                let orphaned: Vec<u64> = self
                    .owners
                    .iter()
                    .filter(|(_, &owner)| owner == sub_id)
                    .map(|(&o, _)| o)
                    .collect();
                for o in orphaned {
                    self.owners.remove(&o);
                    self.dirty |= self.core.nack(o, Some(Duration::ZERO), now);
                }
            }
            Cmd::Credit { sub_id, credits } => {
                if let Some(s) = self.subs.iter_mut().find(|s| s.sub_id == sub_id) {
                    s.credits = s.credits.saturating_add(credits);
                    self.caught_up = false;
                }
            }
            Cmd::Pull {
                max_messages,
                max_bytes,
                expires,
                reply,
            } => {
                self.pulls.push_back(PullWaiter {
                    max_messages: max_messages.clamp(1, 10_000) as usize,
                    max_bytes: if max_bytes == 0 {
                        4 * 1024 * 1024
                    } else {
                        max_bytes as usize
                    },
                    deadline: now + expires.min(Duration::from_secs(300)),
                    records: Vec::new(),
                    bytes: 0,
                    reply,
                });
                self.caught_up = false;
            }
            Cmd::Ack { offsets } => {
                for o in offsets {
                    self.owners.remove(&o);
                    self.dirty |= self.core.ack(o);
                }
            }
            Cmd::Nack { offset, delay_ms } => {
                let delay = (delay_ms > 0).then(|| Duration::from_millis(delay_ms as u64));
                self.dirty |= self.core.nack(offset, delay, now);
            }
            Cmd::InProgress { offsets } => {
                for o in offsets {
                    self.core.in_progress(o, now);
                }
            }
            Cmd::Term { offset, reason } => {
                if let Some(deliveries) = self.core.take_unacked(offset) {
                    self.dirty = true;
                    if !self.dead_letter(offset, deliveries, &reason).await {
                        self.core
                            .reschedule(offset, deliveries, now + Duration::from_secs(1));
                    }
                }
            }
            Cmd::Seek { to, reply } => {
                let r = self.resolve_seek(to).await.map(|off| {
                    self.core.seek(off);
                    self.dirty = true;
                    self.caught_up = false;
                });
                if r.is_ok() {
                    self.persist().await;
                }
                let _ = reply.send(r);
            }
            Cmd::Info { reply } => {
                let _ = reply.send(self.info().await);
            }
            Cmd::Delete { .. } => unreachable!("handled in run"),
        }
    }

    async fn resolve_seek(&self, to: SeekTo) -> Result<u64, ConsumerError> {
        let storage = self.log.storage();
        let (earliest, hwm) = storage
            .stream_bounds(&self.stream)
            .await
            .map_err(storage_err)?;
        Ok(match to {
            SeekTo::Earliest => earliest.0,
            SeekTo::Latest => hwm.0,
            SeekTo::Offset(o) => o.max(earliest.0),
            SeekTo::Time(ms) => storage
                .seek_by_time(&self.stream, ms.saturating_mul(1_000_000))
                .await
                .map_err(storage_err)?
                .0
                .max(earliest.0),
        })
    }

    async fn info(&self) -> ConsumerInfo {
        let (earliest, hwm) = self
            .log
            .storage()
            .stream_bounds(&self.stream)
            .await
            .unwrap_or((Offset(0), Offset(0)));
        ConsumerInfo {
            spec: self.core.spec.clone(),
            next_offset: self.core.next_read,
            ack_floor: self.core.ack_floor(),
            num_unacked: self.core.unacked() as u64,
            num_in_flight: self.core.in_flight() as u64,
            // Approximate: counts filtered-out records too.
            num_waiting: hwm.0.saturating_sub(self.core.next_read.max(earliest.0)),
            lag: hwm.0.saturating_sub(self.core.ack_floor().max(earliest.0)),
            subscribers: self.subs.len() as u64,
            pull_waiters: self.pulls.len() as u64,
            stats: self.core.stats,
        }
    }

    fn has_takers(&self) -> bool {
        !self.pulls.is_empty() || self.subs.iter().any(|s| s.credits > 0)
    }

    fn end_all(&mut self, code: u16, message: &str) {
        for s in self.subs.drain(..) {
            let _ = s.tx.send(SubEvent::Ended {
                code,
                message: message.to_string(),
            });
        }
        for p in self.pulls.drain(..) {
            let _ = p.reply.send(Err(ConsumerError::Ended {
                code,
                message: message.to_string(),
            }));
        }
    }

    async fn persist(&mut self) {
        match self.store.save(&self.core.snapshot()).await {
            Ok(()) => {
                self.dirty = false;
                self.last_persist = Instant::now();
            }
            Err(e) => {
                // Try again on the next persist tick.
                tracing::warn!(consumer = %self.core.spec.name, error = %e,
                               "failed to persist consumer state");
                self.last_persist = Instant::now();
            }
        }
    }

    /// Answer pull waiters that are full or expired.
    fn settle_pulls(&mut self, now: Instant, data_exhausted: bool) {
        let mut keep = VecDeque::with_capacity(self.pulls.len());
        for p in self.pulls.drain(..) {
            let done = p.full()
                || now >= p.deadline
                || (data_exhausted && !p.records.is_empty())
                || p.reply.is_closed();
            if done {
                let _ = p.reply.send(Ok(p.records));
            } else {
                keep.push_back(p);
            }
        }
        self.pulls = keep;
    }

    /// Pick the next taker with room: pull waiters first, then push
    /// subscribers round-robin. Returns a slot index understood by `give`.
    fn next_taker(&mut self) -> Option<Taker> {
        if let Some(i) = self.pulls.iter().position(|p| !p.full()) {
            return Some(Taker::Pull(i));
        }
        let n = self.subs.len();
        for k in 0..n {
            let i = (self.rr + k) % n;
            if self.subs[i].credits > 0 && !self.subs[i].tx.is_closed() {
                self.rr = (i + 1) % n;
                return Some(Taker::Push(i));
            }
        }
        None
    }

    /// Deliver as much as possible: due redeliveries first, then new records.
    async fn pump(&mut self) {
        let now = Instant::now();
        self.subs.retain(|s| !s.tx.is_closed());
        // Pullers that gave up (connection closed) must not be handed records.
        self.pulls.retain(|p| !p.reply.is_closed());
        self.core.expire(now);
        let mut batches: Vec<Vec<WireRecord>> = vec![Vec::new(); self.subs.len()];

        // 1. Redeliveries and dead letters.
        let due = self.core.due(now, READ_BATCH);
        for d in due {
            match d {
                Due::DeadLetter { offset, deliveries } => {
                    self.core.take_unacked(offset);
                    self.dirty = true;
                    if !self
                        .dead_letter(offset, deliveries, "max_deliver exceeded")
                        .await
                    {
                        self.core
                            .reschedule(offset, deliveries, now + Duration::from_secs(1));
                    }
                }
                Due::Redeliver { offset, deliveries } => {
                    let Some(taker) = self.next_taker() else {
                        break;
                    };
                    match self.read_one(offset).await {
                        Ok(Some(rec)) => {
                            let wire = to_wire(&rec, deliveries.saturating_add(1));
                            self.core.delivered(offset, deliveries, now);
                            self.dirty = true;
                            self.give(taker, wire, &mut batches);
                        }
                        Ok(None) => {
                            self.core.gone(offset);
                            self.dirty = true;
                        }
                        Err(e) => {
                            tracing::warn!(consumer = %self.core.spec.name, error = %e,
                                           "redelivery read failed");
                            break;
                        }
                    }
                }
            }
        }

        // 2. New records.
        let mut exhausted = false;
        loop {
            let capacity = self.core.capacity_for_new();
            if capacity == 0 || !self.has_takers() {
                break;
            }
            let want = capacity.min(READ_BATCH);
            let batch = match self
                .log
                .storage()
                .read_batch(
                    &self.stream,
                    Offset(self.core.next_read),
                    ReadLimits {
                        max_records: want,
                        max_bytes: 4 * 1024 * 1024,
                    },
                )
                .await
            {
                Ok(b) => b,
                Err(StorageError::OffsetOutOfRange { earliest, .. }) => {
                    let skipped = earliest.saturating_sub(self.core.next_read);
                    tracing::warn!(consumer = %self.core.spec.name, skipped,
                                   "consumer fell behind retention; skipping ahead");
                    self.core.stats.skipped += skipped;
                    self.core.next_read = earliest;
                    self.dirty = true;
                    continue;
                }
                Err(StorageError::StreamNotFound(_)) => {
                    self.end_all(code::NOT_FOUND, "stream deleted");
                    exhausted = true;
                    break;
                }
                Err(e) => {
                    tracing::warn!(consumer = %self.core.spec.name, error = %e, "read failed");
                    exhausted = true;
                    break;
                }
            };
            if batch.records.is_empty() {
                if batch.next_offset.0 > self.core.next_read {
                    self.core.next_read = batch.next_offset.0;
                    self.dirty = true;
                }
                exhausted = true;
                break;
            }
            let mut progressed = false;
            for rec in &batch.records {
                if rec.offset.0 < self.core.next_read {
                    continue;
                }
                if !self.filters.matches(&rec.subject) {
                    self.core.next_read = rec.offset.0 + 1;
                    self.dirty = true;
                    progressed = true;
                    continue;
                }
                let Some(taker) = self.next_taker() else {
                    break;
                };
                let wire = to_wire(rec, 1);
                self.core.delivered(rec.offset.0, 0, now);
                self.core.next_read = rec.offset.0 + 1;
                self.dirty = true;
                progressed = true;
                self.give(taker, wire, &mut batches);
                if self.core.capacity_for_new() == 0 {
                    break;
                }
            }
            if !progressed {
                break;
            }
            if batch.next_offset.0 >= batch.high_watermark.0
                && self.core.next_read >= batch.high_watermark.0
            {
                exhausted = true;
                break;
            }
        }
        self.caught_up = exhausted;

        // 3. Flush push batches and settle pulls.
        for (i, recs) in batches.into_iter().enumerate() {
            if !recs.is_empty() {
                if let Some(s) = self.subs.get(i) {
                    let _ = s.tx.send(SubEvent::Deliver(recs));
                }
            }
        }
        self.settle_pulls(Instant::now(), exhausted);
    }

    fn give(&mut self, taker: Taker, rec: WireRecord, batches: &mut [Vec<WireRecord>]) {
        match taker {
            Taker::Pull(i) => {
                let p = &mut self.pulls[i];
                p.bytes += wire_size(&rec);
                p.records.push(rec);
            }
            Taker::Push(i) => {
                self.subs[i].credits -= 1;
                self.owners.insert(rec.offset, self.subs[i].sub_id);
                // Drop entries for records that left in-flight some other
                // way (expired, dead-lettered, seek) so the map stays bounded.
                if self.owners.len() > 2 * self.core.capacity_hint() {
                    let core = &self.core;
                    self.owners.retain(|o, _| core.is_in_flight(*o));
                }
                batches[i].push(rec);
            }
        }
    }

    async fn read_one(&self, offset: u64) -> Result<Option<StoredRecord>, StorageError> {
        match self
            .log
            .storage()
            .read_batch(
                &self.stream,
                Offset(offset),
                ReadLimits {
                    max_records: 1,
                    max_bytes: 1,
                },
            )
            .await
        {
            Ok(b) => Ok(b
                .records
                .into_iter()
                .next()
                .filter(|r| r.offset.0 == offset)),
            Err(StorageError::OffsetOutOfRange { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Copy a record to the consumer's DLQ stream (or drop it when none is
    /// configured). Returns false if it should be retried later.
    async fn dead_letter(&mut self, offset: u64, deliveries: u16, reason: &str) -> bool {
        let name = self.core.spec.name.clone();
        let Some(dlq) = self.core.spec.dlq_stream.clone() else {
            tracing::warn!(consumer = %name, offset, reason, "dropping record (no dlq_stream)");
            self.core.stats.dead_lettered += 1;
            self.metrics.record_consumer_dead_letter(&name, "dropped");
            return true;
        };
        let rec = match self.read_one(offset).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                self.core.stats.gone += 1;
                return true;
            }
            Err(_) => return false,
        };
        let Ok(dlq_name) = StreamName::try_from(dlq.as_str()) else {
            tracing::error!(consumer = %name, dlq = %dlq, "invalid dlq_stream name; dropping");
            return true;
        };
        let mut headers = rec.headers.clone();
        headers.retain(|(k, _)| k != crate::broker_append::IDEMPOTENCY_HEADER);
        headers.push(("exspeed-dlq-origin".into(), name.clone()));
        headers.push(("exspeed-dlq-stream".into(), self.stream.as_str().into()));
        headers.push(("exspeed-dlq-original-offset".into(), offset.to_string()));
        headers.push(("exspeed-dlq-deliveries".into(), deliveries.to_string()));
        let mut reason = reason.to_string();
        reason.truncate(4096);
        headers.push(("exspeed-dlq-reason".into(), reason));
        // Deterministic idempotency key: a retried dead-letter write after a
        // crash doesn't duplicate the DLQ record. The payload hash is part of
        // the key: after the source stream is deleted and recreated, a
        // different record can sit at the same offset, and it must not
        // collide with the earlier one's dedup entry in the DLQ.
        headers.push((
            crate::broker_append::IDEMPOTENCY_HEADER.into(),
            format!(
                "dlq:{name}:{}:{offset}:{:016x}",
                self.stream,
                crate::broker_append::hash_body(&rec.value)
            ),
        ));
        let record = Record {
            key: rec.key.clone(),
            value: rec.value.clone(),
            subject: rec.subject.clone(),
            headers,
            timestamp_ns: None,
        };
        if let Err(e) = self.log.ensure_stream(&dlq_name).await {
            tracing::warn!(consumer = %name, error = %e, "cannot create DLQ stream; will retry");
            return false;
        }
        match self.log.append(&dlq_name, record).await {
            Ok(_) => {
                self.core.stats.dead_lettered += 1;
                self.metrics.record_consumer_dead_letter(&name, "dlq");
                true
            }
            // Retrying can never succeed: the key is taken for this window.
            // With the payload hash in the key that means the same payload
            // is already in the DLQ, so count it as written.
            Err(crate::log::LogError::Storage(StorageError::KeyCollision { stored_offset })) => {
                tracing::warn!(
                    consumer = %name,
                    offset,
                    stored_offset,
                    "DLQ already holds this dead letter's key; not retrying"
                );
                self.core.stats.dead_lettered += 1;
                self.metrics.record_consumer_dead_letter(&name, "dlq");
                true
            }
            Err(e) => {
                tracing::warn!(consumer = %name, error = %e, "DLQ append failed; will retry");
                false
            }
        }
    }
}

/// Answer every queued command of an actor that is shutting down, so no
/// subscriber or pull request is left hanging without a terminal reply.
fn drain(rx: &mut mpsc::Receiver<Cmd>, code: u16, message: &str) {
    rx.close();
    while let Ok(cmd) = rx.try_recv() {
        let ended = || ConsumerError::Ended {
            code,
            message: message.to_string(),
        };
        match cmd {
            Cmd::Attach { tx, ready, .. } => {
                let _ = tx.send(SubEvent::Ended {
                    code,
                    message: message.to_string(),
                });
                let _ = ready.send(());
            }
            Cmd::Pull { reply, .. } => {
                let _ = reply.send(Err(ended()));
            }
            Cmd::Seek { reply, .. } | Cmd::Delete { reply } => {
                let _ = reply.send(Err(ended()));
            }
            Cmd::Info { .. }
            | Cmd::Detach { .. }
            | Cmd::Credit { .. }
            | Cmd::Ack { .. }
            | Cmd::Nack { .. }
            | Cmd::Term { .. }
            | Cmd::InProgress { .. } => {}
        }
    }
}

#[derive(Clone, Copy)]
enum Taker {
    Pull(usize),
    Push(usize),
}

fn storage_err(e: StorageError) -> ConsumerError {
    match e {
        StorageError::StreamNotFound(s) => ConsumerError::NotFound(format!("stream '{s}'")),
        other => ConsumerError::Storage(other.to_string()),
    }
}
