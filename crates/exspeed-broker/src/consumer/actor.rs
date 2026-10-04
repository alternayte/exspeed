//! One task per active consumer. It owns the consumer's [`Core`] state and
//! does all I/O: reading the stream, handing records to push subscribers
//! (within their credit) and pull requests, redelivering on timeout or nack,
//! dead-lettering, and persisting snapshots.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use exspeed_common::{msg_time, record_format};
use exspeed_common::{Metrics, Offset, StreamName, SubjectFilters, MAX_RECORDS_BYTES_PER_FRAME};
use exspeed_protocol::client::{code, EncodedRecords, SeekTo};
use exspeed_streams::{RawBatch, ReadLimits, Record, StorageError, StoredRecord, StreamConfig};
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
/// Send a push subscriber's pending records once they reach this size, so
/// one `Deliver` frame stays well below the protocol's payload limit.
const PUSH_FLUSH_BYTES: usize = 4 * 1024 * 1024;

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
        reply: oneshot::Sender<Result<EncodedRecords, ConsumerError>>,
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
    records: EncodedRecords,
    /// The next record didn't fit in the remaining byte budget: answer now
    /// instead of waiting for the deadline.
    stuffed: bool,
    reply: oneshot::Sender<Result<EncodedRecords, ConsumerError>>,
}

impl PullWaiter {
    fn full(&self) -> bool {
        self.stuffed
            || self.records.count() as usize >= self.max_messages
            || self.records.byte_len() >= self.max_bytes
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
    /// The stream's settings (delays, TTLs), reloaded when stream metadata
    /// changes.
    stream_cfg: StreamConfig,
    cfg_seen: Option<u64>,
}

/// What the stream's time settings mean for one record right now.
enum Timing {
    Ready,
    /// Not before this instant.
    Delayed(Instant),
    /// TTL passed and the consumer dead-letters expired records.
    Expired,
}

/// A run of consecutive records from one read buffer that all go to the
/// same taker; handed over as one zero-copy chunk.
struct Run {
    taker: Taker,
    start: usize,
    end: usize,
    count: u32,
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
            stream_cfg: StreamConfig::default(),
            cfg_seen: None,
        })
    }

    /// Reload the stream config after a metadata change (or the first time).
    async fn refresh_stream_config(&mut self) {
        let seen = self.log.metadata_counter();
        if self.cfg_seen == Some(seen) {
            return;
        }
        if let Ok(cfg) = self.log.storage().stream_config(&self.stream).await {
            self.stream_cfg = cfg;
        }
        self.cfg_seen = Some(seen);
    }

    /// Whether expired records must be read (to dead-letter them).
    fn sees_expired(&self) -> bool {
        self.core.spec.dead_letter_expired && self.stream_cfg.has_ttl()
    }

    fn timing(&self, raw: &[u8], now_ns: u64, now: Instant) -> Timing {
        if self.sees_expired()
            && msg_time::expires_at_ns(
                raw,
                self.stream_cfg.allow_msg_ttl,
                self.stream_cfg.msg_ttl_ms,
            )
            .is_some_and(|e| e <= now_ns)
        {
            return Timing::Expired;
        }
        if self.stream_cfg.allow_delayed {
            if let Some(at) = msg_time::deliver_at_ns(raw).filter(|&at| at > now_ns) {
                return Timing::Delayed(now + Duration::from_nanos(at - now_ns));
            }
        }
        Timing::Ready
    }

    async fn read_raw(&self, from: u64, limits: ReadLimits) -> Result<RawBatch, StorageError> {
        let storage = self.log.storage();
        if self.sees_expired() {
            storage
                .read_raw_including_expired(&self.stream, Offset(from), limits)
                .await
        } else {
            storage.read_raw(&self.stream, Offset(from), limits).await
        }
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
                    // Capped like Read so the `Messages` frame fits.
                    max_bytes: if max_bytes == 0 {
                        4 * 1024 * 1024
                    } else {
                        (max_bytes as usize).min(MAX_RECORDS_BYTES_PER_FRAME)
                    },
                    deadline: now + expires.min(Duration::from_secs(300)),
                    records: EncodedRecords::new(),
                    stuffed: false,
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
                    if !self.dead_letter(offset, deliveries, &reason, None).await {
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
            num_delayed: self.core.delayed() as u64,
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

    /// Whether any taker has room for at least one more record.
    fn has_room(&self) -> bool {
        self.pulls.iter().any(|p| !p.full())
            || self.subs.iter().any(|s| s.credits > 0 && !s.tx.is_closed())
    }

    /// Pick the next taker with room for a record of `size` encoded bytes:
    /// pull waiters first, then push subscribers round-robin. Returns a slot
    /// index understood by `give`. `pending` is the run not yet handed over
    /// (its bytes count against its pull waiter's budget). A pull waiter
    /// that already holds records and has no room left for this one is
    /// marked stuffed, so it is answered instead of overflowing its
    /// `Messages` frame; its first record always fits.
    fn next_taker(&mut self, size: usize, pending: Option<&Run>) -> Option<Taker> {
        for (i, p) in self.pulls.iter_mut().enumerate() {
            if p.full() {
                continue;
            }
            let run = pending
                .filter(|r| r.taker == Taker::Pull(i))
                .map_or(0, |r| r.end - r.start);
            let held = p.records.byte_len() + run;
            if held == 0 || held + size <= p.max_bytes {
                return Some(Taker::Pull(i));
            }
            p.stuffed = true;
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
    ///
    /// Records are never decoded here. They are read in their wire encoding
    /// ([`exspeed_streams::StorageEngine::read_raw`]), their
    /// `delivery_count` is patched in place, and each taker receives
    /// zero-copy slices of the read buffer. Only the subject is parsed (in
    /// place, without allocating) when the consumer has subject filters.
    async fn pump(&mut self) {
        self.refresh_stream_config().await;
        let now = Instant::now();
        let now_ns = now_nanos();
        self.subs.retain(|s| !s.tx.is_closed());
        // Pullers that gave up (connection closed) must not be handed records.
        self.pulls.retain(|p| !p.reply.is_closed());
        self.core.expire(now);
        let mut batches: Vec<EncodedRecords> = vec![EncodedRecords::new(); self.subs.len()];

        // 1. Redeliveries and dead letters.
        let due = self.core.due(now, READ_BATCH);
        for d in due {
            match d {
                Due::DeadLetter { offset, deliveries } => {
                    self.core.take_unacked(offset);
                    self.dirty = true;
                    if !self
                        .dead_letter(offset, deliveries, "max_deliver exceeded", None)
                        .await
                    {
                        self.core
                            .reschedule(offset, deliveries, now + Duration::from_secs(1));
                    }
                }
                Due::Redeliver { offset, deliveries } => {
                    if !self.has_room() {
                        break;
                    }
                    match self.read_one_raw(offset).await {
                        Ok(Some(mut rec)) => {
                            match self.timing(&rec, now_ns, now) {
                                Timing::Ready => {}
                                // A delayed record restored after a restart
                                // that isn't due yet.
                                Timing::Delayed(due) if deliveries == 0 => {
                                    self.core.delay(offset, due);
                                    continue;
                                }
                                Timing::Delayed(_) => {}
                                Timing::Expired => {
                                    self.expire(offset, &rec, now).await;
                                    continue;
                                }
                            }
                            let Some(taker) = self.next_taker(rec.len(), None) else {
                                break;
                            };
                            record_format::set_delivery_count(
                                &mut rec,
                                deliveries.saturating_add(1),
                            );
                            self.core.delivered(offset, deliveries, now);
                            self.dirty = true;
                            self.take_credit(taker);
                            self.give(taker, rec.freeze(), 1, &[offset], &mut batches);
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
                .read_raw(
                    self.core.next_read,
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
            if batch.count == 0 {
                let advanced = batch.next_offset.0 > self.core.next_read;
                if advanced {
                    self.core.next_read = batch.next_offset.0;
                    self.dirty = true;
                }
                // Everything read was hidden (expired, superseded): keep
                // going while there is more below the high watermark.
                if advanced && batch.next_offset.0 < batch.high_watermark.0 {
                    continue;
                }
                exhausted = true;
                break;
            }
            let (next_offset, high_watermark) = (batch.next_offset, batch.high_watermark);
            let mut buf = batch.bytes;
            let mut positions = Vec::with_capacity(batch.count);
            for p in record_format::iter(&buf) {
                match p {
                    Ok(p) => positions.push(p),
                    Err(e) => {
                        tracing::warn!(consumer = %self.core.spec.name, error = %e,
                                       "malformed raw batch");
                        break;
                    }
                }
            }
            // Everything handed out from this batch is a first delivery.
            for p in &positions {
                record_format::set_delivery_count(&mut buf[p.range()], 1);
            }
            let buf = buf.freeze();
            let mut run: Option<Run> = None;
            let mut run_offsets: Vec<u64> = Vec::new();
            let mut progressed = false;
            let timed = self.stream_cfg.allow_delayed || self.sees_expired();
            for p in &positions {
                if p.offset < self.core.next_read {
                    continue;
                }
                if !self.filters.is_all() {
                    let matches = record_format::subject(&buf[p.range()])
                        .is_ok_and(|s| self.filters.matches(s));
                    if !matches {
                        self.flush_run(&buf, run.take(), &mut run_offsets, &mut batches);
                        self.core.next_read = p.offset + 1;
                        self.dirty = true;
                        progressed = true;
                        continue;
                    }
                }
                if timed {
                    match self.timing(&buf[p.range()], now_ns, now) {
                        Timing::Ready => {}
                        Timing::Delayed(due) => {
                            self.flush_run(&buf, run.take(), &mut run_offsets, &mut batches);
                            self.core.delay(p.offset, due);
                            self.core.next_read = p.offset + 1;
                            self.dirty = true;
                            progressed = true;
                            if self.core.capacity_for_new() == 0 {
                                break;
                            }
                            continue;
                        }
                        Timing::Expired => {
                            self.flush_run(&buf, run.take(), &mut run_offsets, &mut batches);
                            self.core.next_read = p.offset + 1;
                            self.dirty = true;
                            progressed = true;
                            let raw = buf.slice(p.range());
                            self.expire(p.offset, &raw, now).await;
                            continue;
                        }
                    }
                }
                let Some(taker) = self.next_taker(p.end() - p.start, run.as_ref()) else {
                    break;
                };
                self.take_credit(taker);
                self.core.delivered(p.offset, 0, now);
                self.core.next_read = p.offset + 1;
                self.dirty = true;
                progressed = true;
                match &mut run {
                    Some(r) if r.taker == taker && r.end == p.start => {
                        r.end = p.end();
                        r.count += 1;
                    }
                    _ => {
                        self.flush_run(&buf, run.take(), &mut run_offsets, &mut batches);
                        run = Some(Run {
                            taker,
                            start: p.start,
                            end: p.end(),
                            count: 1,
                        });
                    }
                }
                run_offsets.push(p.offset);
                // Hand the run over as soon as a pull waiter is full, so
                // `next_taker` sees it as full.
                if let (Taker::Pull(i), Some(r)) = (taker, run.as_ref()) {
                    let w = &self.pulls[i];
                    if w.records.count() as usize + r.count as usize >= w.max_messages
                        || w.records.byte_len() + (r.end - r.start) >= w.max_bytes
                    {
                        self.flush_run(&buf, run.take(), &mut run_offsets, &mut batches);
                    }
                }
                if self.core.capacity_for_new() == 0 {
                    break;
                }
            }
            self.flush_run(&buf, run.take(), &mut run_offsets, &mut batches);
            if !progressed {
                break;
            }
            if next_offset.0 >= high_watermark.0 && self.core.next_read >= high_watermark.0 {
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

    /// A push subscriber spends one credit per record.
    fn take_credit(&mut self, taker: Taker) {
        if let Taker::Push(i) = taker {
            self.subs[i].credits -= 1;
        }
    }

    fn flush_run(
        &mut self,
        buf: &Bytes,
        run: Option<Run>,
        offsets: &mut Vec<u64>,
        batches: &mut [EncodedRecords],
    ) {
        if let Some(r) = run {
            let chunk = buf.slice(r.start..r.end);
            let offs = std::mem::take(offsets);
            self.give(r.taker, chunk, r.count, &offs, batches);
        }
    }

    /// Hand `count` encoded records (with offsets `offsets`) to a taker.
    fn give(
        &mut self,
        taker: Taker,
        chunk: Bytes,
        count: u32,
        offsets: &[u64],
        batches: &mut [EncodedRecords],
    ) {
        match taker {
            Taker::Pull(i) => self.pulls[i].records.push_chunk(chunk, count),
            Taker::Push(i) => {
                let sub_id = self.subs[i].sub_id;
                for &o in offsets {
                    self.owners.insert(o, sub_id);
                }
                // Drop entries for records that left in-flight some other
                // way (expired, dead-lettered, seek) so the map stays bounded.
                if self.owners.len() > 2 * self.core.capacity_hint() {
                    let core = &self.core;
                    self.owners.retain(|o, _| core.is_in_flight(*o));
                }
                // Never let one `Deliver` frame grow past the per-frame
                // records budget (a lone record always goes through).
                if !batches[i].is_empty()
                    && batches[i].byte_len() + chunk.len() > MAX_RECORDS_BYTES_PER_FRAME
                {
                    let full = std::mem::take(&mut batches[i]);
                    let _ = self.subs[i].tx.send(SubEvent::Deliver(full));
                }
                batches[i].push_chunk(chunk, count);
                if batches[i].byte_len() >= PUSH_FLUSH_BYTES {
                    let full = std::mem::take(&mut batches[i]);
                    let _ = self.subs[i].tx.send(SubEvent::Deliver(full));
                }
            }
        }
    }

    /// The record at exactly `offset`, in its wire encoding.
    async fn read_one_raw(&self, offset: u64) -> Result<Option<BytesMut>, StorageError> {
        match self
            .read_raw(
                offset,
                ReadLimits {
                    max_records: 1,
                    max_bytes: 1,
                },
            )
            .await
        {
            Ok(b) if b.count > 0 && record_format::offset(&b.bytes) == offset => Ok(Some(b.bytes)),
            Ok(_) | Err(StorageError::OffsetOutOfRange { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// The record at exactly `offset`, decoded (for dead-lettering).
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

    /// An expired record (consumer with `dead_letter_expired`): forget it
    /// and dead-letter it with reason `expired`. A failed dead-letter write
    /// is retried like any other.
    async fn expire(&mut self, offset: u64, raw: &[u8], now: Instant) {
        let deliveries = self.core.take_any(offset).unwrap_or(0);
        self.dirty = true;
        let rec = decode_raw(raw);
        if !self.dead_letter(offset, deliveries, "expired", rec).await {
            self.core
                .reschedule(offset, deliveries.max(1), now + Duration::from_secs(1));
        }
    }

    /// Copy a record to the consumer's DLQ stream (or drop it when none is
    /// configured). Returns false if it should be retried later. `known` is
    /// the record when the caller already has it.
    async fn dead_letter(
        &mut self,
        offset: u64,
        deliveries: u16,
        reason: &str,
        known: Option<StoredRecord>,
    ) -> bool {
        let name = self.core.spec.name.clone();
        let Some(dlq) = self.core.spec.dlq_stream.clone() else {
            tracing::warn!(consumer = %name, offset, reason, "dropping record (no dlq_stream)");
            self.core.stats.dead_lettered += 1;
            self.metrics.record_consumer_dead_letter(&name, "dropped");
            return true;
        };
        let rec = match known {
            Some(r) => r,
            None => match self.read_one(offset).await {
                Ok(Some(r)) => r,
                Ok(None) => {
                    self.core.stats.gone += 1;
                    return true;
                }
                Err(_) => return false,
            },
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
        let mut result = self.log.append(&dlq_name, record.clone()).await;
        if let Err(crate::log::LogError::InvalidRecord(why)) = &result {
            // The DLQ headers pushed the record over the header limit: keep
            // only the DLQ metadata rather than retrying forever.
            tracing::warn!(consumer = %name, offset, reason = %why,
                           "DLQ record too large; dropping its original headers");
            let mut slim = record;
            slim.headers.retain(|(k, _)| {
                k.starts_with("exspeed-dlq-") || k == crate::broker_append::IDEMPOTENCY_HEADER
            });
            result = self.log.append(&dlq_name, slim).await;
        }
        match result {
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Taker {
    Pull(usize),
    Push(usize),
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

/// Decode one raw (wire-encoded) record.
fn decode_raw(raw: &[u8]) -> Option<StoredRecord> {
    let l = record_format::layout(raw).ok()?;
    let b = Bytes::copy_from_slice(raw);
    Some(StoredRecord {
        offset: Offset(l.offset),
        timestamp: l.timestamp_ns,
        subject: l.subject(raw).to_owned(),
        key: l.key.clone().map(|r| b.slice(r)),
        value: b.slice(l.value.clone()),
        headers: l.headers(raw),
    })
}

fn storage_err(e: StorageError) -> ConsumerError {
    match e {
        StorageError::StreamNotFound(s) => ConsumerError::NotFound(format!("stream '{s}'")),
        other => ConsumerError::Storage(other.to_string()),
    }
}
