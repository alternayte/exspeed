//! Pure delivery state of one consumer: no I/O, no async, easy to test.
//!
//! A consumer reads its stream in order (`next_read`). Each record it hands
//! out under `AckPolicy::Explicit` is *in flight* until acked; if the ack
//! doesn't arrive within `ack_wait`, or the record is nacked, it moves to
//! *scheduled* and is redelivered when due (after backoff). Records are
//! dead-lettered once `max_deliver` deliveries have been used up.
//!
//! Everything below the lowest unacked offset (or `next_read` when nothing
//! is unacked) is done: the **ack floor**. Records that don't match the
//! consumer's subject filter are skipped without ever being in flight.
//!
//! A record with a delivery time in the future (`exspeed-delay`,
//! `exspeed-deliver-at`) is *delayed*: held until due, then delivered like a
//! new record. Delayed records don't count against `max_ack_pending` (there
//! may be many), but they hold the ack floor until delivered and acked.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use exspeed_protocol::client::{AckPolicy, ConsumerSpec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InFlight {
    deliveries: u16,
    deadline: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Scheduled {
    /// Deliveries already made (0 for a record restored from a snapshot
    /// that was never delivered is impossible; restored pending records
    /// keep their count).
    deliveries: u16,
    due: Instant,
}

/// Counters exposed in consumer info and metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerStats {
    pub delivered: u64,
    pub redelivered: u64,
    pub acked: u64,
    pub dead_lettered: u64,
    /// Unacked records that disappeared before redelivery (retention or
    /// compaction removed them).
    pub gone: u64,
    /// Records skipped because the consumer fell behind retention.
    pub skipped: u64,
}

/// Durable part of the state, persisted to the `__consumers` stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub spec: ConsumerSpec,
    pub next_read: u64,
    /// Unacked records and how many times each was delivered.
    pub pending: Vec<(u64, u16)>,
    #[serde(default)]
    pub stats: ConsumerStats,
}

/// What the actor should do with a due redelivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Due {
    /// Redeliver; `deliveries` is how many times it was delivered so far.
    Redeliver { offset: u64, deliveries: u16 },
    /// `max_deliver` is used up: dead-letter it.
    DeadLetter { offset: u64, deliveries: u16 },
}

/// Most delayed records one consumer holds; reading new records pauses
/// while it holds this many.
pub const MAX_DELAYED: usize = 100_000;

pub struct Core {
    pub spec: ConsumerSpec,
    /// Next stream offset to read for a first delivery.
    pub next_read: u64,
    in_flight: BTreeMap<u64, InFlight>,
    scheduled: BTreeMap<u64, Scheduled>,
    /// Not yet delivered: due at the instant. Persisted as pending with zero
    /// deliveries; the due time is re-read from the record after a restart.
    delayed: BTreeMap<u64, Instant>,
    /// Priority of delayed records (priority ordering); absent = 0.
    priority: HashMap<u64, u8>,
    pub stats: ConsumerStats,
}

impl Core {
    pub fn new(spec: ConsumerSpec, next_read: u64) -> Self {
        Self {
            spec,
            next_read,
            in_flight: BTreeMap::new(),
            scheduled: BTreeMap::new(),
            delayed: BTreeMap::new(),
            priority: HashMap::new(),
            stats: ConsumerStats::default(),
        }
    }

    /// Rebuild from a snapshot. Every record that was unacked is due for
    /// redelivery immediately: at-least-once across restarts.
    pub fn restore(snapshot: Snapshot, now: Instant) -> Self {
        let mut core = Self::new(snapshot.spec, snapshot.next_read);
        core.stats = snapshot.stats;
        for (offset, deliveries) in snapshot.pending {
            if offset < core.next_read && deliveries == 0 {
                // Delayed: due "now", which makes the actor re-read the
                // record and either deliver it or delay it again.
                core.delayed.insert(offset, now);
            } else if offset < core.next_read {
                core.scheduled.insert(
                    offset,
                    Scheduled {
                        deliveries,
                        due: now,
                    },
                );
            }
        }
        core
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut pending: Vec<(u64, u16)> = self
            .in_flight
            .iter()
            .map(|(o, f)| (*o, f.deliveries))
            .chain(self.scheduled.iter().map(|(o, s)| (*o, s.deliveries)))
            .chain(self.delayed.keys().map(|o| (*o, 0)))
            .collect();
        pending.sort_unstable();
        Snapshot {
            spec: self.spec.clone(),
            next_read: self.next_read,
            pending,
            stats: self.stats,
        }
    }

    fn explicit(&self) -> bool {
        self.spec.ack == AckPolicy::Explicit
    }

    /// Records delivered but not yet acked (in flight or awaiting redelivery).
    pub fn unacked(&self) -> usize {
        self.in_flight.len() + self.scheduled.len()
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Records held back until their delivery time.
    pub fn delayed(&self) -> usize {
        self.delayed.len()
    }

    /// How many *new* records may be handed out now (`max_ack_pending`).
    pub fn capacity_for_new(&self) -> usize {
        if self.delayed.len() >= MAX_DELAYED {
            return 0;
        }
        if !self.explicit() || self.spec.max_ack_pending == 0 {
            return usize::MAX;
        }
        (self.spec.max_ack_pending as usize).saturating_sub(self.unacked())
    }

    /// Lowest offset not yet acked; everything below it is done.
    pub fn ack_floor(&self) -> u64 {
        [
            self.in_flight.keys().next(),
            self.scheduled.keys().next(),
            self.delayed.keys().next(),
        ]
        .into_iter()
        .flatten()
        .copied()
        .min()
        .unwrap_or(self.next_read)
    }

    /// Hold `offset` (never delivered yet) until `due`.
    pub fn delay(&mut self, offset: u64, due: Instant) {
        self.delayed.insert(offset, due);
    }

    /// Hold `offset` (never delivered yet) until `due`; among due records,
    /// higher `priority` is delivered first.
    pub fn delay_with_priority(&mut self, offset: u64, due: Instant, priority: u8) {
        self.delayed.insert(offset, due);
        if priority > 0 {
            self.priority.insert(offset, priority);
        }
    }

    fn ack_wait(&self) -> Duration {
        Duration::from_millis(self.spec.ack_wait_ms.max(1))
    }

    /// Delay before redelivery after `deliveries` attempts.
    pub fn backoff(&self, deliveries: u16) -> Duration {
        let b = &self.spec.backoff_ms;
        if b.is_empty() {
            return Duration::ZERO;
        }
        let i = (deliveries.saturating_sub(1) as usize).min(b.len() - 1);
        Duration::from_millis(b[i])
    }

    /// Record that `offset` was just delivered. `prior` = deliveries before
    /// this one (0 for a first delivery).
    pub fn delivered(&mut self, offset: u64, prior: u16, now: Instant) {
        self.scheduled.remove(&offset);
        self.delayed.remove(&offset);
        self.priority.remove(&offset);
        if prior == 0 {
            self.stats.delivered += 1;
        } else {
            self.stats.redelivered += 1;
        }
        if self.explicit() {
            self.in_flight.insert(
                offset,
                InFlight {
                    deliveries: prior.saturating_add(1),
                    deadline: now + self.ack_wait(),
                },
            );
        }
    }

    /// Ack one record. Unknown or already-acked offsets are ignored (acks
    /// are idempotent). Returns whether anything changed.
    pub fn ack(&mut self, offset: u64) -> bool {
        let hit =
            self.in_flight.remove(&offset).is_some() || self.scheduled.remove(&offset).is_some();
        if hit {
            self.stats.acked += 1;
        }
        hit
    }

    pub fn is_in_flight(&self, offset: u64) -> bool {
        self.in_flight.contains_key(&offset)
    }

    /// Rough upper bound on how many records can be unacked at once.
    pub fn capacity_hint(&self) -> usize {
        (self.spec.max_ack_pending as usize).max(64)
    }

    /// Return a record for redelivery after `delay` (or the backoff).
    pub fn nack(&mut self, offset: u64, delay: Option<Duration>, now: Instant) -> bool {
        let Some(f) = self.in_flight.remove(&offset) else {
            return false;
        };
        let delay = delay.unwrap_or_else(|| self.backoff(f.deliveries));
        self.scheduled.insert(
            offset,
            Scheduled {
                deliveries: f.deliveries,
                due: now + delay,
            },
        );
        true
    }

    /// Reset the ack deadline of an in-flight record.
    pub fn in_progress(&mut self, offset: u64, now: Instant) -> bool {
        let wait = self.ack_wait();
        match self.in_flight.get_mut(&offset) {
            Some(f) => {
                f.deadline = now + wait;
                true
            }
            None => false,
        }
    }

    /// Remove an unacked record for dead-lettering. Returns its delivery
    /// count, or `None` if it isn't unacked.
    pub fn take_unacked(&mut self, offset: u64) -> Option<u16> {
        self.in_flight
            .remove(&offset)
            .map(|f| f.deliveries)
            .or_else(|| self.scheduled.remove(&offset).map(|s| s.deliveries))
    }

    /// Put back a record whose dead-lettering failed, to retry later.
    pub fn reschedule(&mut self, offset: u64, deliveries: u16, due: Instant) {
        self.scheduled.insert(offset, Scheduled { deliveries, due });
    }

    /// Move in-flight records whose ack deadline passed to the redelivery
    /// schedule. Returns how many expired.
    pub fn expire(&mut self, now: Instant) -> usize {
        let expired: Vec<(u64, u16)> = self
            .in_flight
            .iter()
            .filter(|(_, f)| f.deadline <= now)
            .map(|(o, f)| (*o, f.deliveries))
            .collect();
        for (offset, deliveries) in &expired {
            self.in_flight.remove(offset);
            let due = now + self.backoff(*deliveries);
            self.scheduled.insert(
                *offset,
                Scheduled {
                    deliveries: *deliveries,
                    due,
                },
            );
        }
        expired.len()
    }

    /// Up to `max` scheduled or delayed records that are due now, lowest
    /// offset first. A due delayed record is a `Redeliver` with zero prior
    /// deliveries (its first delivery).
    pub fn due(&self, now: Instant, max: usize) -> Vec<Due> {
        let mut out: Vec<Due> = self
            .scheduled
            .iter()
            .filter(|(_, s)| s.due <= now)
            .take(max)
            .map(|(&offset, s)| {
                if self.spec.max_deliver > 0 && s.deliveries as u32 >= self.spec.max_deliver {
                    Due::DeadLetter {
                        offset,
                        deliveries: s.deliveries,
                    }
                } else {
                    Due::Redeliver {
                        offset,
                        deliveries: s.deliveries,
                    }
                }
            })
            .collect();
        out.sort_by_key(|d| match d {
            Due::Redeliver { offset, .. } | Due::DeadLetter { offset, .. } => *offset,
        });
        // Then records never delivered yet: highest priority first, oldest
        // first within a priority.
        let mut fresh: Vec<(u8, u64)> = self
            .delayed
            .iter()
            .filter(|(_, &d)| d <= now)
            .map(|(&o, _)| (self.priority.get(&o).copied().unwrap_or(0), o))
            .collect();
        fresh.sort_by_key(|&(p, o)| (std::cmp::Reverse(p), o));
        out.extend(fresh.into_iter().map(|(_, offset)| Due::Redeliver {
            offset,
            deliveries: 0,
        }));
        out.truncate(max);
        out
    }

    /// Drop a scheduled record that no longer exists in the stream.
    pub fn gone(&mut self, offset: u64) {
        self.priority.remove(&offset);
        if self.scheduled.remove(&offset).is_some()
            || self.in_flight.remove(&offset).is_some()
            || self.delayed.remove(&offset).is_some()
        {
            self.stats.gone += 1;
        }
    }

    /// Drop a record without delivering it (it expired). Returns its
    /// delivery count when it was unacked or delayed.
    pub fn take_any(&mut self, offset: u64) -> Option<u16> {
        self.priority.remove(&offset);
        self.take_unacked(offset)
            .or_else(|| self.delayed.remove(&offset).map(|_| 0))
    }

    /// Earliest instant something needs attention (an ack deadline or a
    /// scheduled redelivery).
    pub fn next_wakeup(&self) -> Option<Instant> {
        [
            self.in_flight.values().map(|f| f.deadline).min(),
            self.scheduled.values().map(|s| s.due).min(),
            self.delayed.values().copied().min(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Reposition: forget all unacked records and continue from `offset`.
    pub fn seek(&mut self, offset: u64) {
        self.in_flight.clear();
        self.scheduled.clear();
        self.delayed.clear();
        self.priority.clear();
        self.next_read = offset;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ConsumerSpec {
        let mut s = ConsumerSpec::new("c", "s");
        s.ack_wait_ms = 1000;
        s.max_deliver = 3;
        s.max_ack_pending = 2;
        s
    }

    #[test]
    fn ack_floor_tracks_lowest_unacked() {
        let now = Instant::now();
        let mut c = Core::new(spec(), 0);
        c.delivered(0, 0, now);
        c.delivered(1, 0, now);
        c.next_read = 2;
        assert_eq!(c.ack_floor(), 0);
        c.ack(1);
        assert_eq!(
            c.ack_floor(),
            0,
            "acking out of order doesn't move the floor"
        );
        c.ack(0);
        assert_eq!(c.ack_floor(), 2);
        assert!(!c.ack(0), "acks are idempotent");
        assert_eq!(c.stats.acked, 2);
    }

    #[test]
    fn max_ack_pending_limits_new_deliveries() {
        let now = Instant::now();
        let mut c = Core::new(spec(), 0);
        assert_eq!(c.capacity_for_new(), 2);
        c.delivered(0, 0, now);
        c.delivered(1, 0, now);
        assert_eq!(c.capacity_for_new(), 0);
        c.ack(0);
        assert_eq!(c.capacity_for_new(), 1);
    }

    #[test]
    fn timeout_redelivers_then_dead_letters() {
        let t0 = Instant::now();
        let mut c = Core::new(spec(), 0);
        c.delivered(5, 0, t0);
        assert!(c.due(t0, 10).is_empty());

        let t1 = t0 + Duration::from_millis(1001);
        assert_eq!(c.expire(t1), 1);
        assert_eq!(
            c.due(t1, 10),
            vec![Due::Redeliver {
                offset: 5,
                deliveries: 1
            }]
        );
        c.delivered(5, 1, t1);
        let t2 = t1 + Duration::from_millis(1001);
        c.expire(t2);
        c.delivered(5, 2, t2);
        let t3 = t2 + Duration::from_millis(1001);
        c.expire(t3);
        assert_eq!(
            c.due(t3, 10),
            vec![Due::DeadLetter {
                offset: 5,
                deliveries: 3
            }]
        );
        assert_eq!(c.take_unacked(5), Some(3));
        assert_eq!(c.unacked(), 0);
        assert_eq!(c.stats.redelivered, 2);
    }

    #[test]
    fn nack_uses_backoff_or_explicit_delay() {
        let t0 = Instant::now();
        let mut s = spec();
        s.backoff_ms = vec![100, 500];
        let mut c = Core::new(s, 0);
        c.delivered(1, 0, t0);
        assert!(c.nack(1, None, t0));
        assert!(c.due(t0 + Duration::from_millis(99), 10).is_empty());
        assert_eq!(c.due(t0 + Duration::from_millis(100), 10).len(), 1);

        c.delivered(1, 1, t0);
        assert!(c.nack(1, Some(Duration::from_millis(7)), t0));
        assert_eq!(c.due(t0 + Duration::from_millis(7), 10).len(), 1);
        assert!(!c.nack(99, None, t0), "unknown offset");
    }

    #[test]
    fn in_progress_extends_deadline() {
        let t0 = Instant::now();
        let mut c = Core::new(spec(), 0);
        c.delivered(1, 0, t0);
        let t1 = t0 + Duration::from_millis(900);
        assert!(c.in_progress(1, t1));
        assert_eq!(c.expire(t0 + Duration::from_millis(1500)), 0);
        assert_eq!(c.expire(t1 + Duration::from_millis(1001)), 1);
    }

    #[test]
    fn ack_policy_none_tracks_nothing() {
        let t0 = Instant::now();
        let mut s = spec();
        s.ack = AckPolicy::None;
        let mut c = Core::new(s, 0);
        c.delivered(0, 0, t0);
        assert_eq!(c.unacked(), 0);
        assert_eq!(c.capacity_for_new(), usize::MAX);
        assert_eq!(c.next_wakeup(), None);
    }

    #[test]
    fn snapshot_restore_redelivers_unacked() {
        let t0 = Instant::now();
        let mut c = Core::new(spec(), 0);
        c.delivered(0, 0, t0);
        c.delivered(1, 0, t0);
        c.next_read = 2;
        c.ack(0);
        let snap = c.snapshot();
        assert_eq!(snap.pending, vec![(1, 1)]);

        let json = serde_json::to_string(&snap).unwrap();
        let back: Snapshot = serde_json::from_str(&json).unwrap();
        let r = Core::restore(back, t0);
        assert_eq!(r.next_read, 2);
        assert_eq!(r.ack_floor(), 1);
        assert_eq!(
            r.due(t0, 10),
            vec![Due::Redeliver {
                offset: 1,
                deliveries: 1
            }]
        );
    }

    #[test]
    fn delayed_records_hold_the_floor_but_not_capacity() {
        let t0 = Instant::now();
        let mut c = Core::new(spec(), 0);
        c.delay(0, t0 + Duration::from_secs(5));
        c.next_read = 1;
        assert_eq!(
            c.capacity_for_new(),
            2,
            "delayed records don't use max_ack_pending"
        );
        assert_eq!(c.ack_floor(), 0);
        assert!(c.due(t0, 10).is_empty());
        assert_eq!(c.next_wakeup(), Some(t0 + Duration::from_secs(5)));
        let later = t0 + Duration::from_secs(5);
        assert_eq!(
            c.due(later, 10),
            vec![Due::Redeliver {
                offset: 0,
                deliveries: 0
            }]
        );
        c.delivered(0, 0, later);
        assert_eq!(c.delayed(), 0);
        assert_eq!(c.unacked(), 1);
        assert_eq!(c.stats.delivered, 1);
    }

    #[test]
    fn delayed_records_survive_a_restart_as_due_now() {
        let t0 = Instant::now();
        let mut c = Core::new(spec(), 0);
        c.delay(3, t0 + Duration::from_secs(60));
        c.next_read = 4;
        let snap = c.snapshot();
        assert_eq!(snap.pending, vec![(3, 0)]);
        let r = Core::restore(snap, t0);
        // Due now: the actor re-reads the record and delays it again.
        assert_eq!(
            r.due(t0, 10),
            vec![Due::Redeliver {
                offset: 3,
                deliveries: 0
            }]
        );
        assert_eq!(r.ack_floor(), 3);
    }

    #[test]
    fn due_records_are_ordered_by_priority() {
        let t0 = Instant::now();
        let mut c = Core::new(spec(), 0);
        c.delay_with_priority(0, t0, 0);
        c.delay_with_priority(1, t0, 9);
        c.delay_with_priority(2, t0, 5);
        c.delay_with_priority(3, t0, 9);
        c.delivered(10, 0, t0);
        c.expire(t0 + Duration::from_secs(2));
        let order: Vec<u64> = c
            .due(t0 + Duration::from_secs(2), 10)
            .into_iter()
            .map(|d| match d {
                Due::Redeliver { offset, .. } | Due::DeadLetter { offset, .. } => offset,
            })
            .collect();
        assert_eq!(
            order,
            [10, 1, 3, 2, 0],
            "redeliveries first, then by priority, oldest first within one"
        );
    }

    #[test]
    fn seek_clears_unacked() {
        let t0 = Instant::now();
        let mut c = Core::new(spec(), 0);
        c.delivered(0, 0, t0);
        c.seek(100);
        assert_eq!(c.unacked(), 0);
        assert_eq!(c.ack_floor(), 100);
    }
}
