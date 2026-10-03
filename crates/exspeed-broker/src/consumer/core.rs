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

use std::collections::BTreeMap;
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

pub struct Core {
    pub spec: ConsumerSpec,
    /// Next stream offset to read for a first delivery.
    pub next_read: u64,
    in_flight: BTreeMap<u64, InFlight>,
    scheduled: BTreeMap<u64, Scheduled>,
    pub stats: ConsumerStats,
}

impl Core {
    pub fn new(spec: ConsumerSpec, next_read: u64) -> Self {
        Self {
            spec,
            next_read,
            in_flight: BTreeMap::new(),
            scheduled: BTreeMap::new(),
            stats: ConsumerStats::default(),
        }
    }

    /// Rebuild from a snapshot. Every record that was unacked is due for
    /// redelivery immediately: at-least-once across restarts.
    pub fn restore(snapshot: Snapshot, now: Instant) -> Self {
        let mut core = Self::new(snapshot.spec, snapshot.next_read);
        core.stats = snapshot.stats;
        for (offset, deliveries) in snapshot.pending {
            if offset < core.next_read {
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

    /// How many *new* records may be handed out now (`max_ack_pending`).
    pub fn capacity_for_new(&self) -> usize {
        if !self.explicit() || self.spec.max_ack_pending == 0 {
            return usize::MAX;
        }
        (self.spec.max_ack_pending as usize).saturating_sub(self.unacked())
    }

    /// Lowest offset not yet acked; everything below it is done.
    pub fn ack_floor(&self) -> u64 {
        let a = self.in_flight.keys().next().copied();
        let b = self.scheduled.keys().next().copied();
        match (a, b) {
            (Some(a), Some(b)) => a.min(b),
            (Some(x), None) | (None, Some(x)) => x,
            (None, None) => self.next_read,
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

    /// Up to `max` scheduled records that are due now, lowest offset first.
    pub fn due(&self, now: Instant, max: usize) -> Vec<Due> {
        self.scheduled
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
            .collect()
    }

    /// Drop a scheduled record that no longer exists in the stream.
    pub fn gone(&mut self, offset: u64) {
        if self.scheduled.remove(&offset).is_some() || self.in_flight.remove(&offset).is_some() {
            self.stats.gone += 1;
        }
    }

    /// Earliest instant something needs attention (an ack deadline or a
    /// scheduled redelivery).
    pub fn next_wakeup(&self) -> Option<Instant> {
        let a = self.in_flight.values().map(|f| f.deadline).min();
        let b = self.scheduled.values().map(|s| s.due).min();
        match (a, b) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (x, None) | (None, x) => x,
        }
    }

    /// Reposition: forget all unacked records and continue from `offset`.
    pub fn seek(&mut self, offset: u64) {
        self.in_flight.clear();
        self.scheduled.clear();
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
        assert_eq!(c.ack_floor(), 0, "acking out of order doesn't move the floor");
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
    fn seek_clears_unacked() {
        let t0 = Instant::now();
        let mut c = Core::new(spec(), 0);
        c.delivered(0, 0, t0);
        c.seek(100);
        assert_eq!(c.unacked(), 0);
        assert_eq!(c.ack_floor(), 100);
    }
}
