//! Per-subject record limit (`max_msgs_per_subject`): the newest offsets of
//! every subject, so readers can hide records that N newer records of the
//! same subject have superseded.
//!
//! Only records below the high watermark count as newer: a record that is
//! not yet visible (not replicated) never hides an older one, so a subject
//! never has fewer than N visible records while a write is in flight.

use std::collections::{HashMap, VecDeque};

/// Newest offsets per subject, ascending. Each deque holds at most `limit`
/// offsets below the high watermark it was last pruned with, plus any at or
/// above it.
#[derive(Debug, Default)]
pub struct SubjectIndex {
    limit: usize,
    map: HashMap<Box<str>, VecDeque<u64>>,
}

impl SubjectIndex {
    pub fn new(limit: u64) -> Self {
        Self {
            limit: limit.clamp(1, usize::MAX as u64) as usize,
            map: HashMap::new(),
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Record that `offset` (greater than any offset observed before for
    /// this subject) holds `subject`.
    pub fn observe(&mut self, subject: &str, offset: u64, hwm: u64) {
        let q = match self.map.get_mut(subject) {
            Some(q) => q,
            None => self.map.entry(subject.into()).or_default(),
        };
        if q.back().is_some_and(|&b| b >= offset) {
            return;
        }
        q.push_back(offset);
        let below = q.partition_point(|&o| o < hwm);
        if below > self.limit {
            q.drain(..below - self.limit);
        }
    }

    /// Whether `offset` (holding `subject`) is hidden: at least `limit`
    /// newer records of the subject are visible.
    pub fn superseded(&self, subject: &str, offset: u64, hwm: u64) -> bool {
        let Some(q) = self.map.get(subject) else {
            return false;
        };
        let newer_visible = q.partition_point(|&o| o < hwm) - q.partition_point(|&o| o <= offset);
        newer_visible >= self.limit
    }

    /// The newest visible offset of `subject`.
    pub fn latest(&self, subject: &str, hwm: u64) -> Option<u64> {
        let q = self.map.get(subject)?;
        let below = q.partition_point(|&o| o < hwm);
        below.checked_sub(1).map(|i| q[i])
    }

    /// Every subject with its newest offset in `[earliest, hwm)`.
    pub fn all_latest(&self, hwm: u64, earliest: u64) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> = self
            .map
            .iter()
            .filter_map(|(s, q)| {
                let below = q.partition_point(|&o| o < hwm);
                let last = q.get(below.checked_sub(1)?)?;
                (*last >= earliest).then(|| (s.to_string(), *last))
            })
            .collect();
        v.sort();
        v
    }

    /// Forget subjects whose every record is below `start` (trimmed).
    pub fn forget_below(&mut self, start: u64) {
        self.map
            .retain(|_, q| q.back().is_some_and(|&last| last >= start));
    }

    pub fn subjects(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_the_newest_n_per_subject() {
        let mut ix = SubjectIndex::new(2);
        for (o, s) in [(0, "a"), (1, "b"), (2, "a"), (3, "a"), (4, "b")] {
            ix.observe(s, o, o + 1);
        }
        let hwm = 5;
        assert!(ix.superseded("a", 0, hwm), "two newer a's are visible");
        assert!(!ix.superseded("a", 2, hwm));
        assert!(!ix.superseded("a", 3, hwm));
        assert!(!ix.superseded("b", 1, hwm));
        assert_eq!(ix.latest("a", hwm), Some(3));
        assert_eq!(ix.latest("zzz", hwm), None);
    }

    #[test]
    fn invisible_records_never_hide_older_ones() {
        let mut ix = SubjectIndex::new(1);
        ix.observe("k", 0, 1);
        // Offset 1 is written but the high watermark is still 1 (not yet
        // replicated): offset 0 stays visible.
        ix.observe("k", 1, 1);
        assert!(!ix.superseded("k", 0, 1));
        assert_eq!(ix.latest("k", 1), Some(0));
        // Once visible, it supersedes offset 0.
        assert!(ix.superseded("k", 0, 2));
        assert_eq!(ix.latest("k", 2), Some(1));
    }

    #[test]
    fn forget_below_drops_trimmed_subjects() {
        let mut ix = SubjectIndex::new(1);
        ix.observe("old", 0, 1);
        ix.observe("new", 5, 6);
        ix.forget_below(3);
        assert_eq!(ix.subjects(), 1);
        assert_eq!(ix.latest("new", 6), Some(5));
    }
}
