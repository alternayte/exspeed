use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use exspeed_streams::StoredRecord;

// ---------------------------------------------------------------------------
// ConsumerConfig — persisted to JSON
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    pub name: String,
    pub stream: String,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub subject_filter: String,
    pub offset: u64,
}

// ---------------------------------------------------------------------------
// DeliveryRecord — sent through the mpsc channel to a subscriber
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DeliveryRecord {
    pub record: StoredRecord,
    pub delivery_attempt: u16,
}

// ---------------------------------------------------------------------------
// ConsumerState — runtime state (not persisted)
// ---------------------------------------------------------------------------

/// Runtime state for a single active subscriber of a consumer.
pub struct SubscriberState {
    /// Channel sender used to deliver records to this subscriber.
    pub delivery_tx: mpsc::Sender<DeliveryRecord>,
    /// Oneshot sender; dropping it signals the delivery task to stop.
    pub cancel_tx: tokio::sync::oneshot::Sender<()>,
}

pub struct ConsumerState {
    pub config: ConsumerConfig,
    /// Active subscribers keyed by subscriber_id.
    /// For non-grouped consumers this map holds at most one entry.
    pub subscribers: HashMap<String, SubscriberState>,
}

// ---------------------------------------------------------------------------
// ConsumerGroup — runtime group membership + routing
// ---------------------------------------------------------------------------

pub struct ConsumerGroup {
    pub name: String,
    pub members: Vec<String>, // kept sorted
}

impl ConsumerGroup {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            members: Vec::new(),
        }
    }

    /// Add a member if not already present, keeping the list sorted.
    pub fn add_member(&mut self, name: impl Into<String>) {
        let name = name.into();
        if let Err(pos) = self.members.binary_search(&name) {
            self.members.insert(pos, name);
        }
    }

    /// Remove a member by name.
    pub fn remove_member(&mut self, name: &str) {
        if let Ok(pos) = self.members.binary_search_by(|m| m.as_str().cmp(name)) {
            self.members.remove(pos);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Route a record to a group member.
    ///
    /// - Keyed records: hash the key and pick member by `hash % len`.
    /// - Keyless records: use `round_robin_counter % len`.
    ///
    /// Panics if the group has no members.
    pub fn route_record(&self, key: Option<&[u8]>, round_robin_counter: u64) -> &str {
        assert!(!self.members.is_empty(), "cannot route to an empty group");
        let idx = match key {
            Some(k) => {
                let mut hasher = DefaultHasher::new();
                k.hash(&mut hasher);
                (hasher.finish() % self.members.len() as u64) as usize
            }
            None => (round_robin_counter % self.members.len() as u64) as usize,
        };
        &self.members[idx]
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_member_keeps_sorted_no_duplicates() {
        let mut g = ConsumerGroup::new("g1");
        g.add_member("charlie");
        g.add_member("alice");
        g.add_member("bob");
        g.add_member("alice"); // duplicate
        assert_eq!(g.members, vec!["alice", "bob", "charlie"]);
    }

    #[test]
    fn remove_member_works() {
        let mut g = ConsumerGroup::new("g1");
        g.add_member("alice");
        g.add_member("bob");
        g.add_member("charlie");
        g.remove_member("bob");
        assert_eq!(g.members, vec!["alice", "charlie"]);
        // removing non-existent is a no-op
        g.remove_member("bob");
        assert_eq!(g.members, vec!["alice", "charlie"]);
    }

    #[test]
    fn is_empty() {
        let mut g = ConsumerGroup::new("g1");
        assert!(g.is_empty());
        g.add_member("alice");
        assert!(!g.is_empty());
        g.remove_member("alice");
        assert!(g.is_empty());
    }

    #[test]
    fn keyed_routing_is_consistent() {
        let mut g = ConsumerGroup::new("g1");
        g.add_member("a");
        g.add_member("b");
        g.add_member("c");

        let key = b"order-42";
        let target1 = g.route_record(Some(key), 0);
        let target2 = g.route_record(Some(key), 999);
        // Same key must always route to the same member regardless of counter.
        assert_eq!(target1, target2);
    }

    #[test]
    fn keyless_routing_round_robins() {
        let mut g = ConsumerGroup::new("g1");
        g.add_member("a");
        g.add_member("b");
        g.add_member("c");

        let targets: Vec<&str> = (0..6).map(|i| g.route_record(None, i)).collect();

        // With 3 members, round-robin should cycle: a, b, c, a, b, c
        assert_eq!(targets, vec!["a", "b", "c", "a", "b", "c"]);
    }
}
