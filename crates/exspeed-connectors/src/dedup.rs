use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

/// In-memory dedup cache with a TTL window.
///
/// Tracks keys that have been seen recently and rejects duplicates within the
/// configured time window.
pub struct DedupCache {
    seen: HashMap<String, Instant>,
    window: Duration,
}

impl DedupCache {
    /// Create a new cache with the given TTL window in seconds.
    pub fn new(window_secs: u64) -> Self {
        Self {
            seen: HashMap::new(),
            window: Duration::from_secs(window_secs),
        }
    }

    /// Check whether `key` is new. Returns `true` if it has not been seen
    /// within the TTL window (i.e. the record should be processed), or `false`
    /// if it is a duplicate (skip).
    pub fn check_and_insert(&mut self, key: &str) -> bool {
        let now = Instant::now();

        if let Some(first_seen) = self.seen.get(key) {
            if now.duration_since(*first_seen) < self.window {
                return false; // duplicate
            }
        }

        self.seen.insert(key.to_string(), now);
        true
    }

    /// Remove entries older than the TTL window.
    pub fn cleanup(&mut self) {
        let now = Instant::now();
        let window = self.window;
        self.seen
            .retain(|_, first_seen| now.duration_since(*first_seen) < window);
    }

    /// Compute a deterministic hash of a byte slice, returned as `"content:{hex}"`.
    pub fn content_hash(data: &[u8]) -> String {
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        let hash = hasher.finish();
        format!("content:{hash:016x}")
    }

    /// Number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_key_is_not_duplicate() {
        let mut cache = DedupCache::new(60);
        assert!(cache.check_and_insert("key-1"));
    }

    #[test]
    fn repeated_key_is_duplicate() {
        let mut cache = DedupCache::new(60);
        assert!(cache.check_and_insert("key-1"));
        assert!(!cache.check_and_insert("key-1"));
    }

    #[test]
    fn different_keys_are_not_duplicates() {
        let mut cache = DedupCache::new(60);
        assert!(cache.check_and_insert("key-1"));
        assert!(cache.check_and_insert("key-2"));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn content_hash_is_deterministic() {
        let data = b"hello world";
        let h1 = DedupCache::content_hash(data);
        let h2 = DedupCache::content_hash(data);
        assert_eq!(h1, h2);
        assert!(h1.starts_with("content:"));
    }

    #[test]
    fn different_data_gives_different_hash() {
        let h1 = DedupCache::content_hash(b"hello");
        let h2 = DedupCache::content_hash(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn cleanup_removes_expired_entries() {
        // Use a 0-second window so everything expires immediately.
        let mut cache = DedupCache::new(0);
        assert!(cache.check_and_insert("key-1"));
        assert!(cache.check_and_insert("key-2"));
        assert_eq!(cache.len(), 2);

        cache.cleanup();
        assert!(cache.is_empty());
    }
}
