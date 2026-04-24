use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use tokio::sync::mpsc;
use tracing::info;

use crate::file::partition::SealedSegmentInfo;

// ---------------------------------------------------------------------------
// CachedSegment
// ---------------------------------------------------------------------------

struct CachedSegment {
    seg_path: PathBuf,
    size_bytes: u64,
    last_accessed: Instant,
}

// ---------------------------------------------------------------------------
// CacheTracker
// ---------------------------------------------------------------------------

/// Tracks which local segments have been uploaded to S3 and are eligible for
/// eviction.  Eviction uses an LRU policy: the least-recently-accessed segment
/// is removed first when the total cached size exceeds `local_max_bytes`.
struct CacheTracker {
    /// Key: (stream_name, base_offset)
    entries: HashMap<(String, u64), CachedSegment>,
    local_max_bytes: u64,
}

impl CacheTracker {
    fn new(local_max_bytes: u64) -> Self {
        Self {
            entries: HashMap::new(),
            local_max_bytes,
        }
    }

    /// Record that `info`'s segment has been successfully uploaded to S3 and
    /// is now a candidate for local eviction.
    fn mark_uploaded(&mut self, info: &SealedSegmentInfo) {
        self.entries.insert(
            (info.stream_name.clone(), info.base_offset),
            CachedSegment {
                seg_path: info.seg_path.clone(),
                size_bytes: info.size_bytes,
                last_accessed: Instant::now(),
            },
        );
    }

    /// Update the last-accessed time for the given segment, preventing it from
    /// being chosen as the LRU victim in the near term.
    #[allow(dead_code)]
    fn touch(&mut self, stream: &str, base_offset: u64) {
        if let Some(entry) = self.entries.get_mut(&(stream.to_string(), base_offset)) {
            entry.last_accessed = Instant::now();
        }
    }

    /// Evict the least-recently-used segments until the total cached size is
    /// within `local_max_bytes`.  Returns the number of segments evicted.
    fn evict_if_needed(&mut self) -> u32 {
        let mut evicted = 0u32;

        loop {
            let total: u64 = self.entries.values().map(|e| e.size_bytes).sum();
            if total <= self.local_max_bytes || self.entries.is_empty() {
                break;
            }

            // Find the LRU entry (minimum last_accessed).
            let lru_key = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_accessed)
                .map(|(k, _)| k.clone())
                .expect("entries is non-empty");

            let entry = self.entries.remove(&lru_key).expect("key just found");

            let seg_path = &entry.seg_path;

            // Remove .seg / .idx / .tix / .bloom files (best-effort).
            let _ = std::fs::remove_file(seg_path);
            let _ = std::fs::remove_file(seg_path.with_extension("idx"));
            let _ = std::fs::remove_file(seg_path.with_extension("tix"));
            let _ = std::fs::remove_file(seg_path.with_extension("bloom"));

            info!(
                stream = %lru_key.0,
                base_offset = lru_key.1,
                size_bytes = entry.size_bytes,
                "evicted local segment (uploaded to S3)",
            );

            evicted += 1;
        }

        evicted
    }
}

// ---------------------------------------------------------------------------
// spawn_cache_manager
// ---------------------------------------------------------------------------

/// Spawn a background task that manages the local segment cache.
///
/// - Receives `SealedSegmentInfo` items from `uploaded_rx` (sent by the S3
///   uploader after a successful upload) and registers them as eviction
///   candidates.
/// - Every 30 seconds, evicts LRU segments if the local cache exceeds
///   `local_max_bytes`.
pub fn spawn_cache_manager(
    mut uploaded_rx: mpsc::UnboundedReceiver<SealedSegmentInfo>,
    local_max_bytes: u64,
) {
    tokio::spawn(async move {
        let mut tracker = CacheTracker::new(local_max_bytes);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

        loop {
            tokio::select! {
                maybe_info = uploaded_rx.recv() => {
                    match maybe_info {
                        Some(info) => {
                            tracker.mark_uploaded(&info);
                        }
                        None => {
                            // Channel closed — uploader has shut down.
                            info!("cache manager task exiting — channel closed");
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    let evicted = tracker.evict_if_needed();
                    if evicted > 0 {
                        info!(evicted, "LRU eviction cycle complete");
                    }
                }
            }
        }
    });
}
