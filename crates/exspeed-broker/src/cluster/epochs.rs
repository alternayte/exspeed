//! Per-stream leader-epoch history (KIP-101 style).
//!
//! For every stream the store keeps:
//!
//! * a `uid` — random, assigned when the leader first sees the stream. It
//!   tells a follower that a stream was deleted and recreated under the same
//!   name;
//! * the epoch history — `(epoch, start_offset)` pairs, ascending. Records at
//!   offsets `[start_i, start_{i+1})` were written by the leader of
//!   `epoch_i`. Offsets before the first entry belong to epoch 0 (written
//!   before the cluster had a lease, e.g. a single node that was later
//!   clustered).
//!
//! A follower that has records up to `next` and whose last record has epoch
//! `e` asks the leader where epoch `e` ends in the leader's log; anything it
//! has beyond that point diverged and is truncated.
//!
//! Stored as one small JSON file per stream under `{data_dir}/cluster/epochs/`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEpochs {
    pub uid: u64,
    /// `(epoch, start_offset)`, ascending in both.
    pub epochs: Vec<(u64, u64)>,
}

impl StreamEpochs {
    pub fn new(uid: u64) -> Self {
        Self {
            uid,
            epochs: Vec::new(),
        }
    }

    /// Record that `epoch` starts writing at `start`. An epoch that starts
    /// where the previous one did replaces it (it wrote nothing).
    pub fn push(&mut self, epoch: u64, start: u64) {
        if let Some(&(last_epoch, last_start)) = self.epochs.last() {
            if last_epoch >= epoch {
                return;
            }
            if last_start >= start {
                self.epochs.pop();
            }
        }
        self.epochs.push((epoch, start));
    }

    /// Epoch of the record at `offset` (0 when before the first entry).
    pub fn epoch_at(&self, offset: u64) -> u64 {
        self.epochs
            .iter()
            .rev()
            .find(|&&(_, start)| start <= offset)
            .map_or(0, |&(e, _)| e)
    }

    /// Epoch of the last record of a log whose next offset is `next`.
    pub fn last_epoch(&self, next: u64) -> u64 {
        if next == 0 {
            0
        } else {
            self.epoch_at(next - 1)
        }
    }

    /// Where `epoch` ends in this log: the start of the first later epoch,
    /// or `next` when `epoch` is the latest.
    pub fn end_offset(&self, epoch: u64, next: u64) -> u64 {
        self.epochs
            .iter()
            .find(|&&(e, _)| e > epoch)
            .map_or(next, |&(_, start)| start.min(next))
    }

    /// Drop entries that start at or after `next` (after a truncation), then
    /// adopt `leader`'s entries that start before `next`. Used by followers,
    /// whose log is a prefix of the leader's.
    pub fn adopt(&mut self, leader: &[(u64, u64)], next: u64) -> bool {
        let adopted: Vec<(u64, u64)> = leader.iter().copied().filter(|&(_, s)| s < next).collect();
        if adopted != self.epochs {
            self.epochs = adopted;
            true
        } else {
            false
        }
    }
}

/// The epoch histories of all local streams.
pub struct EpochStore {
    dir: PathBuf,
    cache: Mutex<HashMap<String, StreamEpochs>>,
}

impl EpochStore {
    pub fn open(data_dir: &Path) -> std::io::Result<Self> {
        let dir = data_dir.join("cluster").join("epochs");
        std::fs::create_dir_all(&dir)?;
        let mut cache = HashMap::new();
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Some(name) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            match std::fs::read(&path)
                .ok()
                .and_then(|b| serde_json::from_slice::<StreamEpochs>(&b).ok())
            {
                Some(e) => {
                    cache.insert(name.to_string(), e);
                }
                None => tracing::warn!(path = %path.display(), "ignoring unreadable epoch file"),
            }
        }
        Ok(Self {
            dir,
            cache: Mutex::new(cache),
        })
    }

    pub fn get(&self, stream: &str) -> Option<StreamEpochs> {
        self.cache.lock().get(stream).cloned()
    }

    /// The history of `stream`, created with a fresh uid (and no epochs) if
    /// missing.
    pub fn get_or_create(&self, stream: &str) -> std::io::Result<StreamEpochs> {
        if let Some(e) = self.get(stream) {
            return Ok(e);
        }
        let e = StreamEpochs::new(rand::random::<u64>() | 1);
        self.put(stream, e.clone())?;
        Ok(e)
    }

    /// Replace the history of `stream` (written atomically).
    pub fn put(&self, stream: &str, epochs: StreamEpochs) -> std::io::Result<()> {
        let path = self.dir.join(format!("{stream}.json"));
        let tmp = self.dir.join(format!("{stream}.json.tmp"));
        std::fs::write(&tmp, serde_json::to_vec(&epochs).expect("serializable"))?;
        std::fs::rename(&tmp, &path)?;
        self.cache.lock().insert(stream.to_string(), epochs);
        Ok(())
    }

    /// Apply `f` to the history of `stream` (creating it if missing) and
    /// persist the result if it changed.
    pub fn update(
        &self,
        stream: &str,
        f: impl FnOnce(&mut StreamEpochs) -> bool,
    ) -> std::io::Result<StreamEpochs> {
        let mut e = self.get_or_create(stream)?;
        if f(&mut e) {
            self.put(stream, e.clone())?;
        }
        Ok(e)
    }

    pub fn remove(&self, stream: &str) {
        self.cache.lock().remove(stream);
        let _ = std::fs::remove_file(self.dir.join(format!("{stream}.json")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_offset_and_last_epoch() {
        let mut h = StreamEpochs::new(1);
        h.push(1, 0);
        h.push(2, 100);
        h.push(4, 150);
        assert_eq!(h.epoch_at(0), 1);
        assert_eq!(h.epoch_at(99), 1);
        assert_eq!(h.epoch_at(100), 2);
        assert_eq!(h.epoch_at(200), 4);
        assert_eq!(h.last_epoch(100), 1);
        assert_eq!(h.last_epoch(0), 0);
        assert_eq!(h.end_offset(1, 300), 100);
        assert_eq!(h.end_offset(2, 300), 150);
        assert_eq!(h.end_offset(3, 300), 150);
        assert_eq!(h.end_offset(4, 300), 300);
        assert_eq!(h.end_offset(0, 300), 0);
    }

    #[test]
    fn empty_epochs_collapse() {
        let mut h = StreamEpochs::new(1);
        h.push(1, 10);
        h.push(2, 10);
        h.push(3, 10);
        assert_eq!(h.epochs, vec![(3, 10)]);
        h.push(2, 20); // stale epoch ignored
        assert_eq!(h.epochs, vec![(3, 10)]);
    }

    #[test]
    fn no_history_means_epoch_zero() {
        let h = StreamEpochs::new(1);
        assert_eq!(h.last_epoch(50), 0);
        assert_eq!(h.end_offset(0, 50), 50);
    }

    #[test]
    fn adopt_takes_the_prefix() {
        let mut f = StreamEpochs::new(1);
        f.push(1, 0);
        f.push(2, 120); // diverged
        assert!(f.adopt(&[(1, 0), (3, 110), (5, 400)], 200));
        assert_eq!(f.epochs, vec![(1, 0), (3, 110)]);
        assert!(!f.adopt(&[(1, 0), (3, 110), (5, 400)], 200));
    }

    #[test]
    fn store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let s = EpochStore::open(dir.path()).unwrap();
        let e = s
            .update("orders", |e| {
                e.push(3, 0);
                true
            })
            .unwrap();
        let s2 = EpochStore::open(dir.path()).unwrap();
        assert_eq!(s2.get("orders"), Some(e));
        s2.remove("orders");
        assert!(EpochStore::open(dir.path())
            .unwrap()
            .get("orders")
            .is_none());
    }
}
