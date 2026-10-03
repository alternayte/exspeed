//! Cluster metadata catalogs kept in compacted internal streams.
//!
//! A [`CatalogStore`] is a small key → definition map stored as records in
//! one internal stream (`__exql_queries`, `__exql_connections`,
//! `__connectors`, …): the record key is the entry's id, the value its
//! definition (JSON), and a delete appends a tombstone (key + empty value),
//! which is also what compaction treats as a delete. Writes go through the
//! broker [`Log`], so they are leader-gated and replicate with the data;
//! any node that becomes leader reads the same catalog back.
//!
//! [`CatalogStore::load`] reads the stream from its earliest offset to the
//! high watermark and keeps the last record per key, dropping keys whose
//! last record is a tombstone. That is correct whether or not compaction
//! has run.
//!
//! [`CatalogStore::migrate_dir`] imports the JSON files older versions
//! kept under `data_dir` into an empty catalog stream, then renames the
//! directory to `<dir>.migrated`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use exspeed_common::StreamName;
use exspeed_streams::{ReadLimits, Record, StorageError, StreamConfig};
use tracing::{info, warn};

use crate::log::{Log, LogError};

/// One catalog stream.
pub struct CatalogStore {
    log: Arc<Log>,
    stream: StreamName,
    subject: String,
}

/// What [`CatalogStore::migrate_dir`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Migration {
    /// No legacy directory.
    NoLegacy,
    /// Legacy files exist but this node can't write now; they are kept and
    /// the import is retried on the next call (next leader tenure).
    Deferred,
    /// The catalog stream already had records, so the legacy files were not
    /// imported; the directory was renamed to the given path.
    SetAside(PathBuf),
    /// `n` entries were imported; the directory was renamed to the given path.
    Imported(usize, PathBuf),
}

impl CatalogStore {
    /// A catalog in internal stream `stream`; records are written with
    /// `subject`.
    pub fn new(log: Arc<Log>, stream: &str, subject: &str) -> Self {
        Self {
            log,
            stream: StreamName::try_from(stream).expect("valid internal stream name"),
            subject: subject.to_string(),
        }
    }

    pub fn stream(&self) -> &StreamName {
        &self.stream
    }

    pub fn log(&self) -> &Arc<Log> {
        &self.log
    }

    /// Settings for a catalog stream: never age out (an entry's only record
    /// may be old) and compact, which keeps just the latest record per key.
    pub fn stream_config() -> StreamConfig {
        StreamConfig {
            max_age_secs: 100 * 365 * 24 * 3600,
            max_bytes: u64::MAX / 2,
            compaction: true,
            ..StreamConfig::default()
        }
    }

    async fn ensure(&self) -> Result<(), LogError> {
        match self.log.storage().stream_bounds(&self.stream).await {
            Ok(_) => Ok(()),
            Err(StorageError::StreamNotFound(_)) => {
                match self
                    .log
                    .create_stream(&self.stream, &Self::stream_config())
                    .await
                {
                    Ok(()) | Err(LogError::Storage(StorageError::StreamAlreadyExists(_))) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Whether the stream has never been written (missing or high watermark
    /// at 0). Compaction never resets the high watermark, so a catalog whose
    /// entries were all deleted is not "empty".
    pub async fn is_empty(&self) -> Result<bool, LogError> {
        match self.log.storage().stream_bounds(&self.stream).await {
            Ok((_, hwm)) => Ok(hwm.0 == 0),
            Err(StorageError::StreamNotFound(_)) => Ok(true),
            Err(e) => Err(e.into()),
        }
    }

    /// The live entries: last record per key, tombstoned keys removed.
    pub async fn load(&self) -> Result<BTreeMap<String, Bytes>, LogError> {
        let storage = self.log.storage();
        let (earliest, hwm) = match storage.stream_bounds(&self.stream).await {
            Ok(b) => b,
            Err(StorageError::StreamNotFound(_)) => return Ok(BTreeMap::new()),
            Err(e) => return Err(e.into()),
        };
        let mut latest: BTreeMap<String, Bytes> = BTreeMap::new();
        let mut from = earliest;
        while from.0 < hwm.0 {
            let batch = storage
                .read_batch(
                    &self.stream,
                    from,
                    ReadLimits {
                        max_records: 1000,
                        max_bytes: 4 * 1024 * 1024,
                    },
                )
                .await?;
            for r in &batch.records {
                if r.offset.0 >= hwm.0 {
                    break;
                }
                let Some(key) = r.key.as_ref() else { continue };
                let key = String::from_utf8_lossy(key).into_owned();
                if r.value.is_empty() {
                    latest.remove(&key);
                } else {
                    latest.insert(key, r.value.clone());
                }
            }
            if batch.records.is_empty() || batch.next_offset.0 <= from.0 {
                break;
            }
            from = batch.next_offset;
        }
        Ok(latest)
    }

    /// Write `value` (non-empty) as the definition of `key`.
    pub async fn put(&self, key: &str, value: Bytes) -> Result<(), LogError> {
        if value.is_empty() {
            return Err(LogError::InvalidRecord(
                "catalog values must not be empty (an empty value is a delete)".into(),
            ));
        }
        self.ensure().await?;
        self.log
            .append(&self.stream, self.record(key, value))
            .await
            .map(|_| ())
    }

    /// Delete `key` (append a tombstone). A no-op when the stream doesn't
    /// exist yet; still leader-gated.
    pub async fn delete(&self, key: &str) -> Result<(), LogError> {
        if !self.log.can_write() {
            return Err(LogError::NotLeader);
        }
        match self.log.storage().stream_bounds(&self.stream).await {
            Ok(_) => {}
            Err(StorageError::StreamNotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        self.log
            .append(&self.stream, self.record(key, Bytes::new()))
            .await
            .map(|_| ())
    }

    fn record(&self, key: &str, value: Bytes) -> Record {
        Record {
            key: Some(Bytes::copy_from_slice(key.as_bytes())),
            value,
            subject: self.subject.clone(),
            headers: vec![],
            timestamp_ns: None,
        }
    }

    /// One-time import of a legacy directory of `*.json` files.
    ///
    /// - No directory: [`Migration::NoLegacy`].
    /// - This node can't write: [`Migration::Deferred`]; nothing changes.
    /// - The stream already has records: the files are not imported (the
    ///   replicated catalog wins) and the directory is renamed.
    /// - Otherwise every file `convert` accepts (it returns the key and the
    ///   value to store) is appended in one batch, then the directory is
    ///   renamed to `<dir>.migrated` (or `<dir>.migrated.N` if that exists).
    ///   Files `convert` rejects are logged and left behind in the renamed
    ///   directory.
    pub async fn migrate_dir<F>(&self, dir: &Path, convert: F) -> Result<Migration, String>
    where
        F: Fn(&Path, &[u8]) -> Result<(String, Bytes), String>,
    {
        if !dir.is_dir() {
            return Ok(Migration::NoLegacy);
        }
        if !self.log.can_write() {
            return Ok(Migration::Deferred);
        }
        if !self.is_empty().await.map_err(|e| e.to_string())? {
            let to = set_aside(dir)?;
            warn!(stream = %self.stream, dir = %dir.display(), moved_to = %to.display(),
                  "catalog stream already has entries; legacy files were not imported");
            return Ok(Migration::SetAside(to));
        }
        let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
            .map_err(|e| format!("read {}: {e}", dir.display()))?
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("json"))
            .collect();
        paths.sort();
        let mut records = Vec::with_capacity(paths.len());
        for path in &paths {
            let converted = std::fs::read(path)
                .map_err(|e| e.to_string())
                .and_then(|bytes| convert(path, &bytes));
            match converted {
                Ok((key, value)) if !value.is_empty() => records.push(self.record(&key, value)),
                Ok(_) => {
                    warn!(path = %path.display(), "legacy file converted to an empty value; skipped")
                }
                Err(e) => warn!(path = %path.display(), "legacy file not imported: {e}"),
            }
        }
        let n = records.len();
        if n > 0 {
            self.ensure().await.map_err(|e| e.to_string())?;
            self.log
                .append_batch(&self.stream, records)
                .await
                .map_err(|e| format!("import into {}: {e}", self.stream))?;
        }
        let to = set_aside(dir)?;
        info!(stream = %self.stream, imported = n, from = %dir.display(), moved_to = %to.display(),
              "migrated legacy catalog files into the replicated log");
        Ok(Migration::Imported(n, to))
    }
}

/// Rename `dir` to `<dir>.migrated` (or `<dir>.migrated.N`).
fn set_aside(dir: &Path) -> Result<PathBuf, String> {
    let name = dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("bad directory name {}", dir.display()))?;
    let mut to = dir.with_file_name(format!("{name}.migrated"));
    let mut n = 1;
    while to.exists() {
        to = dir.with_file_name(format!("{name}.migrated.{n}"));
        n += 1;
    }
    std::fs::rename(dir, &to)
        .map_err(|e| format!("rename {} to {}: {e}", dir.display(), to.display()))?;
    Ok(to)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker_append::BrokerAppend;
    use crate::log::WriteGate;
    use exspeed_common::Metrics;
    use exspeed_storage::memory::MemoryStorage;
    use exspeed_streams::StorageEngine;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Gate(AtomicBool);
    impl WriteGate for Gate {
        fn can_write(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn log_with_gate() -> (Arc<Log>, Arc<Gate>) {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let dedup = Arc::new(BrokerAppend::new(storage.clone(), 300));
        let (metrics, _) = Metrics::new();
        let log = Arc::new(Log::new(
            storage,
            dedup,
            Arc::new(metrics),
            Arc::new(AtomicBool::new(true)),
        ));
        let gate = Arc::new(Gate(AtomicBool::new(true)));
        log.set_write_gate(gate.clone());
        (log, gate)
    }

    fn store(log: &Arc<Log>) -> CatalogStore {
        CatalogStore::new(log.clone(), "__test_catalog", "test.def")
    }

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[tokio::test]
    async fn last_write_wins_and_tombstones_remove() {
        let (log, _) = log_with_gate();
        let s = store(&log);
        assert!(s.is_empty().await.unwrap());
        assert!(s.load().await.unwrap().is_empty(), "no stream yet");
        s.delete("ghost").await.unwrap();
        assert!(
            s.is_empty().await.unwrap(),
            "delete on a missing stream is a no-op"
        );

        s.put("a", b("1")).await.unwrap();
        s.put("b", b("x")).await.unwrap();
        s.put("a", b("2")).await.unwrap();
        s.put("c", b("y")).await.unwrap();
        s.delete("b").await.unwrap();
        let m = store(&log).load().await.unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m["a"], b("2"));
        assert_eq!(m["c"], b("y"));

        // Re-created after a delete.
        s.put("b", b("z")).await.unwrap();
        assert_eq!(store(&log).load().await.unwrap()["b"], b("z"));

        s.delete("a").await.unwrap();
        s.delete("b").await.unwrap();
        s.delete("c").await.unwrap();
        assert!(s.load().await.unwrap().is_empty());
        assert!(!s.is_empty().await.unwrap(), "written once, so not empty");
    }

    #[tokio::test]
    async fn stream_is_compacted_and_internal() {
        let (log, _) = log_with_gate();
        let s = store(&log);
        s.put("a", b("1")).await.unwrap();
        assert!(s.put("a", Bytes::new()).await.is_err(), "empty value");
        let cfg = CatalogStore::stream_config();
        assert!(cfg.compaction);
    }

    #[tokio::test]
    async fn writes_are_leader_gated() {
        let (log, gate) = log_with_gate();
        let s = store(&log);
        s.put("a", b("1")).await.unwrap();
        gate.0.store(false, Ordering::SeqCst);
        assert!(matches!(s.put("a", b("2")).await, Err(LogError::NotLeader)));
        assert!(matches!(s.delete("a").await, Err(LogError::NotLeader)));
        // Reads still work on a follower.
        assert_eq!(s.load().await.unwrap()["a"], b("1"));
    }

    fn convert(path: &Path, bytes: &[u8]) -> Result<(String, Bytes), String> {
        if bytes == b"bad" {
            return Err("unparseable".into());
        }
        let key = path.file_stem().unwrap().to_str().unwrap().to_string();
        Ok((key, Bytes::copy_from_slice(bytes)))
    }

    #[tokio::test]
    async fn migration_imports_once_and_renames() {
        let (log, gate) = log_with_gate();
        let s = store(&log);
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("defs");
        assert_eq!(
            s.migrate_dir(&dir, convert).await.unwrap(),
            Migration::NoLegacy
        );
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("q1.json"), "one").unwrap();
        std::fs::write(dir.join("q2.json"), "two").unwrap();
        std::fs::write(dir.join("broken.json"), "bad").unwrap();
        std::fs::write(dir.join("notes.txt"), "ignored").unwrap();

        // A node that can't write leaves the files alone.
        gate.0.store(false, Ordering::SeqCst);
        assert_eq!(
            s.migrate_dir(&dir, convert).await.unwrap(),
            Migration::Deferred
        );
        assert!(dir.exists());
        gate.0.store(true, Ordering::SeqCst);

        let migrated = tmp.path().join("defs.migrated");
        assert_eq!(
            s.migrate_dir(&dir, convert).await.unwrap(),
            Migration::Imported(2, migrated.clone())
        );
        assert!(!dir.exists());
        assert!(migrated.join("q1.json").exists(), "files are kept");
        let m = s.load().await.unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m["q1"], b("one"));
        assert_eq!(m["q2"], b("two"));

        // Legacy files reappearing (e.g. restored from an old backup) are
        // not imported over a non-empty catalog.
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("q3.json"), "three").unwrap();
        assert_eq!(
            s.migrate_dir(&dir, convert).await.unwrap(),
            Migration::SetAside(tmp.path().join("defs.migrated.1"))
        );
        assert_eq!(s.load().await.unwrap().len(), 2);
    }
}
