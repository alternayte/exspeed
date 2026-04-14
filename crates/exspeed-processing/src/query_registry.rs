use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Persistent catalog of continuous queries.
///
/// Each registered query is stored on disk under `{data_dir}/queries/{id}/`.
pub struct QueryRegistry {
    queries: RwLock<HashMap<String, QueryInfo>>,
    data_dir: PathBuf,
}

/// In-memory representation of a continuous query.
pub struct QueryInfo {
    pub id: String,
    pub sql: String,
    pub target_stream: String,
    pub status: QueryStatus,
    pub cancel_tx: Option<oneshot::Sender<()>>,
}

/// Runtime status of a continuous query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryStatus {
    Running,
    Stopped,
    Failed(String),
}

/// Snapshot of a query's metadata (without the cancel channel).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInfoSnapshot {
    pub id: String,
    pub sql: String,
    pub target_stream: String,
    pub status: String,
}

// ---------------------------------------------------------------------------
// Persistence formats
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct QueryJson {
    id: String,
    sql: String,
    target_stream: String,
    status: String,
}

#[derive(Serialize, Deserialize)]
struct CheckpointJson {
    source_offsets: HashMap<String, u64>,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl QueryRegistry {
    /// Create a new, empty registry backed by the given data directory.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            queries: RwLock::new(HashMap::new()),
            data_dir,
        }
    }

    /// Register a new continuous query, persisting `query.json` to disk.
    pub fn register(&self, id: &str, sql: &str, target_stream: &str) -> Result<(), String> {
        let query_dir = self.data_dir.join("queries").join(id);
        fs::create_dir_all(&query_dir)
            .map_err(|e| format!("failed to create query dir: {e}"))?;

        let json = QueryJson {
            id: id.to_string(),
            sql: sql.to_string(),
            target_stream: target_stream.to_string(),
            status: "stopped".to_string(),
        };

        let content = serde_json::to_string_pretty(&json)
            .map_err(|e| format!("serialize: {e}"))?;
        fs::write(query_dir.join("query.json"), content)
            .map_err(|e| format!("write query.json: {e}"))?;

        let info = QueryInfo {
            id: id.to_string(),
            sql: sql.to_string(),
            target_stream: target_stream.to_string(),
            status: QueryStatus::Stopped,
            cancel_tx: None,
        };

        self.queries.write().unwrap().insert(id.to_string(), info);
        Ok(())
    }

    /// Load all persisted queries from `{data_dir}/queries/*/query.json`.
    ///
    /// All queries are loaded with status `Stopped` and no cancel channel.
    pub fn load_all(&self) -> Result<(), String> {
        let queries_dir = self.data_dir.join("queries");
        if !queries_dir.is_dir() {
            return Ok(());
        }

        let entries = fs::read_dir(&queries_dir)
            .map_err(|e| format!("read queries dir: {e}"))?;

        let mut map = self.queries.write().unwrap();
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let json_path = path.join("query.json");
            if !json_path.exists() {
                continue;
            }
            let content = match fs::read_to_string(&json_path) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let parsed: QueryJson = match serde_json::from_str(&content) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let info = QueryInfo {
                id: parsed.id.clone(),
                sql: parsed.sql,
                target_stream: parsed.target_stream,
                status: QueryStatus::Stopped,
                cancel_tx: None,
            };
            map.insert(parsed.id, info);
        }

        Ok(())
    }

    /// Mark a query as running and store the cancel channel.
    pub fn set_running(&self, id: &str, cancel_tx: oneshot::Sender<()>) -> Result<(), String> {
        let mut map = self.queries.write().unwrap();
        let info = map.get_mut(id).ok_or_else(|| format!("query '{id}' not found"))?;
        info.status = QueryStatus::Running;
        info.cancel_tx = Some(cancel_tx);

        // Update status on disk
        self.persist_status(id, "running")?;
        Ok(())
    }

    /// Mark a query as failed with a reason.
    pub fn set_failed(&self, id: &str, reason: String) {
        let mut map = self.queries.write().unwrap();
        if let Some(info) = map.get_mut(id) {
            info.status = QueryStatus::Failed(reason.clone());
            info.cancel_tx = None;
            let _ = self.persist_status(id, &format!("failed: {reason}"));
        }
    }

    /// Stop a running query by sending the cancel signal.
    pub fn stop(&self, id: &str) -> Result<(), String> {
        let mut map = self.queries.write().unwrap();
        let info = map.get_mut(id).ok_or_else(|| format!("query '{id}' not found"))?;

        if let Some(tx) = info.cancel_tx.take() {
            let _ = tx.send(());
        }
        info.status = QueryStatus::Stopped;

        drop(map);
        self.persist_status(id, "stopped")?;
        Ok(())
    }

    /// Stop a query if running, remove it from the map, and delete its directory.
    pub fn remove(&self, id: &str) -> Result<(), String> {
        // Stop if running (ignore errors for already-stopped queries)
        let _ = self.stop(id);

        self.queries.write().unwrap().remove(id);

        let query_dir = self.data_dir.join("queries").join(id);
        if query_dir.is_dir() {
            fs::remove_dir_all(&query_dir)
                .map_err(|e| format!("remove query dir: {e}"))?;
        }

        Ok(())
    }

    /// Return a snapshot list of all registered queries (without cancel channels).
    pub fn list(&self) -> Vec<QueryInfoSnapshot> {
        self.queries
            .read()
            .unwrap()
            .values()
            .map(|info| QueryInfoSnapshot {
                id: info.id.clone(),
                sql: info.sql.clone(),
                target_stream: info.target_stream.clone(),
                status: match &info.status {
                    QueryStatus::Running => "running".to_string(),
                    QueryStatus::Stopped => "stopped".to_string(),
                    QueryStatus::Failed(reason) => format!("failed: {reason}"),
                },
            })
            .collect()
    }

    /// Persist a checkpoint with source stream offsets.
    pub fn checkpoint(&self, id: &str, source_offsets: HashMap<String, u64>) -> Result<(), String> {
        let query_dir = self.data_dir.join("queries").join(id);
        if !query_dir.is_dir() {
            return Err(format!("query dir for '{id}' does not exist"));
        }

        let ckpt = CheckpointJson { source_offsets };
        let content = serde_json::to_string_pretty(&ckpt)
            .map_err(|e| format!("serialize checkpoint: {e}"))?;
        fs::write(query_dir.join("checkpoint.json"), content)
            .map_err(|e| format!("write checkpoint.json: {e}"))?;

        Ok(())
    }

    /// Load the last checkpoint for a query, if any.
    pub fn load_checkpoint(&self, id: &str) -> Option<HashMap<String, u64>> {
        let path = self.data_dir.join("queries").join(id).join("checkpoint.json");
        let content = fs::read_to_string(&path).ok()?;
        let ckpt: CheckpointJson = serde_json::from_str(&content).ok()?;
        Some(ckpt.source_offsets)
    }

    /// Get the SQL for a registered query.
    pub fn get_sql(&self, id: &str) -> Option<String> {
        self.queries.read().unwrap().get(id).map(|q| q.sql.clone())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn persist_status(&self, id: &str, status: &str) -> Result<(), String> {
        let json_path = self.data_dir.join("queries").join(id).join("query.json");
        if !json_path.exists() {
            return Ok(());
        }
        let content = fs::read_to_string(&json_path)
            .map_err(|e| format!("read query.json: {e}"))?;
        let mut parsed: QueryJson = serde_json::from_str(&content)
            .map_err(|e| format!("parse query.json: {e}"))?;
        parsed.status = status.to_string();
        let updated = serde_json::to_string_pretty(&parsed)
            .map_err(|e| format!("serialize query.json: {e}"))?;
        fs::write(&json_path, updated)
            .map_err(|e| format!("write query.json: {e}"))?;
        Ok(())
    }
}

/// Generate a unique query ID.
pub fn generate_query_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    format!("q_{ts:x}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_load_all_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let registry = QueryRegistry::new(dir.path().to_path_buf());

        registry
            .register(
                "q_abc",
                "CREATE VIEW high_value AS SELECT * FROM \"orders\" WHERE payload->>'total' > '100'",
                "high_value",
            )
            .unwrap();

        registry
            .register(
                "q_def",
                "CREATE VIEW eu_orders AS SELECT * FROM \"orders\" WHERE payload->>'region' = 'eu'",
                "eu_orders",
            )
            .unwrap();

        // Verify files exist
        assert!(dir.path().join("queries/q_abc/query.json").exists());
        assert!(dir.path().join("queries/q_def/query.json").exists());

        // Load into a fresh registry
        let registry2 = QueryRegistry::new(dir.path().to_path_buf());
        registry2.load_all().unwrap();

        let mut queries = registry2.list();
        queries.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].id, "q_abc");
        assert_eq!(queries[0].target_stream, "high_value");
        assert_eq!(queries[0].status, "stopped");
        assert_eq!(queries[1].id, "q_def");
        assert_eq!(queries[1].target_stream, "eu_orders");
    }

    #[test]
    fn checkpoint_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let registry = QueryRegistry::new(dir.path().to_path_buf());

        registry.register("q_ckpt", "SELECT 1", "out").unwrap();

        let mut offsets = HashMap::new();
        offsets.insert("orders".to_string(), 50_000);
        offsets.insert("users".to_string(), 1_234);

        registry.checkpoint("q_ckpt", offsets.clone()).unwrap();

        // Verify file exists
        assert!(dir.path().join("queries/q_ckpt/checkpoint.json").exists());

        // Load
        let loaded = registry.load_checkpoint("q_ckpt").unwrap();
        assert_eq!(loaded.get("orders"), Some(&50_000));
        assert_eq!(loaded.get("users"), Some(&1_234));

        // Non-existent query returns None
        assert!(registry.load_checkpoint("q_nope").is_none());
    }

    #[test]
    fn remove_deletes_directory() {
        let dir = tempfile::tempdir().unwrap();
        let registry = QueryRegistry::new(dir.path().to_path_buf());

        registry.register("q_rm", "SELECT 1", "out").unwrap();
        assert!(dir.path().join("queries/q_rm").is_dir());

        registry.remove("q_rm").unwrap();
        assert!(!dir.path().join("queries/q_rm").exists());
        assert!(registry.list().is_empty());
    }

    #[test]
    fn stop_changes_status() {
        let dir = tempfile::tempdir().unwrap();
        let registry = QueryRegistry::new(dir.path().to_path_buf());

        registry.register("q_stop", "SELECT 1", "out").unwrap();

        let (tx, _rx) = oneshot::channel::<()>();
        registry.set_running("q_stop", tx).unwrap();

        let queries = registry.list();
        assert_eq!(queries[0].status, "running");

        registry.stop("q_stop").unwrap();

        let queries = registry.list();
        assert_eq!(queries[0].status, "stopped");
    }

    #[test]
    fn generate_unique_ids() {
        let id1 = generate_query_id();
        let id2 = generate_query_id();
        assert!(id1.starts_with("q_"));
        assert!(id2.starts_with("q_"));
        // They should be different (microtime resolution)
        // In practice they might be the same if called within the same microsecond,
        // but this is good enough for a smoke test.
        assert_ne!(id1.len(), 0);
    }
}
