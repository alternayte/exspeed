use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::consumer_state::ConsumerConfig;

/// Directory within `data_dir` where consumer JSON files are stored.
fn consumers_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("consumers")
}

/// Path to a specific consumer's JSON file.
fn consumer_path(data_dir: &Path, name: &str) -> PathBuf {
    consumers_dir(data_dir).join(format!("{name}.json"))
}

/// Persist a `ConsumerConfig` as JSON atomically.
///
/// Writes to a sibling `.tmp` file, fsyncs its contents, then renames over the
/// final path and fsyncs the parent directory. A crash at any point leaves
/// either the old file intact or the new file fully durable — never a
/// truncated target. The held-fd invariant asserted in the test suite relies
/// on this rename semantics.
pub fn save_consumer(data_dir: &Path, config: &ConsumerConfig) -> std::io::Result<()> {
    let dir = consumers_dir(data_dir);
    fs::create_dir_all(&dir)?;

    let final_path = consumer_path(data_dir, &config.name);
    let tmp_path = dir.join(format!("{}.json.tmp", config.name));
    let json = serde_json::to_string_pretty(config).map_err(std::io::Error::other)?;

    // Write new content + fsync to a tmp file in the same directory so the
    // subsequent rename is on the same filesystem (atomicity requirement).
    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
    }

    // Atomic replace.
    if let Err(e) = fs::rename(&tmp_path, &final_path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    // fsync the directory so the rename is durable across crash. Best-effort
    // on platforms that do not support directory fsync (e.g. some Windows
    // configurations) — we ignore EINVAL there.
    if let Ok(dir_file) = fs::File::open(&dir) {
        let _ = dir_file.sync_all();
    }

    Ok(())
}

/// Load a single consumer config by name.
pub fn load_consumer(data_dir: &Path, name: &str) -> std::io::Result<ConsumerConfig> {
    let path = consumer_path(data_dir, name);
    let data = fs::read_to_string(&path)?;
    let config: ConsumerConfig = serde_json::from_str(&data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(config)
}

/// Delete a consumer's persisted JSON file.
pub fn delete_consumer(data_dir: &Path, name: &str) -> std::io::Result<()> {
    let path = consumer_path(data_dir, name);
    fs::remove_file(&path)
}

/// Load all consumer configs from the consumers/ directory.
/// Returns an empty `Vec` if the directory does not exist.
pub fn load_all_consumers(data_dir: &Path) -> std::io::Result<Vec<ConsumerConfig>> {
    let dir = consumers_dir(data_dir);
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut configs = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            let data = fs::read_to_string(&path)?;
            let config: ConsumerConfig = serde_json::from_str(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            configs.push(config);
        }
    }

    // Sort by name for deterministic ordering.
    configs.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(configs)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config(name: &str) -> ConsumerConfig {
        ConsumerConfig {
            name: name.to_string(),
            stream: "orders".to_string(),
            group: "workers".to_string(),
            subject_filter: "order.>".to_string(),
            offset: 0,
        }
    }

    #[test]
    fn save_and_load_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cfg = sample_config("consumer-1");
        save_consumer(tmp.path(), &cfg).unwrap();
        let loaded = load_consumer(tmp.path(), "consumer-1").unwrap();
        assert_eq!(loaded.name, cfg.name);
        assert_eq!(loaded.stream, cfg.stream);
        assert_eq!(loaded.group, cfg.group);
        assert_eq!(loaded.subject_filter, cfg.subject_filter);
        assert_eq!(loaded.offset, cfg.offset);
    }

    #[test]
    fn delete_removes_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cfg = sample_config("doomed");
        save_consumer(tmp.path(), &cfg).unwrap();
        assert!(consumer_path(tmp.path(), "doomed").exists());
        delete_consumer(tmp.path(), "doomed").unwrap();
        assert!(!consumer_path(tmp.path(), "doomed").exists());
    }

    #[test]
    fn load_all_empty_returns_empty_vec() {
        let tmp = tempfile::TempDir::new().unwrap();
        let all = load_all_consumers(tmp.path()).unwrap();
        assert!(all.is_empty());
    }

    #[test]
    fn load_all_multiple_returns_all() {
        let tmp = tempfile::TempDir::new().unwrap();
        save_consumer(tmp.path(), &sample_config("beta")).unwrap();
        save_consumer(tmp.path(), &sample_config("alpha")).unwrap();
        save_consumer(tmp.path(), &sample_config("gamma")).unwrap();

        let all = load_all_consumers(tmp.path()).unwrap();
        assert_eq!(all.len(), 3);
        // Should be sorted by name.
        assert_eq!(all[0].name, "alpha");
        assert_eq!(all[1].name, "beta");
        assert_eq!(all[2].name, "gamma");
    }

    #[test]
    fn save_updates_offset() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = sample_config("updater");
        save_consumer(tmp.path(), &cfg).unwrap();
        let loaded = load_consumer(tmp.path(), "updater").unwrap();
        assert_eq!(loaded.offset, 0);

        cfg.offset = 42;
        save_consumer(tmp.path(), &cfg).unwrap();
        let loaded = load_consumer(tmp.path(), "updater").unwrap();
        assert_eq!(loaded.offset, 42);
    }

    /// Overwrites must replace the file via rename (new inode), not truncate in
    /// place. If we truncate the existing inode before writing the new bytes,
    /// a crash mid-write leaves the consumer's offset file empty or partial —
    /// silent state loss on next startup.
    ///
    /// We detect in-place truncation by holding an fd to v1 across the save of
    /// v2. With atomic rename, the held fd still sees v1 (old inode).
    /// With truncate-in-place, the held fd sees v2 (or a partial file).
    #[test]
    fn save_replaces_file_atomically_via_rename() {
        use std::io::{Read, Seek, SeekFrom};

        let tmp = tempfile::TempDir::new().unwrap();
        let mut cfg = sample_config("atomic");
        cfg.offset = 1;
        save_consumer(tmp.path(), &cfg).unwrap();

        // Hold an fd to v1.
        let path = consumer_path(tmp.path(), "atomic");
        let mut held_fd = fs::File::open(&path).unwrap();
        let mut v1_bytes = String::new();
        held_fd.read_to_string(&mut v1_bytes).unwrap();
        assert!(v1_bytes.contains("\"offset\": 1"), "sanity: v1 written");

        // Save v2 over the same path.
        cfg.offset = 2;
        save_consumer(tmp.path(), &cfg).unwrap();

        // Held fd should still see v1 — atomic rename replaces the inode,
        // and POSIX guarantees existing open fds keep pointing to the old one.
        held_fd.seek(SeekFrom::Start(0)).unwrap();
        let mut held_after = String::new();
        held_fd.read_to_string(&mut held_after).unwrap();
        assert!(
            held_after.contains("\"offset\": 1"),
            "non-atomic write detected: held fd sees {held_after:?} instead of v1"
        );

        // And a fresh read through the path sees v2.
        let fresh = load_consumer(tmp.path(), "atomic").unwrap();
        assert_eq!(fresh.offset, 2);
    }

    /// Invariant: after a successful save, no leftover `.tmp` files remain in
    /// the consumers directory. A crash between `.tmp` write and rename would
    /// leave an orphan that we'd ignore on load, but a successful save must
    /// clean up.
    #[test]
    fn save_leaves_no_tempfile_on_success() {
        let tmp = tempfile::TempDir::new().unwrap();
        save_consumer(tmp.path(), &sample_config("clean")).unwrap();

        let dir = tmp.path().join("consumers");
        for entry in fs::read_dir(&dir).unwrap() {
            let p = entry.unwrap().path();
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            assert!(
                !name.ends_with(".tmp"),
                "leftover tmp file after save: {name}"
            );
        }
    }

    /// `load_all_consumers` must ignore stray `.tmp` files — they represent an
    /// in-progress or crashed save, not a real consumer.
    #[test]
    fn load_all_ignores_tmp_files() {
        let tmp = tempfile::TempDir::new().unwrap();
        save_consumer(tmp.path(), &sample_config("real")).unwrap();

        // Drop a stray .tmp file to simulate a crashed save.
        let dir = tmp.path().join("consumers");
        fs::write(dir.join("ghost.json.tmp"), b"garbage").unwrap();

        let all = load_all_consumers(tmp.path()).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "real");
    }
}
