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

/// Persist a `ConsumerConfig` as JSON.  Creates the consumers/ directory if
/// needed and fsyncs the file to ensure durability.
pub fn save_consumer(data_dir: &Path, config: &ConsumerConfig) -> std::io::Result<()> {
    let dir = consumers_dir(data_dir);
    fs::create_dir_all(&dir)?;

    let path = consumer_path(data_dir, &config.name);
    let json = serde_json::to_string_pretty(config).map_err(std::io::Error::other)?;

    let mut file = fs::File::create(&path)?;
    file.write_all(json.as_bytes())?;
    file.sync_all()?;

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
}
