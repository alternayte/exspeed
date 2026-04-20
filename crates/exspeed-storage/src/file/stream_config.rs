use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_AGE_SECS: u64 = 604_800; // 7 days
pub const DEFAULT_MAX_BYTES: u64 = 10_737_418_240; // 10 GB
pub const DEFAULT_DEDUP_WINDOW_SECS: u64 = 300;
pub const DEFAULT_DEDUP_MAX_ENTRIES: u64 = 500_000;

/// Per-stream retention configuration, persisted as `stream.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub max_age_secs: u64,
    pub max_bytes: u64,
    #[serde(default = "default_dedup_window_secs")]
    pub dedup_window_secs: u64,
    #[serde(default = "default_dedup_max_entries")]
    pub dedup_max_entries: u64,
}

fn default_dedup_window_secs() -> u64 { DEFAULT_DEDUP_WINDOW_SECS }
fn default_dedup_max_entries() -> u64 { DEFAULT_DEDUP_MAX_ENTRIES }

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_age_secs: DEFAULT_MAX_AGE_SECS,
            max_bytes: DEFAULT_MAX_BYTES,
            dedup_window_secs: DEFAULT_DEDUP_WINDOW_SECS,
            dedup_max_entries: DEFAULT_DEDUP_MAX_ENTRIES,
        }
    }
}

impl StreamConfig {
    /// Build a `StreamConfig` from a request, substituting defaults for zero values.
    pub fn from_request(
        max_age_secs: u64,
        max_bytes: u64,
        dedup_window_secs: u64,
        dedup_max_entries: u64,
    ) -> Self {
        Self {
            max_age_secs: if max_age_secs == 0 {
                DEFAULT_MAX_AGE_SECS
            } else {
                max_age_secs
            },
            max_bytes: if max_bytes == 0 {
                DEFAULT_MAX_BYTES
            } else {
                max_bytes
            },
            dedup_window_secs: if dedup_window_secs == 0 {
                DEFAULT_DEDUP_WINDOW_SECS
            } else {
                dedup_window_secs
            },
            dedup_max_entries: if dedup_max_entries == 0 {
                DEFAULT_DEDUP_MAX_ENTRIES
            } else {
                dedup_max_entries
            },
        }
    }

    /// Validate stream config parameters.
    pub fn validate(
        max_age_secs: u64,
        _max_bytes: u64,
        dedup_window_secs: u64,
        dedup_max_entries: u64,
    ) -> Result<(), String> {
        if dedup_window_secs > max_age_secs && max_age_secs > 0 {
            return Err(format!(
                "dedup window ({dedup_window_secs}s) exceeds retention ({max_age_secs}s); \
                 either increase retention (--retention) or shorten dedup window (--dedup-window)"
            ));
        }
        if dedup_max_entries < 1 {
            return Err("dedup_max_entries must be ≥ 1".to_string());
        }
        Ok(())
    }

    /// Persist this config to `{stream_dir}/stream.json`, fsyncing the file.
    pub fn save(&self, stream_dir: &Path) -> io::Result<()> {
        fs::create_dir_all(stream_dir)?;
        let path = stream_dir.join("stream.json");
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        let mut file = File::create(&path)?;
        file.write_all(json.as_bytes())?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    /// Load config from `{stream_dir}/stream.json`.
    ///
    /// Returns the default `StreamConfig` if the file does not exist.
    pub fn load(stream_dir: &Path) -> io::Result<Self> {
        let path = stream_dir.join("stream.json");
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = fs::read_to_string(&path)?;
        let config: Self = serde_json::from_str(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn default_values() {
        let cfg = StreamConfig::default();
        assert_eq!(cfg.max_age_secs, DEFAULT_MAX_AGE_SECS);
        assert_eq!(cfg.max_bytes, DEFAULT_MAX_BYTES);
        assert_eq!(DEFAULT_MAX_AGE_SECS, 604_800);
        assert_eq!(DEFAULT_MAX_BYTES, 10_737_418_240);
    }

    #[test]
    fn from_request_zeros_use_defaults() {
        let cfg = StreamConfig::from_request(0, 0, 0, 0);
        assert_eq!(cfg.max_age_secs, DEFAULT_MAX_AGE_SECS);
        assert_eq!(cfg.max_bytes, DEFAULT_MAX_BYTES);
    }

    #[test]
    fn from_request_custom_values() {
        let cfg = StreamConfig::from_request(3600, 1_000_000, 0, 0);
        assert_eq!(cfg.max_age_secs, 3600);
        assert_eq!(cfg.max_bytes, 1_000_000);
    }

    #[test]
    fn save_and_load() {
        let dir = TempDir::new().unwrap();
        let cfg = StreamConfig::from_request(7200, 5_000_000, 0, 0);
        cfg.save(dir.path()).unwrap();

        let loaded = StreamConfig::load(dir.path()).unwrap();
        assert_eq!(loaded.max_age_secs, 7200);
        assert_eq!(loaded.max_bytes, 5_000_000);
    }

    #[test]
    fn load_missing_returns_default() {
        let dir = TempDir::new().unwrap();
        let cfg = StreamConfig::load(dir.path()).unwrap();
        assert_eq!(cfg.max_age_secs, DEFAULT_MAX_AGE_SECS);
        assert_eq!(cfg.max_bytes, DEFAULT_MAX_BYTES);
    }

    #[test]
    fn dedup_defaults() {
        let cfg = StreamConfig::default();
        assert_eq!(cfg.dedup_window_secs, DEFAULT_DEDUP_WINDOW_SECS);
        assert_eq!(cfg.dedup_max_entries, DEFAULT_DEDUP_MAX_ENTRIES);
        assert_eq!(DEFAULT_DEDUP_WINDOW_SECS, 300);
        assert_eq!(DEFAULT_DEDUP_MAX_ENTRIES, 500_000);
    }

    #[test]
    fn validation_rejects_window_longer_than_retention() {
        let err = StreamConfig::validate(600, 10_000_000, 1200, 500_000).unwrap_err();
        assert!(err.contains("dedup window"), "got: {err}");
    }

    #[test]
    fn validation_accepts_window_equal_to_retention() {
        assert!(StreamConfig::validate(600, 10_000_000, 600, 500_000).is_ok());
    }

    #[test]
    fn validation_rejects_zero_max_entries() {
        assert!(StreamConfig::validate(600, 10_000_000, 300, 0).is_err());
    }

    #[test]
    fn legacy_stream_json_loads_with_dedup_defaults() {
        let dir = TempDir::new().unwrap();
        let legacy = r#"{"max_age_secs":3600,"max_bytes":1000000}"#;
        std::fs::write(dir.path().join("stream.json"), legacy).unwrap();
        let loaded = StreamConfig::load(dir.path()).unwrap();
        assert_eq!(loaded.dedup_window_secs, DEFAULT_DEDUP_WINDOW_SECS);
        assert_eq!(loaded.dedup_max_entries, DEFAULT_DEDUP_MAX_ENTRIES);
    }

    #[test]
    fn save_and_load_roundtrip_dedup_fields() {
        let dir = TempDir::new().unwrap();
        let cfg = StreamConfig::from_request(7200, 5_000_000, 600, 100_000);
        cfg.save(dir.path()).unwrap();
        let loaded = StreamConfig::load(dir.path()).unwrap();
        assert_eq!(loaded.dedup_window_secs, 600);
        assert_eq!(loaded.dedup_max_entries, 100_000);
    }
}
