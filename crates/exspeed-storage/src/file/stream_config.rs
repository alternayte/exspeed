//! On-disk persistence of [`StreamConfig`] as `{stream_dir}/stream.json`.

use std::fs;
use std::io;
use std::path::Path;

pub use exspeed_streams::config::{
    StreamConfig, DEFAULT_DEDUP_MAX_ENTRIES, DEFAULT_DEDUP_WINDOW_SECS, DEFAULT_MAX_AGE_SECS,
    DEFAULT_MAX_BYTES, DEFAULT_TOMBSTONE_RETENTION_SECS,
};

use crate::file::fsutil::atomic_write;

/// File I/O for [`StreamConfig`]. Import this trait to call
/// `StreamConfig::load(dir)` / `config.save(dir)`.
pub trait StreamConfigFile: Sized {
    /// Persist to `{stream_dir}/stream.json` atomically (tmp + rename + fsync).
    fn save(&self, stream_dir: &Path) -> io::Result<()>;
    /// Load from `{stream_dir}/stream.json`; defaults if the file is missing.
    fn load(stream_dir: &Path) -> io::Result<Self>;
}

impl StreamConfigFile for StreamConfig {
    fn save(&self, stream_dir: &Path) -> io::Result<()> {
        fs::create_dir_all(stream_dir)?;
        let json = serde_json::to_vec_pretty(self).map_err(io::Error::other)?;
        atomic_write(&stream_dir.join("stream.json"), &json)
    }

    fn load(stream_dir: &Path) -> io::Result<Self> {
        let path = stream_dir.join("stream.json");
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = fs::read_to_string(&path)?;
        serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
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
