use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_AGE_SECS: u64 = 604_800; // 7 days
pub const DEFAULT_MAX_BYTES: u64 = 10_737_418_240; // 10 GB

/// Per-stream retention configuration, persisted as `stream.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub max_age_secs: u64,
    pub max_bytes: u64,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_age_secs: DEFAULT_MAX_AGE_SECS,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

impl StreamConfig {
    /// Build a `StreamConfig` from a request, substituting defaults for zero values.
    pub fn from_request(max_age_secs: u64, max_bytes: u64) -> Self {
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
        }
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
        let cfg = StreamConfig::from_request(0, 0);
        assert_eq!(cfg.max_age_secs, DEFAULT_MAX_AGE_SECS);
        assert_eq!(cfg.max_bytes, DEFAULT_MAX_BYTES);
    }

    #[test]
    fn from_request_custom_values() {
        let cfg = StreamConfig::from_request(3600, 1_000_000);
        assert_eq!(cfg.max_age_secs, 3600);
        assert_eq!(cfg.max_bytes, 1_000_000);
    }

    #[test]
    fn save_and_load() {
        let dir = TempDir::new().unwrap();
        let cfg = StreamConfig::from_request(7200, 5_000_000);
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
}
