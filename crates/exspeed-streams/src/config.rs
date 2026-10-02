//! Per-stream configuration shared by every storage engine.

use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_AGE_SECS: u64 = 604_800; // 7 days
pub const DEFAULT_MAX_BYTES: u64 = 10_737_418_240; // 10 GB
pub const DEFAULT_DEDUP_WINDOW_SECS: u64 = 300;
pub const DEFAULT_DEDUP_MAX_ENTRIES: u64 = 500_000;

/// Retention and dedup settings for one stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub max_age_secs: u64,
    pub max_bytes: u64,
    #[serde(default = "default_dedup_window_secs")]
    pub dedup_window_secs: u64,
    #[serde(default = "default_dedup_max_entries")]
    pub dedup_max_entries: u64,
}

fn default_dedup_window_secs() -> u64 {
    DEFAULT_DEDUP_WINDOW_SECS
}
fn default_dedup_max_entries() -> u64 {
    DEFAULT_DEDUP_MAX_ENTRIES
}

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
    /// Build a config from request values, substituting defaults for zeros.
    pub fn from_request(
        max_age_secs: u64,
        max_bytes: u64,
        dedup_window_secs: u64,
        dedup_max_entries: u64,
    ) -> Self {
        fn or(v: u64, d: u64) -> u64 {
            if v == 0 {
                d
            } else {
                v
            }
        }
        let max_age_secs = or(max_age_secs, DEFAULT_MAX_AGE_SECS);
        // A defaulted dedup window never exceeds retention (a 1 s stream
        // gets a 1 s window); an explicit one is validated by `check`.
        let dedup_window_secs = if dedup_window_secs == 0 {
            DEFAULT_DEDUP_WINDOW_SECS.min(max_age_secs)
        } else {
            dedup_window_secs
        };
        Self {
            max_age_secs,
            max_bytes: or(max_bytes, DEFAULT_MAX_BYTES),
            dedup_window_secs,
            dedup_max_entries: or(dedup_max_entries, DEFAULT_DEDUP_MAX_ENTRIES),
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

    /// Validate this config.
    pub fn check(&self) -> Result<(), String> {
        Self::validate(
            self.max_age_secs,
            self.max_bytes,
            self.dedup_window_secs,
            self.dedup_max_entries,
        )
    }
}
