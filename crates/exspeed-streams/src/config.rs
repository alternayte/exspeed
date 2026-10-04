//! Per-stream configuration shared by every storage engine.

use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_AGE_SECS: u64 = 604_800; // 7 days
pub const DEFAULT_MAX_BYTES: u64 = 10_737_418_240; // 10 GB
pub const DEFAULT_DEDUP_WINDOW_SECS: u64 = 300;
pub const DEFAULT_DEDUP_MAX_ENTRIES: u64 = 500_000;
pub const DEFAULT_TOMBSTONE_RETENTION_SECS: u64 = 86_400; // 24 h
/// Longest accepted stream-wide message TTL: 10 years.
pub const MAX_MSG_TTL_MS: u64 = 10 * 365 * 24 * 3600 * 1000;

pub use exspeed_common::limits::{DiscardPolicy, RetentionPolicy, StreamLimits};

/// Retention, dedup and compaction settings for one stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub max_age_secs: u64,
    pub max_bytes: u64,
    #[serde(default = "default_dedup_window_secs")]
    pub dedup_window_secs: u64,
    #[serde(default = "default_dedup_max_entries")]
    pub dedup_max_entries: u64,
    /// Keep only the latest record per key in sealed segments (log
    /// compaction). Records without a key are never removed; a record with a
    /// key and an empty value is a tombstone that deletes the key. Offsets
    /// are preserved, so a compacted stream has offset gaps.
    #[serde(default)]
    pub compaction: bool,
    /// How long a tombstone survives compaction before it is removed too.
    #[serde(default = "default_tombstone_retention_secs")]
    pub tombstone_retention_secs: u64,
    /// Keep at most this many records (0 = no limit). What happens at the
    /// limit is decided by `discard`.
    #[serde(default)]
    pub max_msgs: u64,
    /// At `max_msgs` (or `max_bytes`, for `New`): drop the oldest records or
    /// reject new ones.
    #[serde(default)]
    pub discard: DiscardPolicy,
    /// Keep only the newest N records per subject (0 = no limit). Older
    /// records of a subject are invisible as soon as N newer ones exist.
    #[serde(default)]
    pub max_msgs_per_subject: u64,
    /// Accept a per-record TTL in the `exspeed-ttl` header.
    #[serde(default)]
    pub allow_msg_ttl: bool,
    /// TTL of every record without its own `exspeed-ttl` (0 = none), in
    /// milliseconds. Expired records are invisible to readers and consumers.
    #[serde(default)]
    pub msg_ttl_ms: u64,
    /// Accept delayed delivery (`exspeed-delay` / `exspeed-deliver-at`).
    #[serde(default)]
    pub allow_delayed: bool,
    /// Whether acknowledgements remove records (`work_queue`, `interest`).
    #[serde(default)]
    pub retention: RetentionPolicy,
}

fn default_dedup_window_secs() -> u64 {
    DEFAULT_DEDUP_WINDOW_SECS
}
fn default_dedup_max_entries() -> u64 {
    DEFAULT_DEDUP_MAX_ENTRIES
}
fn default_tombstone_retention_secs() -> u64 {
    DEFAULT_TOMBSTONE_RETENTION_SECS
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_age_secs: DEFAULT_MAX_AGE_SECS,
            max_bytes: DEFAULT_MAX_BYTES,
            dedup_window_secs: DEFAULT_DEDUP_WINDOW_SECS,
            dedup_max_entries: DEFAULT_DEDUP_MAX_ENTRIES,
            compaction: false,
            tombstone_retention_secs: DEFAULT_TOMBSTONE_RETENTION_SECS,
            max_msgs: 0,
            discard: DiscardPolicy::Old,
            max_msgs_per_subject: 0,
            allow_msg_ttl: false,
            msg_ttl_ms: 0,
            allow_delayed: false,
            retention: RetentionPolicy::Limits,
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
        Self::from_request_with_window(
            max_age_secs,
            max_bytes,
            dedup_window_secs,
            dedup_max_entries,
            DEFAULT_DEDUP_WINDOW_SECS,
        )
    }

    /// Like [`from_request`](Self::from_request), with the server's default
    /// dedup window (`storage.dedup_window_secs`) for a zero window.
    pub fn from_request_with_window(
        max_age_secs: u64,
        max_bytes: u64,
        dedup_window_secs: u64,
        dedup_max_entries: u64,
        default_window_secs: u64,
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
            default_window_secs.max(1).min(max_age_secs)
        } else {
            dedup_window_secs
        };
        Self {
            max_age_secs,
            max_bytes: or(max_bytes, DEFAULT_MAX_BYTES),
            dedup_window_secs,
            dedup_max_entries: or(dedup_max_entries, DEFAULT_DEDUP_MAX_ENTRIES),
            ..Self::default()
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
        )?;
        if self.msg_ttl_ms > crate::config::MAX_MSG_TTL_MS {
            return Err("msg_ttl_ms is longer than 10 years".into());
        }
        if !self.retention.is_limits() && self.compaction {
            return Err(
                "compaction keeps the latest record per key; it can't be combined with \
                 work_queue or interest retention, which remove records once acked"
                    .into(),
            );
        }
        Ok(())
    }

    /// The limit and lifetime settings, as sent over the wire.
    pub fn limits(&self) -> StreamLimits {
        StreamLimits {
            max_msgs: self.max_msgs,
            discard: self.discard,
            max_msgs_per_subject: self.max_msgs_per_subject,
            allow_msg_ttl: self.allow_msg_ttl,
            msg_ttl_ms: self.msg_ttl_ms,
            allow_delayed: self.allow_delayed,
            retention: self.retention,
        }
    }

    /// Set the limit and lifetime settings.
    pub fn with_limits(mut self, l: &StreamLimits) -> Self {
        self.max_msgs = l.max_msgs;
        self.discard = l.discard;
        self.max_msgs_per_subject = l.max_msgs_per_subject;
        self.allow_msg_ttl = l.allow_msg_ttl;
        self.msg_ttl_ms = l.msg_ttl_ms;
        self.allow_delayed = l.allow_delayed;
        self.retention = l.retention;
        self
    }

    /// Whether readers must look at each record's time headers or
    /// timestamp to hide expired records.
    pub fn has_ttl(&self) -> bool {
        self.allow_msg_ttl || self.msg_ttl_ms > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_json_defaults_compaction_off() {
        let cfg: StreamConfig =
            serde_json::from_str(r#"{"max_age_secs":3600,"max_bytes":1000}"#).unwrap();
        assert!(!cfg.compaction);
        assert_eq!(
            cfg.tombstone_retention_secs,
            DEFAULT_TOMBSTONE_RETENTION_SECS
        );
    }
}
