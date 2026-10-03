//! Exponential-backoff policies with full jitter.
//!
//! - [`RetryPolicy`] (`[retry]`): in-place retries of a transient error
//!   before `on_transient_exhausted` applies.
//! - [`RestartPolicy`] (`[restart]`): how the supervisor spaces restarts
//!   after a connector run fails.
//!
//! Pure logic — no I/O.

use std::time::Duration;

use rand::Rng;
use serde::{Deserialize, Serialize};

/// Retry policy for transient (whole-batch) failures.
///
/// Deserialized from a `[retry]` section of a connector's TOML or JSON config.
/// Missing section → `default_transient()`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
    #[serde(default = "default_jitter")]
    pub jitter: bool,
}

fn default_max_retries() -> u32 {
    5
}
fn default_initial_backoff_ms() -> u64 {
    100
}
fn default_max_backoff_ms() -> u64 {
    30_000
}
fn default_multiplier() -> f64 {
    2.0
}
fn default_jitter() -> bool {
    true
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::default_transient()
    }
}

impl RetryPolicy {
    pub fn default_transient() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            multiplier: default_multiplier(),
            jitter: default_jitter(),
        }
    }

    pub fn disabled() -> Self {
        Self {
            max_retries: 0,
            ..Self::default_transient()
        }
    }

    /// Returns the delay before the next retry, or `None` if retries are
    /// exhausted. `attempt` is 0-indexed (0 = first retry, not first try).
    ///
    /// Computed: `min(initial * multiplier^attempt, max_backoff_ms)`.
    /// With `jitter = true`, the returned `Duration` is uniform in
    /// `0..=computed` (the "full jitter" pattern).
    pub fn delay_for(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_retries {
            return None;
        }
        let base = (self.initial_backoff_ms as f64) * self.multiplier.powi(attempt as i32);
        let capped = base.min(self.max_backoff_ms as f64).max(0.0);
        let computed_ms = capped as u64;
        let ms = if self.jitter {
            if computed_ms == 0 {
                0
            } else {
                rand::thread_rng().gen_range(0..=computed_ms)
            }
        } else {
            computed_ms
        };
        Some(Duration::from_millis(ms))
    }
}

/// Supervisor restart backoff. A restart is `stop()` then `start()`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartPolicy {
    #[serde(default = "default_restart_initial_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_restart_max_ms")]
    pub max_backoff_ms: u64,
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
    #[serde(default = "default_jitter")]
    pub jitter: bool,
    /// Consecutive failed runs before the connector is marked `failed`.
    /// 0 = never give up. A run that commits at least one batch resets the
    /// count.
    #[serde(default)]
    pub max_restarts: u32,
}

fn default_restart_initial_ms() -> u64 {
    1_000
}
fn default_restart_max_ms() -> u64 {
    60_000
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            initial_backoff_ms: default_restart_initial_ms(),
            max_backoff_ms: default_restart_max_ms(),
            multiplier: default_multiplier(),
            jitter: default_jitter(),
            max_restarts: 0,
        }
    }
}

impl RestartPolicy {
    /// Delay before restart number `attempt` (0-indexed). Always bounded by
    /// `max_backoff_ms`; with jitter, uniform in `[computed/2, computed]`
    /// so restarts never spin.
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let base = (self.initial_backoff_ms as f64) * self.multiplier.powi(attempt.min(64) as i32);
        let capped = base.min(self.max_backoff_ms as f64).max(0.0) as u64;
        let ms = if self.jitter && capped > 1 {
            rand::thread_rng().gen_range(capped / 2..=capped)
        } else {
            capped
        };
        Duration::from_millis(ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_for_respects_initial_and_multiplier_without_jitter() {
        let p = RetryPolicy {
            max_retries: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 10_000,
            multiplier: 2.0,
            jitter: false,
        };
        assert_eq!(p.delay_for(0), Some(Duration::from_millis(100)));
        assert_eq!(p.delay_for(1), Some(Duration::from_millis(200)));
        assert_eq!(p.delay_for(2), Some(Duration::from_millis(400)));
        assert_eq!(p.delay_for(3), Some(Duration::from_millis(800)));
    }

    #[test]
    fn delay_for_caps_at_max_backoff() {
        let p = RetryPolicy {
            max_retries: 100,
            initial_backoff_ms: 100,
            max_backoff_ms: 500,
            multiplier: 2.0,
            jitter: false,
        };
        assert_eq!(p.delay_for(2), Some(Duration::from_millis(400)));
        assert_eq!(p.delay_for(3), Some(Duration::from_millis(500)));
        assert_eq!(p.delay_for(50), Some(Duration::from_millis(500)));
    }

    #[test]
    fn delay_for_returns_none_past_max_retries() {
        let p = RetryPolicy {
            max_retries: 3,
            ..RetryPolicy::default_transient()
        };
        assert!(p.delay_for(0).is_some());
        assert!(p.delay_for(2).is_some());
        assert!(p.delay_for(3).is_none());
        assert!(p.delay_for(100).is_none());
    }

    #[test]
    fn jitter_stays_within_bound() {
        let p = RetryPolicy {
            max_retries: 10,
            initial_backoff_ms: 1000,
            max_backoff_ms: 10_000,
            multiplier: 2.0,
            jitter: true,
        };
        for _ in 0..1000 {
            let d = p.delay_for(2).unwrap();
            assert!(d <= Duration::from_millis(4000), "got {:?}", d);
        }
    }

    #[test]
    fn disabled_never_retries() {
        let p = RetryPolicy::disabled();
        assert!(p.delay_for(0).is_none());
    }

    #[test]
    fn default_transient_shape() {
        let p = RetryPolicy::default_transient();
        assert_eq!(p.max_retries, 5);
        assert_eq!(p.initial_backoff_ms, 100);
        assert_eq!(p.max_backoff_ms, 30_000);
        assert_eq!(p.multiplier, 2.0);
        assert!(p.jitter);
    }

    #[test]
    fn deserialize_from_toml_with_all_fields() {
        let toml = r#"
            max_retries = 3
            initial_backoff_ms = 200
            max_backoff_ms = 5000
            multiplier = 3.0
            jitter = false
        "#;
        let p: RetryPolicy = toml::from_str(toml).unwrap();
        assert_eq!(p.max_retries, 3);
        assert_eq!(p.initial_backoff_ms, 200);
        assert!(!p.jitter);
    }

    #[test]
    fn restart_delay_is_bounded() {
        let p = RestartPolicy {
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            multiplier: 2.0,
            jitter: false,
            max_restarts: 0,
        };
        assert_eq!(p.delay_for(0), Duration::from_millis(100));
        assert_eq!(p.delay_for(3), Duration::from_millis(800));
        assert_eq!(p.delay_for(10_000), Duration::from_millis(1_000));
        let j = RestartPolicy {
            jitter: true,
            ..p.clone()
        };
        for _ in 0..100 {
            let d = j.delay_for(2);
            assert!(d >= Duration::from_millis(200) && d <= Duration::from_millis(400));
        }
    }

    #[test]
    fn retry_rejects_unknown_keys() {
        assert!(toml::from_str::<RetryPolicy>("max_retires = 3").is_err());
    }

    #[test]
    fn deserialize_from_empty_toml_uses_defaults() {
        let p: RetryPolicy = toml::from_str("").unwrap();
        assert_eq!(p.max_retries, default_max_retries());
        assert_eq!(p.multiplier, default_multiplier());
    }
}
