//! Logging initialization. Selects JSON or human-readable output based on
//! the `LOG_FORMAT` env var (`json` or `text`, default `text`).

use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

impl LogFormat {
    pub fn from_env() -> Self {
        match std::env::var("LOG_FORMAT").ok().as_deref() {
            Some("json") | Some("JSON") => LogFormat::Json,
            _ => LogFormat::Text,
        }
    }
}

/// Initialize the global tracing subscriber. Call once at process start.
pub fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn"));

    let format = LogFormat::from_env();
    match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_current_span(false)
            .with_span_list(false)
            .init(),
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .init(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serialize env-var mutation across the 4 tests in this module.
    /// `cargo test` runs tests in parallel within a binary by default; reading
    /// and writing `LOG_FORMAT` from multiple threads is racy without this lock.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn defaults_to_text() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvVar::clear("LOG_FORMAT");
        assert_eq!(LogFormat::from_env(), LogFormat::Text);
    }

    #[test]
    fn json_when_set() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvVar::set("LOG_FORMAT", "json");
        assert_eq!(LogFormat::from_env(), LogFormat::Json);
    }

    #[test]
    fn json_uppercase_recognised() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvVar::set("LOG_FORMAT", "JSON");
        assert_eq!(LogFormat::from_env(), LogFormat::Json);
    }

    #[test]
    fn unknown_value_falls_back_to_text() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = EnvVar::set("LOG_FORMAT", "yaml");
        assert_eq!(LogFormat::from_env(), LogFormat::Text);
    }

    /// Test guard that restores the previous LOG_FORMAT value on drop.
    struct EnvVar {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, prev }
        }
        fn clear(key: &'static str) -> Self {
            let prev = std::env::var(key).ok();
            std::env::remove_var(key);
            Self { key, prev }
        }
    }

    impl Drop for EnvVar {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
}
