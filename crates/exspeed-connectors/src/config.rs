use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub connector_type: String, // "source" or "sink"
    pub plugin: String, // "http_webhook", "http_sink", "postgres_outbox"
    pub stream: String, // target stream (source publishes to, sink reads from)
    #[serde(default)]
    pub subject_template: String, // sources: derive subject from record
    #[serde(default)]
    pub subject_filter: String, // sinks: filter records by subject
    #[serde(default)]
    pub settings: HashMap<String, String>,
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_dedup_enabled")]
    pub dedup_enabled: bool,
    #[serde(default)]
    pub dedup_key: String,
    #[serde(default = "default_dedup_window")]
    pub dedup_window_secs: u64,
}

fn default_batch_size() -> u32 {
    100
}
fn default_poll_interval() -> u64 {
    50
}
fn default_dedup_enabled() -> bool {
    true
}
fn default_dedup_window() -> u64 {
    86400
}

impl ConnectorConfig {
    /// Load from a JSON file.
    pub fn load_json(path: &Path) -> io::Result<Self> {
        let json = fs::read_to_string(path)?;
        let config: Self = serde_json::from_str(&json)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(config)
    }

    /// Save as JSON.
    pub fn save_json(&self, path: &Path) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        fs::write(path, json)?;
        fs::File::open(path)?.sync_all()?;
        Ok(())
    }

    /// Load from a TOML file.
    ///
    /// TOML format uses `[connector]` table for top-level fields and `[settings]` for plugin config.
    pub fn load_toml(path: &Path) -> io::Result<Self> {
        let text = fs::read_to_string(path)?;
        let toml_val: TomlConnector =
            toml::from_str(&text).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(toml_val.into_config())
    }

    /// Resolve environment variable references in settings values.
    /// `${VAR_NAME}` is replaced with the env var value.
    pub fn resolve_env_vars(&mut self) {
        for value in self.settings.values_mut() {
            *value = resolve_env(value);
        }
    }

    /// Get a setting value, or return an error.
    pub fn setting(&self, key: &str) -> Result<&str, String> {
        self.settings
            .get(key)
            .map(|v| v.as_str())
            .ok_or_else(|| format!("missing required setting: {key}"))
    }

    /// Get a setting value with a default.
    pub fn setting_or(&self, key: &str, default: &str) -> String {
        self.settings
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }
}

/// TOML file structure (different from JSON — uses [connector] and [settings] sections).
#[derive(Deserialize)]
struct TomlConnector {
    connector: TomlConnectorSection,
    #[serde(default)]
    settings: HashMap<String, String>,
}

#[derive(Deserialize)]
struct TomlConnectorSection {
    name: String,
    #[serde(rename = "type")]
    connector_type: String,
    plugin: String,
    stream: String,
    #[serde(default)]
    subject_template: String,
    #[serde(default)]
    subject_filter: String,
    #[serde(default = "default_batch_size")]
    batch_size: u32,
    #[serde(default = "default_poll_interval")]
    poll_interval_ms: u64,
    #[serde(default = "default_dedup_enabled")]
    dedup_enabled: bool,
    #[serde(default)]
    dedup_key: String,
    #[serde(default = "default_dedup_window")]
    dedup_window_secs: u64,
}

impl TomlConnector {
    fn into_config(self) -> ConnectorConfig {
        ConnectorConfig {
            name: self.connector.name,
            connector_type: self.connector.connector_type,
            plugin: self.connector.plugin,
            stream: self.connector.stream,
            subject_template: self.connector.subject_template,
            subject_filter: self.connector.subject_filter,
            settings: self.settings,
            batch_size: self.connector.batch_size,
            poll_interval_ms: self.connector.poll_interval_ms,
            dedup_enabled: self.connector.dedup_enabled,
            dedup_key: self.connector.dedup_key,
            dedup_window_secs: self.connector.dedup_window_secs,
        }
    }
}

/// Replace `${VAR}` with env var value. If var not found, leave as-is.
fn resolve_env(input: &str) -> String {
    let mut result = input.to_string();
    let mut search_from = 0;
    while let Some(rel) = result[search_from..].find("${") {
        let start = search_from + rel;
        if let Some(end_rel) = result[start..].find('}') {
            let var_name = &result[start + 2..start + end_rel];
            match std::env::var(var_name) {
                Ok(val) => {
                    result = format!(
                        "{}{}{}",
                        &result[..start],
                        val,
                        &result[start + end_rel + 1..]
                    );
                    search_from = start + val.len();
                }
                Err(_) => {
                    // Leave as-is, skip past this placeholder
                    search_from = start + end_rel + 1;
                }
            }
        } else {
            break;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn json_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.json");

        let config = ConnectorConfig {
            name: "my-connector".into(),
            connector_type: "source".into(),
            plugin: "http_webhook".into(),
            stream: "events".into(),
            subject_template: "webhook.{$.type}".into(),
            subject_filter: "".into(),
            settings: HashMap::from([("path".into(), "/webhooks/test".into())]),
            batch_size: 100,
            poll_interval_ms: 50,
            dedup_enabled: true,
            dedup_key: String::new(),
            dedup_window_secs: 86400,
        };

        config.save_json(&path).unwrap();
        let loaded = ConnectorConfig::load_json(&path).unwrap();
        assert_eq!(loaded.name, "my-connector");
        assert_eq!(loaded.plugin, "http_webhook");
        assert_eq!(loaded.settings.get("path").unwrap(), "/webhooks/test");
    }

    #[test]
    fn toml_parsing() {
        let toml = r#"
[connector]
name = "pg-outbox"
type = "source"
plugin = "postgres_outbox"
stream = "domain-events"
subject_template = "{aggregate_type}.{event_type}"

[settings]
connection = "postgres://user:pass@host/db"
slot_name = "exspeed_outbox"
outbox_table = "outbox_events"
"#;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.toml");
        std::fs::write(&path, toml).unwrap();

        let config = ConnectorConfig::load_toml(&path).unwrap();
        assert_eq!(config.name, "pg-outbox");
        assert_eq!(config.connector_type, "source");
        assert_eq!(config.plugin, "postgres_outbox");
        assert_eq!(
            config.settings.get("connection").unwrap(),
            "postgres://user:pass@host/db"
        );
    }

    #[test]
    fn env_var_interpolation() {
        std::env::set_var("TEST_SECRET_XYZ", "my-secret-value");
        let mut config = ConnectorConfig {
            name: "test".into(),
            connector_type: "source".into(),
            plugin: "http_webhook".into(),
            stream: "s".into(),
            subject_template: "".into(),
            subject_filter: "".into(),
            settings: HashMap::from([("secret".into(), "${TEST_SECRET_XYZ}".into())]),
            batch_size: 100,
            poll_interval_ms: 50,
            dedup_enabled: true,
            dedup_key: String::new(),
            dedup_window_secs: 86400,
        };
        config.resolve_env_vars();
        assert_eq!(config.settings.get("secret").unwrap(), "my-secret-value");
        std::env::remove_var("TEST_SECRET_XYZ");
    }

    #[test]
    fn env_var_missing_left_as_is() {
        let mut config = ConnectorConfig {
            name: "test".into(),
            connector_type: "source".into(),
            plugin: "test".into(),
            stream: "s".into(),
            subject_template: "".into(),
            subject_filter: "".into(),
            settings: HashMap::from([("val".into(), "${NONEXISTENT_VAR_ABC}".into())]),
            batch_size: 100,
            poll_interval_ms: 50,
            dedup_enabled: true,
            dedup_key: String::new(),
            dedup_window_secs: 86400,
        };
        config.resolve_env_vars();
        assert_eq!(
            config.settings.get("val").unwrap(),
            "${NONEXISTENT_VAR_ABC}"
        );
    }

    #[test]
    fn setting_helpers() {
        let config = ConnectorConfig {
            name: "test".into(),
            connector_type: "source".into(),
            plugin: "test".into(),
            stream: "s".into(),
            subject_template: "".into(),
            subject_filter: "".into(),
            settings: HashMap::from([("url".into(), "http://example.com".into())]),
            batch_size: 100,
            poll_interval_ms: 50,
            dedup_enabled: true,
            dedup_key: String::new(),
            dedup_window_secs: 86400,
        };
        assert_eq!(config.setting("url").unwrap(), "http://example.com");
        assert!(config.setting("missing").is_err());
        assert_eq!(config.setting_or("missing", "default"), "default");
    }
}
