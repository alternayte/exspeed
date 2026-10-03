//! Connector configuration: the TOML file format, the JSON API format, name
//! validation and `${VAR}` substitution.
//!
//! ```toml
//! [connector]
//! name = "orders-cdc"
//! type = "source"
//! plugin = "postgres_cdc"
//! stream = "orders"
//!
//! [settings]            # plugin-specific, typed (numbers, bools, arrays, tables)
//! connection = "${DATABASE_URL}"
//! tables = ["public.orders"]
//!
//! [retry]               # in-place retries of transient errors
//! [restart]             # supervisor restart backoff
//! [transform]
//! sql = "SELECT ..."
//! ```
//!
//! The JSON accepted by `POST /api/v1/connectors` is the same document with
//! the `[connector]` keys at the top level and `transform_sql` instead of a
//! `[transform]` table. Unknown keys are rejected in both formats.

use std::fs;
use std::io::{self, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::retry::{RestartPolicy, RetryPolicy};
use crate::traits::ConnectorError;

/// Plugin settings: a JSON object. TOML files are converted to JSON on load.
pub type Settings = serde_json::Map<String, serde_json::Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorType {
    Source,
    Sink,
}

impl ConnectorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConnectorType::Source => "source",
            ConnectorType::Sink => "sink",
        }
    }
}

impl std::fmt::Display for ConnectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What happens when `[retry]` is exhausted on a transient error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnTransientExhausted {
    /// Hand the error to the supervisor, which restarts the connector with
    /// bounded exponential backoff (status `backoff`), forever.
    #[default]
    LoopForever,
    /// Move the connector to `failed`. It stays there until restarted
    /// through the API or its config changes.
    #[serde(alias = "halt")]
    Fail,
    /// Sinks only: write the batch to `dlq_stream` and move on. Falls back
    /// to `fail` without a `dlq_stream`.
    DlqBatch,
}

/// A connector definition (JSON form).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConnectorConfig {
    pub name: String,
    #[serde(rename = "type", alias = "connector_type")]
    pub connector_type: ConnectorType,
    pub plugin: String,
    /// Stream written by a source, read by a sink.
    pub stream: String,
    /// Sources: subject for produced records. `{var}` placeholders are
    /// plugin variables, `{$.field}` reads the record's JSON value.
    #[serde(default)]
    pub subject_template: String,
    /// Sinks: only deliver records whose subject matches (NATS wildcards).
    #[serde(default)]
    pub subject_filter: String,
    /// Sources: JSON field of the value to use as the record key when the
    /// plugin doesn't set one.
    #[serde(default)]
    pub key_field: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    /// Sleep between polls when there is nothing to do.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,
    /// Sinks: flush + commit at most this often. Default: plugin-specific
    /// (0 = after every batch for unbuffered sinks, 60 s for S3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_interval_ms: Option<u64>,
    /// Stream for poison records. Unset = drop them and count a metric.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dlq_stream: Option<String>,
    #[serde(default)]
    pub on_transient_exhausted: OnTransientExhausted,
    #[serde(default)]
    pub retry: RetryPolicy,
    #[serde(default)]
    pub restart: RestartPolicy,
    /// Sources: ExQL projection/filter applied before append.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub transform_sql: String,
    #[serde(default)]
    pub settings: Settings,
}

pub(crate) fn default_batch_size() -> u32 {
    100
}
pub(crate) fn default_poll_interval() -> u64 {
    50
}

impl ConnectorConfig {
    /// Minimal config, handy for tests and programmatic use.
    pub fn new(
        name: impl Into<String>,
        connector_type: ConnectorType,
        plugin: impl Into<String>,
        stream: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            connector_type,
            plugin: plugin.into(),
            stream: stream.into(),
            subject_template: String::new(),
            subject_filter: String::new(),
            key_field: String::new(),
            batch_size: default_batch_size(),
            poll_interval_ms: default_poll_interval(),
            flush_interval_ms: None,
            dlq_stream: None,
            on_transient_exhausted: OnTransientExhausted::default(),
            retry: RetryPolicy::default(),
            restart: RestartPolicy::default(),
            transform_sql: String::new(),
            settings: Settings::new(),
        }
    }

    pub fn with_setting(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
        self.settings.insert(key.to_string(), value.into());
        self
    }

    /// Load from a JSON file (API-created connectors).
    pub fn load_json(path: &Path) -> io::Result<Self> {
        let json = fs::read_to_string(path)?;
        serde_json::from_str(&json).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Save as JSON atomically (tmp + fsync + rename + dir fsync).
    pub fn save_json(&self, path: &Path) -> io::Result<()> {
        let json = serde_json::to_vec_pretty(self).map_err(io::Error::other)?;
        write_atomic(path, &json)
    }

    /// Load from a TOML file. Settings are kept unresolved (`${VAR}` stays
    /// as written); see [`ConnectorConfig::resolved_settings`].
    pub fn load_toml(path: &Path) -> io::Result<Self> {
        let text = fs::read_to_string(path)?;
        Self::from_toml_str(&text).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub fn from_toml_str(text: &str) -> Result<Self, String> {
        let file: TomlConnector = toml::from_str(text).map_err(|e| e.to_string())?;
        file.into_config()
    }

    /// Settings with `${VAR}` / `${VAR:-default}` substituted from the
    /// process environment. The result is used to build the plugin and is
    /// never persisted.
    pub fn resolved_settings(&self) -> Result<Settings, ConnectorError> {
        let mut out = Settings::new();
        for (k, v) in &self.settings {
            out.insert(k.clone(), resolve_value(v).map_err(ConnectorError::Fatal)?);
        }
        Ok(out)
    }

    /// Check everything that doesn't depend on the plugin.
    pub fn validate_common(&self) -> Result<(), String> {
        validate_name(&self.name)?;
        exspeed_common::StreamName::try_from(self.stream.as_str())
            .map_err(|e| format!("invalid stream name '{}': {e}", self.stream))?;
        if let Some(dlq) = &self.dlq_stream {
            exspeed_common::StreamName::try_from(dlq.as_str())
                .map_err(|e| format!("invalid dlq_stream '{dlq}': {e}"))?;
            if dlq == &self.stream {
                return Err("dlq_stream must differ from stream".into());
            }
        }
        if self.batch_size == 0 {
            return Err("batch_size must be at least 1".into());
        }
        exspeed_common::SubjectFilter::parse(&self.subject_filter)
            .map_err(|e| format!("invalid subject_filter: {e}"))?;
        if !self.transform_sql.is_empty() {
            if self.connector_type == ConnectorType::Sink {
                return Err("transforms are only supported on sources".into());
            }
            crate::transform::Transform::compile(&self.transform_sql)
                .map_err(|e| format!("transform SQL error: {e}"))?;
        }
        Ok(())
    }
}

/// Connector names become file names, Postgres identifiers and metric
/// labels: 1–100 chars of `[A-Za-z0-9_-]`, starting with a letter or digit.
pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("connector name cannot be empty".into());
    }
    if name.len() > 100 {
        return Err("connector name is longer than 100 characters".into());
    }
    let first = name.chars().next().unwrap();
    if !first.is_ascii_alphanumeric() {
        return Err(format!(
            "invalid connector name '{name}': must start with a letter or digit"
        ));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
    {
        return Err(format!(
            "invalid connector name '{name}': character '{bad}' is not allowed \
             (use letters, digits, '_' and '-')"
        ));
    }
    Ok(())
}

/// The form of a name used for external identifiers (replication slots,
/// publications): lowercase, `-` → `_`. Two connectors whose sanitised
/// names are equal are rejected.
pub fn sanitized_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Write `bytes` to `path` atomically: tmp file, fsync, rename, fsync dir.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(dir)?;
    let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
    let tmp = dir.join(format!(".{file_name}.tmp-{}", std::process::id()));
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// TOML file structure
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlConnector {
    connector: TomlConnectorSection,
    #[serde(default)]
    settings: toml::Table,
    #[serde(default)]
    transform: Option<TomlTransform>,
    #[serde(default)]
    retry: RetryPolicy,
    #[serde(default)]
    restart: RestartPolicy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlTransform {
    sql: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlConnectorSection {
    name: String,
    #[serde(rename = "type")]
    connector_type: ConnectorType,
    plugin: String,
    stream: String,
    #[serde(default)]
    subject_template: String,
    #[serde(default)]
    subject_filter: String,
    #[serde(default)]
    key_field: String,
    #[serde(default = "default_batch_size")]
    batch_size: u32,
    #[serde(default = "default_poll_interval")]
    poll_interval_ms: u64,
    #[serde(default)]
    flush_interval_ms: Option<u64>,
    #[serde(default)]
    dlq_stream: Option<String>,
    #[serde(default)]
    on_transient_exhausted: OnTransientExhausted,
}

impl TomlConnector {
    fn into_config(self) -> Result<ConnectorConfig, String> {
        let mut settings = Settings::new();
        for (k, v) in self.settings {
            settings.insert(k, toml_to_json(v));
        }
        let c = self.connector;
        Ok(ConnectorConfig {
            name: c.name,
            connector_type: c.connector_type,
            plugin: c.plugin,
            stream: c.stream,
            subject_template: c.subject_template,
            subject_filter: c.subject_filter,
            key_field: c.key_field,
            batch_size: c.batch_size,
            poll_interval_ms: c.poll_interval_ms,
            flush_interval_ms: c.flush_interval_ms,
            dlq_stream: c.dlq_stream.filter(|s| !s.trim().is_empty()),
            on_transient_exhausted: c.on_transient_exhausted,
            retry: self.retry,
            restart: self.restart,
            transform_sql: self.transform.map(|t| t.sql).unwrap_or_default(),
            settings,
        })
    }
}

/// Convert TOML to JSON; datetimes become RFC 3339 strings.
fn toml_to_json(v: toml::Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        toml::Value::String(s) => J::String(s),
        toml::Value::Integer(i) => J::from(i),
        toml::Value::Float(f) => serde_json::Number::from_f64(f)
            .map(J::Number)
            .unwrap_or(J::Null),
        toml::Value::Boolean(b) => J::Bool(b),
        toml::Value::Datetime(d) => J::String(d.to_string()),
        toml::Value::Array(a) => J::Array(a.into_iter().map(toml_to_json).collect()),
        toml::Value::Table(t) => {
            J::Object(t.into_iter().map(|(k, v)| (k, toml_to_json(v))).collect())
        }
    }
}

// ---------------------------------------------------------------------------
// ${VAR} substitution
// ---------------------------------------------------------------------------

fn resolve_value(v: &serde_json::Value) -> Result<serde_json::Value, String> {
    use serde_json::Value as J;
    Ok(match v {
        J::String(s) => J::String(resolve_env(s)?),
        J::Array(a) => J::Array(a.iter().map(resolve_value).collect::<Result<_, _>>()?),
        J::Object(o) => J::Object(
            o.iter()
                .map(|(k, v)| Ok((k.clone(), resolve_value(v)?)))
                .collect::<Result<_, String>>()?,
        ),
        other => other.clone(),
    })
}

/// Replace `${VAR}` or `${VAR:-default}` with the env var's value.
/// An unset variable without a default is an error.
pub fn resolve_env(input: &str) -> Result<String, String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            out.push_str(&rest[start..]);
            return Ok(out);
        };
        let inner = &after[..end];
        let (var, default) = match inner.find(":-") {
            Some(sep) => (&inner[..sep], Some(&inner[sep + 2..])),
            None => (inner, None),
        };
        match (std::env::var(var), default) {
            (Ok(val), _) => out.push_str(&val),
            (Err(_), Some(d)) => out.push_str(d),
            (Err(_), None) => {
                return Err(format!("environment variable '{var}' is not set"));
            }
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn json_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.json");
        let config = ConnectorConfig::new(
            "my-connector",
            ConnectorType::Source,
            "http_webhook",
            "events",
        )
        .with_setting("path", "test");
        config.save_json(&path).unwrap();
        let loaded = ConnectorConfig::load_json(&path).unwrap();
        assert_eq!(loaded, config);
    }

    /// A sink's `subject_filter` is validated with the same parser the
    /// consumers use (the legacy matcher silently read `a.>.c` as `a.>`).
    #[test]
    fn subject_filter_is_validated() {
        let mut c = ConnectorConfig::new("s", ConnectorType::Sink, "jdbc", "events");
        for ok in ["", "orders.>", "orders.*.created"] {
            c.subject_filter = ok.into();
            c.validate_common().unwrap();
        }
        for bad in ["a.>.c", "a..b", "a.b*", "a b"] {
            c.subject_filter = bad.into();
            let err = c.validate_common().unwrap_err();
            assert!(err.contains("subject_filter"), "{bad}: {err}");
        }
    }

    #[test]
    fn json_accepts_connector_type_alias_and_rejects_unknown() {
        let ok: ConnectorConfig = serde_json::from_str(
            r#"{"name":"a","connector_type":"sink","plugin":"jdbc","stream":"s"}"#,
        )
        .unwrap();
        assert_eq!(ok.connector_type, ConnectorType::Sink);
        let err = serde_json::from_str::<ConnectorConfig>(
            r#"{"name":"a","type":"sink","plugin":"jdbc","stream":"s","bacth_size":1}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("bacth_size"), "{err}");
    }

    #[test]
    fn toml_native_types() {
        let toml = r#"
[connector]
name = "poller"
type = "source"
plugin = "http_poll"
stream = "s"
dlq_stream = "s-dlq"
on_transient_exhausted = "halt"

[settings]
url = "http://x"
interval_secs = 60
enabled = true
headers = { Authorization = "Bearer x" }
tables = ["a", "b"]

[retry]
max_retries = 7

[restart]
max_backoff_ms = 5000

[transform]
sql = "SELECT payload->>'a' AS a"
"#;
        let c = ConnectorConfig::from_toml_str(toml).unwrap();
        assert_eq!(c.settings["interval_secs"], serde_json::json!(60));
        assert_eq!(c.settings["enabled"], serde_json::json!(true));
        assert_eq!(c.settings["headers"]["Authorization"], "Bearer x");
        assert_eq!(c.settings["tables"], serde_json::json!(["a", "b"]));
        assert_eq!(c.dlq_stream.as_deref(), Some("s-dlq"));
        assert_eq!(c.on_transient_exhausted, OnTransientExhausted::Fail);
        assert_eq!(c.retry.max_retries, 7);
        assert_eq!(c.restart.max_backoff_ms, 5000);
        assert!(c.transform_sql.contains("payload"));
    }

    #[test]
    fn toml_rejects_unknown_connector_keys() {
        let toml = r#"
[connector]
name = "x"
type = "sink"
plugin = "jdbc"
stream = "s"
dedup_enabled = true
"#;
        let err = ConnectorConfig::from_toml_str(toml).unwrap_err();
        assert!(err.contains("dedup_enabled"), "{err}");
    }

    #[test]
    fn env_substitution() {
        std::env::set_var("EXSPEED_CFG_TEST_A", "secret");
        std::env::remove_var("EXSPEED_CFG_TEST_UNSET");
        assert_eq!(
            resolve_env("x-${EXSPEED_CFG_TEST_A}-y").unwrap(),
            "x-secret-y"
        );
        assert_eq!(
            resolve_env("${EXSPEED_CFG_TEST_UNSET:-fallback}").unwrap(),
            "fallback"
        );
        assert_eq!(resolve_env("${EXSPEED_CFG_TEST_UNSET:-}").unwrap(), "");
        assert!(resolve_env("${EXSPEED_CFG_TEST_UNSET}").is_err());
        assert_eq!(resolve_env("no vars").unwrap(), "no vars");

        let c = ConnectorConfig::new("a", ConnectorType::Sink, "x", "s")
            .with_setting("nested", serde_json::json!({"h": "${EXSPEED_CFG_TEST_A}"}));
        let r = c.resolved_settings().unwrap();
        assert_eq!(r["nested"]["h"], "secret");
        // The config itself keeps the reference.
        assert_eq!(c.settings["nested"]["h"], "${EXSPEED_CFG_TEST_A}");
        std::env::remove_var("EXSPEED_CFG_TEST_A");
    }

    #[test]
    fn names() {
        assert!(validate_name("orders-cdc_1").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("../x").is_err());
        assert!(validate_name("a/b").is_err());
        assert!(validate_name("a.b").is_err());
        assert!(validate_name("-a").is_err());
        assert!(validate_name(&"a".repeat(101)).is_err());
        assert_eq!(sanitized_name("Orders-CDC"), "orders_cdc");
    }

    #[test]
    fn atomic_write_replaces() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("x.json");
        write_atomic(&p, b"one").unwrap();
        write_atomic(&p, b"two").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"two");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(leftovers.len(), 1);
    }
}
