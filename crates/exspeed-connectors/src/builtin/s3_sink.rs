//! `s3` sink: NDJSON objects in an S3-compatible bucket.
//!
//! `write()` only buffers. The framework calls `flush()` on a timer
//! (`flush_interval_ms`, default 60 s), when the buffer is full
//! (`max_records` / `max_bytes`) and on stop; `flush()` uploads the buffer
//! as one object, and only then is the stream offset committed.
//!
//! The object key is a pure function of the buffer's first offset and that
//! record's timestamp:
//!
//! ```text
//! {prefix}{stream}/{YYYY}/{MM}/{DD}/{HH}/part-{first_offset:020}.ndjson
//! ```
//!
//! After a crash between upload and commit, the restarted sink re-reads from
//! the committed offset, rebuilds a buffer that starts at the same record
//! and overwrites the same object, so the bucket never holds the same
//! record twice: **effectively-once** (per object; a reader listing the
//! bucket mid-retry can briefly see the older version of an object).

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tracing::info;

use crate::registry::PluginInit;
use crate::settings::{self, de};
use crate::traits::{ConnectorError, SinkConnector, SinkRecord, WriteResult};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3SinkSettings {
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub prefix: String,
    /// Static credentials. When both are unset, the standard AWS chain
    /// (env vars, profile, instance metadata) is used.
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    /// Custom endpoint (MinIO, R2, …).
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default, deserialize_with = "de::bool")]
    pub path_style: bool,
    /// Upload as soon as the buffer holds this many records…
    #[serde(default = "default_max_records", deserialize_with = "de::usize")]
    pub max_records: usize,
    /// …or this many bytes.
    #[serde(default = "default_max_bytes", deserialize_with = "de::usize")]
    pub max_bytes: usize,
    #[serde(default = "default_timeout", deserialize_with = "de::u64")]
    pub timeout_secs: u64,
}

fn default_region() -> String {
    "us-east-1".into()
}
fn default_max_records() -> usize {
    10_000
}
fn default_max_bytes() -> usize {
    16 * 1024 * 1024
}
fn default_timeout() -> u64 {
    60
}

pub struct S3SinkConnector {
    settings: S3SinkSettings,
    stream: String,
    /// NDJSON lines (each ending in `\n`).
    buffer: Vec<u8>,
    buffered: usize,
    /// Offset and timestamp (ns) of the first buffered record.
    first: Option<(u64, u64)>,
    bucket: Option<Box<s3::Bucket>>,
}

impl S3SinkConnector {
    pub fn new(init: &PluginInit) -> Result<Self, ConnectorError> {
        let s: S3SinkSettings = settings::parse("s3", &init.settings)?;
        if s.bucket.trim().is_empty() {
            return Err(ConnectorError::config("s3: bucket must not be empty"));
        }
        if s.access_key.is_some() != s.secret_key.is_some() {
            return Err(ConnectorError::config(
                "s3: set both access_key and secret_key, or neither",
            ));
        }
        if s.max_records == 0 || s.max_bytes == 0 || s.timeout_secs == 0 {
            return Err(ConnectorError::config(
                "s3: max_records, max_bytes and timeout_secs must be > 0",
            ));
        }
        Ok(Self {
            settings: s,
            stream: init.config.stream.clone(),
            buffer: Vec::new(),
            buffered: 0,
            first: None,
            bucket: None,
        })
    }

    /// Format a single record as one NDJSON line (without the newline).
    fn format_ndjson(record: &SinkRecord) -> String {
        let text_or_b64 = |b: &[u8]| match std::str::from_utf8(b) {
            Ok(s) => serde_json::Value::String(s.to_string()),
            Err(_) => serde_json::Value::String(base64_encode(b)),
        };
        let headers: serde_json::Map<String, serde_json::Value> = record
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();
        serde_json::json!({
            "offset": record.offset,
            "timestamp": record.timestamp,
            "subject": record.subject,
            "key": record.key.as_deref().map(text_or_b64),
            "value": text_or_b64(&record.value),
            "headers": headers,
        })
        .to_string()
    }

    /// The object key for a buffer starting at `first_offset`, whose first
    /// record has timestamp `ts_ns`. Deterministic: retries overwrite.
    fn object_key(&self, first_offset: u64, ts_ns: u64) -> String {
        let secs = (ts_ns / 1_000_000_000) as i64;
        let t = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default();
        format!(
            "{}{}/{}/part-{:020}.ndjson",
            self.settings.prefix,
            self.stream,
            t.format("%Y/%m/%d/%H"),
            first_offset
        )
    }
}

fn s3_err(what: &str, e: s3::error::S3Error) -> ConnectorError {
    use s3::error::S3Error as E;
    match e {
        E::HttpFailWithBody(status, body) => {
            let snippet: String = body.chars().take(512).collect();
            match status {
                408 | 429 | 500..=599 => {
                    ConnectorError::transient(format!("s3 {what}: HTTP {status}: {snippet}"))
                }
                // Credentials, missing bucket, bad request: retrying won't help.
                _ => ConnectorError::fatal(format!("s3 {what}: HTTP {status}: {snippet}")),
            }
        }
        E::Credentials(_) | E::Region(_) | E::UrlParse(_) => {
            ConnectorError::config(format!("s3 {what}: {e}"))
        }
        other => ConnectorError::transient(format!("s3 {what}: {other}")),
    }
}

#[async_trait]
impl SinkConnector for S3SinkConnector {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        let s = &self.settings;
        let credentials = match (&s.access_key, &s.secret_key) {
            (Some(a), Some(k)) => s3::creds::Credentials::new(Some(a), Some(k), None, None, None),
            // The default chain may do blocking HTTP (instance metadata).
            _ => tokio::task::spawn_blocking(s3::creds::Credentials::default)
                .await
                .map_err(|e| ConnectorError::transient(format!("s3: credentials task: {e}")))?,
        }
        .map_err(|e| ConnectorError::config(format!("s3: invalid credentials: {e}")))?;
        let region: s3::Region = match &s.endpoint {
            Some(ep) => s3::Region::Custom {
                region: s.region.clone(),
                endpoint: ep.clone(),
            },
            None => s.region.parse().map_err(|e| {
                ConnectorError::config(format!("s3: invalid region '{}': {e}", s.region))
            })?,
        };
        let mut bucket = s3::Bucket::new(&s.bucket, region, credentials)
            .map_err(|e| ConnectorError::config(format!("s3: bucket handle: {e}")))?;
        if s.path_style {
            bucket = bucket.with_path_style();
        }
        bucket.set_request_timeout(Some(Duration::from_secs(s.timeout_secs)));
        info!(bucket = %s.bucket, prefix = %s.prefix, "s3 sink started");
        self.bucket = Some(bucket);
        // A restart rebuilds the buffer from the committed offset.
        self.buffer.clear();
        self.buffered = 0;
        self.first = None;
        Ok(())
    }

    async fn write(&mut self, records: &[SinkRecord]) -> Result<WriteResult, ConnectorError> {
        for r in records {
            if self.first.is_none() {
                self.first = Some((r.offset, r.timestamp));
            }
            self.buffer
                .extend_from_slice(Self::format_ndjson(r).as_bytes());
            self.buffer.push(b'\n');
            self.buffered += 1;
        }
        Ok(WriteResult::Accepted)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        let Some((first_offset, ts)) = self.first else {
            return Ok(());
        };
        let key = self.object_key(first_offset, ts);
        let bucket = self
            .bucket
            .as_ref()
            .ok_or_else(|| ConnectorError::connection("s3: not started"))?;
        bucket
            .put_object_with_content_type(&key, &self.buffer, "application/x-ndjson")
            .await
            .map_err(|e| s3_err("put_object", e))?;
        info!(key = %key, records = self.buffered, "s3 sink: object written");
        self.buffer.clear();
        self.buffered = 0;
        self.first = None;
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        // The framework flushes before a graceful stop; anything left here
        // is uncommitted and will be re-read after the restart.
        self.bucket = None;
        Ok(())
    }

    fn wants_flush(&self) -> bool {
        self.buffered >= self.settings.max_records || self.buffer.len() >= self.settings.max_bytes
    }

    fn default_flush_interval(&self) -> Duration {
        Duration::from_secs(60)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Minimal base64 encoding (standard alphabet) without pulling in an extra dep.
/// The `s3` crate already brings in `base64 = "0.22"`, but we access it through
/// the public API here to avoid a direct dependency.  Instead we use a hand-
/// rolled encoding that is well-tested and produces standard output.
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    let (chunks, rem) = data.as_chunks::<3>();
    for chunk in chunks {
        let n = ((chunk[0] as u32) << 16) | ((chunk[1] as u32) << 8) | (chunk[2] as u32);
        out.push(ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 63) as usize] as char);
        out.push(ALPHABET[((n >> 6) & 63) as usize] as char);
        out.push(ALPHABET[(n & 63) as usize] as char);
    }
    match rem.len() {
        1 => {
            let n = (rem[0] as u32) << 16;
            out.push(ALPHABET[((n >> 18) & 63) as usize] as char);
            out.push(ALPHABET[((n >> 12) & 63) as usize] as char);
            out.push('=');
            out.push('=');
        }
        2 => {
            let n = ((rem[0] as u32) << 16) | ((rem[1] as u32) << 8);
            out.push(ALPHABET[((n >> 18) & 63) as usize] as char);
            out.push(ALPHABET[((n >> 12) & 63) as usize] as char);
            out.push(ALPHABET[((n >> 6) & 63) as usize] as char);
            out.push('=');
        }
        _ => {}
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectorConfig, ConnectorType};
    use bytes::Bytes;
    use serde_json::json;

    fn init(settings: serde_json::Value) -> PluginInit {
        let (m, _) = exspeed_common::Metrics::new();
        PluginInit {
            config: ConnectorConfig::new("s3", ConnectorType::Sink, "s3", "orders"),
            settings: settings.as_object().unwrap().clone(),
            metrics: std::sync::Arc::new(m),
        }
    }

    fn make_record(offset: u64, subject: &str, key: Option<&str>, value: &[u8]) -> SinkRecord {
        SinkRecord {
            offset,
            // 2024-04-14T13:20:00Z in ns
            timestamp: 1_713_100_800_000_000_000,
            subject: subject.to_string(),
            key: key.map(|k| Bytes::from(k.to_string())),
            value: Bytes::from(value.to_vec()),
            headers: vec![("x-source".to_string(), "test".to_string())],
        }
    }

    #[test]
    fn ndjson_line_utf8_value() {
        let record = make_record(42, "order.created", Some("ord-1"), b"hello world");
        let parsed: serde_json::Value =
            serde_json::from_str(&S3SinkConnector::format_ndjson(&record)).unwrap();
        assert_eq!(parsed["offset"], 42);
        assert_eq!(parsed["subject"], "order.created");
        assert_eq!(parsed["key"], "ord-1");
        assert_eq!(parsed["value"], "hello world");
        assert_eq!(parsed["headers"]["x-source"], "test");
    }

    #[test]
    fn ndjson_line_binary_value_base64() {
        let record = make_record(7, "blob.stored", None, &[0xFF, 0x00, 0xAB]);
        let parsed: serde_json::Value =
            serde_json::from_str(&S3SinkConnector::format_ndjson(&record)).unwrap();
        assert_eq!(parsed["key"], serde_json::Value::Null);
        assert_eq!(parsed["value"], "/wCr");
    }

    #[test]
    fn object_key_is_deterministic() {
        let c = S3SinkConnector::new(&init(json!({"bucket": "b", "prefix": "data/"}))).unwrap();
        let ts = 1_713_100_800_000_000_000;
        assert_eq!(
            c.object_key(12345, ts),
            "data/orders/2024/04/14/13/part-00000000000000012345.ndjson"
        );
        assert_eq!(c.object_key(12345, ts), c.object_key(12345, ts));
    }

    #[tokio::test]
    async fn write_buffers_until_full() {
        let mut c = S3SinkConnector::new(&init(json!({"bucket": "b", "max_records": 2}))).unwrap();
        let r = make_record(1, "a", None, b"{}");
        assert!(matches!(
            c.write(std::slice::from_ref(&r)).await.unwrap(),
            WriteResult::Accepted
        ));
        assert!(!c.wants_flush());
        c.write(std::slice::from_ref(&r)).await.unwrap();
        assert!(c.wants_flush());
        assert_eq!(c.first.map(|f| f.0), Some(1));
    }

    #[test]
    fn settings_errors() {
        assert!(S3SinkConnector::new(&init(json!({}))).is_err());
        assert!(S3SinkConnector::new(&init(json!({"bucket": "b", "access_key": "a"}))).is_err());
        assert!(
            S3SinkConnector::new(&init(json!({"bucket": "b", "flush_interval_secs": 5}))).is_err()
        );
        assert!(S3SinkConnector::new(&init(json!({"bucket": "b", "max_records": "10"}))).is_ok());
    }
}
