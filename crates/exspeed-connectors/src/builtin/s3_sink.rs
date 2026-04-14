use async_trait::async_trait;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::config::ConnectorConfig;
use crate::traits::{
    ConnectorError, HealthStatus, SinkBatch, SinkConnector, SinkRecord, WriteResult,
};

/// Flush buffer when it reaches this many records, even before the time
/// interval has elapsed.
const FLUSH_SIZE_THRESHOLD: usize = 1_000;

// ---------------------------------------------------------------------------
// Struct
// ---------------------------------------------------------------------------

pub struct S3SinkConnector {
    bucket_name: String,
    region: String,
    prefix: String,
    access_key: String,
    secret_key: String,
    endpoint: Option<String>,
    path_style: bool,
    flush_interval_secs: u64,
    buffer: Vec<String>,
    first_offset: Option<u64>,
    last_flush: Instant,
    bucket: Option<Box<s3::Bucket>>,
}

impl S3SinkConnector {
    pub fn new(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let bucket_name = config
            .setting("bucket")
            .map_err(ConnectorError::Config)?
            .to_string();

        let region = config.setting_or("region", "us-east-1");
        let prefix = config.setting_or("prefix", "");

        let access_key = config
            .setting("access_key")
            .map_err(ConnectorError::Config)?
            .to_string();

        let secret_key = config
            .setting("secret_key")
            .map_err(ConnectorError::Config)?
            .to_string();

        let endpoint_str = config.setting_or("endpoint", "");
        let endpoint = if endpoint_str.is_empty() {
            None
        } else {
            Some(endpoint_str)
        };

        let path_style = config
            .setting_or("path_style", "false")
            .eq_ignore_ascii_case("true");

        let flush_interval_secs = config
            .setting_or("flush_interval_secs", "60")
            .parse::<u64>()
            .map_err(|e| ConnectorError::Config(format!("invalid flush_interval_secs: {e}")))?;

        Ok(Self {
            bucket_name,
            region,
            prefix,
            access_key,
            secret_key,
            endpoint,
            path_style,
            flush_interval_secs,
            buffer: Vec::new(),
            first_offset: None,
            last_flush: Instant::now(),
            bucket: None,
        })
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Format a single record as one NDJSON line.
    fn format_ndjson(record: &SinkRecord) -> String {
        // Value: try UTF-8, fall back to base64.
        let value_str = match std::str::from_utf8(&record.value) {
            Ok(s) => serde_json::Value::String(s.to_string()),
            Err(_) => {
                let encoded = base64_encode(&record.value);
                serde_json::Value::String(encoded)
            }
        };

        // Key: try UTF-8, fall back to base64, None → null.
        let key_str = match &record.key {
            None => serde_json::Value::Null,
            Some(k) => match std::str::from_utf8(k) {
                Ok(s) => serde_json::Value::String(s.to_string()),
                Err(_) => serde_json::Value::String(base64_encode(k)),
            },
        };

        // Headers object.
        let headers_obj: serde_json::Map<String, serde_json::Value> = record
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();

        let mut obj = serde_json::Map::new();
        obj.insert(
            "offset".into(),
            serde_json::Value::Number(record.offset.into()),
        );
        obj.insert(
            "timestamp".into(),
            serde_json::Value::Number(record.timestamp.into()),
        );
        obj.insert(
            "subject".into(),
            serde_json::Value::String(record.subject.clone()),
        );
        obj.insert("key".into(), key_str);
        obj.insert("value".into(), value_str);
        obj.insert("headers".into(), serde_json::Value::Object(headers_obj));

        serde_json::to_string(&obj).unwrap_or_else(|_| "{}".to_string())
    }

    /// Generate the S3 object path for the current buffer.
    /// Format: `{prefix}{year}/{month}/{day}/{hour}/part-{offset:020}.ndjson`
    fn generate_path(&self, first_offset: u64) -> String {
        let (year, month, day, hour) = utc_parts();
        format!(
            "{}{:04}/{:02}/{:02}/{:02}/part-{:020}.ndjson",
            self.prefix, year, month, day, hour, first_offset
        )
    }

    /// Flush the in-memory buffer to S3. No-op if buffer is empty.
    async fn flush_buffer(&mut self) -> Result<(), ConnectorError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let bucket = match &self.bucket {
            Some(b) => b,
            None => {
                return Err(ConnectorError::Connection(
                    "S3 sink: bucket not initialised — call start() first".into(),
                ))
            }
        };

        let first_offset = self.first_offset.unwrap_or(0);
        let path = self.generate_path(first_offset);

        let mut ndjson = self.buffer.join("\n");
        ndjson.push('\n');
        let content = ndjson.into_bytes();

        match bucket
            .put_object_with_content_type(&path, &content, "application/x-ndjson")
            .await
        {
            Ok(_) => {
                info!(
                    path = %path,
                    records = self.buffer.len(),
                    "S3 sink: flushed buffer"
                );
            }
            Err(e) => {
                return Err(ConnectorError::Connection(format!(
                    "S3 sink: put_object failed for path '{path}': {e}"
                )));
            }
        }

        self.buffer.clear();
        self.first_offset = None;
        self.last_flush = Instant::now();
        Ok(())
    }

    fn should_flush(&self) -> bool {
        if self.buffer.len() >= FLUSH_SIZE_THRESHOLD {
            return true;
        }
        self.last_flush.elapsed().as_secs() >= self.flush_interval_secs
    }
}

// ---------------------------------------------------------------------------
// SinkConnector impl
// ---------------------------------------------------------------------------

#[async_trait]
impl SinkConnector for S3SinkConnector {
    async fn start(&mut self) -> Result<(), ConnectorError> {
        let credentials = s3::creds::Credentials::new(
            Some(&self.access_key),
            Some(&self.secret_key),
            None,
            None,
            None,
        )
        .map_err(|e| ConnectorError::Config(format!("S3 sink: invalid credentials: {e}")))?;

        let region: s3::Region = match &self.endpoint {
            Some(ep) => s3::Region::Custom {
                region: self.region.clone(),
                endpoint: ep.clone(),
            },
            None => self.region.parse().map_err(|e| {
                ConnectorError::Config(format!("S3 sink: invalid region '{}': {e}", self.region))
            })?,
        };

        let mut bucket = s3::Bucket::new(&self.bucket_name, region, credentials).map_err(|e| {
            ConnectorError::Connection(format!("S3 sink: failed to create bucket handle: {e}"))
        })?;

        if self.path_style {
            bucket = bucket.with_path_style();
        }

        info!(
            bucket = %self.bucket_name,
            region = %self.region,
            prefix = %self.prefix,
            path_style = self.path_style,
            "S3 sink connected"
        );

        self.bucket = Some(bucket);
        Ok(())
    }

    async fn write(&mut self, batch: SinkBatch) -> Result<WriteResult, ConnectorError> {
        for record in &batch.records {
            if self.first_offset.is_none() {
                self.first_offset = Some(record.offset);
            }
            let line = Self::format_ndjson(record);
            self.buffer.push(line);
        }

        if self.should_flush() {
            self.flush_buffer().await?;
        }

        Ok(WriteResult::AllSuccess)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.flush_buffer().await
    }

    async fn stop(&mut self) -> Result<(), ConnectorError> {
        if let Err(e) = self.flush_buffer().await {
            warn!("S3 sink: error flushing on stop: {e}");
        }
        self.bucket = None;
        Ok(())
    }

    async fn health(&self) -> HealthStatus {
        if self.bucket.is_some() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy("S3 sink: bucket not initialised".into())
        }
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
    let mut chunks = data.chunks_exact(3);
    for chunk in chunks.by_ref() {
        let n = ((chunk[0] as u32) << 16) | ((chunk[1] as u32) << 8) | (chunk[2] as u32);
        out.push(ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 63) as usize] as char);
        out.push(ALPHABET[((n >> 6) & 63) as usize] as char);
        out.push(ALPHABET[(n & 63) as usize] as char);
    }
    let rem = chunks.remainder();
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

/// Decompose the current UTC wall-clock time into (year, month, day, hour).
/// Uses `SystemTime` so we don't need an extra chrono dependency.
fn utc_parts() -> (u32, u32, u32, u32) {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Days since epoch.
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hour = (time_of_day / 3600) as u32;

    // Gregorian calendar computation from days-since-epoch (Jan 1 1970).
    // Using the civil-date algorithm from Howard Hinnant (public domain).
    let z = days as i64 + 719_468;
    let era: i64 = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // day of era
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    (y as u32, m as u32, d as u32, hour)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_record(offset: u64, subject: &str, key: Option<&str>, value: &[u8]) -> SinkRecord {
        SinkRecord {
            offset,
            timestamp: 1_713_100_800,
            subject: subject.to_string(),
            key: key.map(|k| Bytes::from(k.to_string())),
            value: Bytes::from(value.to_vec()),
            headers: vec![("x-source".to_string(), "test".to_string())],
        }
    }

    #[test]
    fn ndjson_line_utf8_value() {
        let record = make_record(42, "order.created", Some("ord-1"), b"hello world");
        let line = S3SinkConnector::format_ndjson(&record);

        let parsed: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(parsed["offset"], 42);
        assert_eq!(parsed["timestamp"], 1_713_100_800_u64);
        assert_eq!(parsed["subject"], "order.created");
        assert_eq!(parsed["key"], "ord-1");
        assert_eq!(parsed["value"], "hello world");
        assert_eq!(parsed["headers"]["x-source"], "test");
    }

    #[test]
    fn ndjson_line_binary_value_base64() {
        // Non-UTF-8 byte sequence → should be base64-encoded.
        let record = make_record(7, "blob.stored", None, &[0xFF, 0x00, 0xAB]);
        let line = S3SinkConnector::format_ndjson(&record);

        let parsed: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(parsed["offset"], 7);
        assert_eq!(parsed["key"], serde_json::Value::Null);
        // base64 of [0xFF, 0x00, 0xAB] = "/wCr"
        assert_eq!(parsed["value"], "/wCr");
    }

    #[test]
    fn path_generation_format() {
        // Build a minimal connector just to exercise generate_path.
        let connector = S3SinkConnector {
            bucket_name: "my-bucket".into(),
            region: "us-east-1".into(),
            prefix: "data/".into(),
            access_key: "key".into(),
            secret_key: "secret".into(),
            endpoint: None,
            path_style: false,
            flush_interval_secs: 60,
            buffer: Vec::new(),
            first_offset: None,
            last_flush: Instant::now(),
            bucket: None,
        };

        let path = connector.generate_path(12345);

        assert!(path.starts_with("data/"), "path prefix: {path}");
        assert!(
            path.ends_with("/part-00000000000000012345.ndjson"),
            "path suffix: {path}"
        );

        let parts: Vec<&str> = path.split('/').collect();
        // parts: ["data", "YYYY", "MM", "DD", "HH", "part-....ndjson"]
        assert_eq!(parts.len(), 6, "expected 6 path segments: {path}");

        let year: u32 = parts[1].parse().expect("year must be numeric");
        let month: u32 = parts[2].parse().expect("month must be numeric");
        let day: u32 = parts[3].parse().expect("day must be numeric");
        let hour: u32 = parts[4].parse().expect("hour must be numeric");

        assert!(year >= 2024, "year sanity: {year}");
        assert!((1..=12).contains(&month), "month range: {month}");
        assert!((1..=31).contains(&day), "day range: {day}");
        assert!(hour < 24, "hour range: {hour}");
    }
}
