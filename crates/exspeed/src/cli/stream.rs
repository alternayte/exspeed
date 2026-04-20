use anyhow::{anyhow, Result};
use serde_json::json;

use crate::cli::client::CliClient;
use crate::cli::format;

/// Create a new stream via the HTTP API.
///
/// `retention` is a human duration like "7d", "24h", "30m".
/// `max_size` is a human size like "10gb", "256mb".
/// `dedup_window` is an optional human duration like "10m", "1h".
/// `dedup_max_entries` is an optional count like "2M", "500k".
pub async fn create(
    client: &CliClient,
    name: &str,
    retention: &str,
    max_size: &str,
    dedup_window: Option<&str>,
    dedup_max_entries: Option<&str>,
) -> Result<()> {
    let retention_secs = parse_duration_secs(retention)?;
    let max_bytes = parse_size_bytes(max_size)?;

    let mut body = json!({
        "name": name,
        "max_age_secs": retention_secs,
        "max_bytes": max_bytes,
    });

    if let Some(w) = dedup_window {
        body["dedup_window_secs"] = json!(parse_duration_secs(w)?);
    }
    if let Some(e) = dedup_max_entries {
        body["dedup_max_entries"] = json!(parse_count_suffix(e)?);
    }

    let (status, resp) = client.post("/api/v1/streams", &body).await?;

    if (200..300).contains(&status) {
        println!("Stream '{}' created", name);
    } else {
        let msg = resp["error"]
            .as_str()
            .unwrap_or("unknown error")
            .to_string();
        return Err(anyhow!("failed to create stream: {msg}"));
    }

    Ok(())
}

/// Update an existing stream's config via PATCH.
///
/// Only the provided fields are updated; unset fields are left unchanged.
pub async fn update(
    client: &CliClient,
    name: &str,
    retention: Option<&str>,
    max_size: Option<&str>,
    dedup_window: Option<&str>,
    dedup_max_entries: Option<&str>,
) -> Result<()> {
    let mut body = serde_json::Map::new();

    if let Some(r) = retention {
        body.insert("max_age_secs".into(), json!(parse_duration_secs(r)?));
    }
    if let Some(s) = max_size {
        body.insert("max_bytes".into(), json!(parse_size_bytes(s)?));
    }
    if let Some(w) = dedup_window {
        body.insert("dedup_window_secs".into(), json!(parse_duration_secs(w)?));
    }
    if let Some(e) = dedup_max_entries {
        body.insert("dedup_max_entries".into(), json!(parse_count_suffix(e)?));
    }

    let body_val = serde_json::Value::Object(body);
    let path = format!("/api/v1/streams/{name}");
    let (status, resp) = client.patch(&path, &body_val).await?;

    if (200..300).contains(&status) {
        println!("Stream '{}' updated", name);
    } else {
        let msg = resp["error"]
            .as_str()
            .unwrap_or("unknown error")
            .to_string();
        return Err(anyhow!("failed to update stream: {msg}"));
    }

    Ok(())
}

/// Delete a stream via the HTTP API.
pub async fn delete(client: &CliClient, name: &str) -> Result<()> {
    let path = format!("/api/v1/streams/{}", name);
    client.delete(&path).await?;
    println!("Stream '{}' deleted", name);
    Ok(())
}

/// List all streams via the HTTP API.
pub async fn list(client: &CliClient, json_output: bool) -> Result<()> {
    let resp = client.get("/api/v1/streams").await?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&resp)?);
        return Ok(());
    }

    let streams = resp
        .as_array()
        .or_else(|| resp["streams"].as_array())
        .cloned()
        .unwrap_or_default();

    let columns = vec![
        "name".to_string(),
        "storage".to_string(),
        "head_offset".to_string(),
    ];

    let rows: Vec<Vec<String>> = streams
        .iter()
        .map(|s| {
            let name = s["name"].as_str().unwrap_or("").to_string();
            let storage_bytes = s["storage_bytes"].as_u64().unwrap_or(0);
            let head_offset = s["head_offset"].as_u64().unwrap_or(0);
            vec![name, format_bytes(storage_bytes), head_offset.to_string()]
        })
        .collect();

    let table = format::format_table(&columns, &rows, 0);
    println!("{}", table);

    Ok(())
}

/// Get detailed info about a single stream.
pub async fn info(client: &CliClient, name: &str, json_output: bool) -> Result<()> {
    let path = format!("/api/v1/streams/{}", name);
    let resp = client.get(&path).await?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&resp)?);
        return Ok(());
    }

    println!("Stream: {}", resp["name"].as_str().unwrap_or(name));
    if let Some(bytes) = resp["storage_bytes"].as_u64() {
        println!("  Storage: {}", format_bytes(bytes));
    }
    if let Some(offset) = resp["head_offset"].as_u64() {
        println!("  Head offset: {}", offset);
    }
    if let Some(retention) = resp["max_age_secs"].as_u64() {
        println!("  Retention: {}s", retention);
    }
    if let Some(max_size) = resp["max_bytes"].as_u64() {
        println!("  Max size: {}", format_bytes(max_size));
    }
    if let Some(dw) = resp["dedup_window_secs"].as_u64() {
        println!("  Dedup window: {}s", dw);
    }
    if let Some(de) = resp["dedup_max_entries"].as_u64() {
        println!("  Dedup max entries: {}", de);
    }

    Ok(())
}

/// List connectors via the HTTP API.
pub async fn list_connectors(client: &CliClient, json_output: bool) -> Result<()> {
    let resp = client.get("/api/v1/connectors").await?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&resp)?);
        return Ok(());
    }

    let connectors = resp
        .as_array()
        .or_else(|| resp["connectors"].as_array())
        .cloned()
        .unwrap_or_default();

    let columns = vec![
        "name".to_string(),
        "plugin".to_string(),
        "type".to_string(),
        "status".to_string(),
    ];

    let rows: Vec<Vec<String>> = connectors
        .iter()
        .map(|c| {
            vec![
                c["name"].as_str().unwrap_or("").to_string(),
                c["plugin"].as_str().unwrap_or("").to_string(),
                c["connector_type"]
                    .as_str()
                    .or_else(|| c["type"].as_str())
                    .unwrap_or("")
                    .to_string(),
                c["status"].as_str().unwrap_or("").to_string(),
            ]
        })
        .collect();

    let table = format::format_table(&columns, &rows, 0);
    println!("{}", table);

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a human-readable duration string to seconds.
///
/// Supported suffixes: `d` (days), `h` (hours), `m` (minutes), `s` (seconds).
///
/// Examples: "7d" -> 604800, "24h" -> 86400, "30m" -> 1800, "120s" -> 120.
pub fn parse_duration_secs(s: &str) -> Result<u64> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        return Err(anyhow!("empty duration string"));
    }

    let (num_str, multiplier) = if let Some(n) = s.strip_suffix('d') {
        (n, 86_400u64)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600u64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60u64)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1u64)
    } else {
        // Try parsing as raw seconds
        return s
            .parse::<u64>()
            .map_err(|_| anyhow!("invalid duration: '{s}' (use e.g. 7d, 24h, 30m, 120s)"));
    };

    let num: u64 = num_str
        .parse()
        .map_err(|_| anyhow!("invalid duration number: '{num_str}'"))?;

    Ok(num * multiplier)
}

/// Parse a human-readable size string to bytes.
///
/// Supported suffixes: `gb`/`g`, `mb`/`m`, `kb`/`k`, `b`.
///
/// Uses binary units (1 GB = 1024^3).
///
/// Examples: "10gb" -> 10737418240, "256mb" -> 268435456.
pub fn parse_size_bytes(s: &str) -> Result<u64> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        return Err(anyhow!("empty size string"));
    }

    let (num_str, multiplier) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1024u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix('g') {
        (n, 1024u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1024u64 * 1024)
    } else if let Some(n) = s.strip_suffix('m') {
        // Ambiguous with minutes -- but in a size context, treat as megabytes
        (n, 1024u64 * 1024)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1024u64)
    } else if let Some(n) = s.strip_suffix('k') {
        (n, 1024u64)
    } else if let Some(n) = s.strip_suffix('b') {
        (n, 1u64)
    } else {
        // Try parsing as raw bytes
        return s
            .parse::<u64>()
            .map_err(|_| anyhow!("invalid size: '{s}' (use e.g. 10gb, 256mb, 1024kb)"));
    };

    let num: u64 = num_str
        .parse()
        .map_err(|_| anyhow!("invalid size number: '{num_str}'"))?;

    Ok(num * multiplier)
}

/// Parse a human-readable count string (e.g. "2M", "500k") to a `u64`.
///
/// Supported suffixes: `M`/`m` (millions), `K`/`k` (thousands).
/// No suffix: parsed as a raw integer.
pub fn parse_count_suffix(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("empty count string"));
    }

    let s_lower = s.to_lowercase();
    let (num_str, multiplier): (&str, u64) = if let Some(n) = s_lower.strip_suffix('m') {
        (n, 1_000_000)
    } else if let Some(n) = s_lower.strip_suffix('k') {
        (n, 1_000)
    } else {
        (s, 1)
    };

    let num: u64 = num_str
        .parse()
        .map_err(|_| anyhow!("invalid count: '{s}' (use e.g. 2M, 500k, 1000)"))?;

    Ok(num * multiplier)
}

/// Format a byte count as a human-readable string.
///
/// Examples: 10737418240 -> "10.0 GB", 268435456 -> "256.0 MB".
pub fn format_bytes(n: u64) -> String {
    const GB: f64 = (1024 * 1024 * 1024) as f64;
    const MB: f64 = (1024 * 1024) as f64;
    const KB: f64 = 1024.0;

    let n_f = n as f64;
    if n_f >= GB {
        format!("{:.1} GB", n_f / GB)
    } else if n_f >= MB {
        format!("{:.1} MB", n_f / MB)
    } else if n_f >= KB {
        format!("{:.1} KB", n_f / KB)
    } else {
        format!("{} B", n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_secs() {
        assert_eq!(parse_duration_secs("7d").unwrap(), 604800);
        assert_eq!(parse_duration_secs("24h").unwrap(), 86400);
        assert_eq!(parse_duration_secs("30m").unwrap(), 1800);
        assert_eq!(parse_duration_secs("120s").unwrap(), 120);
        assert_eq!(parse_duration_secs("3600").unwrap(), 3600);
    }

    #[test]
    fn test_parse_size_bytes() {
        assert_eq!(parse_size_bytes("10gb").unwrap(), 10737418240);
        assert_eq!(parse_size_bytes("256mb").unwrap(), 268435456);
        assert_eq!(parse_size_bytes("1024kb").unwrap(), 1048576);
        assert_eq!(parse_size_bytes("100b").unwrap(), 100);
        assert_eq!(parse_size_bytes("4096").unwrap(), 4096);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(10737418240), "10.0 GB");
        assert_eq!(format_bytes(268435456), "256.0 MB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(500), "500 B");
    }

    #[test]
    fn test_parse_count_suffix() {
        assert_eq!(parse_count_suffix("2M").unwrap(), 2_000_000);
        assert_eq!(parse_count_suffix("500k").unwrap(), 500_000);
        assert_eq!(parse_count_suffix("500K").unwrap(), 500_000);
        assert_eq!(parse_count_suffix("1000").unwrap(), 1_000);
        assert_eq!(parse_count_suffix("1m").unwrap(), 1_000_000);
        assert!(parse_count_suffix("").is_err());
        assert!(parse_count_suffix("abc").is_err());
    }
}
