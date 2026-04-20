// HTTP webhook source is implemented as an axum handler, not a SourceConnector.
// See exspeed-api/src/handlers/webhooks.rs for the handler.
// This module provides the handle_webhook_post() function.

use std::sync::Arc;

use bytes::Bytes;

use crate::config::ConnectorConfig;
use exspeed_common::StreamName;
use exspeed_streams::record::Record;
use exspeed_streams::traits::StorageEngine;

/// Handle an incoming HTTP webhook POST request.
///
/// Validates auth, extracts the subject from the request body using the connector's
/// subject template, builds a [`Record`], and appends it to the configured stream.
///
/// Returns the resulting [`Offset`](exspeed_common::Offset) as a `u64`.
pub async fn handle_webhook_post(
    storage: &Arc<dyn StorageEngine>,
    config: &ConnectorConfig,
    body: Bytes,
    auth_header: Option<&str>,
) -> Result<u64, String> {
    // 1. Validate auth
    let auth_type = config.setting_or("auth_type", "none");
    match auth_type.as_str() {
        "none" => {}
        "bearer" => {
            let secret = config.setting("auth_secret")?;
            let expected = format!("Bearer {}", secret);
            match auth_header {
                Some(value) if value == expected => {}
                _ => return Err("unauthorized: invalid or missing bearer token".to_string()),
            }
        }
        other => {
            return Err(format!("unsupported auth_type: {other}"));
        }
    }

    // 2. Extract subject from body using the template
    let subject = extract_subject(&config.subject_template, &body);

    // 3. Build the record
    let record = Record {
        key: None,
        value: body,
        subject,
        headers: vec![
            ("x-exspeed-source".to_string(), "http_webhook".to_string()),
            ("x-exspeed-connector".to_string(), config.name.clone()),
        ],
        timestamp_ns: None,
    };

    // 4. Resolve stream name and append
    let stream = StreamName::try_from(config.stream.as_str())
        .map_err(|e| format!("invalid stream name: {e}"))?;

    let (offset, _timestamp) = storage
        .append(&stream, &record)
        .await
        .map_err(|e| format!("storage error: {e}"))?;

    Ok(offset.0)
}

/// Interpolate `{$.field}` references in `template` from the top-level fields of a JSON object.
///
/// - If the template contains no `{$` sequences it is returned unchanged.
/// - If the body is not valid JSON, the template is returned unchanged.
/// - If a referenced field is absent the placeholder is replaced with `"unknown"`.
fn extract_subject(template: &str, body: &Bytes) -> String {
    // Fast path: no template variables present
    if !template.contains("{$") {
        return template.to_string();
    }

    // Try to parse the body as a JSON object
    let json: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return template.to_string(),
    };

    let obj = match json.as_object() {
        Some(o) => o,
        None => return template.to_string(),
    };

    // Replace every `{$.field}` occurrence
    let mut result = template.to_string();
    // Collect all placeholders first to avoid mutating while iterating
    let mut placeholders: Vec<(String, String)> = Vec::new();

    let mut search_from = 0usize;
    while let Some(rel_start) = result[search_from..].find("{$.") {
        let start = search_from + rel_start;
        if let Some(rel_end) = result[start..].find('}') {
            let end = start + rel_end;
            let placeholder = result[start..=end].to_string(); // e.g. "{$.type}"
            let field_name = &result[start + 3..end]; // e.g. "type"
            let value = obj
                .get(field_name)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            placeholders.push((placeholder, value));
            search_from = end + 1;
        } else {
            break;
        }
    }

    for (placeholder, value) in placeholders {
        result = result.replacen(&placeholder, &value, 1);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn extract_subject_literal() {
        let result = extract_subject("webhook.received", &body(r#"{"type":"order"}"#));
        assert_eq!(result, "webhook.received");
    }

    #[test]
    fn extract_subject_from_json() {
        let result = extract_subject("webhook.{$.type}", &body(r#"{"type":"order_created"}"#));
        assert_eq!(result, "webhook.order_created");
    }

    #[test]
    fn extract_subject_missing_field() {
        let result = extract_subject("webhook.{$.kind}", &body(r#"{"type":"order_created"}"#));
        assert_eq!(result, "webhook.unknown");
    }

    #[test]
    fn extract_subject_invalid_json() {
        let result = extract_subject("webhook.{$.type}", &body("not json at all"));
        assert_eq!(result, "webhook.{$.type}");
    }
}
