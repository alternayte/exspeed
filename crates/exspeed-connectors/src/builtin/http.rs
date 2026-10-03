//! Shared HTTP rules for `http_sink` and `http_poll`.
//!
//! | Status                  | Class     |
//! |-------------------------|-----------|
//! | 2xx                     | success   |
//! | 408, 425, 429, 5xx      | transient (honours `Retry-After`) |
//! | 401, 403                | fatal (credentials) |
//! | other 4xx               | poison (this request will never succeed) |
//! | timeout / connect error | transient |

use std::time::Duration;

use crate::traits::{ConnectorError, PoisonReason};

pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

pub fn client(timeout: Duration, user_agent: &str) -> Result<reqwest::Client, ConnectorError> {
    reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(timeout.min(Duration::from_secs(10)))
        .user_agent(user_agent)
        .build()
        .map_err(|e| ConnectorError::config(format!("failed to build HTTP client: {e}")))
}

/// Classify a non-2xx response. `None` for 2xx/3xx.
pub fn classify_status(
    status: u16,
    retry_after: Option<&str>,
    body: &str,
) -> Option<ConnectorError> {
    if status < 400 {
        return None;
    }
    let snippet: String = body.chars().take(512).collect();
    Some(match status {
        408 | 425 | 429 | 500..=599 => ConnectorError::Transient {
            message: format!("HTTP {status}: {snippet}"),
            retry_after: retry_after.and_then(parse_retry_after),
        },
        401 | 403 => ConnectorError::fatal(format!("HTTP {status} (check credentials): {snippet}")),
        _ => ConnectorError::Poison(PoisonReason::HttpClientError { status }),
    })
}

pub fn classify_reqwest_error(e: &reqwest::Error) -> ConnectorError {
    if e.is_builder() {
        ConnectorError::config(format!("invalid HTTP request: {e}"))
    } else {
        // Timeouts, refused connections, resets: worth retrying.
        ConnectorError::transient(format!("HTTP request failed: {e}"))
    }
}

/// `Retry-After` as delta-seconds or an HTTP date.
pub fn parse_retry_after(v: &str) -> Option<Duration> {
    let v = v.trim();
    if let Ok(secs) = v.parse::<u64>() {
        return Some(Duration::from_secs(secs.min(3600)));
    }
    let when = chrono::DateTime::parse_from_rfc2822(v).ok()?;
    let delta = when.with_timezone(&chrono::Utc) - chrono::Utc::now();
    Some(Duration::from_millis(
        delta.num_milliseconds().clamp(0, 3_600_000) as u64,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::ErrorKind;

    #[test]
    fn status_taxonomy() {
        assert!(classify_status(200, None, "").is_none());
        assert!(classify_status(204, None, "").is_none());
        for s in [408, 425, 429, 500, 502, 503, 504] {
            assert_eq!(
                classify_status(s, None, "").unwrap().kind(),
                ErrorKind::Transient,
                "{s}"
            );
        }
        for s in [401, 403] {
            assert_eq!(
                classify_status(s, None, "").unwrap().kind(),
                ErrorKind::Fatal,
                "{s}"
            );
        }
        for s in [400, 404, 409, 410, 422] {
            assert_eq!(
                classify_status(s, None, "").unwrap().kind(),
                ErrorKind::Poison,
                "{s}"
            );
        }
    }

    #[test]
    fn retry_after() {
        assert_eq!(parse_retry_after("7"), Some(Duration::from_secs(7)));
        let e = classify_status(429, Some("3"), "").unwrap();
        assert_eq!(e.retry_after(), Some(Duration::from_secs(3)));
        let future = (chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc2822();
        let d = parse_retry_after(&future).unwrap();
        assert!(d > Duration::from_secs(25) && d <= Duration::from_secs(30));
        assert_eq!(parse_retry_after("garbage"), None);
    }
}
