//! Per-message time headers: time-to-live and delayed delivery.
//!
//! - `exspeed-ttl`: the record expires this long after it was appended.
//!   Expired records are invisible to readers and consumers.
//! - `exspeed-delay`: consumers deliver the record no earlier than this long
//!   after it was appended.
//! - `exspeed-deliver-at`: consumers deliver the record no earlier than this
//!   time (milliseconds since the Unix epoch).
//!
//! Durations are a number with a unit (`500ms`, `30s`, `5m`, `2h`, `1d`) or
//! a bare number of milliseconds. Relative values are measured from the
//! record's append timestamp, so a retried publish yields the same expiry
//! and delivery time, and nothing has to be rewritten at publish time.

use crate::record_format;

pub const TTL_HEADER: &str = "exspeed-ttl";
pub const DELAY_HEADER: &str = "exspeed-delay";
pub const DELIVER_AT_HEADER: &str = "exspeed-deliver-at";

/// Longest accepted TTL or delay: 10 years.
pub const MAX_DURATION_MS: u64 = 10 * 365 * 24 * 3600 * 1000;

/// Parse a duration header value into milliseconds.
pub fn parse_duration_ms(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    if num.is_empty() {
        return Err(format!(
            "invalid duration '{s}': expected a number with an optional unit (ms, s, m, h, d)"
        ));
    }
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid duration '{s}': number too large"))?;
    let mult: u64 = match unit.trim() {
        "" | "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        u => {
            return Err(format!(
                "invalid duration '{s}': unknown unit '{u}' (use ms, s, m, h or d)"
            ))
        }
    };
    let ms = n
        .checked_mul(mult)
        .filter(|&ms| ms <= MAX_DURATION_MS)
        .ok_or_else(|| format!("invalid duration '{s}': longer than 10 years"))?;
    Ok(ms)
}

/// Parse an absolute time header value (milliseconds since the Unix epoch).
pub fn parse_epoch_ms(s: &str) -> Result<u64, String> {
    s.trim()
        .parse::<u64>()
        .map_err(|_| format!("invalid time '{s}': expected milliseconds since the Unix epoch"))
}

fn ms_to_ns(ms: u64) -> u64 {
    ms.saturating_mul(1_000_000)
}

/// When a raw record expires (ns since the epoch), from its `exspeed-ttl`
/// header (only when `allow_header`) or the stream default (`default_ms`,
/// 0 = none). The header wins over the default. `None` = never.
pub fn expires_at_ns(rec: &[u8], allow_header: bool, default_ms: u64) -> Option<u64> {
    let ts = record_format::timestamp_ns(rec);
    if allow_header {
        if let Some(ms) =
            record_format::header(rec, TTL_HEADER).and_then(|v| parse_duration_ms(v).ok())
        {
            return Some(ts.saturating_add(ms_to_ns(ms)));
        }
    }
    (default_ms > 0).then(|| ts.saturating_add(ms_to_ns(default_ms)))
}

/// When a raw record may first be delivered (ns since the epoch), from its
/// `exspeed-deliver-at` or `exspeed-delay` header. `None` = immediately.
pub fn deliver_at_ns(rec: &[u8]) -> Option<u64> {
    if let Some(ms) =
        record_format::header(rec, DELIVER_AT_HEADER).and_then(|v| parse_epoch_ms(v).ok())
    {
        return Some(ms_to_ns(ms));
    }
    let ms = record_format::header(rec, DELAY_HEADER).and_then(|v| parse_duration_ms(v).ok())?;
    Some(record_format::timestamp_ns(rec).saturating_add(ms_to_ns(ms)))
}

/// Validate the time headers of a record about to be published.
/// `allow_ttl` / `allow_delay` are the stream's settings.
pub fn check_headers(
    headers: &[(String, String)],
    allow_ttl: bool,
    allow_delay: bool,
) -> Result<(), String> {
    for (k, v) in headers {
        match k.as_str() {
            TTL_HEADER => {
                if !allow_ttl {
                    return Err(format!(
                        "this stream does not accept per-message TTLs (header '{TTL_HEADER}'); \
                         create it with allow_msg_ttl"
                    ));
                }
                if parse_duration_ms(v)? == 0 {
                    return Err(format!("'{TTL_HEADER}' must be greater than zero"));
                }
            }
            DELAY_HEADER | DELIVER_AT_HEADER => {
                if !allow_delay {
                    return Err(format!(
                        "this stream does not accept delayed delivery (header '{k}'); \
                         create it with allow_delayed"
                    ));
                }
                if k == DELAY_HEADER {
                    parse_duration_ms(v)?;
                } else {
                    parse_epoch_ms(v)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration_ms("250"), Ok(250));
        assert_eq!(parse_duration_ms("250ms"), Ok(250));
        assert_eq!(parse_duration_ms("30s"), Ok(30_000));
        assert_eq!(parse_duration_ms("5m"), Ok(300_000));
        assert_eq!(parse_duration_ms("2h"), Ok(7_200_000));
        assert_eq!(parse_duration_ms(" 1d "), Ok(86_400_000));
        assert!(parse_duration_ms("").is_err());
        assert!(parse_duration_ms("s").is_err());
        assert!(parse_duration_ms("5x").is_err());
        assert!(parse_duration_ms("-5s").is_err());
        assert!(parse_duration_ms("99999999999999999999").is_err());
        assert!(parse_duration_ms("4000d").is_err());
    }

    fn rec(ts_ns: u64, headers: &[(&str, &str)]) -> Vec<u8> {
        let headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut buf = Vec::new();
        record_format::encode(
            &mut buf,
            &record_format::Fields {
                offset: 7,
                timestamp_ns: ts_ns,
                delivery_count: 0,
                subject: "a.b",
                key: None,
                value: b"v",
                headers: &headers,
            },
        )
        .unwrap();
        buf
    }

    #[test]
    fn expiry_and_delivery_times() {
        let r = rec(1_000_000_000, &[("x", "y"), (TTL_HEADER, "2s")]);
        assert_eq!(expires_at_ns(&r, true, 0), Some(3_000_000_000));
        assert_eq!(
            expires_at_ns(&r, false, 0),
            None,
            "header ignored when not allowed"
        );
        assert_eq!(expires_at_ns(&r, false, 500), Some(1_500_000_000));
        let plain = rec(1_000_000_000, &[]);
        assert_eq!(expires_at_ns(&plain, true, 0), None);
        assert_eq!(deliver_at_ns(&plain), None);

        let d = rec(1_000_000_000, &[(DELAY_HEADER, "1s")]);
        assert_eq!(deliver_at_ns(&d), Some(2_000_000_000));
        let a = rec(1_000_000_000, &[(DELIVER_AT_HEADER, "5000")]);
        assert_eq!(deliver_at_ns(&a), Some(5_000_000_000));
    }

    #[test]
    fn publish_checks() {
        let h = |k: &str, v: &str| vec![(k.to_string(), v.to_string())];
        assert!(check_headers(&h(TTL_HEADER, "5s"), true, false).is_ok());
        assert!(check_headers(&h(TTL_HEADER, "5s"), false, true).is_err());
        assert!(check_headers(&h(TTL_HEADER, "0"), true, true).is_err());
        assert!(check_headers(&h(TTL_HEADER, "soon"), true, true).is_err());
        assert!(check_headers(&h(DELAY_HEADER, "5s"), false, true).is_ok());
        assert!(check_headers(&h(DELAY_HEADER, "5s"), true, false).is_err());
        assert!(check_headers(&h(DELIVER_AT_HEADER, "123"), false, true).is_ok());
        assert!(check_headers(&h(DELIVER_AT_HEADER, "tomorrow"), false, true).is_err());
        assert!(check_headers(&h("other", "x"), false, false).is_ok());
    }
}
