use crate::cli::client::CliClient;
use crate::cli::format;
use anyhow::Result;

/// How long one records request waits for new data while following.
const FOLLOW_WAIT_MS: u64 = 10_000;

/// Follow a stream through `GET /api/v1/streams/{name}/records`, long-polling
/// (`wait_ms`) once caught up. Needs only `subscribe` on the stream and works
/// against any node, followers included.
pub async fn run(
    client: &CliClient,
    stream: &str,
    last: Option<usize>,
    no_follow: bool,
    subject: Option<&str>,
    from_beginning: bool,
    json_output: bool,
) -> Result<()> {
    // The records endpoint reports the high watermark; it only needs the
    // subscribe permission (the stream info endpoint needs admin).
    let probe = client
        .get(&format!("/api/v1/streams/{stream}/records?limit=1"))
        .await?;
    let head = probe["high_watermark"].as_u64().unwrap_or(0);
    let mut from: u64 = if from_beginning {
        0
    } else if let Some(n) = last {
        head.saturating_sub(n as u64)
    } else {
        head
    };
    let filter = subject.unwrap_or("");

    let mut caught_up = false;
    loop {
        // Once caught up, let the server hold the request until records
        // arrive instead of polling.
        let wait = if caught_up && !no_follow {
            FOLLOW_WAIT_MS
        } else {
            0
        };
        let path = format!(
            "/api/v1/streams/{stream}/records?from={from}&limit=500&filter={}&wait_ms={wait}",
            encode_query(filter)
        );
        let page = client.get(&path).await?;
        let records = page["records"].as_array().cloned().unwrap_or_default();
        for r in &records {
            if json_output {
                println!("{}", serde_json::to_string(r)?);
            } else {
                println!("{}", format::format_tail_line(r));
            }
        }
        let next = page["next_offset"].as_u64().unwrap_or(from);
        caught_up = next >= page["high_watermark"].as_u64().unwrap_or(next);
        from = next;
        if caught_up && no_follow {
            break;
        }
    }
    Ok(())
}

/// Percent-encode a query-string value (subject filters contain `>`/`*`).
fn encode_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn encodes_wildcards() {
        assert_eq!(super::encode_query("orders.>"), "orders.%3E");
        assert_eq!(super::encode_query("a.*"), "a.%2A");
    }
}
