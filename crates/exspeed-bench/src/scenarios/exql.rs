use std::time::Instant;

use anyhow::Result;
use serde_json::json;

use crate::driver::exspeed::{self, ExspeedClient};
use crate::profile::Profile;
use crate::report::ExqlResult;

/// Binary-search the highest sustained input rate that the ExQL continuous
/// query engine can keep up with.
///
/// # Arguments
/// - `tcp_addr` — Exspeed TCP broker address.
/// - `api_addr` — Exspeed HTTP API address.
/// - `profile`  — Benchmark profile (controls per-iteration duration).
/// - `low`      — Lower bound for the binary search (msg/s).
/// - `high`     — Upper bound for the binary search (msg/s).
/// - `iterations` — Number of binary-search steps.
pub async fn run(
    tcp_addr: &str,
    api_addr: &str,
    profile: &Profile,
    low: u64,
    high: u64,
    iterations: u32,
) -> Result<ExqlResult> {
    let source = "bench-exql-source";
    let target = "bench-exql-output";
    let payload_bytes: usize = 1024;
    let subjects: u32 = 1;

    // Ensure source + target streams exist.
    let mut setup = ExspeedClient::connect(tcp_addr).await?;
    setup.ensure_stream(source).await?;
    setup.ensure_stream(target).await?;

    // Build the continuous query SQL.
    // Syntax: CREATE VIEW <target> AS SELECT ... FROM <source> EMIT CHANGES
    // The endpoint is POST /api/v1/queries/continuous with body {"sql": "..."}.
    let query_sql = format!(
        r#"CREATE VIEW "{target}" AS SELECT subject, COUNT(*) FROM "{source}" GROUP BY subject EMIT CHANGES"#
    );

    let http = reqwest::Client::new();
    // Strip any leading scheme so callers can pass either "http://host:port" or
    // "host:port" — the CLI default is "http://localhost:8080" (scheme included).
    let api_base = api_addr
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let url = format!("http://{api_base}/api/v1/queries/continuous");
    // Best-effort: warn on failure (e.g. view already exists from a prior run,
    // or auth is required in non-embedded deployments). The binary-search still
    // exercises the producer path and yields a valid sustained_input_rate; the
    // ExqlResult is most meaningful when the query was actually created.
    match http
        .post(&url)
        .json(&json!({ "sql": query_sql }))
        .send()
        .await
    {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(
                url,
                error = %e,
                "continuous-query POST failed; binary-search will still run \
                 but ExqlResult.sustained_input_rate may not reflect query load"
            );
        }
    }

    // Binary-search the sustainable rate.
    let mut lo = low;
    let mut hi = high;
    let mut best = lo;
    let mut any_passed = false;

    for _ in 0..iterations {
        let candidate = (lo + hi) / 2;
        let origin = Instant::now();
        let stats = exspeed::run_producer_at_rate(
            tcp_addr,
            source,
            payload_bytes,
            profile.exql_duration,
            candidate,
            origin,
        )
        .await?;

        // "Sustained" heuristic: the producer completed all interval-driven
        // publishes without dropping any. If the broker accepted >= 95% of the
        // expected message count the rate is considered sustained.
        let expected =
            (candidate as f64 * profile.exql_duration.as_secs_f64()) as u64;
        let tolerance = (expected as f64 * 0.05) as u64;
        let passed = expected.saturating_sub(stats.messages) < tolerance;

        if passed {
            best = candidate;
            lo = candidate;
            any_passed = true;
        } else {
            hi = candidate;
        }
    }

    let (sustained, warning) = if any_passed {
        (best, None)
    } else {
        (0, Some("no-candidate-passed".to_string()))
    };

    Ok(ExqlResult {
        query: query_sql,
        payload_bytes,
        subjects,
        sustained_input_rate: sustained,
        warning,
    })
}
