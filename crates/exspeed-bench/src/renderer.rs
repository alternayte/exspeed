use anyhow::{anyhow, Result};
use std::fmt::Write as _;

use crate::profile::ProfileKind;
use crate::report::BenchResult;

/// README snippet — headline numbers only. Always renders; it is the caller's
/// responsibility to avoid writing this into README.md from a local profile.
pub fn readme_snippet(r: &BenchResult) -> String {
    let pub_1k = r.scenarios.publish.iter().find(|p| p.payload_bytes == 1024);
    let lat = r.scenarios.latency.as_ref();
    let fan_hi = r.scenarios.fanout.iter().max_by_key(|f| f.consumers);
    let exql = r.scenarios.exql.as_ref();

    let mut s = String::new();
    let _ = writeln!(s, "## Benchmarks\n");
    let _ = writeln!(
        s,
        "On a single {sku} ({vcpu} vCPU, {ram_gb} GB, {storage}), Exspeed sustains:\n",
        sku = display_sku(&r.host.sku),
        vcpu = r.host.vcpu,
        ram_gb = r.host.ram_gb,
        storage = r.host.storage,
    );
    let _ = writeln!(s, "| Workload              | Result                          |");
    let _ = writeln!(s, "|-----------------------|---------------------------------|");
    if let Some(p) = pub_1k {
        let mb = p.mb_per_sec;
        let _ = writeln!(
            s,
            "| Publish, 1 KB payload | ~{msg} msg/s (~{mb:.0} MB/s)           |",
            msg = si(p.msg_per_sec),
        );
    }
    if let Some(l) = lat {
        let rate = si(l.target_rate as f64);
        let p50 = l.latency_us.p50 as f64 / 1000.0;
        let p99 = l.latency_us.p99 as f64 / 1000.0;
        let p999 = l.latency_us.p999 as f64 / 1000.0;
        let _ = writeln!(
            s,
            "| E2E latency @ {rate}/s | p50 {p50:.1} ms / p99 {p99:.1} ms / p99.9 {p999:.1} ms |",
        );
    }
    if let Some(f) = fan_hi {
        let _ = writeln!(
            s,
            "| Fan-out, {n} consumers | ~{rate} msg/s aggregate delivery |",
            n = f.consumers,
            rate = si(f.aggregate_consumer_rate),
        );
    }
    if let Some(e) = exql {
        let _ = writeln!(
            s,
            "| ExQL continuous query | sustains ~{r} msg/s input        |",
            r = si(e.sustained_input_rate as f64),
        );
    }
    let _ = writeln!(
        s,
        "\nFull methodology, per-scenario tables, and reproduction steps in [BENCHMARKS.md](BENCHMARKS.md).\nResults refreshed {date} (git `{sha}`).",
        date = r.run_timestamp.format("%Y-%m-%d"),
        sha = &r.git_sha,
    );

    s
}

/// Strict wrapper — refuses to generate a publishable README snippet from a
/// local profile run (guards against accidentally committing laptop numbers).
pub fn readme_snippet_strict(r: &BenchResult) -> Result<String> {
    if r.profile == ProfileKind::Local {
        return Err(anyhow!("refusing to render readme snippet from local profile"));
    }
    Ok(readme_snippet(r))
}

pub fn benchmarks_md(r: &BenchResult) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# Exspeed Benchmarks\n");
    let _ = writeln!(
        s,
        "_Last refreshed: **{date}** (git `{sha}`, exspeed {ver})_\n",
        date = r.run_timestamp.format("%Y-%m-%d"),
        sha = r.git_sha,
        ver = r.exspeed_version
    );
    let _ = writeln!(s, "## How these numbers were produced\n");
    let _ = writeln!(
        s,
        "- **Host:** {sku} — {vcpu} vCPU, {ram} GB RAM, {storage}",
        sku = display_sku(&r.host.sku),
        vcpu = r.host.vcpu,
        ram = r.host.ram_gb,
        storage = r.host.storage
    );
    let _ = writeln!(
        s,
        "- **OS / kernel:** {os} / {kernel}",
        os = r.host.os,
        kernel = r.host.kernel
    );
    let _ = writeln!(s, "- **Broker + workload driver on the same box.**");
    let _ = writeln!(s, "- **Profile:** `{:?}`.", r.profile);
    let _ = writeln!(s, "- **git_sha:** `{}`\n", r.git_sha);

    let _ = writeln!(s, "## How to reproduce\n");
    let _ = writeln!(s, "```bash");
    let _ = writeln!(s, "git checkout {}", r.git_sha);
    let _ = writeln!(s, "cargo build --release -p exspeed-bench");
    let _ = writeln!(s, "./target/release/exspeed-bench all --profile reference \\");
    let _ = writeln!(s, "  --output bench/results/refresh.json");
    let _ = writeln!(
        s,
        "./target/release/exspeed-bench render bench/results/refresh.json --out BENCHMARKS.md"
    );
    let _ = writeln!(s, "```\n");

    let _ = writeln!(s, "## Publish\n");
    let _ = writeln!(s, "| Payload | msg/s | MB/s | Duration |");
    let _ = writeln!(s, "|---------|-------|------|----------|");
    for p in &r.scenarios.publish {
        let _ = writeln!(
            s,
            "| {} B | {:.0} | {:.1} | {:.1}s |",
            p.payload_bytes, p.msg_per_sec, p.mb_per_sec, p.duration_sec
        );
    }
    let _ = writeln!(s);

    if let Some(l) = &r.scenarios.latency {
        let _ = writeln!(s, "## Latency\n");
        let _ = writeln!(
            s,
            "At a sustained **{}** msg/s (payload {} B) over {:.0}s:\n",
            si(l.target_rate as f64),
            l.payload_bytes,
            l.duration_sec
        );
        let _ = writeln!(s, "| p50 | p90 | p99 | p99.9 | p99.99 | max |");
        let _ = writeln!(s, "|-----|-----|-----|-------|--------|-----|");
        let _ = writeln!(
            s,
            "| {}µs | {}µs | {}µs | {}µs | {}µs | {}µs |",
            l.latency_us.p50,
            l.latency_us.p90,
            l.latency_us.p99,
            l.latency_us.p999,
            l.latency_us.p9999,
            l.latency_us.max
        );
        let _ = writeln!(s);
    }

    if !r.scenarios.fanout.is_empty() {
        let _ = writeln!(s, "## Fan-out\n");
        let _ = writeln!(
            s,
            "| Consumers | Producer rate | Aggregate consumer rate | Max lag (msgs) |"
        );
        let _ = writeln!(
            s,
            "|-----------|---------------|-------------------------|----------------|"
        );
        for f in &r.scenarios.fanout {
            let _ = writeln!(
                s,
                "| {} | {} | {:.0} | {} |",
                f.consumers, f.producer_rate, f.aggregate_consumer_rate, f.max_lag_msgs
            );
        }
        let _ = writeln!(s);
    }

    if let Some(e) = &r.scenarios.exql {
        let _ = writeln!(s, "## ExQL\n");
        let _ = writeln!(s, "Query: `{}`\n", e.query);
        let _ = writeln!(
            s,
            "Sustained input rate with payload {} B and {} distinct subjects: **{} msg/s**.\n",
            e.payload_bytes, e.subjects, e.sustained_input_rate
        );
    }

    s
}

fn display_sku(sku: &str) -> String {
    match sku {
        "hetzner-ccx33" => "Hetzner CCX33".into(),
        other => other.to_owned(),
    }
}

fn si(n: f64) -> String {
    if n >= 1_000_000.0 {
        format!("{:.1}M", n / 1_000_000.0)
    } else if n >= 1_000.0 {
        format!("{:.0}k", n / 1_000.0)
    } else {
        format!("{:.0}", n)
    }
}
