use exspeed_bench::{host::Host, profile::ProfileKind, report::*, renderer};

fn sample() -> BenchResult {
    BenchResult {
        schema_version: 1,
        profile: ProfileKind::Reference,
        exspeed_version: "0.1.1".into(),
        git_sha: "abcdef1".into(),
        host: Host {
            sku: "hetzner-ccx33".into(),
            vcpu: 8,
            ram_gb: 32,
            storage: "nvme-ext4".into(),
            kernel: "6.8".into(),
            os: "Ubuntu 24.04".into(),
        },
        run_timestamp: chrono::DateTime::parse_from_rfc3339("2026-04-20T12:34:56Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        scenarios: Scenarios {
            publish: vec![PublishResult {
                payload_bytes: 1024,
                msg_per_sec: 400_000.0,
                mb_per_sec: 400.0,
                duration_sec: 60.0,
            }],
            latency: Some(LatencyResult {
                payload_bytes: 1024,
                target_rate: 100_000,
                duration_sec: 60.0,
                latency_us: LatencyPercentiles {
                    p50: 500,
                    p90: 900,
                    p99: 2_500,
                    p999: 8_000,
                    p9999: 20_000,
                    max: 40_000,
                },
            }),
            fanout: vec![FanoutResult {
                consumers: 64,
                producer_rate: 50_000,
                aggregate_consumer_rate: 3_100_000.0,
                max_lag_msgs: 42,
            }],
            exql: Some(ExqlResult {
                query: "SELECT subject, COUNT(*) FROM bench GROUP BY subject EMIT CHANGES".into(),
                payload_bytes: 1024,
                subjects: 8,
                sustained_input_rate: 75_000,
            }),
        },
    }
}

#[test]
fn readme_snippet_contains_headline_numbers() {
    let s = renderer::readme_snippet(&sample());
    assert!(s.contains("Hetzner CCX33"));
    assert!(s.contains("400")); // publish
    assert!(s.contains("p99"));
    assert!(s.contains("64 consumers"));
    assert!(s.contains("BENCHMARKS.md"));
}

#[test]
fn benchmarks_md_renders_per_scenario_sections() {
    let s = renderer::benchmarks_md(&sample());
    for needle in ["## Publish", "## Latency", "## Fan-out", "## ExQL", "git_sha", "reproduce"] {
        assert!(s.contains(needle), "missing {needle}");
    }
}

#[test]
fn renderer_refuses_to_overwrite_readme_from_local_profile() {
    let mut s = sample();
    s.profile = ProfileKind::Local;
    let err = renderer::readme_snippet_strict(&s).unwrap_err();
    assert!(err.to_string().contains("local"));
}
