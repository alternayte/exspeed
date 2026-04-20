mod embedded_server;
use embedded_server::start;
use exspeed_bench::profile::Profile;

#[tokio::test]
async fn publish_scenario_returns_nonzero_throughput_for_each_payload() {
    let srv = start().await;
    let mut profile = Profile::local();
    // Keep the test fast but exercise multiple payload sizes.
    profile.publish_duration = std::time::Duration::from_secs(2);
    profile.publish_payload_sizes = vec![256, 1024];
    let results = exspeed_bench::scenarios::publish::run(&srv.tcp_addr, &profile).await.unwrap();
    assert_eq!(results.len(), 2);
    for r in &results {
        assert!(r.msg_per_sec > 0.0);
        assert!(r.mb_per_sec > 0.0);
    }
}

#[tokio::test]
async fn latency_scenario_reports_sensible_percentiles() {
    let srv = start().await;
    let mut profile = Profile::local();
    profile.latency_duration = std::time::Duration::from_secs(3);
    profile.latency_target_rate = 2_000;
    let r = exspeed_bench::scenarios::latency::run(&srv.tcp_addr, &profile).await.unwrap();
    assert!(r.latency_us.p50 > 0);
    assert!(r.latency_us.p99 >= r.latency_us.p50);
    assert!(r.latency_us.max >= r.latency_us.p99);
    assert!(r.latency_us.p50 < 1_000_000, "p50 should be < 1s in an embedded test");
}

#[tokio::test]
async fn fanout_scenario_returns_one_row_per_consumer_count() {
    let srv = start().await;
    let mut profile = Profile::local();
    profile.fanout_duration = std::time::Duration::from_secs(3);
    profile.fanout_producer_rate = 1_000;
    profile.fanout_consumer_counts = vec![1, 2];
    let results = exspeed_bench::scenarios::fanout::run(&srv.tcp_addr, &profile).await.unwrap();
    assert_eq!(results.len(), 2);
    for r in &results {
        assert!(r.aggregate_consumer_rate >= 0.0);
    }
}

#[tokio::test]
async fn exql_scenario_reports_a_sustained_rate() {
    let srv = start().await;
    let mut profile = Profile::local();
    profile.exql_duration = std::time::Duration::from_secs(3);
    // Narrow search range so the test finishes quickly.
    let r = exspeed_bench::scenarios::exql::run(
        &srv.tcp_addr,
        &srv.api_addr,
        &profile,
        1_000,    // low
        4_000,    // high
        4,        // iterations
    ).await.unwrap();
    assert!(r.sustained_input_rate >= 1_000);
    assert!(r.sustained_input_rate <= 4_000);
}
