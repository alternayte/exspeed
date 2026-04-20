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
