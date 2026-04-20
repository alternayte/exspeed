use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProfileKind {
    Reference,
    Local,
}

#[derive(Debug, Clone)]
pub struct Profile {
    pub kind: ProfileKind,
    pub publish_duration: Duration,
    pub latency_duration: Duration,
    pub latency_target_rate: u64,
    pub fanout_duration: Duration,
    pub fanout_producer_rate: u64,
    pub fanout_consumer_counts: Vec<usize>,
    pub exql_duration: Duration,
    pub publish_payload_sizes: Vec<usize>,
}

impl Profile {
    pub fn reference() -> Self {
        Self {
            kind: ProfileKind::Reference,
            publish_duration: Duration::from_secs(60),
            latency_duration: Duration::from_secs(60),
            latency_target_rate: 100_000,
            fanout_duration: Duration::from_secs(60),
            fanout_producer_rate: 50_000,
            fanout_consumer_counts: vec![1, 4, 16, 64],
            exql_duration: Duration::from_secs(60),
            publish_payload_sizes: vec![100, 1024, 10 * 1024],
        }
    }

    pub fn local() -> Self {
        Self {
            kind: ProfileKind::Local,
            publish_duration: Duration::from_secs(5),
            latency_duration: Duration::from_secs(10),
            latency_target_rate: 10_000,
            fanout_duration: Duration::from_secs(5),
            fanout_producer_rate: 5_000,
            fanout_consumer_counts: vec![1, 4],
            exql_duration: Duration::from_secs(10),
            publish_payload_sizes: vec![1024],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_profile_uses_60s_durations() {
        let p = Profile::reference();
        assert_eq!(p.publish_duration, Duration::from_secs(60));
        assert_eq!(p.latency_target_rate, 100_000);
        assert_eq!(p.fanout_consumer_counts, vec![1, 4, 16, 64]);
    }

    #[test]
    fn local_profile_is_short_and_tagged() {
        let p = Profile::local();
        assert_eq!(p.kind, ProfileKind::Local);
        assert!(p.latency_duration <= Duration::from_secs(15));
    }
}
