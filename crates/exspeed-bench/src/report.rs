use serde::{Deserialize, Serialize};
use crate::host::Host;
use crate::profile::ProfileKind;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResult {
    pub schema_version: u32,
    pub profile: ProfileKind,
    pub exspeed_version: String,
    pub git_sha: String,
    pub host: Host,
    pub run_timestamp: chrono::DateTime<chrono::Utc>,
    pub scenarios: Scenarios,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Scenarios {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub publish: Vec<PublishResult>,
    pub latency: Option<LatencyResult>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub fanout: Vec<FanoutResult>,
    pub exql: Option<ExqlResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishResult {
    pub payload_bytes: usize,
    pub msg_per_sec: f64,
    pub mb_per_sec: f64,
    pub duration_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyResult {
    pub payload_bytes: usize,
    pub target_rate: u64,
    pub duration_sec: f64,
    pub latency_us: LatencyPercentiles,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub p9999: u64,
    pub max: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutResult {
    pub consumers: usize,
    pub producer_rate: u64,
    pub aggregate_consumer_rate: f64,
    pub max_lag_msgs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExqlResult {
    pub query: String,
    pub payload_bytes: usize,
    pub subjects: u32,
    pub sustained_input_rate: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_json() {
        let r = BenchResult {
            schema_version: SCHEMA_VERSION,
            profile: ProfileKind::Local,
            exspeed_version: "0.1.1".into(),
            git_sha: "abcdef1".into(),
            host: Host {
                sku: "laptop".into(),
                vcpu: 8,
                ram_gb: 32,
                storage: "ssd".into(),
                kernel: "k".into(),
                os: "o".into(),
            },
            run_timestamp: chrono::Utc::now(),
            scenarios: Scenarios {
                publish: vec![PublishResult {
                    payload_bytes: 1024,
                    msg_per_sec: 100.0,
                    mb_per_sec: 0.1,
                    duration_sec: 2.0,
                }],
                ..Default::default()
            },
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: BenchResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.schema_version, SCHEMA_VERSION);
        assert_eq!(back.scenarios.publish.len(), 1);
        assert_eq!(back.scenarios.publish[0].payload_bytes, 1024);
    }
}
