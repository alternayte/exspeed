use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Host {
    pub sku: String,
    pub vcpu: u32,
    pub ram_gb: u64,
    pub storage: String,
    pub kernel: String,
    pub os: String,
}

impl Host {
    /// Detect from the running machine. `sku` and `storage` are user-supplied
    /// (no reliable autodetect), so they default to "unknown" and should be
    /// overridden via CLI flags on reference runs.
    pub fn detect(sku: Option<String>, storage: Option<String>) -> Self {
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        sys.refresh_cpu_list(sysinfo::CpuRefreshKind::everything());
        Self {
            sku: sku.unwrap_or_else(|| "unknown".into()),
            vcpu: sys.cpus().len() as u32,
            ram_gb: sys.total_memory() / 1024 / 1024 / 1024,
            storage: storage.unwrap_or_else(|| "unknown".into()),
            kernel: sysinfo::System::kernel_version().unwrap_or_else(|| "unknown".into()),
            os: sysinfo::System::long_os_version().unwrap_or_else(|| "unknown".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_reports_at_least_one_vcpu() {
        let h = Host::detect(None, None);
        assert!(h.vcpu >= 1);
    }

    #[test]
    fn sku_override_is_preserved() {
        let h = Host::detect(Some("hetzner-ccx33".into()), None);
        assert_eq!(h.sku, "hetzner-ccx33");
    }
}
