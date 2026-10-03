//! In-process lease backend. Nodes running in one process (integration
//! tests) share a lease table by using the same namespace
//! ([`MemoryLeaseBackend::named`]). Supports simulated partitions: calls
//! made on behalf of a partitioned holder fail as if the backend were
//! unreachable.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use super::{AcquireRequest, LeaderLease, LeaseError, LeaseRecord, Refresh};

#[derive(Default)]
pub struct MemoryLeaseBackend {
    leases: Mutex<HashMap<String, LeaseRecord>>,
    partitioned: Mutex<HashSet<String>>,
}

fn registry() -> &'static Mutex<HashMap<String, Arc<MemoryLeaseBackend>>> {
    static R: OnceLock<Mutex<HashMap<String, Arc<MemoryLeaseBackend>>>> = OnceLock::new();
    R.get_or_init(Default::default)
}

impl MemoryLeaseBackend {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The process-wide backend for `namespace`, created on first use.
    pub fn named(namespace: &str) -> Arc<Self> {
        registry()
            .lock()
            .entry(namespace.to_string())
            .or_default()
            .clone()
    }

    /// Make every call on behalf of `holder` fail (or succeed again).
    pub fn set_partitioned(&self, holder: &str, partitioned: bool) {
        let mut p = self.partitioned.lock();
        if partitioned {
            p.insert(holder.to_string());
        } else {
            p.remove(holder);
        }
    }

    /// Expire a lease now, as if its holder had stopped heartbeating long ago.
    pub fn force_expire(&self, name: &str) {
        if let Some(r) = self.leases.lock().get_mut(name) {
            r.expires_at = chrono::Utc::now() - chrono::Duration::milliseconds(1);
        }
    }

    fn check(&self, holder: &str) -> Result<(), LeaseError> {
        if self.partitioned.lock().contains(holder) {
            Err(LeaseError::Connection("partitioned (simulated)".into()))
        } else {
            Ok(())
        }
    }
}

fn expiry(ttl: Duration) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now() + chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::MAX)
}

#[async_trait]
impl LeaderLease for MemoryLeaseBackend {
    fn supports_coordination(&self) -> bool {
        true
    }

    async fn try_acquire(&self, req: &AcquireRequest) -> Result<Option<LeaseRecord>, LeaseError> {
        self.check(&req.holder)?;
        let mut leases = self.leases.lock();
        let now = chrono::Utc::now();
        let (epoch, isr) = match leases.get(&req.name) {
            Some(cur) => {
                if cur.expires_at > now && cur.holder != req.holder {
                    return Ok(None);
                }
                if req.require_isr && !cur.isr.is_empty() && !cur.isr.contains(&req.holder) {
                    return Ok(None);
                }
                (cur.epoch + 1, cur.isr.clone())
            }
            None => (1, Vec::new()),
        };
        let rec = LeaseRecord {
            name: req.name.clone(),
            holder: req.holder.clone(),
            epoch,
            expires_at: expiry(req.ttl),
            replication_endpoint: req.replication_endpoint.clone(),
            client_endpoint: req.client_endpoint.clone(),
            isr,
        };
        leases.insert(req.name.clone(), rec.clone());
        Ok(Some(rec))
    }

    async fn refresh(
        &self,
        name: &str,
        holder: &str,
        epoch: u64,
        ttl: Duration,
    ) -> Result<Refresh, LeaseError> {
        self.check(holder)?;
        let mut leases = self.leases.lock();
        match leases.get_mut(name) {
            Some(r)
                if r.holder == holder && r.epoch == epoch && r.expires_at > chrono::Utc::now() =>
            {
                r.expires_at = expiry(ttl);
                Ok(Refresh::Held)
            }
            _ => Ok(Refresh::Lost),
        }
    }

    async fn release(&self, name: &str, holder: &str, epoch: u64) -> Result<(), LeaseError> {
        self.check(holder)?;
        let mut leases = self.leases.lock();
        if let Some(r) = leases.get_mut(name) {
            if r.holder == holder && r.epoch == epoch {
                r.expires_at = chrono::Utc::now() - chrono::Duration::milliseconds(1);
            }
        }
        Ok(())
    }

    async fn set_isr(
        &self,
        name: &str,
        holder: &str,
        epoch: u64,
        isr: &[String],
    ) -> Result<bool, LeaseError> {
        self.check(holder)?;
        let mut leases = self.leases.lock();
        match leases.get_mut(name) {
            Some(r)
                if r.holder == holder && r.epoch == epoch && r.expires_at > chrono::Utc::now() =>
            {
                r.isr = isr.to_vec();
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn get(&self, name: &str) -> Result<Option<LeaseRecord>, LeaseError> {
        Ok(self.leases.lock().get(name).cloned())
    }

    async fn list_all(&self) -> Result<Vec<LeaseRecord>, LeaseError> {
        let now = chrono::Utc::now();
        let mut v: Vec<_> = self
            .leases
            .lock()
            .values()
            .filter(|r| r.expires_at > now)
            .cloned()
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(v)
    }
}
