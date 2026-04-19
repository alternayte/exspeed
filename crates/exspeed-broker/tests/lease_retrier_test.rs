#![cfg(test)]
//! Verify the LeaseRetrier trait contract with a mock target. Does not
//! exercise postgres/redis — concrete lease-backend behavior is covered
//! by their own integration tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use exspeed_broker::lease_retrier::{spawn_lease_retrier, LeaseRetrierTarget};

struct CountingTarget {
    count: AtomicU32,
}

#[async_trait::async_trait]
impl LeaseRetrierTarget for CountingTarget {
    async fn attempt_acquire_unheld(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
    fn name(&self) -> &'static str {
        "counting"
    }
}

#[tokio::test]
async fn retrier_ticks_target_periodically() {
    // Use a short TTL so the retrier ticks ~1s (TTL/3).
    std::env::set_var("EXSPEED_LEASE_TTL_SECS", "3");
    let target = Arc::new(CountingTarget {
        count: AtomicU32::new(0),
    });
    spawn_lease_retrier(vec![target.clone() as Arc<dyn LeaseRetrierTarget>]);

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    std::env::remove_var("EXSPEED_LEASE_TTL_SECS");

    // TTL/3 = 1s; we skip t=0, so ~2 ticks land in 3s. Be lenient (>=1)
    // so CI timing noise doesn't flake.
    let c = target.count.load(Ordering::SeqCst);
    assert!(c >= 1, "expected at least 1 tick in 3s, got {c}");
}
