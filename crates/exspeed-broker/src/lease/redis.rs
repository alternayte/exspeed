//! Redis lease backend. Uses `SET key value NX PX ttl_ms` for acquire and
//! Lua scripts for compare-and-swap on refresh and release. Keys namespaced
//! under `EXSPEED_LEASE_REDIS_KEY_PREFIX` (default `exspeed:lease:`).
//!
//! This is the canonical Redlock single-instance pattern: SET-NX wins the
//! lease atomically, and the value (a UUID) is checked before refresh/release
//! so a process whose lease has been stolen cannot accidentally clobber the
//! new holder's entry.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use tokio::sync::{oneshot, watch, Mutex};
use tracing::{debug, trace, warn};
use uuid::Uuid;

use super::{LeaderLease, LeaseError, LeaseGuard, LeaseInfo};

/// Lua script: refresh TTL only if the stored value still matches our
/// holder UUID. Returns 1 if refreshed, 0 if not (lost or deleted).
const REFRESH_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
    return 0
end
"#;

/// Lua script: delete key only if its value matches our holder UUID.
const RELEASE_LUA: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"#;

pub struct RedisLeaseBackend {
    inner: Arc<Inner>,
}

struct Inner {
    conn: Mutex<redis::aio::MultiplexedConnection>,
    prefix: String,
    heartbeat_interval: Duration,
}

impl RedisLeaseBackend {
    pub async fn from_env() -> Result<Self, LeaseError> {
        let url = env::var("EXSPEED_OFFSET_STORE_REDIS_URL").map_err(|_| {
            LeaseError::Connection("EXSPEED_OFFSET_STORE_REDIS_URL is required".to_string())
        })?;
        let prefix = env::var("EXSPEED_LEASE_REDIS_KEY_PREFIX")
            .unwrap_or_else(|_| "exspeed:lease:".to_string());

        let client = redis::Client::open(url.as_str())
            .map_err(|e| LeaseError::Connection(format!("redis client: {e}")))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| LeaseError::Connection(format!("redis connect: {e}")))?;

        let heartbeat_interval = super::heartbeat_interval_from_env();

        Ok(Self {
            inner: Arc::new(Inner {
                conn: Mutex::new(conn),
                prefix,
                heartbeat_interval,
            }),
        })
    }
}

fn key(prefix: &str, name: &str) -> String {
    format!("{prefix}{name}")
}

#[async_trait]
impl LeaderLease for RedisLeaseBackend {
    fn supports_coordination(&self) -> bool {
        true
    }

    async fn try_acquire(
        &self,
        name: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseGuard>, LeaseError> {
        let holder_id = Uuid::new_v4();
        let k = key(&self.inner.prefix, name);
        let value = holder_id.to_string();
        let ttl_ms = ttl.as_millis() as u64;

        let mut conn = self.inner.conn.lock().await;
        // SET NX PX — returns Some("OK") on win, None if key already exists.
        let res: Option<String> = redis::cmd("SET")
            .arg(&k)
            .arg(&value)
            .arg("NX")
            .arg("PX")
            .arg(ttl_ms)
            .query_async(&mut *conn)
            .await
            .map_err(|e| LeaseError::Backend(format!("redis SET NX: {e}")))?;
        drop(conn);

        if res.as_deref() == Some("OK") {
            Ok(Some(spawn_heartbeat(
                self.inner.clone(),
                name.to_string(),
                holder_id,
                ttl,
            )))
        } else {
            Ok(None)
        }
    }

    async fn list_all(&self) -> Result<Vec<LeaseInfo>, LeaseError> {
        let pattern = format!("{}*", self.inner.prefix);
        let mut conn = self.inner.conn.lock().await;

        // SCAN-based iteration; collect all matching keys before issuing
        // GET/PTTL commands (the iter holds a borrow on `conn`).
        let mut iter: redis::AsyncIter<String> = conn
            .scan_match(&pattern)
            .await
            .map_err(|e| LeaseError::Backend(format!("scan: {e}")))?;

        let mut keys: Vec<String> = Vec::new();
        while let Some(k) = iter.next_item().await {
            keys.push(k);
        }
        drop(iter);

        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            let holder_str: Option<String> = conn
                .get(&k)
                .await
                .map_err(|e| LeaseError::Backend(format!("get: {e}")))?;
            let pttl_ms: i64 = conn
                .pttl(&k)
                .await
                .map_err(|e| LeaseError::Backend(format!("pttl: {e}")))?;
            if let Some(hs) = holder_str {
                if pttl_ms <= 0 {
                    continue; // expired or no-TTL key
                }
                let name = k
                    .strip_prefix(&self.inner.prefix)
                    .unwrap_or(&k)
                    .to_string();
                let holder = match Uuid::parse_str(&hs) {
                    Ok(u) => u,
                    Err(_) => continue,
                };
                let expires_at = chrono::Utc::now() + chrono::Duration::milliseconds(pttl_ms);
                out.push(LeaseInfo {
                    name,
                    holder,
                    expires_at,
                });
            }
        }

        Ok(out)
    }
}

/// Spawn a heartbeat task that refreshes the lease every
/// `heartbeat_interval` and CAS-releases on drop. Returns the LeaseGuard.
fn spawn_heartbeat(
    inner: Arc<Inner>,
    name: String,
    holder_id: Uuid,
    ttl: Duration,
) -> LeaseGuard {
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let (lost_tx, lost_rx) = watch::channel(false);

    let inner_hb = inner.clone();
    let name_hb = name.clone();
    let holder_str = holder_id.to_string();
    tokio::spawn(async move {
        let mut consecutive_failures = 0u32;
        let mut interval = tokio::time::interval(inner_hb.heartbeat_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate tick — don't heartbeat at t=0, only after the
        // interval has elapsed once.
        interval.tick().await;

        tokio::pin!(cancel_rx);
        loop {
            tokio::select! {
                _ = &mut cancel_rx => {
                    // Graceful release: CAS-delete via Lua so we never
                    // delete a key that has already been re-acquired by
                    // another holder.
                    let k = key(&inner_hb.prefix, &name_hb);
                    let mut conn = inner_hb.conn.lock().await;
                    let r: redis::RedisResult<i32> = redis::Script::new(RELEASE_LUA)
                        .key(&k)
                        .arg(&holder_str)
                        .invoke_async(&mut *conn)
                        .await;
                    if let Err(e) = r {
                        warn!(error = %e, lease = %name_hb, "redis lease release failed");
                    }
                    trace!(lease = %name_hb, "lease released");
                    break;
                }
                _ = interval.tick() => {
                    let k = key(&inner_hb.prefix, &name_hb);
                    let mut conn = inner_hb.conn.lock().await;
                    let r: redis::RedisResult<i32> = redis::Script::new(REFRESH_LUA)
                        .key(&k)
                        .arg(&holder_str)
                        .arg(ttl.as_millis() as u64)
                        .invoke_async(&mut *conn)
                        .await;
                    drop(conn);
                    match r {
                        Ok(1) => {
                            consecutive_failures = 0;
                            trace!(lease = %name_hb, "heartbeat ok");
                        }
                        Ok(_) => {
                            consecutive_failures += 1;
                            debug!(
                                lease = %name_hb,
                                consecutive_failures,
                                "heartbeat found lease stolen or missing"
                            );
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            warn!(
                                lease = %name_hb,
                                consecutive_failures,
                                error = %e,
                                "heartbeat backend error"
                            );
                        }
                    }
                    if consecutive_failures >= 2 {
                        warn!(
                            lease = %name_hb,
                            "lease lost after 2 consecutive heartbeat failures"
                        );
                        let _ = lost_tx.send(true);
                        break;
                    }
                }
            }
        }
    });

    LeaseGuard {
        name,
        holder_id,
        on_lost: lost_rx,
        _cancel_heartbeat: cancel_tx,
    }
}
