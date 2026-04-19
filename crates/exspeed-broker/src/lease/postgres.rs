//! Postgres lease backend. Stores leases as rows in `{schema}.exspeed_leases`
//! with `name` (PK), `holder` (UUID), and `expires_at` (TIMESTAMPTZ). All
//! state transitions are single atomic SQL statements — no application-level
//! locking needed.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{oneshot, watch, Mutex};
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use super::{LeaderLease, LeaseError, LeaseGuard, LeaseInfo};

pub struct PostgresLeaseBackend {
    inner: Arc<Inner>,
}

struct Inner {
    client: Mutex<Client>,
    schema: String,
    heartbeat_interval: Duration,
}

impl PostgresLeaseBackend {
    pub async fn from_env() -> Result<Self, LeaseError> {
        let url = env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").map_err(|_| {
            LeaseError::Connection("EXSPEED_OFFSET_STORE_POSTGRES_URL is required".to_string())
        })?;
        let schema = env::var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA")
            .unwrap_or_else(|_| "public".to_string());

        let (client, connection) = tokio_postgres::connect(&url, NoTls)
            .await
            .map_err(|e| LeaseError::Connection(format!("postgres connect failed: {e}")))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!(error = %e, "postgres lease backend connection error");
            }
        });

        let heartbeat_interval = super::heartbeat_interval_from_env();

        let inner = Arc::new(Inner {
            client: Mutex::new(client),
            schema,
            heartbeat_interval,
        });

        ensure_schema(&inner).await?;
        Ok(Self { inner })
    }
}

async fn ensure_schema(inner: &Inner) -> Result<(), LeaseError> {
    let client = inner.client.lock().await;
    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", inner.schema);
    client
        .execute(&create_schema, &[])
        .await
        .map_err(|e| LeaseError::Connection(format!("create schema: {e}")))?;

    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {}.exspeed_leases (
            name        TEXT PRIMARY KEY,
            holder      UUID NOT NULL,
            expires_at  TIMESTAMPTZ NOT NULL
         )",
        inner.schema
    );
    client
        .execute(&create_table, &[])
        .await
        .map_err(|e| LeaseError::Connection(format!("create table: {e}")))?;

    let create_idx = format!(
        "CREATE INDEX IF NOT EXISTS exspeed_leases_expires_idx
             ON {}.exspeed_leases (expires_at)",
        inner.schema
    );
    client
        .execute(&create_idx, &[])
        .await
        .map_err(|e| LeaseError::Connection(format!("create index: {e}")))?;

    Ok(())
}

#[async_trait]
impl LeaderLease for PostgresLeaseBackend {
    fn supports_coordination(&self) -> bool {
        true
    }

    async fn try_acquire(
        &self,
        name: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseGuard>, LeaseError> {
        let holder_id = Uuid::new_v4();
        let ttl_secs = ttl.as_secs_f64();

        let sql = format!(
            "INSERT INTO {schema}.exspeed_leases (name, holder, expires_at)
             VALUES ($1, $2, now() + make_interval(secs => $3))
             ON CONFLICT (name) DO UPDATE
             SET holder     = EXCLUDED.holder,
                 expires_at = EXCLUDED.expires_at
             WHERE {schema}.exspeed_leases.expires_at < now()
             RETURNING holder",
            schema = self.inner.schema
        );

        let client = self.inner.client.lock().await;
        let row = client
            .query_opt(&sql, &[&name, &holder_id, &ttl_secs])
            .await
            .map_err(|e| LeaseError::Backend(format!("acquire query: {e}")))?;
        drop(client);

        let returned_holder: Option<Uuid> = row.map(|r| r.get(0));

        if returned_holder == Some(holder_id) {
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
        let sql = format!(
            "SELECT name, holder, expires_at FROM {}.exspeed_leases
             WHERE expires_at > now() ORDER BY name",
            self.inner.schema
        );

        let client = self.inner.client.lock().await;
        let rows = client
            .query(&sql, &[])
            .await
            .map_err(|e| LeaseError::Backend(format!("list query: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| LeaseInfo {
                name: r.get(0),
                holder: r.get(1),
                expires_at: r.get(2),
            })
            .collect())
    }
}

/// Spawn a heartbeat task that refreshes the lease every
/// `heartbeat_interval` and releases on drop. Returns the LeaseGuard.
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
    tokio::spawn(async move {
        let mut consecutive_failures = 0u32;
        let mut interval = tokio::time::interval(inner_hb.heartbeat_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate tick — don't heartbeat at t=0, only after interval elapses.
        interval.tick().await;

        tokio::pin!(cancel_rx);
        loop {
            tokio::select! {
                _ = &mut cancel_rx => {
                    // Graceful release: compare-and-delete.
                    let sql = format!(
                        "DELETE FROM {}.exspeed_leases WHERE name = $1 AND holder = $2",
                        inner_hb.schema
                    );
                    let client = inner_hb.client.lock().await;
                    if let Err(e) = client.execute(&sql, &[&name_hb, &holder_id]).await {
                        warn!(error = %e, lease = %name_hb, "lease release failed");
                    }
                    trace!(lease = %name_hb, "lease released");
                    break;
                }
                _ = interval.tick() => {
                    match refresh(&inner_hb, &name_hb, &holder_id, ttl).await {
                        Ok(true) => {
                            consecutive_failures = 0;
                            trace!(lease = %name_hb, "heartbeat ok");
                        }
                        Ok(false) => {
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
                        warn!(lease = %name_hb, "lease lost after 2 consecutive heartbeat failures");
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

/// Refresh returns Ok(true) if we still own the lease, Ok(false) if not,
/// Err on backend failure.
async fn refresh(
    inner: &Inner,
    name: &str,
    holder_id: &Uuid,
    ttl: Duration,
) -> Result<bool, LeaseError> {
    let ttl_secs = ttl.as_secs_f64();
    let sql = format!(
        "UPDATE {}.exspeed_leases
         SET expires_at = now() + make_interval(secs => $3)
         WHERE name = $1 AND holder = $2
         RETURNING name",
        inner.schema
    );
    let client = inner.client.lock().await;
    let row = client
        .query_opt(&sql, &[&name, holder_id, &ttl_secs])
        .await
        .map_err(|e| LeaseError::Backend(format!("heartbeat query: {e}")))?;
    Ok(row.is_some())
}
