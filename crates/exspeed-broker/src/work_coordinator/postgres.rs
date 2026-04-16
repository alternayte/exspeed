use std::env;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use super::{ClaimedMessage, WorkCoordinator, WorkCoordinatorError};

pub struct PostgresWorkCoordinator {
    client: Mutex<Client>,
    schema: String,
}

impl PostgresWorkCoordinator {
    /// Build from environment variables. Reuses the offset store's Postgres connection.
    pub async fn from_env() -> Result<Self, WorkCoordinatorError> {
        let url = env::var("EXSPEED_OFFSET_STORE_POSTGRES_URL").map_err(|_| {
            WorkCoordinatorError::Connection(
                "EXSPEED_OFFSET_STORE_POSTGRES_URL is required".to_string(),
            )
        })?;
        let schema = env::var("EXSPEED_OFFSET_STORE_POSTGRES_SCHEMA")
            .unwrap_or_else(|_| "public".to_string());

        let (client, connection) = tokio_postgres::connect(&url, NoTls).await.map_err(|e| {
            WorkCoordinatorError::Connection(format!("postgres connect failed: {e}"))
        })?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "postgres work coordinator connection error");
            }
        });

        let coordinator = Self {
            client: Mutex::new(client),
            schema,
        };

        coordinator.ensure_tables().await?;
        Ok(coordinator)
    }

    async fn ensure_tables(&self) -> Result<(), WorkCoordinatorError> {
        let client = self.client.lock().await;

        let state_sql = format!(
            "CREATE TABLE IF NOT EXISTS {}.exspeed_group_state (
                name             TEXT PRIMARY KEY,
                committed_offset BIGINT NOT NULL DEFAULT -1,
                delivery_head    BIGINT NOT NULL DEFAULT -1
            )",
            self.schema
        );
        client
            .execute(&state_sql, &[])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("create group_state: {e}")))?;

        let pending_sql = format!(
            "CREATE TABLE IF NOT EXISTS {}.exspeed_group_pending (
                group_name    TEXT NOT NULL,
                record_offset BIGINT NOT NULL,
                delivered_to  TEXT,
                delivered_at  TIMESTAMPTZ,
                attempts      INT NOT NULL DEFAULT 0,
                PRIMARY KEY (group_name, record_offset)
            )",
            self.schema
        );
        client
            .execute(&pending_sql, &[])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("create group_pending: {e}")))?;

        let index_sql = format!(
            "CREATE INDEX IF NOT EXISTS exspeed_group_pending_available
                ON {}.exspeed_group_pending (group_name, record_offset)
                WHERE delivered_to IS NULL",
            self.schema
        );
        client
            .execute(&index_sql, &[])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("create index: {e}")))?;

        Ok(())
    }
}

#[async_trait]
impl WorkCoordinator for PostgresWorkCoordinator {
    fn supports_coordination(&self) -> bool {
        true
    }

    async fn claim_batch(
        &self,
        group: &str,
        subscriber_id: &str,
        n: usize,
        ack_timeout_secs: u64,
    ) -> Result<Vec<ClaimedMessage>, WorkCoordinatorError> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let client = self.client.lock().await;
        let sql = format!(
            "WITH candidates AS (
                SELECT group_name, record_offset
                FROM {schema}.exspeed_group_pending
                WHERE group_name = $1
                  AND (delivered_to IS NULL
                       OR delivered_at + make_interval(secs => $4) < NOW())
                ORDER BY record_offset
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {schema}.exspeed_group_pending p
            SET delivered_to = $2,
                delivered_at = NOW(),
                attempts = p.attempts + 1
            FROM candidates c
            WHERE p.group_name = c.group_name AND p.record_offset = c.record_offset
            RETURNING p.record_offset, p.attempts",
            schema = self.schema
        );
        let timeout = ack_timeout_secs as f64;
        let limit = n as i64;
        let rows = client
            .query(&sql, &[&group, &subscriber_id, &limit, &timeout])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("claim_batch failed: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| {
                let offset: i64 = row.get("record_offset");
                let attempts: i32 = row.get("attempts");
                ClaimedMessage {
                    offset: offset as u64,
                    attempts: attempts as u32,
                }
            })
            .collect())
    }

    async fn enqueue(
        &self,
        group: &str,
        offsets: &[u64],
    ) -> Result<(), WorkCoordinatorError> {
        if offsets.is_empty() {
            return Ok(());
        }
        let client = self.client.lock().await;

        // Ensure group_state row exists.
        let upsert_state = format!(
            "INSERT INTO {schema}.exspeed_group_state (name, committed_offset, delivery_head)
             VALUES ($1, -1, -1)
             ON CONFLICT (name) DO NOTHING",
            schema = self.schema
        );
        client
            .execute(&upsert_state, &[&group])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("ensure group_state: {e}")))?;

        // Insert each offset. ON CONFLICT DO NOTHING so enqueue is idempotent.
        let insert_pending = format!(
            "INSERT INTO {schema}.exspeed_group_pending (group_name, record_offset)
             VALUES ($1, $2)
             ON CONFLICT (group_name, record_offset) DO NOTHING",
            schema = self.schema
        );
        for o in offsets {
            let off = *o as i64;
            client
                .execute(&insert_pending, &[&group, &off])
                .await
                .map_err(|e| WorkCoordinatorError::Connection(format!("enqueue: {e}")))?;
        }

        // Advance delivery_head to max(offsets).
        let max_offset = *offsets.iter().max().unwrap() as i64;
        let update_head = format!(
            "UPDATE {schema}.exspeed_group_state
             SET delivery_head = GREATEST(delivery_head, $2)
             WHERE name = $1",
            schema = self.schema
        );
        client
            .execute(&update_head, &[&group, &max_offset])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("advance head: {e}")))?;

        Ok(())
    }

    async fn delivery_head(&self, group: &str) -> Result<u64, WorkCoordinatorError> {
        let client = self.client.lock().await;
        let sql = format!(
            "SELECT delivery_head FROM {schema}.exspeed_group_state WHERE name = $1",
            schema = self.schema
        );
        let rows = client
            .query(&sql, &[&group])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("delivery_head: {e}")))?;
        match rows.first() {
            Some(row) => {
                let v: i64 = row.get("delivery_head");
                if v < 0 {
                    Ok(0)
                } else {
                    Ok(v as u64)
                }
            }
            None => Ok(0),
        }
    }

    async fn pending_count(&self, group: &str) -> Result<usize, WorkCoordinatorError> {
        let client = self.client.lock().await;
        let sql = format!(
            "SELECT COUNT(*) AS c FROM {schema}.exspeed_group_pending WHERE group_name = $1",
            schema = self.schema
        );
        let rows = client
            .query(&sql, &[&group])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("pending_count: {e}")))?;
        let c: i64 = rows[0].get("c");
        Ok(c as usize)
    }

    async fn ack(&self, group: &str, offset: u64) -> Result<(), WorkCoordinatorError> {
        let client = self.client.lock().await;
        let off = offset as i64;
        let delete_sql = format!(
            "DELETE FROM {schema}.exspeed_group_pending
             WHERE group_name = $1 AND record_offset = $2",
            schema = self.schema
        );
        client
            .execute(&delete_sql, &[&group, &off])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("ack delete: {e}")))?;

        // Advance committed_offset: MIN(pending.record_offset) - 1, or delivery_head if pending empty.
        let advance_sql = format!(
            "UPDATE {schema}.exspeed_group_state s
             SET committed_offset = COALESCE(
                 (SELECT MIN(p.record_offset) - 1
                  FROM {schema}.exspeed_group_pending p
                  WHERE p.group_name = s.name),
                 s.delivery_head
             )
             WHERE s.name = $1",
            schema = self.schema
        );
        client
            .execute(&advance_sql, &[&group])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("advance committed: {e}")))?;

        Ok(())
    }

    async fn nack(&self, group: &str, offset: u64) -> Result<u32, WorkCoordinatorError> {
        let client = self.client.lock().await;
        let off = offset as i64;

        let sql = format!(
            "UPDATE {schema}.exspeed_group_pending
             SET delivered_to = NULL, delivered_at = NULL
             WHERE group_name = $1 AND record_offset = $2
             RETURNING attempts",
            schema = self.schema
        );
        let rows = client
            .query(&sql, &[&group, &off])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("nack: {e}")))?;
        match rows.first() {
            Some(row) => {
                let a: i32 = row.get("attempts");
                Ok(a as u32)
            }
            None => Ok(0),
        }
    }

    async fn committed_offset(&self, group: &str) -> Result<u64, WorkCoordinatorError> {
        let client = self.client.lock().await;
        let sql = format!(
            "SELECT committed_offset FROM {schema}.exspeed_group_state WHERE name = $1",
            schema = self.schema
        );
        let rows = client
            .query(&sql, &[&group])
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("committed_offset: {e}")))?;
        match rows.first() {
            Some(row) => {
                let v: i64 = row.get("committed_offset");
                if v < 0 {
                    Ok(0)
                } else {
                    Ok(v as u64)
                }
            }
            None => Ok(0),
        }
    }
}
