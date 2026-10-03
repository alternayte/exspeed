//! Postgres lease backend: one row per lease in
//! `{schema}.exspeed_cluster_leases`. Every transition is a single
//! conditional statement evaluated against the database clock (`now()`), so
//! node clocks don't matter. Calls are bounded by a timeout and the
//! connection is re-established after an error.

use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row};
use tracing::{error, warn};

use super::{AcquireRequest, LeaderLease, LeaseError, LeaseRecord, Refresh};

pub struct PostgresLeaseBackend {
    url: String,
    table: String,
    schema: String,
    call_timeout: Duration,
    client: Mutex<Option<Client>>,
}

const COLUMNS: &str = "name, holder, epoch, expires_at, replication_endpoint, client_endpoint, isr";

impl PostgresLeaseBackend {
    pub async fn connect(
        url: &str,
        schema: &str,
        call_timeout: Duration,
    ) -> Result<Self, LeaseError> {
        if !schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
            || schema.is_empty()
        {
            return Err(LeaseError::Connection(format!(
                "invalid postgres schema name `{schema}`"
            )));
        }
        let b = Self {
            url: url.to_string(),
            table: format!("{schema}.exspeed_cluster_leases"),
            schema: schema.to_string(),
            call_timeout,
            client: Mutex::new(None),
        };
        let client = b.open().await?;
        b.ensure_schema(&client).await?;
        *b.client.lock().await = Some(client);
        Ok(b)
    }

    async fn open(&self) -> Result<Client, LeaseError> {
        let fut = tokio_postgres::connect(&self.url, NoTls);
        let (client, connection) = tokio::time::timeout(self.call_timeout, fut)
            .await
            .map_err(|_| LeaseError::Timeout)?
            .map_err(|e| LeaseError::Connection(format!("postgres connect failed: {e}")))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!(error = %e, "postgres lease connection closed");
            }
        });
        Ok(client)
    }

    async fn ensure_schema(&self, client: &Client) -> Result<(), LeaseError> {
        let ddl = format!(
            "CREATE SCHEMA IF NOT EXISTS {schema};
             CREATE TABLE IF NOT EXISTS {table} (
                 name                 TEXT PRIMARY KEY,
                 holder               TEXT NOT NULL,
                 epoch                BIGINT NOT NULL,
                 expires_at           TIMESTAMPTZ NOT NULL,
                 replication_endpoint TEXT,
                 client_endpoint      TEXT,
                 isr                  TEXT[] NOT NULL DEFAULT '{{}}'
             );",
            schema = self.schema,
            table = self.table
        );
        client
            .batch_execute(&ddl)
            .await
            .map_err(|e| LeaseError::Connection(format!("create lease table: {e}")))
    }

    /// Run one statement with a timeout, reconnecting first if the previous
    /// call broke the connection.
    async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Option<Row>, LeaseError> {
        let mut guard = self.client.lock().await;
        if guard.as_ref().is_none_or(|c| c.is_closed()) {
            *guard = Some(self.open().await?);
        }
        let client = guard.as_ref().expect("connected above");
        match tokio::time::timeout(self.call_timeout, client.query_opt(sql, params)).await {
            Ok(Ok(row)) => Ok(row),
            Ok(Err(e)) => {
                warn!(error = %e, "postgres lease query failed");
                *guard = None;
                Err(LeaseError::Backend(e.to_string()))
            }
            Err(_) => {
                *guard = None;
                Err(LeaseError::Timeout)
            }
        }
    }

    async fn query(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<Row>, LeaseError> {
        let mut guard = self.client.lock().await;
        if guard.as_ref().is_none_or(|c| c.is_closed()) {
            *guard = Some(self.open().await?);
        }
        let client = guard.as_ref().expect("connected above");
        match tokio::time::timeout(self.call_timeout, client.query(sql, params)).await {
            Ok(Ok(rows)) => Ok(rows),
            Ok(Err(e)) => {
                *guard = None;
                Err(LeaseError::Backend(e.to_string()))
            }
            Err(_) => {
                *guard = None;
                Err(LeaseError::Timeout)
            }
        }
    }
}

fn record(row: &Row) -> LeaseRecord {
    let epoch: i64 = row.get(2);
    LeaseRecord {
        name: row.get(0),
        holder: row.get(1),
        epoch: epoch as u64,
        expires_at: row.get(3),
        replication_endpoint: row.get(4),
        client_endpoint: row.get(5),
        isr: row.get(6),
    }
}

#[async_trait]
impl LeaderLease for PostgresLeaseBackend {
    fn supports_coordination(&self) -> bool {
        true
    }

    async fn try_acquire(&self, req: &AcquireRequest) -> Result<Option<LeaseRecord>, LeaseError> {
        let sql = format!(
            "INSERT INTO {table} AS l ({COLUMNS})
             VALUES ($1, $2, 1, now() + make_interval(secs => $3), $4, $5, '{{}}')
             ON CONFLICT (name) DO UPDATE
             SET holder = EXCLUDED.holder,
                 epoch = l.epoch + 1,
                 expires_at = EXCLUDED.expires_at,
                 replication_endpoint = EXCLUDED.replication_endpoint,
                 client_endpoint = EXCLUDED.client_endpoint
             WHERE (l.expires_at < now() OR l.holder = EXCLUDED.holder)
               AND (NOT $6 OR cardinality(l.isr) = 0 OR EXCLUDED.holder = ANY(l.isr))
             RETURNING {COLUMNS}",
            table = self.table
        );
        let ttl = req.ttl.as_secs_f64();
        let row = self
            .query_opt(
                &sql,
                &[
                    &req.name,
                    &req.holder,
                    &ttl,
                    &req.replication_endpoint,
                    &req.client_endpoint,
                    &req.require_isr,
                ],
            )
            .await?;
        Ok(row.as_ref().map(record))
    }

    async fn refresh(
        &self,
        name: &str,
        holder: &str,
        epoch: u64,
        ttl: Duration,
    ) -> Result<Refresh, LeaseError> {
        let sql = format!(
            "UPDATE {table} SET expires_at = now() + make_interval(secs => $4)
             WHERE name = $1 AND holder = $2 AND epoch = $3 AND expires_at > now()
             RETURNING name",
            table = self.table
        );
        let row = self
            .query_opt(&sql, &[&name, &holder, &(epoch as i64), &ttl.as_secs_f64()])
            .await?;
        Ok(if row.is_some() {
            Refresh::Held
        } else {
            Refresh::Lost
        })
    }

    async fn release(&self, name: &str, holder: &str, epoch: u64) -> Result<(), LeaseError> {
        let sql = format!(
            "UPDATE {table} SET expires_at = now() - interval '1 millisecond'
             WHERE name = $1 AND holder = $2 AND epoch = $3
             RETURNING name",
            table = self.table
        );
        self.query_opt(&sql, &[&name, &holder, &(epoch as i64)])
            .await?;
        Ok(())
    }

    async fn set_isr(
        &self,
        name: &str,
        holder: &str,
        epoch: u64,
        isr: &[String],
    ) -> Result<bool, LeaseError> {
        let sql = format!(
            "UPDATE {table} SET isr = $4
             WHERE name = $1 AND holder = $2 AND epoch = $3 AND expires_at > now()
             RETURNING name",
            table = self.table
        );
        let isr: Vec<String> = isr.to_vec();
        let row = self
            .query_opt(&sql, &[&name, &holder, &(epoch as i64), &isr])
            .await?;
        Ok(row.is_some())
    }

    async fn get(&self, name: &str) -> Result<Option<LeaseRecord>, LeaseError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM {table} WHERE name = $1",
            table = self.table
        );
        Ok(self.query_opt(&sql, &[&name]).await?.as_ref().map(record))
    }

    async fn list_all(&self) -> Result<Vec<LeaseRecord>, LeaseError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM {table} WHERE expires_at > now() ORDER BY name",
            table = self.table
        );
        Ok(self.query(&sql, &[]).await?.iter().map(record).collect())
    }
}
