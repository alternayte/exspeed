//! Test helpers for DB-backed integration tests.
//!
//! Tests gate on env vars: `EXSPEED_POSTGRES_URL` / `EXSPEED_MYSQL_URL`.
//! When unset, `postgres_url()` / `mysql_url()` return `None`; callers should
//! skip with a log via the `require_postgres!` / `require_mysql!` macros.

use std::sync::atomic::{AtomicU64, Ordering};

static TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn postgres_url() -> Option<String> {
    std::env::var("EXSPEED_POSTGRES_URL").ok()
}

pub fn mysql_url() -> Option<String> {
    std::env::var("EXSPEED_MYSQL_URL").ok()
}

/// Generate a unique test-scoped identifier (prefix + process-id + monotonic
/// counter). Always valid per the schema DSL ident rules.
pub fn unique_table(prefix: &str) -> String {
    let n = TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", prefix, std::process::id(), n)
}

#[macro_export]
macro_rules! require_postgres {
    () => {
        match $crate::common::db::postgres_url() {
            Some(u) => u,
            None => {
                eprintln!("SKIP: EXSPEED_POSTGRES_URL not set");
                return;
            }
        }
    };
}

#[macro_export]
macro_rules! require_mysql {
    () => {
        match $crate::common::db::mysql_url() {
            Some(u) => u,
            None => {
                eprintln!("SKIP: EXSPEED_MYSQL_URL not set");
                return;
            }
        }
    };
}

pub async fn drop_table_postgres(url: &str, table: &str) {
    if let Ok(pool) = sqlx::postgres::PgPool::connect(url).await {
        let _ = sqlx::query(&format!("DROP TABLE IF EXISTS \"{}\"", table))
            .execute(&pool)
            .await;
        pool.close().await;
    }
}

pub async fn drop_table_mysql(url: &str, table: &str) {
    if let Ok(pool) = sqlx::mysql::MySqlPool::connect(url).await {
        let _ = sqlx::query(&format!("DROP TABLE IF EXISTS `{}`", table))
            .execute(&pool)
            .await;
        pool.close().await;
    }
}
