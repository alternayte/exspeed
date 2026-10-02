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

pub fn mssql_url() -> Option<String> {
    std::env::var("EXSPEED_MSSQL_URL").ok()
}

pub async fn drop_table_mssql(url: &str, table: &str) {
    let normalized = if url.to_ascii_lowercase().starts_with("mssql://") {
        format!("sqlserver://{}", &url[8..])
    } else {
        url.to_string()
    };
    let u = match url::Url::parse(&normalized) {
        Ok(u) => u,
        Err(_) => return,
    };
    let mut cfg = tiberius::Config::new();
    if let Some(h) = u.host_str() { cfg.host(h); }
    cfg.port(u.port().unwrap_or(1433));
    cfg.authentication(tiberius::AuthMethod::sql_server(
        u.username(),
        u.password().unwrap_or(""),
    ));
    let db = u.path().trim_start_matches('/');
    if !db.is_empty() { cfg.database(db); }
    for (k, v) in u.query_pairs() {
        if k.eq_ignore_ascii_case("trust_server_certificate") && v.eq_ignore_ascii_case("true") {
            cfg.trust_cert();
        }
    }
    let tcp = match tokio::net::TcpStream::connect(cfg.get_addr()).await {
        Ok(t) => t,
        Err(_) => return,
    };
    tcp.set_nodelay(true).ok();
    use tokio_util::compat::TokioAsyncWriteCompatExt;
    let tcp = tcp.compat_write();
    let mut client = match tiberius::Client::connect(cfg, tcp).await {
        Ok(c) => c,
        Err(_) => return,
    };
    let sql = format!("IF OBJECT_ID(N'[{t}]', N'U') IS NOT NULL DROP TABLE [{t}]", t = table);
    let _ = client.simple_query(sql).await;
}

#[macro_export]
macro_rules! require_mssql {
    () => {
        match $crate::common::db::mssql_url() {
            Some(u) => u,
            None => {
                eprintln!("SKIP: EXSPEED_MSSQL_URL not set");
                return;
            }
        }
    };
}
