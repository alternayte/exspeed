//! sqlx::AnyPool backend. Used for Postgres and MySQL.

use async_trait::async_trait;
use sqlx::AnyPool;

use super::backend::{BackendError, Param, SinkBackend};

pub(super) struct SqlxBackend {
    pool: AnyPool,
}

impl SqlxBackend {
    pub(super) async fn connect(url: &str) -> Result<Self, BackendError> {
        sqlx::any::install_default_drivers();
        let pool = AnyPool::connect(url)
            .await
            .map_err(|e| BackendError::Pool(format!("sqlx::AnyPool::connect: {e}")))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl SinkBackend for SqlxBackend {
    async fn execute_ddl(&self, sql: &str) -> Result<(), BackendError> {
        sqlx::query(sql)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn execute_row(&self, sql: &str, params: &[Param]) -> Result<(), BackendError> {
        let mut q = sqlx::query(sql);
        for p in params {
            q = bind_param(q, p);
        }
        q.execute(&self.pool).await.map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn close(&self) {
        self.pool.close().await;
    }
}

fn bind_param<'q>(
    q: sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>>,
    p: &Param,
) -> sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>> {
    match p {
        Param::Null => q.bind(None::<String>),
        Param::Bool(b) => q.bind(*b),
        Param::I64(i) => q.bind(*i),
        Param::F64(f) => q.bind(*f),
        Param::Text(s) => q.bind(s.clone()),
        Param::Timestamptz(dt) => {
            q.bind(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        }
        Param::JsonText(s) => q.bind(s.clone()),
    }
}

fn map_sqlx_err(e: sqlx::Error) -> BackendError {
    let sqlstate = e
        .as_database_error()
        .and_then(|db| db.code())
        .map(|c| c.to_string())
        .unwrap_or_default();
    BackendError::Sql {
        sqlstate,
        message: e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_param_accepts_all_variants() {
        let q = sqlx::query("SELECT 1");
        let q = bind_param(q, &Param::Null);
        let q = bind_param(q, &Param::Bool(true));
        let q = bind_param(q, &Param::I64(42));
        let q = bind_param(q, &Param::F64(3.14));
        let q = bind_param(q, &Param::Text("hello".into()));
        let q = bind_param(q, &Param::Timestamptz(chrono::Utc::now()));
        let q = bind_param(q, &Param::JsonText(r#"{"a":1}"#.into()));
        drop(q);
    }
}
