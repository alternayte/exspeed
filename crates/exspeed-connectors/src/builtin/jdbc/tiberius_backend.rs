//! MSSQL backend using `tiberius` + `bb8-tiberius`. Separate from
//! `SqlxBackend` because sqlx::Any does not support SQL Server.

use async_trait::async_trait;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use tiberius::{Config, Query};
use url::Url;

use super::backend::{BackendError, Param, SinkBackend};

pub(super) struct TiberiusBackend {
    pool: Pool<ConnectionManager>,
}

impl TiberiusBackend {
    pub(super) async fn connect(raw_url: &str) -> Result<Self, BackendError> {
        let config = parse_url(raw_url)?;
        let mgr = ConnectionManager::build(config)
            .map_err(|e| BackendError::Pool(format!("bb8-tiberius build: {e}")))?;
        let pool = Pool::builder()
            .max_size(8)
            .build(mgr)
            .await
            .map_err(|e| BackendError::Pool(format!("bb8 pool build: {e}")))?;
        Ok(Self { pool })
    }
}

fn parse_url(raw: &str) -> Result<Config, BackendError> {
    // Normalize `mssql://` → `sqlserver://` so `url::Url::parse` accepts it,
    // then build tiberius Config by hand. We avoid Config::from_ado_string
    // because its error messages are poor.
    let normalized = if raw.to_ascii_lowercase().starts_with("mssql://") {
        format!("sqlserver://{}", &raw[8..])
    } else {
        raw.to_string()
    };
    let u = Url::parse(&normalized)
        .map_err(|e| BackendError::Pool(format!("url parse: {e}")))?;

    let mut cfg = Config::new();
    if let Some(host) = u.host_str() {
        cfg.host(host);
    }
    cfg.port(u.port().unwrap_or(1433));
    if !u.username().is_empty() {
        let user = percent_encoding::percent_decode_str(u.username())
            .decode_utf8_lossy()
            .into_owned();
        let pass = u
            .password()
            .map(|p| {
                percent_encoding::percent_decode_str(p)
                    .decode_utf8_lossy()
                    .into_owned()
            })
            .unwrap_or_default();
        cfg.authentication(tiberius::AuthMethod::sql_server(&user, &pass));
    }
    let db = u.path().trim_start_matches('/');
    if !db.is_empty() {
        cfg.database(db);
    }

    let mut trust = false;
    for (k, v) in u.query_pairs() {
        if k.eq_ignore_ascii_case("trust_server_certificate") && v.eq_ignore_ascii_case("true") {
            trust = true;
        }
    }
    if trust {
        cfg.trust_cert();
    }

    Ok(cfg)
}

#[async_trait]
impl SinkBackend for TiberiusBackend {
    async fn execute_ddl(&self, sql: &str) -> Result<(), BackendError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| BackendError::Pool(format!("pool get: {e}")))?;
        conn.simple_query(sql)
            .await
            .map_err(map_tiberius_err)?
            .into_results()
            .await
            .map_err(map_tiberius_err)?;
        Ok(())
    }

    async fn execute_row(&self, sql: &str, params: &[Param]) -> Result<(), BackendError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| BackendError::Pool(format!("pool get: {e}")))?;
        let mut q = Query::new(sql);
        for p in params {
            bind_param(&mut q, p);
        }
        q.execute(&mut *conn).await.map_err(map_tiberius_err)?;
        Ok(())
    }

    async fn close(&self) {
        // bb8 Pool does not require explicit close; connections drop on last ref.
    }
}

fn bind_param(q: &mut Query<'_>, p: &Param) {
    match p {
        Param::Null => q.bind(Option::<&str>::None),
        Param::Bool(b) => q.bind(*b),
        Param::I64(i) => q.bind(*i),
        Param::F64(f) => q.bind(*f),
        Param::Text(s) => q.bind(s.clone()),
        Param::Timestamptz(dt) => q.bind(*dt),
        Param::JsonText(s) => q.bind(s.clone()),
    }
}

fn map_tiberius_err(e: tiberius::error::Error) -> BackendError {
    use tiberius::error::Error as E;
    match &e {
        E::Server(t) => BackendError::Sql {
            sqlstate: t.code().to_string(),
            message: t.message().to_string(),
        },
        E::Io { kind, message } => BackendError::Io(format!("{kind:?}: {message}")),
        _ => BackendError::Sql { sqlstate: String::new(), message: e.to_string() },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_url_basic() {
        let cfg = parse_url("mssql://sa:pw@localhost:1433/master").unwrap();
        let _ = cfg;
    }

    #[test]
    fn parse_url_sqlserver_scheme() {
        assert!(parse_url("sqlserver://sa:pw@localhost/master").is_ok());
    }

    #[test]
    fn parse_url_with_trust_flag() {
        assert!(parse_url("mssql://sa:pw@localhost/master?trust_server_certificate=true").is_ok());
    }

    #[test]
    fn parse_url_rejects_bad() {
        assert!(parse_url("not-a-url").is_err());
    }
}
