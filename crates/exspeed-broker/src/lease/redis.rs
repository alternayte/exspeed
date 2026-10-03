//! Redis lease backend. Each lease is a hash at `{prefix}{name}` (fields
//! `name`, `holder`, `epoch`, `exp` in ms, `repl`, `client`, `isr`), and
//! `{prefix}__names` indexes them. Every transition is a Lua script that reads
//! the Redis server clock (`TIME`), so node clocks don't matter. The hash
//! never expires on its own: an expired lease keeps its epoch and ISR.

use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::{AcquireRequest, LeaderLease, LeaseError, LeaseRecord, Refresh};

const NOW_LUA: &str = r#"
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
"#;

const ACQUIRE_LUA: &str = r#"
local cur = redis.call('HMGET', KEYS[1], 'holder', 'epoch', 'exp', 'isr')
local epoch = 0
local isr = ''
if cur[1] then
  if tonumber(cur[3]) > now and cur[1] ~= ARGV[1] then return false end
  isr = cur[4] or ''
  if ARGV[5] == '1' and isr ~= '' and
     not string.find(',' .. isr .. ',', ',' .. ARGV[1] .. ',', 1, true) then
    return false
  end
  epoch = tonumber(cur[2])
end
epoch = epoch + 1
local exp = now + tonumber(ARGV[2])
redis.call('HSET', KEYS[1], 'name', ARGV[6], 'holder', ARGV[1], 'epoch', epoch, 'exp', exp,
           'repl', ARGV[3], 'client', ARGV[4], 'isr', isr)
redis.call('SADD', KEYS[2], ARGV[6])
return {epoch, exp}
"#;

const REFRESH_LUA: &str = r#"
local cur = redis.call('HMGET', KEYS[1], 'holder', 'epoch', 'exp')
if cur[1] == ARGV[1] and cur[2] == ARGV[2] and tonumber(cur[3]) > now then
  redis.call('HSET', KEYS[1], 'exp', now + tonumber(ARGV[3]))
  return 1
end
return 0
"#;

const RELEASE_LUA: &str = r#"
local cur = redis.call('HMGET', KEYS[1], 'holder', 'epoch')
if cur[1] == ARGV[1] and cur[2] == ARGV[2] then
  redis.call('HSET', KEYS[1], 'exp', now - 1)
  return 1
end
return 0
"#;

const SET_ISR_LUA: &str = r#"
local cur = redis.call('HMGET', KEYS[1], 'holder', 'epoch', 'exp')
if cur[1] == ARGV[1] and cur[2] == ARGV[2] and tonumber(cur[3]) > now then
  redis.call('HSET', KEYS[1], 'isr', ARGV[3])
  return 1
end
return 0
"#;

pub struct RedisLeaseBackend {
    client: redis::Client,
    conn: Mutex<Option<redis::aio::MultiplexedConnection>>,
    prefix: String,
    call_timeout: Duration,
}

impl RedisLeaseBackend {
    pub async fn connect(
        url: &str,
        prefix: &str,
        call_timeout: Duration,
    ) -> Result<Self, LeaseError> {
        let client = redis::Client::open(url)
            .map_err(|e| LeaseError::Connection(format!("redis client: {e}")))?;
        let b = Self {
            client,
            conn: Mutex::new(None),
            prefix: prefix.to_string(),
            call_timeout,
        };
        b.connection().await?;
        Ok(b)
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, LeaseError> {
        let mut guard = self.conn.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let c = tokio::time::timeout(
            self.call_timeout,
            self.client.get_multiplexed_async_connection(),
        )
        .await
        .map_err(|_| LeaseError::Timeout)?
        .map_err(|e| LeaseError::Connection(format!("redis connect: {e}")))?;
        *guard = Some(c.clone());
        Ok(c)
    }

    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    fn index(&self) -> String {
        format!("{}__names", self.prefix)
    }

    async fn script<T: redis::FromRedisValue>(
        &self,
        body: &str,
        keys: &[String],
        args: &[String],
    ) -> Result<T, LeaseError> {
        let mut conn = self.connection().await?;
        let script = redis::Script::new(&format!("{NOW_LUA}{body}"));
        let mut inv = script.prepare_invoke();
        for k in keys {
            inv.key(k);
        }
        for a in args {
            inv.arg(a);
        }
        match tokio::time::timeout(self.call_timeout, inv.invoke_async(&mut conn)).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                *self.conn.lock().await = None;
                Err(LeaseError::Backend(e.to_string()))
            }
            Err(_) => {
                *self.conn.lock().await = None;
                Err(LeaseError::Timeout)
            }
        }
    }

    async fn cmd<T: redis::FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T, LeaseError> {
        let mut conn = self.connection().await?;
        match tokio::time::timeout(self.call_timeout, cmd.query_async(&mut conn)).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                *self.conn.lock().await = None;
                Err(LeaseError::Backend(e.to_string()))
            }
            Err(_) => {
                *self.conn.lock().await = None;
                Err(LeaseError::Timeout)
            }
        }
    }

    async fn read(&self, name: &str) -> Result<Option<LeaseRecord>, LeaseError> {
        let mut c = redis::cmd("HMGET");
        c.arg(self.key(name))
            .arg("holder")
            .arg("epoch")
            .arg("exp")
            .arg("repl")
            .arg("client")
            .arg("isr");
        let v: Vec<Option<String>> = self.cmd(c).await?;
        let Some(holder) = v.first().cloned().flatten() else {
            return Ok(None);
        };
        let field = |i: usize| v.get(i).cloned().flatten().unwrap_or_default();
        let opt = |s: String| if s.is_empty() { None } else { Some(s) };
        let exp_ms: i64 = field(2).parse().unwrap_or(0);
        Ok(Some(LeaseRecord {
            name: name.to_string(),
            holder,
            epoch: field(1).parse().unwrap_or(0),
            expires_at: chrono::DateTime::from_timestamp_millis(exp_ms).unwrap_or_default(),
            replication_endpoint: opt(field(3)),
            client_endpoint: opt(field(4)),
            isr: field(5)
                .split(',')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
        }))
    }
}

#[async_trait]
impl LeaderLease for RedisLeaseBackend {
    fn supports_coordination(&self) -> bool {
        true
    }

    async fn try_acquire(&self, req: &AcquireRequest) -> Result<Option<LeaseRecord>, LeaseError> {
        let won: Option<(i64, i64)> = self
            .script(
                ACQUIRE_LUA,
                &[self.key(&req.name), self.index()],
                &[
                    req.holder.clone(),
                    req.ttl.as_millis().to_string(),
                    req.replication_endpoint.clone().unwrap_or_default(),
                    req.client_endpoint.clone().unwrap_or_default(),
                    if req.require_isr { "1" } else { "0" }.to_string(),
                    req.name.clone(),
                ],
            )
            .await?;
        if won.is_none() {
            return Ok(None);
        }
        self.read(&req.name).await
    }

    async fn refresh(
        &self,
        name: &str,
        holder: &str,
        epoch: u64,
        ttl: Duration,
    ) -> Result<Refresh, LeaseError> {
        let ok: i64 = self
            .script(
                REFRESH_LUA,
                &[self.key(name)],
                &[
                    holder.to_string(),
                    epoch.to_string(),
                    ttl.as_millis().to_string(),
                ],
            )
            .await?;
        Ok(if ok == 1 {
            Refresh::Held
        } else {
            Refresh::Lost
        })
    }

    async fn release(&self, name: &str, holder: &str, epoch: u64) -> Result<(), LeaseError> {
        let _: i64 = self
            .script(
                RELEASE_LUA,
                &[self.key(name)],
                &[holder.to_string(), epoch.to_string()],
            )
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
        let ok: i64 = self
            .script(
                SET_ISR_LUA,
                &[self.key(name)],
                &[holder.to_string(), epoch.to_string(), isr.join(",")],
            )
            .await?;
        Ok(ok == 1)
    }

    async fn get(&self, name: &str) -> Result<Option<LeaseRecord>, LeaseError> {
        self.read(name).await
    }

    async fn list_all(&self) -> Result<Vec<LeaseRecord>, LeaseError> {
        let mut c = redis::cmd("SMEMBERS");
        c.arg(self.index());
        let mut names: Vec<String> = self.cmd(c).await?;
        names.sort();
        let mut out = Vec::new();
        for n in names {
            if let Some(r) = self.read(&n).await? {
                if r.is_live() {
                    out.push(r);
                }
            }
        }
        Ok(out)
    }
}
