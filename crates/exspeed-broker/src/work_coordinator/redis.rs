use std::env;

use async_trait::async_trait;
use redis::AsyncCommands;
use tokio::sync::Mutex;

use super::{ClaimedMessage, WorkCoordinator, WorkCoordinatorError};

/// Lua script that atomically claims up to N available messages.
///
/// KEYS[1] = pending sorted-set key
/// ARGV[1] = subscriber_id
/// ARGV[2] = now_ms (current time in ms, as string of integer)
/// ARGV[3] = ack_timeout_ms (as string)
/// ARGV[4] = n (batch size, as string)
///
/// Returns a flat array: [offset1, attempts1, offset2, attempts2, ...]
const LUA_CLAIM: &str = r#"
    local key = KEYS[1]
    local sub_id = ARGV[1]
    local now_ms = tonumber(ARGV[2])
    local timeout_ms = tonumber(ARGV[3])
    local n = tonumber(ARGV[4])

    local claimed = {}
    local members = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
    local i = 1
    while i <= #members and #claimed / 2 < n do
        local raw = members[i]
        local offset = tonumber(members[i + 1])
        local data = cjson.decode(raw)
        local is_available = (not data.delivered_to)
            or (data.delivered_at_ms and (data.delivered_at_ms + timeout_ms < now_ms))

        if is_available then
            data.delivered_to = sub_id
            data.delivered_at_ms = now_ms
            data.attempts = (data.attempts or 0) + 1

            redis.call('ZREM', key, raw)
            redis.call('ZADD', key, offset, cjson.encode(data))

            table.insert(claimed, offset)
            table.insert(claimed, data.attempts)
        end
        i = i + 2
    end

    return claimed
"#;

/// Lua script that removes an acked offset and recomputes committed_offset.
///
/// KEYS[1] = pending sorted-set key
/// KEYS[2] = group state hash key
/// ARGV[1] = offset to ack (as integer string)
const LUA_ACK: &str = r#"
    local pending_key = KEYS[1]
    local state_key = KEYS[2]
    local target_offset = tonumber(ARGV[1])

    local members = redis.call('ZRANGEBYSCORE', pending_key, target_offset, target_offset)
    for i = 1, #members do
        redis.call('ZREM', pending_key, members[i])
    end

    local remaining = redis.call('ZRANGE', pending_key, 0, 0, 'WITHSCORES')
    if #remaining == 0 then
        local head = redis.call('HGET', state_key, 'delivery_head')
        if head then
            redis.call('HSET', state_key, 'committed_offset', head)
        end
    else
        local min_score = tonumber(remaining[2])
        redis.call('HSET', state_key, 'committed_offset', tostring(min_score - 1))
    end

    return 1
"#;

/// Lua script for nack: set delivered_to back to nil, preserve attempts.
///
/// KEYS[1] = pending sorted-set key
/// ARGV[1] = offset (integer string)
///
/// Returns the attempts count, or 0 if the entry was not present.
const LUA_NACK: &str = r#"
    local key = KEYS[1]
    local target_offset = tonumber(ARGV[1])
    local members = redis.call('ZRANGEBYSCORE', key, target_offset, target_offset)
    if #members == 0 then return 0 end

    local raw = members[1]
    local data = cjson.decode(raw)
    local attempts = data.attempts or 0
    data.delivered_to = nil
    data.delivered_at_ms = nil

    redis.call('ZREM', key, raw)
    redis.call('ZADD', key, target_offset, cjson.encode(data))
    return attempts
"#;

pub struct RedisWorkCoordinator {
    conn: Mutex<redis::aio::MultiplexedConnection>,
    key_prefix: String,
}

impl RedisWorkCoordinator {
    pub async fn from_env() -> Result<Self, WorkCoordinatorError> {
        let url = env::var("EXSPEED_OFFSET_STORE_REDIS_URL").map_err(|_| {
            WorkCoordinatorError::Connection(
                "EXSPEED_OFFSET_STORE_REDIS_URL is required".to_string(),
            )
        })?;
        let key_prefix = env::var("EXSPEED_WORK_COORDINATOR_REDIS_KEY_PREFIX")
            .unwrap_or_else(|_| "exspeed:coord:".to_string());

        let client = redis::Client::open(url.as_str())
            .map_err(|e| WorkCoordinatorError::Connection(format!("redis client: {e}")))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("redis connect: {e}")))?;

        Ok(Self {
            conn: Mutex::new(conn),
            key_prefix,
        })
    }

    fn state_key(&self, group: &str) -> String {
        format!("{}group:{}", self.key_prefix, group)
    }

    fn pending_key(&self, group: &str) -> String {
        format!("{}pending:{}", self.key_prefix, group)
    }

    fn now_ms() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[async_trait]
impl WorkCoordinator for RedisWorkCoordinator {
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
        let mut conn = self.conn.lock().await;
        let key = self.pending_key(group);
        let now_ms = Self::now_ms();
        let timeout_ms = ack_timeout_secs * 1000;

        let script = redis::Script::new(LUA_CLAIM);
        let result: Vec<i64> = script
            .key(&key)
            .arg(subscriber_id)
            .arg(now_ms)
            .arg(timeout_ms)
            .arg(n as u64)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("redis claim lua: {e}")))?;

        let mut claimed = Vec::with_capacity(result.len() / 2);
        let mut iter = result.iter();
        while let (Some(off), Some(att)) = (iter.next(), iter.next()) {
            claimed.push(ClaimedMessage {
                offset: *off as u64,
                attempts: *att as u32,
            });
        }
        Ok(claimed)
    }

    async fn enqueue(
        &self,
        group: &str,
        offsets: &[u64],
    ) -> Result<(), WorkCoordinatorError> {
        if offsets.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock().await;
        let pending_key = self.pending_key(group);
        let state_key = self.state_key(group);

        // For each offset, ZADD NX so we don't overwrite existing in-flight entries.
        for &offset in offsets {
            let member = r#"{"attempts":0}"#.to_string();
            let _: i64 = redis::cmd("ZADD")
                .arg(&pending_key)
                .arg("NX")
                .arg(offset as i64)
                .arg(&member)
                .query_async(&mut *conn)
                .await
                .map_err(|e| WorkCoordinatorError::Connection(format!("redis zadd: {e}")))?;
        }

        // Advance delivery_head to max(offsets) — only if higher.
        let max_offset = *offsets.iter().max().unwrap() as i64;
        let current: Option<i64> = conn
            .hget(&state_key, "delivery_head")
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("redis hget head: {e}")))?;
        let new_head = match current {
            Some(h) if h >= max_offset => h,
            _ => max_offset,
        };
        conn.hset::<_, _, _, ()>(&state_key, "delivery_head", new_head)
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("redis hset head: {e}")))?;
        Ok(())
    }

    async fn delivery_head(&self, group: &str) -> Result<u64, WorkCoordinatorError> {
        let mut conn = self.conn.lock().await;
        let state_key = self.state_key(group);
        let h: Option<i64> = conn
            .hget(&state_key, "delivery_head")
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("redis delivery_head: {e}")))?;
        Ok(h.filter(|v| *v >= 0).map(|v| v as u64).unwrap_or(0))
    }

    async fn pending_count(&self, group: &str) -> Result<usize, WorkCoordinatorError> {
        let mut conn = self.conn.lock().await;
        let pending_key = self.pending_key(group);
        let c: i64 = conn
            .zcard(&pending_key)
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("redis zcard: {e}")))?;
        Ok(c as usize)
    }

    async fn ack(&self, group: &str, offset: u64) -> Result<(), WorkCoordinatorError> {
        let mut conn = self.conn.lock().await;
        let pending_key = self.pending_key(group);
        let state_key = self.state_key(group);

        let script = redis::Script::new(LUA_ACK);
        let _: i64 = script
            .key(&pending_key)
            .key(&state_key)
            .arg(offset as i64)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("redis ack lua: {e}")))?;
        Ok(())
    }

    async fn nack(&self, group: &str, offset: u64) -> Result<u32, WorkCoordinatorError> {
        let mut conn = self.conn.lock().await;
        let pending_key = self.pending_key(group);

        let script = redis::Script::new(LUA_NACK);
        let attempts: i64 = script
            .key(&pending_key)
            .arg(offset as i64)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("redis nack lua: {e}")))?;
        Ok(attempts as u32)
    }

    async fn committed_offset(&self, group: &str) -> Result<u64, WorkCoordinatorError> {
        let mut conn = self.conn.lock().await;
        let state_key = self.state_key(group);
        let c: Option<i64> = conn
            .hget(&state_key, "committed_offset")
            .await
            .map_err(|e| WorkCoordinatorError::Connection(format!("redis committed: {e}")))?;
        Ok(c.filter(|v| *v >= 0).map(|v| v as u64).unwrap_or(0))
    }
}
