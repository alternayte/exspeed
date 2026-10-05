package io.exspeed.client;

/** What a KV revision holds. */
public enum KvOp {
  /** A value. */
  PUT,
  /** A delete tombstone (header {@code exspeed-kv-op: DEL}); the value is empty. */
  DELETE,
  /** A purge tombstone (header {@code exspeed-kv-op: PURGE}); the value is empty. */
  PURGE
}
