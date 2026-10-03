import type { ConnectionOptions as TlsConnectionOptions } from "node:tls";
import { ExspeedError } from "./errors.js";
import type { WirePublishRecord, WireStreamSpec } from "./protocol/messages.js";
import type { Headers } from "./protocol/buffer.js";

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

export interface ReconnectOptions {
  /** Give up after this many failed attempts in a row. Default: unlimited. */
  maxAttempts?: number;
  /** Delay before the first attempt; doubles each attempt. Default 100 ms. */
  initialDelayMs?: number;
  /** Upper bound for the delay between attempts. Default 5000 ms. */
  maxDelayMs?: number;
}

export interface ClientOptions {
  /** Default `127.0.0.1`. */
  host?: string;
  /** Default `5933`. */
  port?: number;
  /**
   * Cluster seed addresses (`"host:port"`). When set, the client connects to
   * whichever node is the leader, following the leader hints followers
   * return, and finds the new leader again after a failover. Overrides
   * `host`/`port`.
   */
  servers?: string[];
  /** Bearer token, when the server runs with auth. */
  token?: string;
  /**
   * `true` for TLS with the system CAs, or Node `tls.connect` options
   * (`ca`, `cert`, `key`, `servername`, `rejectUnauthorized`, ...).
   */
  tls?: boolean | TlsConnectionOptions;
  /** Sent in the handshake and shown in server logs. Default `exspeed-ts`. */
  clientId?: string;
  /** How long to wait for a response (on top of a pull's or read's own wait). Default 30 000 ms. */
  requestTimeoutMs?: number;
  /** Ping interval; the server drops connections idle for 120 s. `0` disables. Default 20 000 ms. */
  keepaliveMs?: number;
  /**
   * Reconnect automatically when the connection drops (default `true`).
   * Pending requests fail with `ConnectionError`; subscriptions are
   * re-established. See the README section "Reconnection".
   */
  reconnect?: boolean | ReconnectOptions;
}

/** From the server's handshake reply. */
export interface ServerInfo {
  serverVersion: string;
  nodeId: string;
  /** The leader's client address when the connected node is not the leader. */
  leader: string | null;
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

/** Stream settings. Omitted (or 0) numeric fields mean "server default". */
export interface StreamSpec {
  name: string;
  maxAgeSecs?: number;
  maxBytes?: number;
  dedupWindowSecs?: number;
  dedupMaxEntries?: number;
  /** Keep only the latest record per key. */
  compaction?: boolean;
}

export interface StreamInfo {
  name: string;
  earliestOffset: number;
  nextOffset: number;
  records: number;
  config: {
    maxAgeSecs: number;
    maxBytes: number;
    dedupWindowSecs: number;
    dedupMaxEntries: number;
    [k: string]: unknown;
  };
  internal: boolean;
  [k: string]: unknown;
}

export function toWireStreamSpec(spec: StreamSpec | string): WireStreamSpec {
  const s = typeof spec === "string" ? { name: spec } : spec;
  if (!s.name) throw new ExspeedError("stream name is required");
  return {
    name: s.name,
    maxAgeSecs: s.maxAgeSecs ?? 0,
    maxBytes: s.maxBytes ?? 0,
    dedupWindowSecs: s.dedupWindowSecs ?? 0,
    dedupMaxEntries: s.dedupMaxEntries ?? 0,
    compaction: s.compaction ?? false,
  };
}

// ---------------------------------------------------------------------------
// Publishing
// ---------------------------------------------------------------------------

/**
 * A record value: bytes are sent as-is, strings as UTF-8, anything else is
 * JSON-encoded.
 */
export type Value = Uint8Array | string | number | boolean | null | object;

export type HeadersInit = Record<string, string> | Array<[string, string]>;

export interface PublishInput {
  subject: string;
  value: Value;
  /** Partition/compaction key. */
  key?: Uint8Array | string;
  headers?: HeadersInit;
  /**
   * Idempotency key: a retry with the same `msgId` and body returns the
   * original offset with `duplicate: true` instead of writing again. See
   * `newMsgId()`.
   */
  msgId?: string;
}

export interface PublishResult {
  offset: number;
  /** True when `msgId` matched an earlier publish; nothing was written. */
  duplicate: boolean;
}

export function encodeValue(value: Value): Buffer {
  if (value instanceof Uint8Array) {
    return Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  }
  if (typeof value === "string") return Buffer.from(value, "utf8");
  const json = JSON.stringify(value);
  if (json === undefined) throw new ExspeedError(`value is not JSON-serializable: ${String(value)}`);
  return Buffer.from(json, "utf8");
}

export function toHeaders(h: HeadersInit | undefined): Headers {
  if (!h) return [];
  if (Array.isArray(h)) return h.map(([k, v]) => [String(k), String(v)]);
  return Object.entries(h).map(([k, v]) => [k, String(v)]);
}

export function toWirePublishRecord(r: PublishInput): WirePublishRecord {
  if (!r || typeof r.subject !== "string") throw new ExspeedError("record.subject is required");
  return {
    subject: r.subject,
    key: r.key === undefined ? null : encodeValue(r.key),
    value: encodeValue(r.value),
    headers: toHeaders(r.headers),
    msgId: r.msgId ?? null,
  };
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

export interface ReadOptions {
  /** First offset to read. Default 0. */
  from?: number;
  /** Default 100 (the server caps it at 10 000). */
  maxRecords?: number;
  /** Byte budget per response; 0 = server default (1 MiB). */
  maxBytes?: number;
  /** Long-poll: when caught up, wait up to this long for new records. Default 0. */
  waitMs?: number;
  /** NATS-style subject filter (`orders.*`, `orders.>`). Default: all. */
  filter?: string;
}

// ---------------------------------------------------------------------------
// Consumers
// ---------------------------------------------------------------------------

/** Where a new consumer starts. */
export type DeliverPolicy =
  | "all"
  | "new"
  | { fromOffset: number }
  /** First record at or after this time (ms since epoch, or a Date). */
  | { fromTime: number | Date };

export interface ConsumerSpec {
  name: string;
  stream: string;
  /** Subject filters (`orders.placed`, `orders.eu.>`). Empty = all. */
  filterSubjects?: string[];
  /** Default `"all"`. */
  deliver?: DeliverPolicy;
  /** `"explicit"` (default): each record must be acked. `"none"`: at-most-once. */
  ack?: "explicit" | "none";
  /** Redeliver a record not acked within this time. Server default 30 000. */
  ackWaitMs?: number;
  /** Dead-letter after this many deliveries; 0 = never. Server default 5. */
  maxDeliver?: number;
  /** Redelivery delays by delivery count (the last repeats). Empty = immediately. */
  backoffMs?: number[];
  /** Pause delivery while this many records await an ack. Server default 1000. */
  maxAckPending?: number;
  /** Stream that receives dead letters. Unset = they are dropped (and counted). */
  dlqStream?: string;
  /** Deleted when the connection that created it closes. */
  ephemeral?: boolean;
}

export interface ConsumerInfo {
  spec: Required<Omit<ConsumerSpec, "dlqStream" | "deliver">> & {
    dlqStream: string | null;
    deliver: "all" | "new" | { fromOffset: number } | { fromTime: number };
  };
  /** Next stream offset to be delivered for the first time. */
  nextOffset: number;
  /** Everything below this offset is acked (or filtered out). */
  ackFloor: number;
  numUnacked: number;
  numInFlight: number;
  numWaiting: number;
  lag: number;
  subscribers: number;
  pullWaiters: number;
  stats: {
    delivered: number;
    redelivered: number;
    acked: number;
    deadLettered: number;
    gone: number;
    skipped: number;
  };
  [k: string]: unknown;
}

/**
 * The snake_case JSON the server expects, keys in the same order as the
 * Rust `ConsumerSpec` (so a fully specified spec serializes byte-for-byte
 * like the Rust client's). Omitted fields take the server's defaults.
 */
export function toWireConsumerSpec(spec: ConsumerSpec): Record<string, unknown> {
  if (!spec?.name) throw new ExspeedError("consumer name is required");
  if (!spec.stream) throw new ExspeedError("consumer stream is required");
  const out: Record<string, unknown> = { name: spec.name, stream: spec.stream };
  const set = (k: string, v: unknown) => {
    if (v !== undefined) out[k] = v;
  };
  set("filter_subjects", spec.filterSubjects);
  set("deliver", spec.deliver === undefined ? undefined : toWireDeliver(spec.deliver));
  set("ack", spec.ack);
  set("ack_wait_ms", spec.ackWaitMs);
  set("max_deliver", spec.maxDeliver);
  set("backoff_ms", spec.backoffMs);
  set("max_ack_pending", spec.maxAckPending);
  set("dlq_stream", spec.dlqStream);
  set("ephemeral", spec.ephemeral);
  return out;
}

function toWireDeliver(d: DeliverPolicy): unknown {
  if (d === "all" || d === "new") return d;
  if ("fromOffset" in d) return { from_offset: d.fromOffset };
  if ("fromTime" in d) {
    const t = d.fromTime instanceof Date ? d.fromTime.getTime() : d.fromTime;
    return { from_time: t };
  }
  throw new ExspeedError(`invalid deliver policy: ${JSON.stringify(d)}`);
}

/** Where to move a consumer's cursor. */
export type SeekTarget =
  | "earliest"
  | "latest"
  | { earliest: true }
  | { latest: true }
  | { offset: number }
  /** Ms since epoch, or a Date. */
  | { timeMs: number | Date };

export interface SubscribeOptions {
  /**
   * Credit window: how many records the server may push before the app has
   * consumed them. The SDK returns credit as you iterate. Default 256.
   */
  window?: number;
}

export interface PullOptions {
  /** Default 100. */
  maxMessages?: number;
  /** Byte budget; 0 = server default. */
  maxBytes?: number;
  /** Wait up to this long for at least one message. Default 5000 ms. */
  expiresMs?: number;
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  rowCount: number;
  executionTimeMs: number;
}

export interface Metadata {
  nodeId: string;
  isLeader: boolean;
  leader: string | null;
  serverVersion: string;
}

/** Recursively convert snake_case object keys to camelCase. */
export function camelize<T = unknown>(v: unknown): T {
  if (Array.isArray(v)) return v.map((x) => camelize(x)) as T;
  if (v && typeof v === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, x] of Object.entries(v)) {
      out[k.replace(/_([a-z0-9])/g, (_, c: string) => c.toUpperCase())] = camelize(x);
    }
    return out as T;
  }
  return v as T;
}
