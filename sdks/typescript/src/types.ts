import type { ConnectionOptions as TlsConnectionOptions } from "node:tls";
import { ExspeedError } from "./errors.js";
import type { WirePublishRecord, WireStreamLimits, WireStreamSpec } from "./protocol/messages.js";
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
  /** Most records the stream holds; 0 = no limit. What happens at the limit is `discard`. */
  maxMsgs?: number;
  /** At `maxMsgs`: `"old"` (default) drops the oldest records, `"new"` rejects new ones. */
  discard?: "old" | "new";
  /** Most records kept per subject (older ones are removed); 0 = no limit. */
  maxMsgsPerSubject?: number;
  /** Accept the per-record `ttl` publish option (header `exspeed-ttl`). */
  allowMsgTtl?: boolean;
  /** Default lifetime of every record, in ms; 0 = none. */
  msgTtlMs?: number;
  /** Accept the `delay` / `deliverAt` publish options (delayed delivery to consumers). */
  allowDelayed?: boolean;
  /**
   * `"limits"` (default): records stay until a limit removes them.
   * `"work_queue"`: at most one consumer; a record is removed once acked.
   * `"interest"`: a record is removed once every consumer acked it.
   */
  retention?: "limits" | "work_queue" | "interest";
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
    maxMsgs?: number;
    discard?: "old" | "new";
    maxMsgsPerSubject?: number;
    allowMsgTtl?: boolean;
    msgTtlMs?: number;
    allowDelayed?: boolean;
    retention?: "limits" | "work_queue" | "interest";
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
    limits: toWireStreamLimits(s),
  };
}

/** The limits trailer, or `null` when every limit is at its default (then nothing extra is sent). */
function toWireStreamLimits(s: StreamSpec): WireStreamLimits | null {
  const limits: WireStreamLimits = {
    max_msgs: s.maxMsgs ?? 0,
    discard: s.discard ?? "old",
    max_msgs_per_subject: s.maxMsgsPerSubject ?? 0,
    allow_msg_ttl: s.allowMsgTtl ?? false,
    msg_ttl_ms: s.msgTtlMs ?? 0,
    allow_delayed: s.allowDelayed ?? false,
    retention: s.retention ?? "limits",
  };
  const isDefault =
    limits.max_msgs === 0 &&
    limits.discard === "old" &&
    limits.max_msgs_per_subject === 0 &&
    !limits.allow_msg_ttl &&
    limits.msg_ttl_ms === 0 &&
    !limits.allow_delayed &&
    limits.retention === "limits";
  return isDefault ? null : limits;
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
  /**
   * Expire the record this long after it is appended: ms, or a duration
   * string such as `"30s"` (units ms, s, m, h, d). The stream needs
   * `allowMsgTtl`. Sent as the header `exspeed-ttl`.
   */
  ttl?: number | string;
  /**
   * Deliver to consumers no earlier than this long after the append: ms, or
   * a duration string. The stream needs `allowDelayed`. Header `exspeed-delay`.
   */
  delay?: number | string;
  /**
   * Deliver to consumers no earlier than this time (ms since the epoch, or
   * a Date). The stream needs `allowDelayed`. Header `exspeed-deliver-at`.
   */
  deliverAt?: number | Date;
  /**
   * 0 (default) to 9, higher first, for consumers with a `priorityWindow`.
   * Header `exspeed-priority`.
   */
  priority?: number;
}

/** Header names behind the time and priority publish options. */
export const TTL_HEADER = "exspeed-ttl";
export const DELAY_HEADER = "exspeed-delay";
export const DELIVER_AT_HEADER = "exspeed-deliver-at";
export const PRIORITY_HEADER = "exspeed-priority";

const DURATION = /^\d+\s*(ms|s|m|h|d)?$/;

/** A duration header value: whole ms as `"<n>ms"`, a duration string as-is. */
function durationHeader(name: string, v: number | string, min: number): string {
  if (typeof v === "number") {
    if (!Number.isFinite(v) || v < 0) throw new ExspeedError(`${name} must be a non-negative number of ms, got ${v}`);
    return `${Math.max(min, Math.ceil(v))}ms`;
  }
  const t = v.trim();
  if (!DURATION.test(t)) {
    throw new ExspeedError(`invalid ${name} '${v}': expected a number with an optional unit (ms, s, m, h, d)`);
  }
  return t;
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
  const headers = toHeaders(r.headers);
  if (r.ttl !== undefined) headers.push([TTL_HEADER, durationHeader("ttl", r.ttl, 1)]);
  if (r.delay !== undefined) headers.push([DELAY_HEADER, durationHeader("delay", r.delay, 0)]);
  if (r.deliverAt !== undefined) {
    const t = r.deliverAt instanceof Date ? r.deliverAt.getTime() : r.deliverAt;
    if (!Number.isSafeInteger(t) || t < 0) throw new ExspeedError(`invalid deliverAt: ${String(r.deliverAt)}`);
    headers.push([DELIVER_AT_HEADER, String(t)]);
  }
  if (r.priority !== undefined) {
    if (!Number.isInteger(r.priority) || r.priority < 0 || r.priority > 9) {
      throw new ExspeedError(`priority must be an integer from 0 to 9, got ${r.priority}`);
    }
    headers.push([PRIORITY_HEADER, String(r.priority)]);
  }
  return {
    subject: r.subject,
    key: r.key === undefined ? null : encodeValue(r.key),
    value: encodeValue(r.value),
    headers,
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
  /**
   * Dead-letter records whose TTL expires before they are acked (to
   * `dlqStream`, reason `expired`) instead of dropping them silently.
   */
  deadLetterExpired?: boolean;
  /** Only records whose headers have these exact values, combined by `headerMatch`. */
  filterHeaders?: Record<string, string>;
  /** `"all"` (default): every `filterHeaders` entry must match. `"any"`: at least one. */
  headerMatch?: "all" | "any";
  /**
   * Deliver to one subscription at a time (the oldest connected); the next
   * takes over when it goes away. Pulls are refused.
   */
  singleActive?: boolean;
  /**
   * Look this many records ahead and deliver higher `priority` first
   * (0 = strictly in order). Server maximum 10 000.
   */
  priorityWindow?: number;
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
  /** Records held back until their delivery time (`delay` / `deliverAt`). */
  numDelayed: number;
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
  set("dead_letter_expired", spec.deadLetterExpired);
  if (spec.filterHeaders && Object.keys(spec.filterHeaders).length > 0) {
    out.filter_headers = Object.fromEntries(Object.entries(spec.filterHeaders).map(([k, v]) => [k, String(v)]));
  }
  set("header_match", spec.headerMatch);
  set("single_active", spec.singleActive);
  set("priority_window", spec.priorityWindow);
  return out;
}

/**
 * camelize a consumer info reply, keeping `filter_headers` keys (header
 * names) as they are, and `{}` when the server omitted it.
 */
export function toConsumerInfo(raw: unknown): ConsumerInfo {
  const info = camelize<ConsumerInfo>(raw);
  const rawSpec = (raw as { spec?: { filter_headers?: Record<string, string> } } | null)?.spec;
  if (info && typeof info === "object" && info.spec) info.spec.filterHeaders = { ...(rawSpec?.filter_headers ?? {}) };
  return info;
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
// Core messaging
// ---------------------------------------------------------------------------

export interface CorePublishOptions {
  headers?: HeadersInit;
  /**
   * Ask receivers to answer on this subject. The publish then fails with
   * `ServerError` 404 when nobody received it. `request()` sets this for you.
   */
  replyTo?: string;
}

export interface CoreSubscribeOptions {
  /** Queue group: each message goes to one member of the group. */
  queue?: string;
}

export interface CoreRequestOptions {
  /** How long to wait for the first response. Default: the client's `requestTimeoutMs`. */
  timeoutMs?: number;
  headers?: HeadersInit;
}

// ---------------------------------------------------------------------------
// Key-value buckets
// ---------------------------------------------------------------------------

export interface KvBucketOptions {
  /** Values kept per key, 1 to 64. Default 1. */
  history?: number;
  /** Keys expire this long after their last put (ms). Default: never. */
  ttlMs?: number;
  /** Size limit in bytes. Default: the server's. */
  maxBytes?: number;
}

export interface KvPutOptions {
  /** Expire this key this long after the put (ms). */
  ttlMs?: number;
  /** Only if the key is at this revision (0 = absent), else `ServerError` 409. */
  expectedRevision?: number;
}

export interface KvDeleteOptions {
  /** Only if the key is at this revision, else `ServerError` 409. */
  expectedRevision?: number;
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
