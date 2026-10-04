/**
 * Every request and response of client protocol v2 with its binary
 * encoding. This mirrors `Request` / `Response` in
 * `crates/exspeed-protocol/src/client.rs`, in both directions (the server
 * side is used by the unit tests' fake server).
 */
import { ProtocolError } from "../errors.js";
import { Reader, Writer, type Headers } from "./buffer.js";
import { crc32c } from "./crc32c.js";
import { OpCode } from "./constants.js";
import { encodeFrame } from "./frame.js";

// ---------------------------------------------------------------------------
// Shared structures
// ---------------------------------------------------------------------------

/** `PublishRecord`: str subject, opt<bytes> key, bytes value, headers, opt<str> msg_id. */
export interface WirePublishRecord {
  subject: string;
  key: Uint8Array | null;
  value: Uint8Array;
  headers: Headers;
  msgId: string | null;
}

/**
 * `WireRecord`: u32 len (bytes after this field), u32 crc (CRC32C of the
 * bytes after delivery_count), u16 delivery_count, u64 offset, u64
 * timestamp_ns, str subject, opt<bytes> key, bytes value, headers. This is
 * also how the server stores records on disk.
 */
export interface WireRecord {
  offset: number;
  /** Append time, nanoseconds since the Unix epoch. */
  timestampNs: bigint;
  deliveryCount: number;
  subject: string;
  key: Buffer | null;
  value: Buffer;
  headers: Headers;
}

/**
 * `StreamLimits` (`crates/exspeed-common/src/limits.rs`) exactly as serde
 * serializes it: snake_case keys, in declaration order.
 */
export interface WireStreamLimits {
  max_msgs: number;
  discard: "old" | "new";
  max_msgs_per_subject: number;
  allow_msg_ttl: boolean;
  msg_ttl_ms: number;
  allow_delayed: boolean;
  retention: "limits" | "work_queue" | "interest";
}

/**
 * `StreamSpec`: str name, then u64 fields (0 = server default) and a u8
 * flag, then `bytes(JSON)` of the {@link WireStreamLimits} only when they
 * are not all defaults (so older servers keep accepting the request).
 */
export interface WireStreamSpec {
  name: string;
  maxAgeSecs: number;
  maxBytes: number;
  dedupWindowSecs: number;
  dedupMaxEntries: number;
  compaction: boolean;
  /** Absent (or null) = every limit at its default; nothing is sent. */
  limits?: WireStreamLimits | null;
}

/** Seek kinds: 0 earliest, 1 latest, 2 offset, 3 time (ms since epoch). */
export enum SeekKind {
  Earliest = 0,
  Latest = 1,
  Offset = 2,
  Time = 3,
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

export type Request =
  | { type: "Connect"; clientId: string; token: string | null }
  | { type: "Ping" }
  | { type: "Metadata" }
  | { type: "Publish"; stream: string; record: WirePublishRecord }
  | { type: "PublishBatch"; stream: string; records: WirePublishRecord[] }
  | { type: "CreateStream"; spec: WireStreamSpec }
  | { type: "UpdateStream"; spec: WireStreamSpec }
  | { type: "DeleteStream"; name: string }
  | { type: "StreamInfo"; name: string }
  | { type: "ListStreams" }
  | { type: "Query"; sql: string }
  /** `spec` is the snake_case ConsumerSpec JSON object. */
  | { type: "CreateConsumer"; spec: Record<string, unknown> }
  | { type: "DeleteConsumer"; name: string }
  | { type: "ConsumerInfo"; name: string }
  | { type: "ListConsumers"; stream: string | null }
  | { type: "SeekConsumer"; consumer: string; kind: SeekKind; value: number }
  | { type: "Subscribe"; consumer: string; credits: number }
  | { type: "Credit"; subId: number; credits: number }
  | { type: "Unsubscribe"; subId: number }
  | { type: "Pull"; consumer: string; maxMessages: number; maxBytes: number; expiresMs: number }
  | { type: "Ack"; consumer: string; offsets: number[] }
  | { type: "Nack"; consumer: string; offset: number; delayMs: number }
  | { type: "Term"; consumer: string; offset: number; reason: string }
  | { type: "InProgress"; consumer: string; offsets: number[] }
  | {
      type: "Read";
      stream: string;
      from: number;
      maxRecords: number;
      maxBytes: number;
      waitMs: number;
      filter: string;
    }
  /** Correlation id 0 = fire-and-forget; with `replyTo`, 404 when nobody received it. */
  | { type: "CorePublish"; subject: string; replyTo: string | null; headers: Headers; value: Uint8Array }
  /** Answered with `SubscribeOk` (core sub ids have the high bit set). */
  | { type: "CoreSubscribe"; subject: string; queue: string | null }
  | { type: "KvCreateBucket"; bucket: string; history: number; ttlMs: number; maxBytes: number }
  /** Answered with `PublishOk` (offset = the new revision). */
  | {
      type: "KvPut";
      bucket: string;
      key: string;
      value: Uint8Array;
      expectedRevision: number | null;
      ttlMs: number | null;
    }
  /** Answered with `Messages` (one record, raw stream offset) or 404. */
  | { type: "KvGet"; bucket: string; key: string; revision: number | null }
  | { type: "KvDelete"; bucket: string; key: string; purge: boolean; expectedRevision: number | null }
  /** Answered with a JSON array of strings. */
  | { type: "KvKeys"; bucket: string; filter: string }
  | { type: "KvHistory"; bucket: string; key: string };

export type RequestType = Request["type"];

const REQUEST_OPCODES: Record<RequestType, OpCode> = {
  Connect: OpCode.Connect,
  Ping: OpCode.Ping,
  Metadata: OpCode.Metadata,
  Publish: OpCode.Publish,
  PublishBatch: OpCode.PublishBatch,
  CreateStream: OpCode.CreateStream,
  UpdateStream: OpCode.UpdateStream,
  DeleteStream: OpCode.DeleteStream,
  StreamInfo: OpCode.StreamInfo,
  ListStreams: OpCode.ListStreams,
  Query: OpCode.Query,
  CreateConsumer: OpCode.CreateConsumer,
  DeleteConsumer: OpCode.DeleteConsumer,
  ConsumerInfo: OpCode.ConsumerInfo,
  ListConsumers: OpCode.ListConsumers,
  SeekConsumer: OpCode.SeekConsumer,
  Subscribe: OpCode.Subscribe,
  Credit: OpCode.Credit,
  Unsubscribe: OpCode.Unsubscribe,
  Pull: OpCode.Pull,
  Ack: OpCode.Ack,
  Nack: OpCode.Nack,
  Term: OpCode.Term,
  InProgress: OpCode.InProgress,
  Read: OpCode.Read,
  CorePublish: OpCode.CorePublish,
  CoreSubscribe: OpCode.CoreSubscribe,
  KvCreateBucket: OpCode.KvCreateBucket,
  KvPut: OpCode.KvPut,
  KvGet: OpCode.KvGet,
  KvDelete: OpCode.KvDelete,
  KvKeys: OpCode.KvKeys,
  KvHistory: OpCode.KvHistory,
};

export function requestOpcode(req: Request): OpCode {
  return REQUEST_OPCODES[req.type];
}

function writePublishRecord(w: Writer, r: WirePublishRecord): void {
  w.str(r.subject);
  w.opt(r.key, (w, k) => w.bytes(k));
  w.bytes(r.value);
  w.headers(r.headers);
  w.opt(r.msgId, (w, m) => w.str(m));
}

function writeStreamSpec(w: Writer, s: WireStreamSpec): void {
  w.str(s.name);
  w.u64(s.maxAgeSecs);
  w.u64(s.maxBytes);
  w.u64(s.dedupWindowSecs);
  w.u64(s.dedupMaxEntries);
  w.u8(s.compaction ? 1 : 0);
  if (s.limits) w.bytes(Buffer.from(JSON.stringify(s.limits), "utf8"));
}

function writeOffsets(w: Writer, offsets: number[]): void {
  w.u32(offsets.length);
  for (const o of offsets) w.u64(o);
}

/** Encode a request's payload (no frame header). */
export function encodeRequest(req: Request): Buffer {
  const w = new Writer();
  switch (req.type) {
    case "Connect":
      w.str(req.clientId);
      w.opt(req.token, (w, t) => w.str(t));
      break;
    case "Ping":
    case "Metadata":
    case "ListStreams":
      break;
    case "Publish":
      w.str(req.stream);
      writePublishRecord(w, req.record);
      break;
    case "PublishBatch":
      w.str(req.stream);
      w.u32(req.records.length);
      for (const r of req.records) writePublishRecord(w, r);
      break;
    case "CreateStream":
    case "UpdateStream":
      writeStreamSpec(w, req.spec);
      break;
    case "DeleteStream":
    case "StreamInfo":
    case "DeleteConsumer":
    case "ConsumerInfo":
      w.str(req.name);
      break;
    case "Query":
      w.lstr(req.sql);
      break;
    case "CreateConsumer":
      w.bytes(Buffer.from(JSON.stringify(req.spec), "utf8"));
      break;
    case "ListConsumers":
      w.opt(req.stream, (w, s) => w.str(s));
      break;
    case "SeekConsumer":
      w.str(req.consumer);
      w.u8(req.kind);
      w.u64(req.value);
      break;
    case "Subscribe":
      w.str(req.consumer);
      w.u32(req.credits);
      break;
    case "Credit":
      w.u32(req.subId);
      w.u32(req.credits);
      break;
    case "Unsubscribe":
      w.u32(req.subId);
      break;
    case "Pull":
      w.str(req.consumer);
      w.u32(req.maxMessages);
      w.u32(req.maxBytes);
      w.u32(req.expiresMs);
      break;
    case "Ack":
    case "InProgress":
      w.str(req.consumer);
      writeOffsets(w, req.offsets);
      break;
    case "Nack":
      w.str(req.consumer);
      w.u64(req.offset);
      w.u32(req.delayMs);
      break;
    case "Term":
      w.str(req.consumer);
      w.u64(req.offset);
      w.str(req.reason);
      break;
    case "Read":
      w.str(req.stream);
      w.u64(req.from);
      w.u32(req.maxRecords);
      w.u32(req.maxBytes);
      w.u32(req.waitMs);
      w.str(req.filter);
      break;
    case "CorePublish":
      w.str(req.subject);
      w.opt(req.replyTo, (w, r) => w.str(r));
      w.headers(req.headers);
      w.bytes(req.value);
      break;
    case "CoreSubscribe":
      w.str(req.subject);
      w.opt(req.queue, (w, q) => w.str(q));
      break;
    case "KvCreateBucket":
      w.str(req.bucket);
      w.u64(req.history);
      w.u64(req.ttlMs);
      w.u64(req.maxBytes);
      break;
    case "KvPut":
      w.str(req.bucket);
      w.str(req.key);
      w.bytes(req.value);
      w.opt(req.expectedRevision, (w, r) => w.u64(r));
      w.opt(req.ttlMs, (w, t) => w.u64(t));
      break;
    case "KvGet":
      w.str(req.bucket);
      w.str(req.key);
      w.opt(req.revision, (w, r) => w.u64(r));
      break;
    case "KvDelete":
      w.str(req.bucket);
      w.str(req.key);
      w.u8(req.purge ? 1 : 0);
      w.opt(req.expectedRevision, (w, r) => w.u64(r));
      break;
    case "KvKeys":
      w.str(req.bucket);
      w.str(req.filter);
      break;
    case "KvHistory":
      w.str(req.bucket);
      w.str(req.key);
      break;
    default: {
      const never: never = req;
      throw new ProtocolError(`unknown request ${(never as Request).type}`);
    }
  }
  return w.finish();
}

/** Encode a request as a complete frame. */
export function requestFrame(req: Request, correlationId: number): Buffer {
  return encodeFrame(requestOpcode(req), correlationId, encodeRequest(req));
}

function readPublishRecord(r: Reader): WirePublishRecord {
  return {
    subject: r.str(),
    key: r.opt((r) => r.bytes()),
    value: r.bytes(),
    headers: r.headers(),
    msgId: r.opt((r) => r.str()),
  };
}

function readStreamSpec(r: Reader): WireStreamSpec {
  const spec: WireStreamSpec = {
    name: r.str(),
    maxAgeSecs: r.u64(),
    maxBytes: r.u64(),
    dedupWindowSecs: r.u64(),
    dedupMaxEntries: r.u64(),
    compaction: r.u8() !== 0,
  };
  if (r.remaining > 0) {
    try {
      spec.limits = JSON.parse(r.bytes().toString("utf8")) as WireStreamLimits;
    } catch (e) {
      throw new ProtocolError(`invalid stream limits: ${(e as Error).message}`);
    }
  }
  return spec;
}

function readOffsets(r: Reader): number[] {
  const n = r.count(8);
  const out: number[] = [];
  for (let i = 0; i < n; i++) out.push(r.u64());
  return out;
}

/** Decode a request payload (what the server does). */
export function decodeRequest(opcode: number, payload: Buffer): Request {
  const r = new Reader(payload);
  let req: Request;
  switch (opcode) {
    case OpCode.Connect:
      req = { type: "Connect", clientId: r.str(), token: r.opt((r) => r.str()) };
      break;
    case OpCode.Ping:
      req = { type: "Ping" };
      break;
    case OpCode.Metadata:
      req = { type: "Metadata" };
      break;
    case OpCode.Publish:
      req = { type: "Publish", stream: r.str(), record: readPublishRecord(r) };
      break;
    case OpCode.PublishBatch: {
      const stream = r.str();
      // Smallest record: 2 + 1 + 4 + 2 + 1 = 10 bytes. (The Rust decoder
      // currently uses 16, which rejects batches of very small records.)
      const n = r.count(10);
      const records: WirePublishRecord[] = [];
      for (let i = 0; i < n; i++) records.push(readPublishRecord(r));
      req = { type: "PublishBatch", stream, records };
      break;
    }
    case OpCode.CreateStream:
      req = { type: "CreateStream", spec: readStreamSpec(r) };
      break;
    case OpCode.UpdateStream:
      req = { type: "UpdateStream", spec: readStreamSpec(r) };
      break;
    case OpCode.DeleteStream:
      req = { type: "DeleteStream", name: r.str() };
      break;
    case OpCode.StreamInfo:
      req = { type: "StreamInfo", name: r.str() };
      break;
    case OpCode.ListStreams:
      req = { type: "ListStreams" };
      break;
    case OpCode.Query:
      req = { type: "Query", sql: r.lstr() };
      break;
    case OpCode.CreateConsumer: {
      const raw = r.bytes();
      let spec: Record<string, unknown>;
      try {
        spec = JSON.parse(raw.toString("utf8")) as Record<string, unknown>;
      } catch (e) {
        throw new ProtocolError(`invalid consumer spec: ${(e as Error).message}`);
      }
      req = { type: "CreateConsumer", spec };
      break;
    }
    case OpCode.DeleteConsumer:
      req = { type: "DeleteConsumer", name: r.str() };
      break;
    case OpCode.ConsumerInfo:
      req = { type: "ConsumerInfo", name: r.str() };
      break;
    case OpCode.ListConsumers:
      req = { type: "ListConsumers", stream: r.opt((r) => r.str()) };
      break;
    case OpCode.SeekConsumer: {
      const consumer = r.str();
      const kind = r.u8();
      const value = r.u64();
      if (kind > 3) throw new ProtocolError(`unknown seek kind ${kind}`);
      req = { type: "SeekConsumer", consumer, kind, value };
      break;
    }
    case OpCode.Subscribe:
      req = { type: "Subscribe", consumer: r.str(), credits: r.u32() };
      break;
    case OpCode.Credit:
      req = { type: "Credit", subId: r.u32(), credits: r.u32() };
      break;
    case OpCode.Unsubscribe:
      req = { type: "Unsubscribe", subId: r.u32() };
      break;
    case OpCode.Pull:
      req = {
        type: "Pull",
        consumer: r.str(),
        maxMessages: r.u32(),
        maxBytes: r.u32(),
        expiresMs: r.u32(),
      };
      break;
    case OpCode.Ack:
      req = { type: "Ack", consumer: r.str(), offsets: readOffsets(r) };
      break;
    case OpCode.InProgress:
      req = { type: "InProgress", consumer: r.str(), offsets: readOffsets(r) };
      break;
    case OpCode.Nack:
      req = { type: "Nack", consumer: r.str(), offset: r.u64(), delayMs: r.u32() };
      break;
    case OpCode.Term:
      req = { type: "Term", consumer: r.str(), offset: r.u64(), reason: r.str() };
      break;
    case OpCode.Read:
      req = {
        type: "Read",
        stream: r.str(),
        from: r.u64(),
        maxRecords: r.u32(),
        maxBytes: r.u32(),
        waitMs: r.u32(),
        filter: r.str(),
      };
      break;
    case OpCode.CorePublish:
      req = {
        type: "CorePublish",
        subject: r.str(),
        replyTo: r.opt((r) => r.str()),
        headers: r.headers(),
        value: r.bytes(),
      };
      break;
    case OpCode.CoreSubscribe:
      req = { type: "CoreSubscribe", subject: r.str(), queue: r.opt((r) => r.str()) };
      break;
    case OpCode.KvCreateBucket:
      req = { type: "KvCreateBucket", bucket: r.str(), history: r.u64(), ttlMs: r.u64(), maxBytes: r.u64() };
      break;
    case OpCode.KvPut:
      req = {
        type: "KvPut",
        bucket: r.str(),
        key: r.str(),
        value: r.bytes(),
        expectedRevision: r.opt((r) => r.u64()),
        ttlMs: r.opt((r) => r.u64()),
      };
      break;
    case OpCode.KvGet:
      req = { type: "KvGet", bucket: r.str(), key: r.str(), revision: r.opt((r) => r.u64()) };
      break;
    case OpCode.KvDelete:
      req = {
        type: "KvDelete",
        bucket: r.str(),
        key: r.str(),
        purge: r.u8() !== 0,
        expectedRevision: r.opt((r) => r.u64()),
      };
      break;
    case OpCode.KvKeys:
      req = { type: "KvKeys", bucket: r.str(), filter: r.str() };
      break;
    case OpCode.KvHistory:
      req = { type: "KvHistory", bucket: r.str(), key: r.str() };
      break;
    default:
      throw new ProtocolError(`opcode 0x${opcode.toString(16)} is not a client request`);
  }
  r.finish();
  return req;
}

// ---------------------------------------------------------------------------
// Responses and pushes
// ---------------------------------------------------------------------------

export type Response =
  | { type: "Ok" }
  | { type: "Pong" }
  /** `detail` is raw JSON bytes when present. */
  | { type: "Error"; code: number; message: string; detail: Buffer | null }
  | { type: "ConnectOk"; serverVersion: string; nodeId: string; leader: string | null }
  | { type: "PublishOk"; offset: number; duplicate: boolean }
  | { type: "PublishBatchOk"; results: { offset: number; duplicate: boolean }[] }
  | { type: "SubscribeOk"; subId: number }
  /** Push (correlation id 0). */
  | { type: "Deliver"; subId: number; records: WireRecord[] }
  /** Push (correlation id 0). */
  | { type: "SubscriptionEnded"; subId: number; code: number; message: string }
  | { type: "Messages"; records: WireRecord[] }
  | { type: "ReadResult"; nextOffset: number; highWatermark: number; records: WireRecord[] }
  /** Raw UTF-8 JSON. */
  | { type: "Json"; json: Buffer }
  /** Push of a core message for a `CoreSubscribe` (correlation id 0). */
  | { type: "CoreMsg"; subId: number; subject: string; replyTo: string | null; headers: Headers; value: Buffer };

export type ResponseType = Response["type"];

const RESPONSE_OPCODES: Record<ResponseType, OpCode> = {
  Ok: OpCode.Ok,
  Pong: OpCode.Pong,
  Error: OpCode.Error,
  ConnectOk: OpCode.ConnectOk,
  PublishOk: OpCode.PublishOk,
  PublishBatchOk: OpCode.PublishBatchOk,
  SubscribeOk: OpCode.SubscribeOk,
  Deliver: OpCode.Deliver,
  SubscriptionEnded: OpCode.SubscriptionEnded,
  Messages: OpCode.Messages,
  ReadResult: OpCode.ReadResult,
  Json: OpCode.Json,
  CoreMsg: OpCode.CoreMsg,
};

export function responseOpcode(resp: Response): OpCode {
  return RESPONSE_OPCODES[resp.type];
}

/** Size of the smallest valid record. */
export const MIN_RECORD_LEN = 35;
const CRC_START = 10;

/** Encode one record (length, CRC, delivery count, fields). */
export function encodeRecord(r: WireRecord): Buffer {
  const body = new Writer();
  body.u64(r.offset);
  body.u64(r.timestampNs);
  body.str(r.subject);
  body.opt(r.key, (w, k) => w.bytes(k));
  body.bytes(r.value);
  body.headers(r.headers);
  const b = body.finish();
  return new Writer(CRC_START + b.length)
    .u32(CRC_START - 4 + b.length)
    .u32(crc32c(b))
    .u16(r.deliveryCount)
    .raw(b)
    .finish();
}

/** Whether a complete encoded record's CRC matches its contents. */
export function verifyRecordCrc(record: Uint8Array): boolean {
  if (record.length < MIN_RECORD_LEN) return false;
  const view = new DataView(record.buffer, record.byteOffset, record.byteLength);
  return view.getUint32(4, true) === crc32c(record.subarray(CRC_START));
}

function writeRecord(w: Writer, r: WireRecord): void {
  w.raw(encodeRecord(r));
}

function writeRecords(w: Writer, records: WireRecord[]): void {
  w.u32(records.length);
  for (const r of records) writeRecord(w, r);
}

/** Encode a response payload (what the server does). */
export function encodeResponse(resp: Response): Buffer {
  const w = new Writer();
  switch (resp.type) {
    case "Ok":
    case "Pong":
      break;
    case "Error":
      w.u16(resp.code);
      w.str(resp.message);
      w.opt(resp.detail, (w, d) => w.bytes(d));
      break;
    case "ConnectOk":
      w.str(resp.serverVersion);
      w.str(resp.nodeId);
      w.opt(resp.leader, (w, l) => w.str(l));
      break;
    case "PublishOk":
      w.u64(resp.offset);
      w.u8(resp.duplicate ? 1 : 0);
      break;
    case "PublishBatchOk":
      w.u32(resp.results.length);
      for (const r of resp.results) {
        w.u64(r.offset);
        w.u8(r.duplicate ? 1 : 0);
      }
      break;
    case "SubscribeOk":
      w.u32(resp.subId);
      break;
    case "Deliver":
      w.u32(resp.subId);
      writeRecords(w, resp.records);
      break;
    case "SubscriptionEnded":
      w.u32(resp.subId);
      w.u16(resp.code);
      w.str(resp.message);
      break;
    case "Messages":
      writeRecords(w, resp.records);
      break;
    case "ReadResult":
      w.u64(resp.nextOffset);
      w.u64(resp.highWatermark);
      writeRecords(w, resp.records);
      break;
    case "Json":
      w.raw(resp.json);
      break;
    case "CoreMsg":
      w.u32(resp.subId);
      w.str(resp.subject);
      w.opt(resp.replyTo, (w, r) => w.str(r));
      w.headers(resp.headers);
      w.bytes(resp.value);
      break;
    default: {
      const never: never = resp;
      throw new ProtocolError(`unknown response ${(never as Response).type}`);
    }
  }
  return w.finish();
}

/** Encode a response as a complete frame. */
export function responseFrame(resp: Response, correlationId: number): Buffer {
  return encodeFrame(responseOpcode(resp), correlationId, encodeResponse(resp));
}

function readRecord(r: Reader): WireRecord {
  const len = r.u32();
  if (len + 4 < MIN_RECORD_LEN) throw new ProtocolError(`record length ${len + 4} too small`);
  const rr = new Reader(r.raw(len));
  rr.u32(); // CRC (see verifyRecordCrc)
  const deliveryCount = rr.u16();
  const rec: WireRecord = {
    offset: rr.u64(),
    timestampNs: rr.u64big(),
    deliveryCount,
    subject: rr.str(),
    key: rr.opt((r) => r.bytes()),
    value: rr.bytes(),
    headers: rr.headers(),
  };
  rr.finish();
  return rec;
}

function readRecords(r: Reader): WireRecord[] {
  const n = r.count(MIN_RECORD_LEN);
  const out: WireRecord[] = [];
  for (let i = 0; i < n; i++) out.push(readRecord(r));
  return out;
}

/** Decode a response or push payload. */
export function decodeResponse(opcode: number, payload: Buffer): Response {
  if (opcode === OpCode.Json) return { type: "Json", json: payload };
  const r = new Reader(payload);
  let resp: Response;
  switch (opcode) {
    case OpCode.Ok:
      resp = { type: "Ok" };
      break;
    case OpCode.Pong:
      resp = { type: "Pong" };
      break;
    case OpCode.Error:
      resp = { type: "Error", code: r.u16(), message: r.str(), detail: r.opt((r) => r.bytes()) };
      break;
    case OpCode.ConnectOk:
      resp = {
        type: "ConnectOk",
        serverVersion: r.str(),
        nodeId: r.str(),
        leader: r.opt((r) => r.str()),
      };
      break;
    case OpCode.PublishOk:
      resp = { type: "PublishOk", offset: r.u64(), duplicate: r.u8() !== 0 };
      break;
    case OpCode.PublishBatchOk: {
      const n = r.count(9);
      const results: { offset: number; duplicate: boolean }[] = [];
      for (let i = 0; i < n; i++) results.push({ offset: r.u64(), duplicate: r.u8() !== 0 });
      resp = { type: "PublishBatchOk", results };
      break;
    }
    case OpCode.SubscribeOk:
      resp = { type: "SubscribeOk", subId: r.u32() };
      break;
    case OpCode.Deliver:
      resp = { type: "Deliver", subId: r.u32(), records: readRecords(r) };
      break;
    case OpCode.SubscriptionEnded:
      resp = { type: "SubscriptionEnded", subId: r.u32(), code: r.u16(), message: r.str() };
      break;
    case OpCode.Messages:
      resp = { type: "Messages", records: readRecords(r) };
      break;
    case OpCode.ReadResult:
      resp = {
        type: "ReadResult",
        nextOffset: r.u64(),
        highWatermark: r.u64(),
        records: readRecords(r),
      };
      break;
    case OpCode.CoreMsg:
      resp = {
        type: "CoreMsg",
        subId: r.u32(),
        subject: r.str(),
        replyTo: r.opt((r) => r.str()),
        headers: r.headers(),
        value: r.bytes(),
      };
      break;
    default:
      throw new ProtocolError(`opcode 0x${opcode.toString(16)} is not a server response`);
  }
  r.finish();
  return resp;
}
