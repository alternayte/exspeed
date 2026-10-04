import { describe, expect, it } from "vitest";
import {
  FrameParser,
  MAX_PAYLOAD_SIZE,
  OpCode,
  Reader,
  SeekKind,
  Writer,
  crc32c,
  decodeRequest,
  decodeResponse,
  encodeFrame,
  encodeRecord,
  encodeRequest,
  encodeResponse,
  requestFrame,
  requestOpcode,
  responseFrame,
  responseOpcode,
  verifyRecordCrc,
  type Request,
  type Response,
  type WirePublishRecord,
  type WireRecord,
} from "../../src/protocol/index.js";
import { ExspeedError, ProtocolError } from "../../src/errors.js";
import { toWireConsumerSpec, toWirePublishRecord, toWireStreamSpec } from "../../src/types.js";

/** Hex string (spaces and `|` ignored) to Buffer. */
function hex(s: string): Buffer {
  return Buffer.from(s.replace(/[\s|]/g, ""), "hex");
}

const b = (s: string) => Buffer.from(s, "utf8");

/** Same as `rec(i)` in the Rust protocol tests. */
function rec(i: number): WireRecord {
  return {
    offset: i,
    timestampNs: 1_700_000_000_000_000_000n + BigInt(i),
    deliveryCount: i % 3,
    subject: `orders.${i}`,
    key: i % 2 === 0 ? b(`k${i}`) : null,
    value: Buffer.alloc(i % 7, i),
    headers: [["h", `v${i}`]],
  };
}

const pr: WirePublishRecord = {
  subject: "a.b",
  key: b("k"),
  value: b('{"x":1}'),
  headers: [["h1", "v1"]],
  msgId: "m-1",
};
const emptyPr: WirePublishRecord = { subject: "", key: null, value: Buffer.alloc(0), headers: [], msgId: null };
const spec = { name: "s", maxAgeSecs: 1, maxBytes: 2, dedupWindowSecs: 3, dedupMaxEntries: 4, compaction: true };
/** Same as `limited` in the Rust round-trip test. */
const limitedSpec = toWireStreamSpec({
  name: "q",
  maxMsgs: 10,
  discard: "new",
  maxMsgsPerSubject: 1,
  allowMsgTtl: true,
  msgTtlMs: 5000,
  allowDelayed: true,
  retention: "work_queue",
});

const consumerSpec = toWireConsumerSpec({
  name: "c",
  stream: "s",
  filterSubjects: ["orders.>"],
  deliver: { fromTime: 123 },
  ack: "explicit",
  ackWaitMs: 30000,
  maxDeliver: 5,
  backoffMs: [100, 1000],
  maxAckPending: 1000,
  dlqStream: "s-dlq",
  ephemeral: false,
  deadLetterExpired: false,
  headerMatch: "all",
  singleActive: false,
  priorityWindow: 0,
});

const allRequests: Request[] = [
  { type: "Connect", clientId: "c", token: "t" },
  { type: "Connect", clientId: "c", token: null },
  { type: "Ping" },
  { type: "Metadata" },
  { type: "Publish", stream: "s", record: pr },
  { type: "PublishBatch", stream: "s", records: [pr, emptyPr] },
  { type: "CreateStream", spec },
  { type: "UpdateStream", spec },
  { type: "DeleteStream", name: "s" },
  { type: "StreamInfo", name: "s" },
  { type: "ListStreams" },
  { type: "Query", sql: "SELECT 1" },
  { type: "CreateConsumer", spec: consumerSpec },
  { type: "DeleteConsumer", name: "c" },
  { type: "ConsumerInfo", name: "c" },
  { type: "ListConsumers", stream: null },
  { type: "ListConsumers", stream: "s" },
  { type: "SeekConsumer", consumer: "c", kind: SeekKind.Time, value: 9 },
  { type: "SeekConsumer", consumer: "c", kind: SeekKind.Latest, value: 0 },
  { type: "Subscribe", consumer: "c", credits: 100 },
  { type: "Credit", subId: 3, credits: 10 },
  { type: "Unsubscribe", subId: 3 },
  { type: "Pull", consumer: "c", maxMessages: 10, maxBytes: 1024, expiresMs: 500 },
  { type: "Ack", consumer: "c", offsets: [1, 2, 3] },
  { type: "Nack", consumer: "c", offset: 4, delayMs: 100 },
  { type: "Term", consumer: "c", offset: 5, reason: "bad" },
  { type: "InProgress", consumer: "c", offsets: [6] },
  { type: "Read", stream: "s", from: 7, maxRecords: 100, maxBytes: 1 << 20, waitMs: 1000, filter: "a.*" },
  { type: "CreateStream", spec: limitedSpec },
  { type: "CorePublish", subject: "a.b", replyTo: "r", headers: [["h", "v"]], value: b("x") },
  { type: "CorePublish", subject: "a", replyTo: null, headers: [], value: Buffer.alloc(0) },
  { type: "CoreSubscribe", subject: "a.*", queue: "q" },
  { type: "CoreSubscribe", subject: "a.*", queue: null },
  { type: "KvCreateBucket", bucket: "b", history: 5, ttlMs: 1000, maxBytes: 0 },
  { type: "KvPut", bucket: "b", key: "k", value: b("v"), expectedRevision: 0, ttlMs: null },
  { type: "KvPut", bucket: "b", key: "k", value: b("v"), expectedRevision: null, ttlMs: 500 },
  { type: "KvGet", bucket: "b", key: "k", revision: 3 },
  { type: "KvGet", bucket: "b", key: "k", revision: null },
  { type: "KvDelete", bucket: "b", key: "k", purge: true, expectedRevision: 7 },
  { type: "KvDelete", bucket: "b", key: "k", purge: false, expectedRevision: null },
  { type: "KvKeys", bucket: "b", filter: "a.*" },
  { type: "KvHistory", bucket: "b", key: "k" },
];

const allResponses: Response[] = [
  { type: "Ok" },
  { type: "Pong" },
  { type: "Error", code: 404, message: "nope", detail: null },
  { type: "Error", code: 503, message: "not leader", detail: b('{"leader":"h:1"}') },
  { type: "ConnectOk", serverVersion: "0.6.0", nodeId: "n1", leader: "h:5933" },
  { type: "PublishOk", offset: 9, duplicate: true },
  {
    type: "PublishBatchOk",
    results: [
      { offset: 1, duplicate: false },
      { offset: 1, duplicate: true },
    ],
  },
  { type: "SubscribeOk", subId: 2 },
  { type: "Deliver", subId: 2, records: [0, 1, 2, 3, 4].map(rec) },
  { type: "SubscriptionEnded", subId: 2, code: 404, message: "consumer deleted" },
  { type: "Messages", records: [rec(1)] },
  { type: "ReadResult", nextOffset: 10, highWatermark: 12, records: [0, 1, 2].map(rec) },
  { type: "Json", json: b('{"a":1}') },
  { type: "CoreMsg", subId: 0x80000001, subject: "a", replyTo: "r", headers: [["h", "v"]], value: b("x") },
  { type: "CoreMsg", subId: 0x80000002, subject: "a.b", replyTo: null, headers: [], value: Buffer.alloc(0) },
];

/** Normalise Buffers/Uint8Arrays so toEqual compares bytes. */
function norm(v: unknown): unknown {
  if (v instanceof Uint8Array) return Buffer.from(v).toString("hex");
  if (Array.isArray(v)) return v.map(norm);
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, norm(x)]));
  return v;
}

describe("primitives", () => {
  it("round-trips every primitive", () => {
    const w = new Writer(4); // forces growth
    w.u8(255).u16(65535).u32(0xffffffff).u64(Number.MAX_SAFE_INTEGER).u64(0n);
    w.str("héllo").lstr("SELECT 'ü'").bytes(b("raw"));
    w.opt(null, (w, v: string) => w.str(v)).opt("x", (w, v) => w.str(v));
    w.headers([["k", "v"], ["k", "v2"]]);
    const r = new Reader(w.finish());
    expect(r.u8()).toBe(255);
    expect(r.u16()).toBe(65535);
    expect(r.u32()).toBe(0xffffffff);
    expect(r.u64()).toBe(Number.MAX_SAFE_INTEGER);
    expect(r.u64()).toBe(0);
    expect(r.str()).toBe("héllo");
    expect(r.lstr()).toBe("SELECT 'ü'");
    expect(r.bytes().toString()).toBe("raw");
    expect(r.opt((r) => r.str())).toBeNull();
    expect(r.opt((r) => r.str())).toBe("x");
    expect(r.headers()).toEqual([["k", "v"], ["k", "v2"]]);
    r.finish();
  });

  it("encodes str as u16 length + UTF-8, little-endian", () => {
    expect(new Writer().str("é").finish()).toEqual(hex("0200 c3a9"));
    expect(new Writer().u64(0x0102030405060708n).finish()).toEqual(hex("0807060504030201"));
  });

  it("rejects truncated, trailing and malformed input", () => {
    expect(() => new Reader(hex("01")).u16()).toThrow(ProtocolError);
    expect(() => new Reader(hex("0500 6162")).str()).toThrow(/truncated/);
    expect(() => new Reader(hex("00")).finish()).toThrow(/trailing/);
    expect(() => new Reader(hex("02")).opt((r) => r.u8())).toThrow(/option flag/);
    expect(() => new Reader(hex("0200 c328")).str()).toThrow(/UTF-8/);
    expect(() => new Reader(hex("ffffffffffffffff")).u64()).toThrow(/MAX_SAFE_INTEGER/);
    expect(() => new Reader(hex("ffff")).headers()).toThrow(/header count/);
  });

  it("rejects values that don't fit the wire types", () => {
    expect(() => new Writer().str("x".repeat(70_000))).toThrow(ExspeedError);
    expect(() => new Writer().u64(-1)).toThrow(ExspeedError);
    expect(() => new Writer().u64(1.5)).toThrow(ExspeedError);
  });
});

describe("frames", () => {
  it("encodes the 10-byte header", () => {
    expect(encodeFrame(0x01, 7, hex("aabb"))).toEqual(hex("02 01 07000000 02000000 aabb"));
  });

  it("parses frames split at every byte and several per chunk", () => {
    const stream = Buffer.concat([
      responseFrame({ type: "Pong" }, 1),
      responseFrame({ type: "PublishOk", offset: 3, duplicate: false }, 2),
      responseFrame({ type: "Deliver", subId: 9, records: [rec(2)] }, 0),
    ]);
    const p = new FrameParser();
    const frames = [];
    for (let i = 0; i < stream.length; i++) frames.push(...p.push(stream.subarray(i, i + 1)));
    expect(frames.map((f) => [f.opcode, f.correlationId])).toEqual([
      [OpCode.Pong, 1],
      [OpCode.PublishOk, 2],
      [OpCode.Deliver, 0],
    ]);
    expect(p.pending).toBe(0);
    expect(new FrameParser().push(stream)).toHaveLength(3);
  });

  it("rejects a bad version or an oversize length", () => {
    expect(() => new FrameParser().push(hex("01 80 00000000 00000000"))).toThrow(/version/);
    const big = Buffer.alloc(10);
    big[0] = 2;
    big[1] = 0x80;
    big.writeUInt32LE(MAX_PAYLOAD_SIZE + 1, 6);
    expect(() => new FrameParser().push(big)).toThrow(/too large/);
    expect(() => encodeFrame(0x10, 1, Buffer.alloc(MAX_PAYLOAD_SIZE + 1))).toThrow(/too large/);
  });
});

describe("requests", () => {
  it.each(allRequests.map((r) => [r.type, r] as const))("%s round-trips", (_t, req) => {
    const frame = requestFrame(req, 42);
    const [f] = new FrameParser().push(frame);
    expect(f!.opcode).toBe(requestOpcode(req));
    expect(f!.correlationId).toBe(42);
    expect(norm(decodeRequest(f!.opcode, f!.payload))).toEqual(norm(req));
  });

  it("rejects truncated and trailing payloads", () => {
    expect(() => decodeRequest(OpCode.Ping, hex("09"))).toThrow(/trailing/);
    const sub = encodeRequest({ type: "Subscribe", consumer: "c", credits: 1 });
    expect(() => decodeRequest(OpCode.Subscribe, sub.subarray(0, sub.length - 1))).toThrow(/truncated/);
  });

  it("rejects hostile counts without allocating", () => {
    const w = new Writer().str("s").u32(0xffffffff);
    expect(() => decodeRequest(OpCode.PublishBatch, w.finish())).toThrow(/exceeds payload/);
  });

  // Fixtures derived from `Writer` in crates/exspeed-protocol/src/client.rs.
  const fixtures: [string, Request, string][] = [
    ["Connect", { type: "Connect", clientId: "c", token: "t" }, "0100 63 | 01 0100 74"],
    ["Connect without token", { type: "Connect", clientId: "c", token: null }, "0100 63 | 00"],
    ["Ping", { type: "Ping" }, ""],
    [
      "Publish",
      { type: "Publish", stream: "s", record: pr },
      `0100 73
       0300 612e62
       01 01000000 6b
       07000000 7b2278223a317d
       0100 0200 6831 0200 7631
       01 0300 6d2d31`,
    ],
    [
      "PublishBatch",
      { type: "PublishBatch", stream: "s", records: [pr, emptyPr] },
      `0100 73 | 02000000
       0300 612e62 01 01000000 6b 07000000 7b2278223a317d 0100 0200 6831 0200 7631 01 0300 6d2d31
       0000 00 00000000 0000 00`,
    ],
    [
      "CreateStream",
      { type: "CreateStream", spec },
      "0100 73 0100000000000000 0200000000000000 0300000000000000 0400000000000000 01",
    ],
    ["Query", { type: "Query", sql: "SELECT 1" }, "08000000 53454c4543542031"],
    ["ListConsumers all", { type: "ListConsumers", stream: null }, "00"],
    ["ListConsumers stream", { type: "ListConsumers", stream: "s" }, "01 0100 73"],
    ["Seek time", { type: "SeekConsumer", consumer: "c", kind: SeekKind.Time, value: 9 }, "0100 63 03 0900000000000000"],
    ["Seek latest", { type: "SeekConsumer", consumer: "c", kind: SeekKind.Latest, value: 0 }, "0100 63 01 0000000000000000"],
    ["Subscribe", { type: "Subscribe", consumer: "c", credits: 100 }, "0100 63 64000000"],
    ["Credit", { type: "Credit", subId: 3, credits: 10 }, "03000000 0a000000"],
    ["Unsubscribe", { type: "Unsubscribe", subId: 3 }, "03000000"],
    [
      "Pull",
      { type: "Pull", consumer: "c", maxMessages: 10, maxBytes: 1024, expiresMs: 500 },
      "0100 63 0a000000 00040000 f4010000",
    ],
    [
      "Ack",
      { type: "Ack", consumer: "c", offsets: [1, 2, 3] },
      "0100 63 03000000 0100000000000000 0200000000000000 0300000000000000",
    ],
    ["Nack", { type: "Nack", consumer: "c", offset: 4, delayMs: 100 }, "0100 63 0400000000000000 64000000"],
    ["Term", { type: "Term", consumer: "c", offset: 5, reason: "bad" }, "0100 63 0500000000000000 0300 626164"],
    [
      "Read",
      { type: "Read", stream: "s", from: 7, maxRecords: 100, maxBytes: 1 << 20, waitMs: 1000, filter: "a.*" },
      "0100 73 0700000000000000 64000000 00001000 e8030000 0300 612e2a",
    ],
    // The fixtures below were generated with the Rust encoder (Request::into_frame).
    [
      "CreateStream with limits",
      { type: "CreateStream", spec: limitedSpec },
      `0100 71 0000000000000000 0000000000000000 0000000000000000 0000000000000000 00
       8d000000 ${b(
         '{"max_msgs":10,"discard":"new","max_msgs_per_subject":1,"allow_msg_ttl":true,' +
           '"msg_ttl_ms":5000,"allow_delayed":true,"retention":"work_queue"}',
       ).toString("hex")}`,
    ],
    [
      "CorePublish",
      { type: "CorePublish", subject: "a.b", replyTo: "r", headers: [["h", "v"]], value: b("x") },
      "0300 612e62 | 01 0100 72 | 0100 0100 68 0100 76 | 01000000 78",
    ],
    [
      "CorePublish bare",
      { type: "CorePublish", subject: "a", replyTo: null, headers: [], value: Buffer.alloc(0) },
      "0100 61 | 00 | 0000 | 00000000",
    ],
    ["CoreSubscribe", { type: "CoreSubscribe", subject: "a.*", queue: "q" }, "0300 612e2a 01 0100 71"],
    ["CoreSubscribe bare", { type: "CoreSubscribe", subject: "a.*", queue: null }, "0300 612e2a 00"],
    [
      "KvCreateBucket",
      { type: "KvCreateBucket", bucket: "b", history: 5, ttlMs: 1000, maxBytes: 0 },
      "0100 62 0500000000000000 e803000000000000 0000000000000000",
    ],
    [
      "KvPut expecting revision 0",
      { type: "KvPut", bucket: "b", key: "k", value: b("v"), expectedRevision: 0, ttlMs: null },
      "0100 62 0100 6b 01000000 76 01 0000000000000000 00",
    ],
    [
      "KvPut with TTL",
      { type: "KvPut", bucket: "b", key: "k", value: b("v"), expectedRevision: null, ttlMs: 500 },
      "0100 62 0100 6b 01000000 76 00 01 f401000000000000",
    ],
    ["KvGet at revision", { type: "KvGet", bucket: "b", key: "k", revision: 3 }, "0100 62 0100 6b 01 0300000000000000"],
    ["KvGet", { type: "KvGet", bucket: "b", key: "k", revision: null }, "0100 62 0100 6b 00"],
    [
      "KvDelete purge",
      { type: "KvDelete", bucket: "b", key: "k", purge: true, expectedRevision: 7 },
      "0100 62 0100 6b 01 01 0700000000000000",
    ],
    ["KvDelete", { type: "KvDelete", bucket: "b", key: "k", purge: false, expectedRevision: null }, "0100 62 0100 6b 00 00"],
    ["KvKeys", { type: "KvKeys", bucket: "b", filter: "a.*" }, "0100 62 0300 612e2a"],
    ["KvHistory", { type: "KvHistory", bucket: "b", key: "k" }, "0100 62 0100 6b"],
  ];

  it.each(fixtures)("%s matches the Rust encoding byte for byte", (_name, req, expected) => {
    expect(encodeRequest(req).toString("hex")).toBe(hex(expected).toString("hex"));
  });

  it("Connect frame matches byte for byte (header included)", () => {
    const frame = requestFrame({ type: "Connect", clientId: "c", token: "t" }, 1);
    expect(frame).toEqual(hex("02 01 01000000 07000000 | 0100 63 01 0100 74"));
  });

  it("ConsumerSpec JSON matches serde_json's output for the same spec", () => {
    // serde_json::to_vec(&spec) in the Rust round-trip test.
    const rust =
      '{"name":"c","stream":"s","filter_subjects":["orders.>"],"deliver":{"from_time":123},' +
      '"ack":"explicit","ack_wait_ms":30000,"max_deliver":5,"backoff_ms":[100,1000],' +
      '"max_ack_pending":1000,"dlq_stream":"s-dlq","ephemeral":false,' +
      '"dead_letter_expired":false,"header_match":"all","single_active":false,"priority_window":0}';
    const payload = encodeRequest({ type: "CreateConsumer", spec: consumerSpec });
    expect(payload.readUInt32LE(0)).toBe(rust.length);
    expect(payload.subarray(4).toString()).toBe(rust);
  });

  it("maps header filters, single-active and priority settings like serde_json", () => {
    const full = toWireConsumerSpec({
      name: "c",
      stream: "s",
      filterSubjects: ["orders.>"],
      deliver: { fromTime: 123 },
      ack: "explicit",
      ackWaitMs: 30000,
      maxDeliver: 5,
      backoffMs: [100, 1000],
      maxAckPending: 1000,
      dlqStream: "s-dlq",
      ephemeral: false,
      deadLetterExpired: true,
      filterHeaders: { tenant: "acme" },
      headerMatch: "any",
      singleActive: true,
      priorityWindow: 50,
    });
    expect(JSON.stringify(full)).toBe(
      '{"name":"c","stream":"s","filter_subjects":["orders.>"],"deliver":{"from_time":123},' +
        '"ack":"explicit","ack_wait_ms":30000,"max_deliver":5,"backoff_ms":[100,1000],' +
        '"max_ack_pending":1000,"dlq_stream":"s-dlq","ephemeral":false,"dead_letter_expired":true,' +
        '"filter_headers":{"tenant":"acme"},"header_match":"any","single_active":true,"priority_window":50}',
    );
    // An empty header filter is left out, as serde does.
    expect(toWireConsumerSpec({ name: "c", stream: "s", filterHeaders: {} })).toEqual({ name: "c", stream: "s" });
  });

  it("omits unset ConsumerSpec fields so the server applies its defaults", () => {
    expect(JSON.stringify(toWireConsumerSpec({ name: "c", stream: "s" }))).toBe('{"name":"c","stream":"s"}');
    expect(toWireConsumerSpec({ name: "c", stream: "s", deliver: { fromOffset: 5 }, ack: "none" })).toEqual({
      name: "c",
      stream: "s",
      deliver: { from_offset: 5 },
      ack: "none",
    });
    expect(toWireConsumerSpec({ name: "c", stream: "s", deliver: { fromTime: new Date(42) } }).deliver).toEqual({
      from_time: 42,
    });
  });
});

describe("stream limits", () => {
  it("sends no limits trailer when every limit is at its default", () => {
    const plain = toWireStreamSpec({ name: "s", maxAgeSecs: 1, discard: "old", retention: "limits", allowMsgTtl: false });
    expect(plain.limits).toBeNull();
    const payload = encodeRequest({ type: "CreateStream", spec: plain });
    expect(payload.toString("hex")).toBe(
      hex("0100 73 0100000000000000 0000000000000000 0000000000000000 0000000000000000 00").toString("hex"),
    );
    // And an old-style spec without the trailer decodes without limits.
    expect(decodeRequest(OpCode.CreateStream, payload)).toEqual({
      type: "CreateStream",
      spec: { name: "s", maxAgeSecs: 1, maxBytes: 0, dedupWindowSecs: 0, dedupMaxEntries: 0, compaction: false },
    });
  });

  it("sends every limit, serde-style, once any one is set", () => {
    const one = toWireStreamSpec({ name: "s", allowDelayed: true });
    expect(JSON.stringify(one.limits)).toBe(
      '{"max_msgs":0,"discard":"old","max_msgs_per_subject":0,"allow_msg_ttl":false,' +
        '"msg_ttl_ms":0,"allow_delayed":true,"retention":"limits"}',
    );
    for (const s of [{ maxMsgs: 1 }, { discard: "new" as const }, { maxMsgsPerSubject: 2 }, { allowMsgTtl: true }, { msgTtlMs: 3 }, { retention: "interest" as const }]) {
      expect(toWireStreamSpec({ name: "s", ...s }).limits).not.toBeNull();
    }
  });
});

describe("publish options", () => {
  it("become the same headers as the Rust PublishRecord builders", () => {
    const r = toWirePublishRecord({
      subject: "a",
      value: "v",
      headers: { "trace-id": "t" },
      ttl: 500,
      delay: "2000ms",
      deliverAt: 1_700_000_000_000,
      priority: 7,
    });
    // PublishRecord::new("a", "v").ttl(500ms).delay(2s).deliver_at(1700000000000).priority(7)
    expect(r.headers).toEqual([
      ["trace-id", "t"],
      ["exspeed-ttl", "500ms"],
      ["exspeed-delay", "2000ms"],
      ["exspeed-deliver-at", "1700000000000"],
      ["exspeed-priority", "7"],
    ]);
  });

  it("accepts duration strings and Dates, and rejects bad values", () => {
    const r = toWirePublishRecord({ subject: "a", value: "v", ttl: "30s", delay: 0, deliverAt: new Date(42) });
    expect(r.headers).toEqual([
      ["exspeed-ttl", "30s"],
      ["exspeed-delay", "0ms"],
      ["exspeed-deliver-at", "42"],
    ]);
    expect(toWirePublishRecord({ subject: "a", value: "v", ttl: 0.2 }).headers).toEqual([["exspeed-ttl", "1ms"]]);
    expect(() => toWirePublishRecord({ subject: "a", value: "v", ttl: "soon" })).toThrow(ExspeedError);
    expect(() => toWirePublishRecord({ subject: "a", value: "v", delay: -1 })).toThrow(ExspeedError);
    expect(() => toWirePublishRecord({ subject: "a", value: "v", priority: 10 })).toThrow(ExspeedError);
    expect(() => toWirePublishRecord({ subject: "a", value: "v", priority: 1.5 })).toThrow(ExspeedError);
    expect(() => toWirePublishRecord({ subject: "a", value: "v", deliverAt: -5 })).toThrow(ExspeedError);
  });
});

describe("responses", () => {
  it.each(allResponses.map((r) => [r.type, r] as const))("%s round-trips", (_t, resp) => {
    const [f] = new FrameParser().push(responseFrame(resp, 7));
    expect(f!.opcode).toBe(responseOpcode(resp));
    expect(norm(decodeResponse(f!.opcode, f!.payload))).toEqual(norm(resp));
  });

  it("rejects hostile record counts and unknown opcodes", () => {
    const w = new Writer().u64(0).u64(0).u32(0xffffffff);
    expect(() => decodeResponse(OpCode.ReadResult, w.finish())).toThrow(/exceeds payload/);
    expect(() => decodeResponse(OpCode.Publish, Buffer.alloc(0))).toThrow(/not a server response/);
  });

  // Fixtures derived from `Writer` in crates/exspeed-protocol/src/client.rs.
  const fixtures: [string, Response, string][] = [
    ["Error", { type: "Error", code: 404, message: "nope", detail: null }, "9401 0400 6e6f7065 00"],
    [
      "Error with detail",
      { type: "Error", code: 503, message: "not leader", detail: b('{"leader":"h:1"}') },
      "f701 0a00 6e6f74206c6561646572 01 10000000 7b226c6561646572223a22683a31227d",
    ],
    [
      "ConnectOk",
      { type: "ConnectOk", serverVersion: "0.6.0", nodeId: "n1", leader: "h:5933" },
      "0500 302e362e30 0200 6e31 01 0600 683a35393333",
    ],
    ["PublishOk", { type: "PublishOk", offset: 9, duplicate: true }, "0900000000000000 01"],
    [
      "PublishBatchOk",
      {
        type: "PublishBatchOk",
        results: [
          { offset: 1, duplicate: false },
          { offset: 1, duplicate: true },
        ],
      },
      "02000000 0100000000000000 00 0100000000000000 01",
    ],
    ["SubscribeOk", { type: "SubscribeOk", subId: 2 }, "02000000"],
    [
      "SubscriptionEnded",
      { type: "SubscriptionEnded", subId: 2, code: 404, message: "consumer deleted" },
      "02000000 9401 1000 636f6e73756d65722064656c65746564",
    ],
    [
      "Deliver",
      { type: "Deliver", subId: 2, records: [rec(2)] },
      `02000000 | 01000000
       36000000 06760cef 0200
       0200000000000000 02002a36fe9c9717
       0800 6f72646572732e32
       01 02000000 6b32
       02000000 0202
       0100 0100 68 0200 7632`,
    ],
    [
      "Messages",
      { type: "Messages", records: [rec(0)] },
      `01000000
       34000000 b6f9c422 0000
       0000000000000000 00002a36fe9c9717
       0800 6f72646572732e30
       01 02000000 6b30
       00000000
       0100 0100 68 0200 7630`,
    ],
    [
      "ReadResult",
      { type: "ReadResult", nextOffset: 10, highWatermark: 12, records: [rec(1)] },
      `0a00000000000000 0c00000000000000 01000000
       2f000000 f194cd5a 0100
       0100000000000000 01002a36fe9c9717
       0800 6f72646572732e31
       00
       01000000 01
       0100 0100 68 0200 7631`,
    ],
    [
      "CoreMsg",
      { type: "CoreMsg", subId: 0x80000001, subject: "a", replyTo: "r", headers: [["h", "v"]], value: b("x") },
      "01000080 0100 61 01 0100 72 0100 0100 68 0100 76 01000000 78",
    ],
  ];

  it.each(fixtures)("%s decodes from the Rust encoding", (_name, resp, bytes) => {
    const op = responseOpcode(resp);
    expect(norm(decodeResponse(op, hex(bytes)))).toEqual(norm(resp));
    expect(encodeResponse(resp).toString("hex")).toBe(hex(bytes).toString("hex"));
  });
});

describe("records", () => {
  it("carry a CRC32C that ignores delivery_count", () => {
    const enc = encodeRecord(rec(2));
    expect(enc.length).toBe(0x36 + 4);
    expect(verifyRecordCrc(enc)).toBe(true);
    // The server patches delivery_count (bytes 8..10) in place.
    enc.writeUInt16LE(7, 8);
    expect(verifyRecordCrc(enc)).toBe(true);
    const payload = Buffer.concat([hex("01000000"), enc]);
    const resp = decodeResponse(OpCode.Messages, payload);
    expect(resp.type === "Messages" && resp.records[0]!.deliveryCount).toBe(7);
    enc[enc.length - 1] ^= 1;
    expect(verifyRecordCrc(enc)).toBe(false);
  });

  it("crc32c matches the standard check value", () => {
    expect(crc32c(Buffer.from("123456789"))).toBe(0xe3069283);
  });

  it("rejects a record length that disagrees with its contents", () => {
    const enc = encodeRecord(rec(1));
    enc.writeUInt32LE(enc.readUInt32LE(0) + 1, 0);
    const payload = Buffer.concat([hex("01000000"), enc, hex("00")]);
    expect(() => decodeResponse(OpCode.Messages, payload)).toThrow(ProtocolError);
  });
});
