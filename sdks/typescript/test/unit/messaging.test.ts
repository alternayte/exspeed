/**
 * Core pub/sub, request-reply, KV buckets and the newer consumer info
 * fields, against the scriptable fake server.
 */
import { afterEach, describe, expect, it } from "vitest";
import { ConnectionError, ExspeedClient, ExspeedError, ServerError, TimeoutError } from "../../src/index.js";
import type { Request, WireRecord } from "../../src/protocol/index.js";
import { FakeServer, type FakeConn } from "./fake-server.js";

let server: FakeServer;
let client: ExspeedClient | null = null;

async function setup(opts: Parameters<typeof ExspeedClient.connect>[0] = {}) {
  server = await FakeServer.start();
  client = await ExspeedClient.connect({ port: server.port, keepaliveMs: 0, reconnect: false, ...opts });
  return client;
}

afterEach(async () => {
  await client?.close();
  client = null;
  await server?.close();
});

const ok = (conn: FakeConn, corr: number) => conn.reply(corr, { type: "Ok" });

/** A KV record as the server stores it: subject = key, raw stream offset. */
const kvRec = (offset: number, key: string, value: string, op?: "DEL" | "PURGE"): WireRecord => ({
  offset,
  timestampNs: 1_700_000_000_000_000_000n + BigInt(offset),
  deliveryCount: 0,
  subject: key,
  key: null,
  value: Buffer.from(value),
  headers: op ? [["exspeed-kv-op", op]] : [],
});

describe("core pub/sub", () => {
  it("publishes a core message and waits for Ok", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "CorePublish") ok(conn, corr);
    };
    await c.publishCore("orders.created", { id: 1 }, { headers: { "trace-id": "t" } });
    const [p] = server.last.of("CorePublish");
    expect(p!.corr).not.toBe(0);
    expect(p!.req).toMatchObject({ subject: "orders.created", replyTo: null, headers: [["trace-id", "t"]] });
    expect(Buffer.from(p!.req.value).toString()).toBe('{"id":1}');
  });

  it("subscribes (with a queue group), keeps pushes behind SubscribeOk, responds and unsubscribes", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "CoreSubscribe") {
        conn.replyMany([
          [corr, { type: "SubscribeOk", subId: 0x80000001 }],
          [0, { type: "CoreMsg", subId: 0x80000001, subject: "svc.echo", replyTo: "_INBOX.x.1", headers: [["h", "1"]], value: Buffer.from('{"q":1}') }],
          [0, { type: "CoreMsg", subId: 0x80000099, subject: "other", replyTo: null, headers: [], value: Buffer.from("ignored") }],
        ]);
      } else if (req.type === "CorePublish" || req.type === "Unsubscribe") ok(conn, corr);
    };
    const sub = await c.subscribeCore("svc.*", { queue: "workers" });
    expect(server.last.of("CoreSubscribe")[0]!.req).toEqual({ type: "CoreSubscribe", subject: "svc.*", queue: "workers" });
    expect(sub.id).toBe(0x80000001);
    const m = (await sub.next({ timeoutMs: 1000 }))!;
    expect([m.subject, m.replyTo, m.header("h"), m.json()]).toEqual(["svc.echo", "_INBOX.x.1", "1", { q: 1 }]);
    await m.respond("pong");
    const resp = server.last.of("CorePublish")[0]!.req;
    expect([resp.subject, resp.replyTo, Buffer.from(resp.value).toString()]).toEqual(["_INBOX.x.1", null, "pong"]);
    expect(await sub.next({ timeoutMs: 100 })).toBeNull(); // the other sub's message isn't routed here

    await sub.unsubscribe();
    expect(server.last.of("Unsubscribe")[0]!.req).toEqual({ type: "Unsubscribe", subId: 0x80000001 });
    expect(sub.endReason).toEqual({ code: 0, message: "unsubscribed" });
  });

  it("refuses to respond to a message without replyTo", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "CoreSubscribe") {
        conn.replyMany([
          [corr, { type: "SubscribeOk", subId: 0x80000001 }],
          [0, { type: "CoreMsg", subId: 0x80000001, subject: "a", replyTo: null, headers: [], value: Buffer.from("x") }],
        ]);
      }
    };
    const sub = await c.subscribeCore("a");
    const m = (await sub.next())!;
    await expect(m.respond("no")).rejects.toBeInstanceOf(ExspeedError);
  });

  it("ends when the server ends it (503), after yielding buffered messages", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "CoreSubscribe") {
        conn.replyMany([
          [corr, { type: "SubscribeOk", subId: 0x80000002 }],
          [0, { type: "CoreMsg", subId: 0x80000002, subject: "a", replyTo: null, headers: [], value: Buffer.from("1") }],
          [0, { type: "SubscriptionEnded", subId: 0x80000002, code: 503, message: "leadership moved" }],
        ]);
      }
    };
    const sub = await c.subscribeCore("a");
    const seen: string[] = [];
    for await (const m of sub) seen.push(m.text());
    expect(seen).toEqual(["1"]);
    expect(sub.endReason).toEqual({ code: 503, message: "leadership moved" });
  });

  it("unsubscribes when a for-await loop breaks", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "CoreSubscribe") {
        conn.replyMany([
          [corr, { type: "SubscribeOk", subId: 0x80000003 }],
          [0, { type: "CoreMsg", subId: 0x80000003, subject: "a", replyTo: null, headers: [], value: Buffer.from("1") }],
        ]);
      } else if (req.type === "Unsubscribe") ok(conn, corr);
    };
    const sub = await c.subscribeCore("a");
    for await (const _m of sub) break;
    expect(server.last.of("Unsubscribe")[0]!.req).toEqual({ type: "Unsubscribe", subId: 0x80000003 });
    expect(sub.closed).toBe(true);
  });
});

describe("request-reply", () => {
  /** Answers CoreSubscribe for the inbox and records requests; replies are sent by the test. */
  function inboxServer(): { subs: string[] } {
    const state = { subs: [] as string[] };
    let id = 0x80000010;
    server.handler = (conn, corr, req) => {
      if (req.type === "CoreSubscribe") {
        state.subs.push(req.subject);
        conn.reply(corr, { type: "SubscribeOk", subId: id++ });
      } else if (req.type === "CorePublish") ok(conn, corr);
    };
    return state;
  }

  it("shares one inbox subscription and routes responses by the last subject token", async () => {
    const c = await setup();
    const state = inboxServer();
    const a = c.request("svc.a", "1");
    const b = c.request("svc.b", { n: 2 }, { headers: { h: "v" } });
    await server.until(() => server.last.of("CorePublish").length === 2);
    expect(state.subs).toHaveLength(1);
    expect(state.subs[0]).toMatch(/^_INBOX\.[0-9a-f]+\.\*$/);
    const prefix = state.subs[0]!.slice(0, -2);
    const [pa, pb] = server.last.of("CorePublish");
    expect(pa!.req.replyTo).toBe(`${prefix}.1`);
    expect(pb!.req.replyTo).toBe(`${prefix}.2`);
    expect(pb!.req.headers).toEqual([["h", "v"]]);
    // Out of order, on the inbox subscription.
    server.last.reply(0, { type: "CoreMsg", subId: 0x80000010, subject: pb!.req.replyTo!, replyTo: null, headers: [], value: Buffer.from("B") });
    server.last.reply(0, { type: "CoreMsg", subId: 0x80000010, subject: pa!.req.replyTo!, replyTo: null, headers: [], value: Buffer.from("A") });
    expect((await a).text()).toBe("A");
    expect((await b).text()).toBe("B");

    const third = c.request("svc.a", "3");
    await server.until(() => server.last.of("CorePublish").length === 3);
    expect(state.subs).toHaveLength(1); // still the same inbox
    const p3 = server.last.of("CorePublish")[2]!;
    server.last.reply(0, { type: "CoreMsg", subId: 0x80000010, subject: p3.req.replyTo!, replyTo: null, headers: [], value: Buffer.from("C") });
    expect((await third).text()).toBe("C");
  });

  it("rejects at once with 404 when there are no responders", async () => {
    const c = await setup();
    inboxServer();
    server.handler = ((prev) => (conn: FakeConn, corr: number, req: Request) => {
      if (req.type === "CorePublish") {
        conn.reply(corr, { type: "Error", code: 404, message: "no responders for 'svc.none'", detail: null });
        return true;
      }
      return prev(conn, corr, req);
    })(server.handler);
    const t = Date.now();
    const err = await c.request("svc.none", "x", { timeoutMs: 5_000 }).catch((e) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect(err.code).toBe(404);
    expect(Date.now() - t).toBeLessThan(1_000);
  });

  it("times out, and ignores a late response", async () => {
    const c = await setup();
    inboxServer();
    const err = await c.request("svc.slow", "x", { timeoutMs: 100 }).catch((e) => e);
    expect(err).toBeInstanceOf(TimeoutError);
    const p = server.last.of("CorePublish")[0]!;
    server.last.reply(0, { type: "CoreMsg", subId: 0x80000010, subject: p.req.replyTo!, replyTo: null, headers: [], value: Buffer.from("late") });
    await c.ping(); // the late response was dropped without trouble
  });

  it("fails waiting requests when the inbox ends, and subscribes a new one next time", async () => {
    const c = await setup();
    const state = inboxServer();
    const pending = c.request("svc.a", "x", { timeoutMs: 5_000 });
    await server.until(() => server.last.of("CorePublish").length === 1);
    server.last.reply(0, { type: "SubscriptionEnded", subId: 0x80000010, code: 503, message: "leadership moved" });
    const err = await pending.catch((e) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect(err.code).toBe(503);

    const again = c.request("svc.a", "y");
    await server.until(() => server.last.of("CorePublish").length === 2);
    expect(state.subs).toHaveLength(2);
    expect(state.subs[1]).not.toBe(state.subs[0]);
    const p = server.last.of("CorePublish")[1]!;
    expect(p.req.replyTo!.startsWith(state.subs[1]!.slice(0, -2))).toBe(true);
    server.last.reply(0, { type: "CoreMsg", subId: 0x80000011, subject: p.req.replyTo!, replyTo: null, headers: [], value: Buffer.from("ok") });
    expect((await again).text()).toBe("ok");
  });

  it("after a reconnect: re-subscribes core subscriptions and sets up a new inbox", async () => {
    server = await FakeServer.start();
    let id = 0x80000020;
    server.handler = (conn, corr, req) => {
      if (req.type === "CoreSubscribe") conn.reply(corr, { type: "SubscribeOk", subId: id++ });
      else if (req.type === "CorePublish") ok(conn, corr);
    };
    client = await ExspeedClient.connect({ port: server.port, keepaliveMs: 0, reconnect: { initialDelayMs: 10, maxDelayMs: 20 } });
    const c = client;
    const sub = await c.subscribeCore("events.>", { queue: "g" });
    const pending = c.request("svc.a", "x", { timeoutMs: 5_000 });
    await server.until(() => server.last.of("CorePublish").length === 1);
    const firstInbox = server.last.of("CoreSubscribe")[1]!.req.subject;

    const reconnected = new Promise((r) => c.once("reconnect", r));
    server.conns[0]!.socket.destroy();
    expect(await pending.catch((e) => e)).toBeInstanceOf(ConnectionError);
    await reconnected;
    const second = server.conns[1]!;
    expect(second.of("CoreSubscribe").map((r) => r.req)).toEqual([
      { type: "CoreSubscribe", subject: "events.>", queue: "g" },
    ]);
    second.reply(0, { type: "CoreMsg", subId: sub.id, subject: "events.x", replyTo: null, headers: [], value: Buffer.from("after") });
    expect((await sub.next({ timeoutMs: 1000 }))!.text()).toBe("after");

    const again = c.request("svc.a", "y");
    await server.until(() => second.of("CorePublish").length === 1);
    const inbox = second.of("CoreSubscribe")[1]!.req.subject;
    expect(inbox).not.toBe(firstInbox);
    const p = second.of("CorePublish")[0]!;
    second.reply(0, { type: "CoreMsg", subId: id - 1, subject: p.req.replyTo!, replyTo: null, headers: [], value: Buffer.from("ok") });
    expect((await again).text()).toBe("ok");
  });
});

describe("kv buckets", () => {
  it("encodes create, put, createKey, update, delete and purge; revisions come from PublishOk", async () => {
    const c = await setup();
    let rev = 0;
    server.handler = (conn, corr, req) => {
      if (req.type === "KvCreateBucket") ok(conn, corr);
      if (req.type === "KvPut" || req.type === "KvDelete") conn.reply(corr, { type: "PublishOk", offset: ++rev, duplicate: false });
    };
    const kv = c.kv("cfg");
    expect(kv.stream).toBe("KV_cfg");
    await kv.create({ history: 5, ttlMs: 60_000 });
    await c.kv("plain").create();
    expect(await kv.put("a", { on: true }, { ttlMs: 500 })).toBe(1);
    expect(await kv.createKey("b", "x")).toBe(2);
    expect(await kv.update("b", "y", 2)).toBe(3);
    expect(await kv.delete("a")).toBe(4);
    expect(await kv.purge("b", { expectedRevision: 3 })).toBe(5);

    const reqs = server.last.received.map((r) => r.req).filter((r) => r.type.startsWith("Kv"));
    const norm = (r: Request) =>
      "value" in r && r.value instanceof Uint8Array ? { ...r, value: Buffer.from(r.value).toString() } : r;
    expect(reqs.map(norm)).toEqual([
      { type: "KvCreateBucket", bucket: "cfg", history: 5, ttlMs: 60_000, maxBytes: 0 },
      { type: "KvCreateBucket", bucket: "plain", history: 0, ttlMs: 0, maxBytes: 0 },
      { type: "KvPut", bucket: "cfg", key: "a", value: '{"on":true}', expectedRevision: null, ttlMs: 500 },
      { type: "KvPut", bucket: "cfg", key: "b", value: "x", expectedRevision: 0, ttlMs: null },
      { type: "KvPut", bucket: "cfg", key: "b", value: "y", expectedRevision: 2, ttlMs: null },
      { type: "KvDelete", bucket: "cfg", key: "a", purge: false, expectedRevision: null },
      { type: "KvDelete", bucket: "cfg", key: "b", purge: true, expectedRevision: 3 },
    ]);
  });

  it("passes a CAS conflict through as ServerError 409", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "KvPut") {
        conn.reply(corr, { type: "Error", code: 409, message: "wrong revision", detail: Buffer.from('{"current_revision":7}') });
      }
    };
    const err = await c.kv("b").update("k", "v", 3).catch((e) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect([err.code, err.detail]).toEqual([409, { current_revision: 7 }]);
  });

  it("turns records into entries with revision = offset + 1", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type !== "KvGet") return;
      if (req.key === "live") conn.reply(corr, { type: "Messages", records: [kvRec(4, "live", '{"n":1}')] });
      else if (req.key === "gone") conn.reply(corr, { type: "Error", code: 404, message: "key 'gone' not found", detail: null });
      else if (req.key === "old") conn.reply(corr, { type: "Messages", records: [kvRec(req.revision! - 1, "old", "v1")] });
      else conn.reply(corr, { type: "Messages", records: [] });
    };
    const kv = c.kv("b");
    const e = (await kv.get("live"))!;
    expect([e.key, e.revision, e.op, e.json()]).toEqual(["live", 5, "put", { n: 1 }]);
    expect(e.timestampNs).toBe(1_700_000_000_000_000_004n);
    expect(e.timestamp).toBe(1_700_000_000_000);
    expect(await kv.get("gone")).toBeNull();
    expect(await kv.get("empty")).toBeNull();
    const old = (await kv.getRevision("old", 2))!;
    expect([old.revision, old.text()]).toEqual([2, "v1"]);
    expect(server.last.of("KvGet").map((r) => r.req.revision)).toEqual([null, null, null, 2]);
  });

  it("throws when the bucket doesn't exist", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "KvGet") conn.reply(corr, { type: "Error", code: 404, message: "bucket 'nope' not found", detail: null });
    };
    const err = await c.kv("nope").get("k").catch((e) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect(err.code).toBe(404);
  });

  it("lists keys and history (tombstones included)", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "KvKeys") conn.reply(corr, { type: "Json", json: Buffer.from('["a.1","a.2"]') });
      if (req.type === "KvHistory") {
        conn.reply(corr, {
          type: "Messages",
          records: [kvRec(0, "k", "v1"), kvRec(3, "k", "", "DEL"), kvRec(5, "k", "v2"), kvRec(6, "k", "", "PURGE")],
        });
      }
    };
    const kv = c.kv("b");
    expect(await kv.keys("a.*")).toEqual(["a.1", "a.2"]);
    expect(await kv.keys()).toEqual(["a.1", "a.2"]);
    expect(server.last.of("KvKeys").map((r) => r.req.filter)).toEqual(["a.*", ""]);
    const h = await kv.history("k");
    expect(h.map((e) => [e.revision, e.op])).toEqual([
      [1, "put"],
      [4, "delete"],
      [6, "put"],
      [7, "purge"],
    ]);
  });

  it("watches: the live keys first (sorted by revision), then every change", async () => {
    const c = await setup();
    const reads: Extract<Request, { type: "Read" }>[] = [];
    server.handler = (conn, corr, req) => {
      if (req.type !== "Read") return;
      reads.push(req);
      if (req.waitMs === 0 && req.from === 0) {
        // Snapshot, page 1 of 2 (high watermark 5).
        conn.reply(corr, {
          type: "ReadResult",
          nextOffset: 3,
          highWatermark: 5,
          records: [kvRec(0, "a", "a1"), kvRec(1, "b", "b1"), kvRec(2, "a", "a2")],
        });
      } else if (req.waitMs === 0 && req.from === 3) {
        conn.reply(corr, {
          type: "ReadResult",
          nextOffset: 5,
          highWatermark: 6,
          records: [kvRec(3, "c", "c1"), kvRec(4, "b", "", "DEL")],
        });
      } else {
        conn.reply(corr, {
          type: "ReadResult",
          nextOffset: 7,
          highWatermark: 7,
          records: [kvRec(5, "a", "a3"), kvRec(6, "c", "", "DEL")],
        });
      }
    };
    const w = c.kv("b").watch("x.>");
    const seen: [string, number, string][] = [];
    for await (const e of w) {
      seen.push([e.key, e.revision, e.op]);
      if (seen.length === 4) break;
    }
    // b was deleted before the snapshot ended: left out. a (rev 3) before c (rev 4).
    expect(seen).toEqual([
      ["a", 3, "put"],
      ["c", 4, "put"],
      ["a", 6, "put"],
      ["c", 7, "delete"],
    ]);
    expect(reads.map((r) => [r.stream, r.from, r.waitMs, r.filter])).toEqual([
      ["KV_b", 0, 0, "x.>"],
      ["KV_b", 3, 0, "x.>"],
      ["KV_b", 5, 10_000, "x.>"],
    ]);
    expect(w.closed).toBe(true);
    expect(await w.next()).toBeNull();
  });

  it("returns null from next({ timeoutMs }) while a long-poll is pending, keeping what it brings", async () => {
    const c = await setup();
    let held: { conn: FakeConn; corr: number } | null = null;
    server.handler = (conn, corr, req) => {
      if (req.type !== "Read") return;
      if (req.waitMs === 0) conn.reply(corr, { type: "ReadResult", nextOffset: 0, highWatermark: 0, records: [] });
      else held = { conn, corr };
    };
    const w = c.kv("b").watch();
    expect(await w.next({ timeoutMs: 100 })).toBeNull();
    await server.until(() => held !== null);
    held!.conn.reply(held!.corr, { type: "ReadResult", nextOffset: 1, highWatermark: 1, records: [kvRec(0, "k", "v")] });
    const e = (await w.next({ timeoutMs: 1000 }))!;
    expect([e.key, e.revision]).toEqual(["k", 1]);
    expect(server.last.of("Read")).toHaveLength(2); // the timed-out next() didn't start another read
    w.stop();
  });
});

describe("consumer info", () => {
  it("keeps filterHeaders keys as sent and camelizes the rest (numDelayed)", async () => {
    const c = await setup();
    const info = {
      spec: { name: "c", stream: "s", filter_headers: { x_tenant_id: "acme" }, header_match: "any", single_active: true, priority_window: 5, dead_letter_expired: true },
      num_delayed: 2,
      stats: {},
    };
    server.handler = (conn, corr, req) => {
      if (req.type === "ConsumerInfo" || req.type === "CreateConsumer") conn.reply(corr, { type: "Json", json: Buffer.from(JSON.stringify(info)) });
      if (req.type === "ListConsumers") conn.reply(corr, { type: "Json", json: Buffer.from(JSON.stringify([info, { spec: { name: "d", stream: "s" } }])) });
    };
    const i = await c.consumerInfo("c");
    expect(i.spec).toMatchObject({ filterHeaders: { x_tenant_id: "acme" }, headerMatch: "any", singleActive: true, priorityWindow: 5, deadLetterExpired: true });
    expect(i.numDelayed).toBe(2);
    const created = await c.createConsumer({ name: "c", stream: "s", filterHeaders: { x_tenant_id: "acme" }, headerMatch: "any" });
    expect(created.spec.filterHeaders).toEqual({ x_tenant_id: "acme" });
    expect(server.last.of("CreateConsumer")[0]!.req.spec).toMatchObject({ filter_headers: { x_tenant_id: "acme" }, header_match: "any" });
    const list = await c.listConsumers();
    expect(list.map((x) => x.spec.filterHeaders)).toEqual([{ x_tenant_id: "acme" }, {}]);
  });
});
