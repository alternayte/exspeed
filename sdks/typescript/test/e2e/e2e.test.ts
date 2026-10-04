/**
 * End-to-end tests against a real exspeed server (see ./server.ts for how
 * the binary is found). Skipped when no binary is available.
 */
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ConnectionError, ExspeedClient, Message, ServerError, newMsgId } from "../../src/index.js";
import { TestServer, eventually, serverBin, uniq } from "./server.js";

describe.skipIf(!serverBin)("e2e: real server", () => {
  let server: TestServer;
  let client: ExspeedClient;

  beforeAll(async () => {
    server = await TestServer.start();
    client = await server.connect({ clientId: "e2e" });
  });

  afterAll(async () => {
    await client?.close();
    await server?.stop();
  });

  /** A fresh stream (and its name). */
  async function stream(prefix = "s"): Promise<string> {
    const name = uniq(prefix);
    await client.createStream({ name });
    return name;
  }

  describe("basics", () => {
    it("pings and reports metadata", async () => {
      expect(await client.ping()).toBeGreaterThanOrEqual(0);
      const md = await client.metadata();
      expect(md.isLeader).toBe(true);
      expect(md.serverVersion).toBe(client.serverInfo.serverVersion);
      expect(md.nodeId).toBe(client.serverInfo.nodeId);
    });

    it("manages streams: idempotent create, conflict, info, list, update, delete", async () => {
      const name = uniq("admin");
      await client.createStream({ name, maxAgeSecs: 3600 });
      await client.createStream({ name, maxAgeSecs: 3600 }); // same settings: ok
      const conflict = await client.createStream({ name, maxAgeSecs: 60 }).catch((e) => e);
      expect(conflict).toBeInstanceOf(ServerError);
      expect(conflict.code).toBe(409);

      await client.publish(name, { subject: "admin.created", value: { n: 1 } });
      const info = await client.streamInfo(name);
      expect(info).toMatchObject({ name, earliestOffset: 0, nextOffset: 1, records: 1, internal: false });
      expect(info.config.maxAgeSecs).toBe(3600);
      expect((await client.listStreams()).map((s) => s.name)).toContain(name);

      await client.updateStream({ name, maxAgeSecs: 7200 });
      expect((await client.streamInfo(name)).config.maxAgeSecs).toBe(7200);

      const c = uniq("admin-c");
      await client.createConsumer({ name: c, stream: name });
      const busy = await client.deleteStream(name).catch((e) => e);
      expect(busy.code).toBe(409);
      expect(busy.detail).toEqual({ consumers: [c] });
      await client.deleteConsumer(c);
      await client.deleteStream(name);
      expect((await client.streamInfo(name).catch((e) => e)).code).toBe(404);
    });

    it("rejects internal stream names and unknown streams", async () => {
      expect((await client.createStream("__nope").catch((e) => e)).code).toBe(403);
      expect((await client.publish(uniq("missing"), { subject: "x.y", value: "v" }).catch((e) => e)).code).toBe(404);
    });
  });

  describe("publishing and reading", () => {
    it("publishes and reads back with subject filters", async () => {
      const s = await stream("orders");
      const subjects = ["orders.placed", "orders.shipped", "orders.eu.placed", "payments.done", "orders.placed"];
      for (const [i, subject] of subjects.entries()) {
        const r = await client.publish(s, {
          subject,
          value: { i },
          key: `k${i}`,
          headers: { "x-index": String(i) },
        });
        expect(r).toEqual({ offset: i, duplicate: false });
      }

      const all = await client.read(s);
      expect(all.records.map((r) => r.subject)).toEqual(subjects);
      expect(all.nextOffset).toBe(5);
      expect(all.highWatermark).toBe(5);
      const first = all.records[0]!;
      expect(first.json()).toEqual({ i: 0 });
      expect(first.key?.toString()).toBe("k0");
      expect(first.header("x-index")).toBe("0");
      expect(first.timestamp).toBeGreaterThan(Date.now() - 60_000);

      const oneToken = await client.read(s, { filter: "orders.*" });
      expect(oneToken.records.map((r) => r.offset)).toEqual([0, 1, 4]);
      const multi = await client.read(s, { filter: "orders.>" });
      expect(multi.records.map((r) => r.offset)).toEqual([0, 1, 2, 4]);
      const page = await client.read(s, { from: 1, maxRecords: 2 });
      expect(page.records.map((r) => r.offset)).toEqual([1, 2]);
      expect(page.nextOffset).toBe(3);

      const bad = await client.read(s, { filter: "orders.>.x" }).catch((e) => e);
      expect(bad).toBeInstanceOf(ServerError);
      expect(bad.code).toBe(400);
    });

    it("long-polls a read until new data arrives", async () => {
      const s = await stream();
      const start = Date.now();
      const pending = client.read(s, { from: 0, waitMs: 5_000 });
      await new Promise((r) => setTimeout(r, 200));
      await client.publish(s, { subject: "late.arrival", value: "hello" });
      const r = await pending;
      expect(r.records.map((x) => x.text())).toEqual(["hello"]);
      expect(Date.now() - start).toBeLessThan(4_000);
    });

    it("publishes batches and deduplicates by msgId", async () => {
      const s = await stream();
      const [m1, m2, m3] = [newMsgId(), newMsgId(), newMsgId()];
      const first = await client.publishBatch(s, [
        { subject: "orders.placed", value: { id: 1 }, msgId: m1 },
        { subject: "orders.placed", value: { id: 2 }, msgId: m2 },
      ]);
      expect(first).toEqual([
        { offset: 0, duplicate: false },
        { offset: 1, duplicate: false },
      ]);
      const retry = await client.publishBatch(s, [
        { subject: "orders.placed", value: { id: 1 }, msgId: m1 },
        { subject: "orders.placed", value: { id: 3 }, msgId: m3 },
      ]);
      expect(retry).toEqual([
        { offset: 0, duplicate: true },
        { offset: 2, duplicate: false },
      ]);
      expect(await client.publish(s, { subject: "orders.placed", value: { id: 2 }, msgId: m2 })).toEqual({
        offset: 1,
        duplicate: true,
      });

      const reused = await client.publish(s, { subject: "orders.placed", value: { id: 99 }, msgId: m1 }).catch((e) => e);
      expect(reused).toBeInstanceOf(ServerError);
      expect(reused.code).toBe(409);
      expect(reused.detail).toEqual({ stored_offset: 0 });
      expect((await client.streamInfo(s)).nextOffset).toBe(3);
    });

    it("keeps the coalescing publisher's records in call order", async () => {
      const s = await stream();
      const p = client.publisher({ maxBatchRecords: 64 });
      const n = 1000;
      const results = await Promise.all(
        Array.from({ length: n }, (_, i) => p.publish(s, { subject: "seq.value", value: { i } })),
      );
      expect(results.map((r) => r.offset)).toEqual(Array.from({ length: n }, (_, i) => i));
      await p.close();

      const seen: number[] = [];
      let from = 0;
      while (seen.length < n) {
        const r = await client.read(s, { from, maxRecords: 500 });
        for (const rec of r.records) seen.push(rec.json<{ i: number }>().i);
        from = r.nextOffset;
      }
      expect(seen).toEqual(Array.from({ length: n }, (_, i) => i));
    });
  });

  describe("consumers", () => {
    async function publishN(s: string, n: number, subject = "work.item"): Promise<void> {
      await client.publishBatch(
        s,
        Array.from({ length: n }, (_, i) => ({ subject, value: { i } })),
      );
    }

    it("creates a consumer, subscribes, receives and acks", async () => {
      const s = await stream();
      const c = uniq("billing");
      const info = await client.createConsumer({ name: c, stream: s, filterSubjects: ["work.>"] });
      expect(info.spec).toMatchObject({ name: c, stream: s, filterSubjects: ["work.>"], deliver: "all", ack: "explicit" });
      // Idempotent for the same spec; 409 for a different one.
      await client.createConsumer({ name: c, stream: s, filterSubjects: ["work.>"] });
      expect((await client.createConsumer({ name: c, stream: s }).catch((e) => e)).code).toBe(409);
      expect((await client.listConsumers(s)).map((i) => i.spec.name)).toEqual([c]);

      await publishN(s, 3);
      await client.publish(s, { subject: "other.thing", value: "filtered out" });
      const sub = await client.subscribe(c, { window: 10 });
      const got: Message[] = [];
      for await (const m of sub) {
        got.push(m);
        m.ack();
        if (got.length === 3) break;
      }
      expect(got.map((m) => [m.offset, m.deliveryCount, m.json()])).toEqual([
        [0, 1, { i: 0 }],
        [1, 1, { i: 1 }],
        [2, 1, { i: 2 }],
      ]);
      expect(sub.endReason).toEqual({ code: 0, message: "unsubscribed" });
      const after = await eventually(async () => {
        const i = await client.consumerInfo(c);
        return i.numUnacked === 0 && i.stats.acked === 3 ? i : null;
      });
      expect(after.ackFloor).toBeGreaterThanOrEqual(3);
    });

    it("never pushes more than the credit window, and continues as messages are consumed", async () => {
      const s = await stream();
      const c = uniq("credit");
      await client.createConsumer({ name: c, stream: s });
      await publishN(s, 50);
      const sub = await client.subscribe(c, { window: 4 });
      await eventually(async () => sub.buffered === 4);
      await new Promise((r) => setTimeout(r, 300));
      expect(sub.buffered).toBe(4); // nothing beyond the window

      const offsets: number[] = [];
      while (offsets.length < 50) {
        const m = await sub.next({ timeoutMs: 5_000 });
        expect(m).not.toBeNull();
        expect(sub.buffered).toBeLessThanOrEqual(4);
        offsets.push(m!.offset);
        m!.ack();
      }
      expect(offsets).toEqual(Array.from({ length: 50 }, (_, i) => i));
      await sub.unsubscribe();
    });

    it("redelivers a nacked message with a higher deliveryCount", async () => {
      const s = await stream();
      const c = uniq("nack");
      await client.createConsumer({ name: c, stream: s });
      await client.publish(s, { subject: "work.item", value: "retry me" });
      const sub = await client.subscribe(c, { window: 10 });
      const first = (await sub.next({ timeoutMs: 5_000 }))!;
      expect(first.deliveryCount).toBe(1);
      await first.nack();
      const second = (await sub.next({ timeoutMs: 5_000 }))!;
      expect(second.offset).toBe(first.offset);
      expect(second.deliveryCount).toBe(2);
      await second.nack(200);
      const t = Date.now();
      const third = (await sub.next({ timeoutMs: 5_000 }))!;
      expect(third.deliveryCount).toBe(3);
      expect(Date.now() - t).toBeGreaterThanOrEqual(150);
      await client.ack(c, [third.offset]); // confirmed ack
      expect((await client.consumerInfo(c)).numUnacked).toBe(0);
      await sub.unsubscribe();
    });

    it("shares work between two clients on one consumer, each record exactly once", async () => {
      const s = await stream();
      const c = uniq("shared");
      await client.createConsumer({ name: c, stream: s });
      const other = await server.connect({ clientId: "e2e-2" });
      try {
        const subA = await client.subscribe(c, { window: 8 });
        const subB = await other.subscribe(c, { window: 8 });
        const seen = new Map<string, number[]>([
          ["a", []],
          ["b", []],
        ]);
        const total = 200;
        const drain = (name: string, sub: typeof subA) =>
          (async () => {
            for await (const m of sub) {
              expect(m.deliveryCount).toBe(1);
              seen.get(name)!.push(m.offset);
              m.ack();
              await new Promise((r) => setTimeout(r, 1)); // let the other subscriber get a share
            }
          })();
        const done = Promise.all([drain("a", subA), drain("b", subB)]);
        await publishN(s, total);
        await eventually(async () => seen.get("a")!.length + seen.get("b")!.length >= total, 15_000);
        await new Promise((r) => setTimeout(r, 200)); // anything extra would show up now
        await subA.unsubscribe();
        await subB.unsubscribe();
        await done;
        const a = seen.get("a")!;
        const b = seen.get("b")!;
        expect(a.length).toBeGreaterThan(0);
        expect(b.length).toBeGreaterThan(0);
        expect([...a, ...b].sort((x, y) => x - y)).toEqual(Array.from({ length: total }, (_, i) => i));
      } finally {
        await other.close();
      }
    });

    it("dead-letters after maxDeliver, and immediately on term", async () => {
      const s = await stream();
      const dlq = await stream("dlq");
      const c = uniq("dlq-c");
      await client.createConsumer({ name: c, stream: s, maxDeliver: 2, dlqStream: dlq });
      await client.publish(s, { subject: "work.poison", value: "bad" });
      await client.publish(s, { subject: "work.terminal", value: "worse" });

      const [m1] = await client.pull(c, { maxMessages: 1, expiresMs: 2_000 });
      expect(m1!.deliveryCount).toBe(1);
      await m1!.nack();
      const [m2] = await client.pull(c, { maxMessages: 1, expiresMs: 2_000 });
      expect([m2!.offset, m2!.deliveryCount]).toEqual([0, 2]);
      await m2!.nack();

      const [t] = await client.pull(c, { maxMessages: 1, expiresMs: 2_000 });
      expect(t!.offset).toBe(1);
      await t!.term("cannot parse");

      const dead = await eventually(async () => {
        const r = await client.read(dlq);
        return r.records.length === 2 ? r.records : null;
      });
      expect(dead.map((d) => d.text())).toEqual(["bad", "worse"]);
      expect(dead[0]!.header("exspeed-dlq-origin")).toBe(c);
      expect(dead[0]!.header("exspeed-dlq-stream")).toBe(s);
      expect(dead[0]!.header("exspeed-dlq-original-offset")).toBe("0");
      expect(dead[0]!.header("exspeed-dlq-deliveries")).toBe("2");
      expect(dead[1]!.header("exspeed-dlq-original-offset")).toBe("1");
      expect(dead[1]!.header("exspeed-dlq-reason")).toContain("cannot parse");
      const info = await client.consumerInfo(c);
      expect(info.stats.deadLettered).toBe(2);
      expect(await client.pull(c, { expiresMs: 300 })).toEqual([]);
    });

    it("long-polls a pull until a message arrives, and times out empty", async () => {
      const s = await stream();
      const c = uniq("pull");
      await client.createConsumer({ name: c, stream: s });

      const t0 = Date.now();
      expect(await client.pull(c, { expiresMs: 300 })).toEqual([]);
      expect(Date.now() - t0).toBeGreaterThanOrEqual(250);

      const t1 = Date.now();
      const pending = client.pull(c, { maxMessages: 10, expiresMs: 10_000 });
      await new Promise((r) => setTimeout(r, 200));
      await client.publish(s, { subject: "work.item", value: "now" });
      const msgs = await pending;
      expect(msgs.map((m) => m.text())).toEqual(["now"]);
      expect(Date.now() - t1).toBeLessThan(5_000);
      // A long pull doesn't block other requests on the same connection.
      const slow = client.pull(c, { expiresMs: 1_000 });
      expect(await client.ping()).toBeLessThan(500);
      await slow;
      msgs[0]!.ack();
    });

    it("seeks a consumer to an offset, the start, the end and a time", async () => {
      const s = await stream();
      const c = uniq("seek");
      await client.createConsumer({ name: c, stream: s, ack: "none" });
      await publishN(s, 5);
      const offsets = async () => (await client.pull(c, { maxMessages: 100, expiresMs: 300 })).map((m) => m.offset);

      expect(await offsets()).toEqual([0, 1, 2, 3, 4]);
      await client.seek(c, { offset: 2 });
      expect(await offsets()).toEqual([2, 3, 4]);
      await client.seek(c, "earliest");
      expect(await offsets()).toEqual([0, 1, 2, 3, 4]);
      await client.seek(c, { latest: true });
      expect(await offsets()).toEqual([]);
      await client.seek(c, { timeMs: 0 });
      expect(await offsets()).toEqual([0, 1, 2, 3, 4]);
      await client.seek(c, { timeMs: new Date(Date.now() + 60_000) });
      expect(await offsets()).toEqual([]);
      expect((await client.seek(uniq("nobody"), "earliest").catch((e) => e)).code).toBe(404);
    });

    it("removes an ephemeral consumer when its connection closes", async () => {
      const s = await stream();
      const c = uniq("eph");
      const owner = await server.connect();
      await owner.createConsumer({ name: c, stream: s, ephemeral: true, deliver: "new" });
      expect((await client.consumerInfo(c)).spec.ephemeral).toBe(true);
      await owner.close();
      const err = await eventually(async () => {
        const e = await client.consumerInfo(c).catch((x) => x);
        return e instanceof ServerError ? e : null;
      });
      expect(err.code).toBe(404);
    });

    it("ends subscriptions with 404 when the consumer is deleted", async () => {
      const s = await stream();
      const c = uniq("gone");
      await client.createConsumer({ name: c, stream: s });
      const sub = await client.subscribe(c);
      const next = sub.next();
      await client.deleteConsumer(c);
      expect(await next).toBeNull();
      expect(sub.endReason?.code).toBe(404);
    });
  });

  it("runs SQL queries", async () => {
    const s = uniq("q").replace(/-/g, "_");
    await client.createStream(s);
    await client.publishBatch(
      s,
      [1, 2, 3].map((i) => ({ subject: "metrics.cpu", value: { region: i === 2 ? "us" : "eu", i } })),
    );
    const r = await client.query(`SELECT COUNT(*) AS cnt FROM "${s}"`);
    expect(r.columns).toEqual(["cnt"]);
    expect(r.rows).toEqual([[3]]);
    expect(r.rowCount).toBe(1);
    expect(typeof r.executionTimeMs).toBe("number");
    const bad = await client.query("SELEKT nonsense").catch((e) => e);
    expect(bad).toBeInstanceOf(ServerError);
    expect(bad.code).toBe(400);
  });
});

describe.skipIf(!serverBin)("e2e: auth", () => {
  let server: TestServer;

  beforeAll(async () => {
    server = await TestServer.start({ authToken: "s3cret-token" });
  });

  afterAll(async () => {
    await server?.stop();
  });

  it("rejects a wrong or missing token with 401", async () => {
    for (const token of ["wrong", undefined]) {
      const err = await server.connect({ token }).catch((e) => e);
      expect(err).toBeInstanceOf(ServerError);
      expect(err.code).toBe(401);
    }
  });

  it("accepts the right token", async () => {
    const c = await server.connect({ token: "s3cret-token" });
    try {
      const s = uniq("authed");
      await c.createStream(s);
      expect((await c.publish(s, { subject: "auth.ok", value: "yes" })).offset).toBe(0);
      expect((await c.query(`SELECT COUNT(*) AS n FROM "${s}"`)).rows).toEqual([[1]]);
    } finally {
      await c.close();
    }
  });
});

describe.skipIf(!serverBin)("e2e: reconnection", () => {
  let server: TestServer;

  beforeAll(async () => {
    server = await TestServer.start();
  });

  afterAll(async () => {
    await server?.stop();
  });

  it("re-subscribes after the server restarts; unacked records are redelivered", async () => {
    const client = await server.connect({ reconnect: { initialDelayMs: 50, maxDelayMs: 200 } });
    try {
      const s = uniq("durable");
      const c = uniq("durable-c");
      await client.createStream(s);
      await client.createConsumer({ name: c, stream: s });
      await client.publish(s, { subject: "work.item", value: "before" });
      const sub = await client.subscribe(c, { window: 10 });
      const m1 = (await sub.next({ timeoutMs: 5_000 }))!;
      expect(m1.text()).toBe("before"); // not acked

      const events: string[] = [];
      client.on("disconnect", () => events.push("disconnect"));
      const reconnected = new Promise((r) => client.once("reconnect", r));
      await expect(
        (async () => {
          server = await server.restart();
        })(),
      ).resolves.toBeUndefined();
      await reconnected;
      expect(events).toEqual(["disconnect"]);
      expect(client.connected).toBe(true);

      const again = (await sub.next({ timeoutMs: 10_000 }))!;
      expect(again.text()).toBe("before");
      // (The delivery count may restart at 1: the server persists consumer
      // state in periodic snapshots.)
      again.ack();
      await client.publish(s, { subject: "work.item", value: "after" });
      const m2 = (await sub.next({ timeoutMs: 5_000 }))!;
      expect(m2.text()).toBe("after");
      m2.ack();
      expect(sub.closed).toBe(false);
    } finally {
      await client.close();
    }
  });

  it("fails requests with ConnectionError while disconnected (reconnect off)", async () => {
    const client = await server.connect({ reconnect: false });
    const closed = new Promise((r) => client.once("close", r));
    server = await server.restart();
    await closed;
    await expect(client.ping()).rejects.toBeInstanceOf(ConnectionError);
  });
});

const hasOpenssl = spawnSync("openssl", ["version"]).status === 0;

describe.skipIf(!serverBin || !hasOpenssl)("e2e: TLS", () => {
  let server: TestServer;
  let dir: string;
  let ca: Buffer;

  beforeAll(async () => {
    dir = mkdtempSync(join(tmpdir(), "exspeed-sdk-tls-"));
    const r = spawnSync(
      "openssl",
      [
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-keyout", join(dir, "key.pem"), "-out", join(dir, "cert.pem"),
        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
      ],
      { stdio: "pipe" },
    );
    if (r.status !== 0) throw new Error(`openssl failed: ${r.stderr}`);
    ca = readFileSync(join(dir, "cert.pem"));
    server = await TestServer.start({ tlsCert: join(dir, "cert.pem"), tlsKey: join(dir, "key.pem") });
  });

  afterAll(async () => {
    await server?.stop();
    rmSync(dir, { recursive: true, force: true });
  });

  it("connects over TLS with a custom CA", async () => {
    const c = await server.connect({ tls: { ca } });
    try {
      const s = uniq("tls");
      await c.createStream(s);
      expect((await c.publish(s, { subject: "tls.ok", value: "secure" })).offset).toBe(0);
    } finally {
      await c.close();
    }
  });

  it("refuses an untrusted certificate and plain TCP", async () => {
    await expect(server.connect({ tls: true, requestTimeoutMs: 3_000 })).rejects.toBeInstanceOf(ConnectionError);
    await expect(server.connect({ requestTimeoutMs: 3_000 })).rejects.toBeInstanceOf(ConnectionError);
  });
});

describe.skipIf(!serverBin || !hasOpenssl)("e2e: mutual TLS", () => {
  let server: TestServer;
  let dir: string;
  let ca: Buffer;
  let cert: Buffer;
  let key: Buffer;

  function openssl(...args: string[]): void {
    const r = spawnSync("openssl", args, { stdio: "pipe", cwd: dir });
    if (r.status !== 0) throw new Error(`openssl ${args[0]} failed: ${r.stderr}`);
  }

  beforeAll(async () => {
    dir = mkdtempSync(join(tmpdir(), "exspeed-sdk-mtls-"));
    // One CA signs both the server's and the client's certificate.
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-keyout", "ca.key", "-out", "ca.pem", "-subj", "/CN=exspeed-test-ca");
    for (const [name, cn, san] of [
      ["server", "localhost", "subjectAltName=DNS:localhost,IP:127.0.0.1"],
      ["client", "orders.internal", "subjectAltName=DNS:orders.internal"],
    ] as const) {
      openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", `${name}.key`, "-out", `${name}.csr`, "-subj", `/CN=${cn}`);
      writeFileSync(join(dir, `${name}.ext`), `${san}\n`);
      openssl("x509", "-req", "-in", `${name}.csr`, "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-days", "1", "-out", `${name}.pem`, "-extfile", `${name}.ext`);
    }
    ca = readFileSync(join(dir, "ca.pem"));
    cert = readFileSync(join(dir, "client.pem"));
    key = readFileSync(join(dir, "client.key"));
    server = await TestServer.start({
      tlsCert: join(dir, "server.pem"),
      tlsKey: join(dir, "server.key"),
      tlsClientCa: join(dir, "ca.pem"),
    });
  });

  afterAll(async () => {
    await server?.stop();
    rmSync(dir, { recursive: true, force: true });
  });

  it("connects with a client certificate", async () => {
    const c = await server.connect({ tls: { ca, cert, key } });
    try {
      const s = uniq("mtls");
      await c.createStream(s);
      expect((await c.publish(s, { subject: "mtls.ok", value: "mutual" })).offset).toBe(0);
    } finally {
      await c.close();
    }
  });

  it("is refused without one", async () => {
    await expect(server.connect({ tls: { ca }, requestTimeoutMs: 3_000 })).rejects.toBeInstanceOf(ConnectionError);
  });
});
