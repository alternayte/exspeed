/**
 * End-to-end tests of stream limits, time headers, routing settings, core
 * pub/sub, request-reply and KV buckets against a real exspeed server (see
 * ./server.ts for how the binary is found). Skipped when no binary is
 * available.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { CoreMessage, ExspeedClient, ServerError, TimeoutError, type KvEntry } from "../../src/index.js";
import { TestServer, eventually, serverBin, uniq } from "./server.js";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe.skipIf(!serverBin)("e2e: limits, core messaging and KV", () => {
  let server: TestServer;
  let client: ExspeedClient;

  beforeAll(async () => {
    server = await TestServer.start();
    client = await server.connect({ clientId: "e2e-messaging" });
  });

  afterAll(async () => {
    await client?.close();
    await server?.stop();
  });

  describe("stream limits and time headers", () => {
    it("hides records whose TTL has passed from reads and consumers", async () => {
      const s = uniq("ttl");
      await client.createStream({ name: s, allowMsgTtl: true });
      const info = await client.streamInfo(s);
      expect(info.config.allowMsgTtl).toBe(true);

      await client.publish(s, { subject: "jobs.a", value: "short", ttl: 150 });
      await client.publish(s, { subject: "jobs.a", value: "keep" });
      await client.publish(s, { subject: "jobs.a", value: "long", ttl: "1h" });
      const c = uniq("ttl-c");
      await client.createConsumer({ name: c, stream: s });
      await sleep(400);

      expect((await client.read(s)).records.map((r) => r.text())).toEqual(["keep", "long"]);
      const got = await client.pull(c, { maxMessages: 10, expiresMs: 500 });
      expect(got.map((m) => m.text())).toEqual(["keep", "long"]);
      expect(got[1]!.header("exspeed-ttl")).toBe("1h");
    });

    it("rejects time headers on a stream that doesn't allow them", async () => {
      const s = uniq("plain");
      await client.createStream(s);
      for (const opts of [{ ttl: 1000 }, { delay: 1000 }]) {
        const err = await client.publish(s, { subject: "a.b", value: "x", ...opts }).catch((e) => e);
        expect(err).toBeInstanceOf(ServerError);
        expect(err.code).toBe(400);
      }
    });

    it("delivers delayed records to consumers when due", async () => {
      const s = uniq("delay");
      await client.createStream({ name: s, allowDelayed: true });
      const c = uniq("delay-c");
      await client.createConsumer({ name: c, stream: s });
      await client.publish(s, { subject: "later.a", value: "delayed", delay: 700 });
      await client.publish(s, { subject: "later.a", value: "at", deliverAt: Date.now() + 900 });
      await client.publish(s, { subject: "later.a", value: "now" });

      const first = await client.pull(c, { maxMessages: 10, expiresMs: 300 });
      expect(first.map((m) => m.text())).toEqual(["now"]);
      await client.ack(c, first.map((m) => m.offset));
      expect((await client.consumerInfo(c)).numDelayed).toBe(2);

      const due: string[] = [];
      await eventually(async () => {
        for (const m of await client.pull(c, { maxMessages: 10, expiresMs: 200 })) {
          due.push(m.text());
          m.ack();
        }
        return due.length === 2;
      });
      expect(due).toEqual(["delayed", "at"]);
      // A stateless read sees every record right away: delays apply to consumers.
      expect((await client.read(s)).records.map((r) => r.text())).toEqual(["delayed", "at", "now"]);
    });

    it("keeps at most maxMsgs, dropping the oldest or rejecting new ones", async () => {
      const old = uniq("max-old");
      await client.createStream({ name: old, maxMsgs: 2 });
      for (const v of ["1", "2", "3"]) await client.publish(old, { subject: "m.a", value: v });
      expect((await client.read(old)).records.map((r) => r.text())).toEqual(["2", "3"]);

      const strict = uniq("max-new");
      await client.createStream({ name: strict, maxMsgs: 2, discard: "new" });
      await client.publish(strict, { subject: "m.a", value: "1" });
      await client.publish(strict, { subject: "m.a", value: "2" });
      const err = await client.publish(strict, { subject: "m.a", value: "3" }).catch((e) => e);
      expect(err).toBeInstanceOf(ServerError);
      expect(err.code).toBe(429);
      expect((await client.read(strict)).records.map((r) => r.text())).toEqual(["1", "2"]);
      expect((await client.streamInfo(strict)).config).toMatchObject({ maxMsgs: 2, discard: "new" });
    });
  });

  describe("consumer routing", () => {
    it("filters by headers (all / any)", async () => {
      const s = uniq("hdr");
      await client.createStream(s);
      for (const [region, tier, v] of [
        ["eu", "gold", "a"],
        ["us", "gold", "b"],
        ["eu", "free", "c"],
        ["asia", "free", "d"],
      ] as const) {
        await client.publish(s, { subject: "e.x", value: v, headers: { region, tier } });
      }
      const all = uniq("hdr-all");
      const info = await client.createConsumer({ name: all, stream: s, filterHeaders: { region: "eu", tier: "gold" } });
      expect(info.spec.filterHeaders).toEqual({ region: "eu", tier: "gold" });
      expect((await client.pull(all, { expiresMs: 300 })).map((m) => m.text())).toEqual(["a"]);

      const any = uniq("hdr-any");
      await client.createConsumer({ name: any, stream: s, filterHeaders: { region: "eu", tier: "gold" }, headerMatch: "any" });
      expect((await client.pull(any, { expiresMs: 300 })).map((m) => m.text())).toEqual(["a", "b", "c"]);
    });

    it("delivers higher priorities first within the priority window", async () => {
      const s = uniq("prio");
      await client.createStream(s);
      for (const [v, priority] of [
        ["low1", 0],
        ["high1", 9],
        ["mid", 5],
        ["low2", 0],
        ["high2", 9],
      ] as const) {
        await client.publish(s, { subject: "t.x", value: v, priority });
      }
      const c = uniq("prio-c");
      await client.createConsumer({ name: c, stream: s, priorityWindow: 100 });
      const order: string[] = [];
      while (order.length < 5) {
        const got = await client.pull(c, { maxMessages: 10, expiresMs: 500 });
        await client.ack(c, got.map((m) => m.offset));
        order.push(...got.map((m) => m.text()));
      }
      expect(order).toEqual(["high1", "high2", "mid", "low1", "low2"]);
    });
  });

  describe("core pub/sub", () => {
    it("fans out to every matching subscription and stores nothing", async () => {
      const [a, b] = [await server.connect(), await server.connect()];
      try {
        const all = await a.subscribeCore("orders.>");
        const eu = await b.subscribeCore("orders.eu.*");
        await client.publishCore("orders.eu.created", { id: 1 }, { headers: { "trace-id": "t1" } });
        await client.publishCore("orders.us.created", "2");
        await client.publishCore("billing.x", "ignored");

        const m1 = (await all.next({ timeoutMs: 5_000 }))!;
        expect([m1.subject, m1.json(), m1.header("trace-id"), m1.replyTo]).toEqual(["orders.eu.created", { id: 1 }, "t1", null]);
        expect((await all.next({ timeoutMs: 5_000 }))!.subject).toBe("orders.us.created");
        expect((await eu.next({ timeoutMs: 5_000 }))!.subject).toBe("orders.eu.created");
        expect(await eu.next({ timeoutMs: 200 })).toBeNull();
        expect(await all.next({ timeoutMs: 200 })).toBeNull();

        // A late subscriber sees only what comes next.
        const late = await a.subscribeCore("orders.>");
        expect(await late.next({ timeoutMs: 200 })).toBeNull();
        await late.unsubscribe();
        await all.unsubscribe();
        await client.publishCore("orders.eu.created", "3");
        expect((await eu.next({ timeoutMs: 5_000 }))!.text()).toBe("3");
        expect(all.closed).toBe(true);
      } finally {
        await a.close();
        await b.close();
      }
    });

    it("splits messages across a queue group", async () => {
      const [w1, w2] = [await server.connect(), await server.connect()];
      try {
        const subject = `jobs.${uniq("q")}`;
        const s1 = await w1.subscribeCore(subject, { queue: "workers" });
        const s2 = await w2.subscribeCore(subject, { queue: "workers" });
        for (let i = 0; i < 20; i++) await client.publishCore(subject, String(i));
        const count = async (s: typeof s1) => {
          let n = 0;
          while (await s.next({ timeoutMs: 300 })) n++;
          return n;
        };
        const [n1, n2] = [await count(s1), await count(s2)];
        expect(n1 + n2).toBe(20);
        expect(n1).toBeGreaterThan(0);
        expect(n2).toBeGreaterThan(0);
      } finally {
        await w1.close();
        await w2.close();
      }
    });
  });

  describe("request-reply", () => {
    it("answers requests through one inbox, and fails fast with no responders", async () => {
      const svc = await server.connect();
      try {
        const reqs = await svc.subscribeCore("svc.upper", { queue: "svc" });
        const responder = (async () => {
          for await (const m of reqs) await m.respond(m.text().toUpperCase());
        })();

        const r = await client.request("svc.upper", "hello", { timeoutMs: 5_000 });
        expect(r).toBeInstanceOf(CoreMessage);
        expect(r.text()).toBe("HELLO");
        const many = await Promise.all(
          Array.from({ length: 20 }, (_, i) => client.request("svc.upper", `m${i}`, { timeoutMs: 5_000 })),
        );
        expect(many.map((m) => m.text())).toEqual(Array.from({ length: 20 }, (_, i) => `M${i}`));

        const t = Date.now();
        const err = await client.request("svc.nobody", "x", { timeoutMs: 5_000 }).catch((e) => e);
        expect(err).toBeInstanceOf(ServerError);
        expect(err.code).toBe(404);
        expect(Date.now() - t).toBeLessThan(2_000);

        await reqs.unsubscribe();
        await responder;
      } finally {
        await svc.close();
      }
    });

    it("times out when a responder never answers", async () => {
      const svc = await server.connect();
      try {
        const subject = `svc.${uniq("silent")}`;
        const silent = await svc.subscribeCore(subject);
        const err = await client.request(subject, "x", { timeoutMs: 300 }).catch((e) => e);
        expect(err).toBeInstanceOf(TimeoutError);
        expect((await silent.next({ timeoutMs: 1_000 }))!.replyTo).toMatch(/^_INBOX\./);
      } finally {
        await svc.close();
      }
    });
  });

  describe("kv buckets", () => {
    it("puts, gets, compares-and-sets, deletes and lists keys", async () => {
      const kv = client.kv(uniq("cfg"));
      await kv.create({ history: 3 });
      await kv.create({ history: 3 }); // idempotent
      expect(await kv.get("app.mode")).toBeNull();

      const r1 = await kv.put("app.mode", "dev");
      const r2 = await kv.put("app.mode", { mode: "prod" });
      expect(r1).toBe(1);
      expect(r2).toBe(2);
      const e = (await kv.get("app.mode"))!;
      expect([e.key, e.revision, e.op, e.json()]).toEqual(["app.mode", r2, "put", { mode: "prod" }]);
      expect((await kv.getRevision("app.mode", r1))!.text()).toBe("dev");

      // Compare-and-set.
      const created = await kv.createKey("app.port", "8080");
      const dup = await kv.createKey("app.port", "9090").catch((err) => err);
      expect(dup).toBeInstanceOf(ServerError);
      expect(dup.code).toBe(409);
      const updated = await kv.update("app.port", "9090", created);
      const stale = await kv.update("app.port", "1", created).catch((err) => err);
      expect(stale.code).toBe(409);
      expect(stale.detail).toMatchObject({ current_revision: updated });

      await kv.put("db.url", "postgres://");
      expect(await kv.keys()).toEqual(["app.mode", "app.port", "db.url"]);
      expect(await kv.keys("app.*")).toEqual(["app.mode", "app.port"]);

      const del = await kv.delete("app.port");
      expect(del).toBeGreaterThan(updated);
      expect(await kv.get("app.port")).toBeNull();
      expect(await kv.keys("app.*")).toEqual(["app.mode"]);
      expect((await kv.history("app.port")).map((h) => [h.text(), h.op])).toEqual([
        ["8080", "put"],
        ["9090", "put"],
        ["", "delete"],
      ]);
      // A deleted key can be created again.
      expect(await kv.createKey("app.port", "7070")).toBeGreaterThan(del);

      await kv.purge("app.mode");
      expect(await kv.get("app.mode")).toBeNull();

      const missing = await client.kv(uniq("missing")).get("x").catch((err) => err);
      expect(missing).toBeInstanceOf(ServerError);
      expect(missing.code).toBe(404);

      await kv.destroy();
      expect((await client.streamInfo(kv.stream).catch((err) => err)).code).toBe(404);
    });

    it("expires keys after their TTL", async () => {
      const kv = client.kv(uniq("ttl"));
      await kv.create();
      await kv.put("session.a", "x", { ttlMs: 200 });
      await kv.put("session.b", "y");
      await sleep(500);
      expect(await kv.get("session.a")).toBeNull();
      expect((await kv.get("session.b"))!.text()).toBe("y");
    });

    it("watches: current values first, then every change", async () => {
      const kv = client.kv(uniq("watch"));
      await kv.create();
      await kv.put("user.1", "alice");
      await kv.put("user.2", "bob");
      await kv.put("user.1", "alice2");
      await kv.put("other.x", "filtered");
      await kv.put("user.3", "gone");
      await kv.delete("user.3");

      const w = kv.watch("user.*");
      const take = async (n: number): Promise<KvEntry[]> => {
        const out: KvEntry[] = [];
        while (out.length < n) {
          const e = await w.next({ timeoutMs: 5_000 });
          if (!e) throw new Error(`watch stalled after ${out.length} entries`);
          out.push(e);
        }
        return out;
      };
      const snapshot = await take(2);
      expect(snapshot.map((e) => [e.key, e.text(), e.revision])).toEqual([
        ["user.2", "bob", 2],
        ["user.1", "alice2", 3],
      ]);

      await kv.put("user.4", "dave");
      await kv.put("other.y", "filtered");
      await kv.delete("user.2");
      const changes = await take(2);
      expect(changes.map((e) => [e.key, e.op])).toEqual([
        ["user.4", "put"],
        ["user.2", "delete"],
      ]);
      expect(await w.next({ timeoutMs: 200 })).toBeNull();
      w.stop();
      expect(await w.next()).toBeNull();
    });
  });
});
