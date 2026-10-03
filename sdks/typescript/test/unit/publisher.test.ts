import { afterEach, describe, expect, it } from "vitest";
import { ExspeedClient, ServerError } from "../../src/index.js";
import type { Request } from "../../src/protocol/index.js";
import { FakeServer, type FakeConn } from "./fake-server.js";

let server: FakeServer;
let client: ExspeedClient;
let nextOffset = 0;

/** Auto-acknowledge publishes with increasing offsets. */
function autoAck(conn: FakeConn, corr: number, req: Request): void {
  if (req.type === "Publish") conn.reply(corr, { type: "PublishOk", offset: nextOffset++, duplicate: false });
  if (req.type === "PublishBatch") {
    conn.reply(corr, {
      type: "PublishBatchOk",
      results: req.records.map(() => ({ offset: nextOffset++, duplicate: false })),
    });
  }
}

async function setup(handler = autoAck) {
  nextOffset = 0;
  server = await FakeServer.start(handler);
  client = await ExspeedClient.connect({ port: server.port, keepaliveMs: 0, reconnect: false });
}

afterEach(async () => {
  await client?.close();
  await server?.close();
});

const subjects = (reqs: Request[]) =>
  reqs.flatMap((r) =>
    r.type === "PublishBatch" ? r.records.map((x) => x.subject) : r.type === "Publish" ? [r.record.subject] : [],
  );

describe("Publisher", () => {
  it("coalesces publishes from one tick into a single batch, in call order", async () => {
    await setup();
    const p = client.publisher();
    const results = await Promise.all(
      Array.from({ length: 100 }, (_, i) => p.publish("s", { subject: `n.${i}`, value: String(i) })),
    );
    expect(results.map((r) => r.offset)).toEqual(Array.from({ length: 100 }, (_, i) => i));
    const sent = server.last.received.slice(1).map((r) => r.req);
    expect(sent.map((r) => r.type)).toEqual(["PublishBatch"]);
    expect(subjects(sent)).toEqual(Array.from({ length: 100 }, (_, i) => `n.${i}`));
  });

  it("sends a lone record as a plain Publish", async () => {
    await setup();
    const r = await client.publisher().publish("s", { subject: "one", value: "x" });
    expect(r).toEqual({ offset: 0, duplicate: false });
    expect(server.last.received[1]!.req.type).toBe("Publish");
  });

  it("splits by maxBatchRecords and by stream, keeping order", async () => {
    await setup();
    const p = client.publisher({ maxBatchRecords: 3 });
    const calls = [];
    for (let i = 0; i < 4; i++) calls.push(p.publish("a", { subject: `a.${i}`, value: "" }));
    for (let i = 0; i < 2; i++) calls.push(p.publish("b", { subject: `b.${i}`, value: "" }));
    calls.push(p.publish("a", { subject: "a.4", value: "" }));
    await Promise.all(calls);
    const sent = server.last.received.slice(1).map((r) => r.req);
    const shape = sent.map((r) => [
      r.type,
      "stream" in r ? r.stream : "",
      r.type === "PublishBatch" ? r.records.length : 1,
    ]);
    // A full queue (3 records) is flushed at once, split into same-stream runs.
    expect(shape).toEqual([
      ["PublishBatch", "a", 3],
      ["Publish", "a", 1],
      ["PublishBatch", "b", 2],
      ["Publish", "a", 1],
    ]);
    expect(subjects(sent)).toEqual(["a.0", "a.1", "a.2", "a.3", "b.0", "b.1", "a.4"]);
  });

  it("waits for a batch window before sending", async () => {
    await setup();
    const p = client.publisher({ batchWindowMs: 30 });
    const a = p.publish("s", { subject: "x", value: "1" });
    await new Promise((r) => setTimeout(r, 5));
    const b = p.publish("s", { subject: "y", value: "2" });
    await Promise.all([a, b]);
    expect(server.last.received.slice(1).map((r) => r.req.type)).toEqual(["PublishBatch"]);
  });

  it("bounds records in flight and keeps order while waiting", async () => {
    const held: [FakeConn, number, Request][] = [];
    await setup((conn, corr, req) => {
      if (req.type === "Publish" || req.type === "PublishBatch") held.push([conn, corr, req]);
    });
    const p = client.publisher({ maxInFlight: 2 });
    const all = Array.from({ length: 5 }, (_, i) => p.publish("s", { subject: `n.${i}`, value: "" }));
    await server.until(() => held.length === 1);
    await new Promise((r) => setTimeout(r, 30));
    expect(held).toHaveLength(1); // first two only
    expect(p.pending).toBe(2);
    while (held.length > 0 || p.pending > 0) {
      const next = held.shift();
      if (next) autoAck(...next);
      await new Promise((r) => setTimeout(r, 10));
    }
    await Promise.all(all);
    expect(subjects(server.last.received.map((r) => r.req))).toEqual(["n.0", "n.1", "n.2", "n.3", "n.4"]);
  });

  it("rejects every record of a failed batch with the server's error", async () => {
    await setup((conn, corr, req) => {
      if (req.type === "PublishBatch") conn.reply(corr, { type: "Error", code: 403, message: "forbidden", detail: null });
    });
    const p = client.publisher();
    const results = await Promise.allSettled([
      p.publish("s", { subject: "a", value: "" }),
      p.publish("s", { subject: "b", value: "" }),
    ]);
    for (const r of results) {
      expect(r.status).toBe("rejected");
      expect((r as PromiseRejectedResult).reason).toBeInstanceOf(ServerError);
    }
    await p.flush();
    expect(p.pending).toBe(0);
  });

  it("flush waits for everything accepted; close rejects later publishes", async () => {
    await setup();
    const p = client.publisher();
    const done: number[] = [];
    for (let i = 0; i < 10; i++) void p.publish("s", { subject: "x", value: "" }).then((r) => done.push(r.offset));
    await p.flush();
    expect(p.pending).toBe(0);
    await p.close();
    await expect(p.publish("s", { subject: "x", value: "" })).rejects.toThrow(/closed/);
  });
});
