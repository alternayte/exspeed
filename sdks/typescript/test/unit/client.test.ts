import { afterEach, describe, expect, it } from "vitest";
import { ConnectionError, ExspeedClient, ServerError, TimeoutError } from "../../src/index.js";
import type { WireRecord } from "../../src/protocol/index.js";
import { FakeServer } from "./fake-server.js";

const rec = (offset: number, value = `v${offset}`): WireRecord => ({
  offset,
  timestampNs: 1_700_000_000_000_000_000n + BigInt(offset),
  deliveryCount: 1,
  subject: "orders.placed",
  key: null,
  value: Buffer.from(value),
  headers: [["h", "1"]],
});

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

describe("handshake", () => {
  it("sends client id and token and exposes server info", async () => {
    const c = await setup({ token: "secret", clientId: "unit" });
    expect(server.last.received[0]!.req).toEqual({ type: "Connect", clientId: "unit", token: "secret" });
    expect(server.last.received[0]!.corr).not.toBe(0);
    expect(c.serverInfo).toEqual({ serverVersion: "test", nodeId: "n1", leader: null });
  });

  it("rejects with ServerError 401 when the token is refused", async () => {
    server = await FakeServer.start((conn, corr, req) => {
      if (req.type !== "Connect") return;
      conn.reply(corr, { type: "Error", code: 401, message: "unauthorized", detail: null });
      conn.socket.end();
      return true;
    });
    const err = await ExspeedClient.connect({ port: server.port, token: "bad" }).catch((e) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect(err.code).toBe(401);
  });

  it("fails with ConnectionError when nothing listens", async () => {
    const s = await FakeServer.start();
    const port = s.port;
    await s.close();
    await expect(ExspeedClient.connect({ port })).rejects.toBeInstanceOf(ConnectionError);
  });
});

describe("requests", () => {
  it("matches out-of-order responses by correlation id", async () => {
    const c = await setup();
    server.handler = () => true; // answer manually
    const a = c.publish("s", { subject: "a", value: "1" });
    const b = c.publish("s", { subject: "b", value: "2" });
    await server.until(() => server.last.of("Publish").length === 2);
    const [ra, rb] = server.last.of("Publish");
    server.last.reply(rb!.corr, { type: "PublishOk", offset: 2, duplicate: false });
    server.last.reply(ra!.corr, { type: "PublishOk", offset: 1, duplicate: true });
    expect(await a).toEqual({ offset: 1, duplicate: true });
    expect(await b).toEqual({ offset: 2, duplicate: false });
  });

  it("encodes values: bytes as-is, strings as UTF-8, objects as JSON", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "PublishBatch") {
        conn.reply(corr, { type: "PublishBatchOk", results: req.records.map((_, i) => ({ offset: i, duplicate: false })) });
      }
    };
    await c.publishBatch("s", [
      { subject: "a", value: new Uint8Array([1, 2]) },
      { subject: "a", value: "hé" },
      { subject: "a", value: { id: 1 }, key: "k", headers: { x: "y" }, msgId: "m1" },
    ]);
    const recs = server.last.of("PublishBatch")[0]!.req.records;
    expect(Buffer.from(recs[0]!.value)).toEqual(Buffer.from([1, 2]));
    expect(Buffer.from(recs[1]!.value).toString()).toBe("hé");
    expect(Buffer.from(recs[2]!.value).toString()).toBe('{"id":1}');
    expect(Buffer.from(recs[2]!.key!).toString()).toBe("k");
    expect(recs[2]!.headers).toEqual([["x", "y"]]);
    expect(recs[2]!.msgId).toBe("m1");
  });

  it("surfaces code, message, detail and the leader hint of server errors", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "StreamInfo") {
        conn.reply(corr, { type: "Error", code: 503, message: "not the leader", detail: Buffer.from('{"leader":"h2:5933"}') });
      }
    };
    const err = await c.streamInfo("s").catch((e) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect(err.code).toBe(503);
    expect(err.message).toBe("not the leader");
    expect(err.detail).toEqual({ leader: "h2:5933" });
    expect(err.leaderHint).toBe("h2:5933");
  });

  it("times out requests the server never answers", async () => {
    const c = await setup({ requestTimeoutMs: 100 });
    server.handler = () => true;
    await expect(c.metadata()).rejects.toBeInstanceOf(TimeoutError);
  });

  it("gives pull and read extra time for their server-side wait", async () => {
    const c = await setup({ requestTimeoutMs: 50 });
    server.handler = (conn, corr, req) => {
      if (req.type === "Pull") setTimeout(() => conn.reply(corr, { type: "Messages", records: [rec(3)] }), 150);
      return req.type === "Pull";
    };
    const msgs = await c.pull("c", { expiresMs: 200 });
    expect(msgs.map((m) => m.offset)).toEqual([3]);
    expect(server.last.of("Pull")[0]!.req).toMatchObject({ consumer: "c", maxMessages: 100, expiresMs: 200 });
  });

  it("camelizes JSON replies", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "Metadata") {
        conn.reply(corr, {
          type: "Json",
          json: Buffer.from('{"node_id":"n1","is_leader":true,"leader":null,"server_version":"x"}'),
        });
      }
    };
    expect(await c.metadata()).toEqual({ nodeId: "n1", isLeader: true, leader: null, serverVersion: "x" });
  });

  it("fails pending requests with ConnectionError when the connection drops", async () => {
    const c = await setup();
    server.handler = () => true;
    const p = c.metadata();
    await server.until(() => server.last.of("Metadata").length === 1);
    server.last.socket.destroy();
    await expect(p).rejects.toBeInstanceOf(ConnectionError);
    await expect(c.ping()).rejects.toBeInstanceOf(ConnectionError);
  });
});

describe("subscriptions", () => {
  it("keeps Deliver frames that arrive in the same chunk as SubscribeOk", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type !== "Subscribe") return;
      conn.replyMany([
        [corr, { type: "SubscribeOk", subId: 5 }],
        [0, { type: "Deliver", subId: 5, records: [rec(0), rec(1)] }],
      ]);
      return true;
    };
    const sub = await c.subscribe("billing", { window: 10 });
    expect(sub.id).toBe(5);
    const m0 = await sub.next({ timeoutMs: 500 });
    const m1 = await sub.next({ timeoutMs: 500 });
    expect([m0?.offset, m1?.offset]).toEqual([0, 1]);
    expect(m0!.text()).toBe("v0");
    expect(m0!.header("h")).toBe("1");
    expect(m0!.deliveryCount).toBe(1);
    expect(m0!.consumer).toBe("billing");
  });

  it("returns credit in batches of half the window as messages are taken", async () => {
    const c = await setup();
    let subCorr = 0;
    server.handler = (conn, corr, req) => {
      if (req.type === "Subscribe") {
        subCorr = corr;
        conn.reply(corr, { type: "SubscribeOk", subId: 9 });
      }
    };
    const sub = await c.subscribe("c", { window: 4 });
    expect(server.last.of("Subscribe")[0]!.req.credits).toBe(4);
    expect(subCorr).not.toBe(0);
    server.last.reply(0, { type: "Deliver", subId: 9, records: [0, 1, 2, 3].map((i) => rec(i)) });
    await sub.next();
    expect(server.last.of("Credit")).toHaveLength(0);
    await sub.next();
    await server.until(() => server.last.of("Credit").length === 1);
    expect(server.last.of("Credit")[0]).toEqual({ corr: 0, req: { type: "Credit", subId: 9, credits: 2 } });
    await sub.next();
    await sub.next();
    await server.until(() => server.last.of("Credit").length === 2);
  });

  it("acks fire-and-forget and settles with nack/term/inProgress", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "Subscribe") {
        conn.replyMany([
          [corr, { type: "SubscribeOk", subId: 1 }],
          [0, { type: "Deliver", subId: 1, records: [rec(7)] }],
        ]);
      } else if (req.type === "Nack" || req.type === "Term" || req.type === "InProgress") {
        conn.reply(corr, { type: "Ok" });
      }
    };
    const sub = await c.subscribe("c");
    const m = (await sub.next())!;
    m.ack();
    await m.nack(250);
    await m.term("poison");
    await m.inProgress();
    const got = server.last.received.slice(2).map((r) => [r.corr === 0, r.req]);
    expect(got).toEqual([
      [true, { type: "Ack", consumer: "c", offsets: [7] }],
      [false, { type: "Nack", consumer: "c", offset: 7, delayMs: 250 }],
      [false, { type: "Term", consumer: "c", offset: 7, reason: "poison" }],
      [false, { type: "InProgress", consumer: "c", offsets: [7] }],
    ]);
  });

  it("sends acks made in the same tick as one frame, before any later request", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "Subscribe") {
        conn.replyMany([
          [corr, { type: "SubscribeOk", subId: 1 }],
          [0, { type: "Deliver", subId: 1, records: [rec(1), rec(2), rec(3)] }],
        ]);
      }
    };
    const sub = await c.subscribe("c");
    const msgs = [(await sub.next())!, (await sub.next())!, (await sub.next())!];
    for (const m of msgs) m.ack();
    void c.ping();
    await server.until(() => server.last.of("Ping").length === 1);
    // (window 256: no Credit is due after three messages)
    expect(server.last.received.slice(2).map((r) => r.req.type)).toEqual(["Ack", "Ping"]);
    expect(server.last.of("Ack").map((r) => [r.corr, r.req.offsets])).toEqual([[0, [1, 2, 3]]]);
  });

  it("flushes queued acks on close", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "Subscribe") {
        conn.replyMany([
          [corr, { type: "SubscribeOk", subId: 1 }],
          [0, { type: "Deliver", subId: 1, records: [rec(4)] }],
        ]);
      }
    };
    const sub = await c.subscribe("c");
    (await sub.next())!.ack();
    const conn = server.last;
    await c.close();
    client = null;
    await server.until(() => conn.of("Ack").length === 1);
  });

  it("reports failed fire-and-forget requests as 'error' events", async () => {
    const c = await setup();
    const errors: ServerError[] = [];
    c.on("error", (e) => errors.push(e));
    server.last.reply(0, { type: "Error", code: 404, message: "consumer 'x' not found", detail: null });
    await server.until(() => errors.length === 1).catch(() => {});
    await new Promise((r) => setTimeout(r, 50));
    expect(errors[0]?.code).toBe(404);
  });

  it("ends on SubscriptionEnded after yielding buffered records", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "Subscribe") {
        conn.replyMany([
          [corr, { type: "SubscribeOk", subId: 3 }],
          [0, { type: "Deliver", subId: 3, records: [rec(0)] }],
          [0, { type: "SubscriptionEnded", subId: 3, code: 404, message: "consumer deleted" }],
        ]);
      }
    };
    const sub = await c.subscribe("c");
    const seen: number[] = [];
    for await (const m of sub) seen.push(m.offset);
    expect(seen).toEqual([0]);
    expect(sub.endReason).toEqual({ code: 404, message: "consumer deleted" });
  });

  it("unsubscribes when a for-await loop breaks", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "Subscribe") {
        conn.replyMany([
          [corr, { type: "SubscribeOk", subId: 4 }],
          [0, { type: "Deliver", subId: 4, records: [rec(0), rec(1)] }],
        ]);
      } else if (req.type === "Unsubscribe") conn.reply(corr, { type: "Ok" });
    };
    const sub = await c.subscribe("c");
    for await (const _m of sub) break;
    expect(server.last.of("Unsubscribe")[0]!.req).toEqual({ type: "Unsubscribe", subId: 4 });
    expect(sub.endReason).toEqual({ code: 0, message: "unsubscribed" });
    expect(await sub.next()).toBeNull();
  });

  it("ends subscriptions with 503 when the connection is lost and reconnect is off", async () => {
    const c = await setup();
    server.handler = (conn, corr, req) => {
      if (req.type === "Subscribe") conn.reply(corr, { type: "SubscribeOk", subId: 1 });
    };
    const sub = await c.subscribe("c");
    const closed = new Promise((r) => c.once("close", r));
    const next = sub.next();
    server.last.socket.destroy();
    expect(await next).toBeNull();
    await closed;
    expect(sub.endReason?.code).toBe(503);
    expect(c.connected).toBe(false);
  });
});

describe("reconnection", () => {
  it("reconnects, re-creates ephemeral consumers and re-subscribes", async () => {
    server = await FakeServer.start();
    let nextSub = 1;
    server.handler = (conn, corr, req) => {
      if (req.type === "Subscribe") conn.reply(corr, { type: "SubscribeOk", subId: nextSub++ });
      if (req.type === "CreateConsumer") conn.reply(corr, { type: "Json", json: Buffer.from("{}") });
    };
    client = await ExspeedClient.connect({
      port: server.port,
      keepaliveMs: 0,
      reconnect: { initialDelayMs: 10, maxDelayMs: 20 },
    });
    const c = client;
    await c.createConsumer({ name: "tmp", stream: "s", ephemeral: true });
    const sub = await c.subscribe("tmp", { window: 8 });
    expect(sub.id).toBe(1);
    server.last.reply(0, { type: "Deliver", subId: 1, records: [rec(0)] });
    expect((await sub.next())!.offset).toBe(0);

    const events: string[] = [];
    c.on("disconnect", () => events.push("disconnect"));
    const reconnected = new Promise((r) => c.once("reconnect", r));
    server.conns[0]!.socket.destroy();
    await reconnected;
    events.push("reconnect");
    expect(events).toEqual(["disconnect", "reconnect"]);
    expect(server.conns).toHaveLength(2);
    const second = server.conns[1]!;
    expect(second.received.map((r) => r.req.type)).toEqual(["Connect", "CreateConsumer", "Subscribe"]);
    expect(second.of("Subscribe")[0]!.req).toEqual({ type: "Subscribe", consumer: "tmp", credits: 8 });
    expect(sub.id).toBe(2);

    second.reply(0, { type: "Deliver", subId: 2, records: [rec(1)] });
    expect((await sub.next({ timeoutMs: 1000 }))!.offset).toBe(1);
    expect(c.connected).toBe(true);
  });

  it("ends a subscription whose re-subscribe fails", async () => {
    server = await FakeServer.start();
    let first = true;
    server.handler = (conn, corr, req) => {
      if (req.type !== "Subscribe") return;
      if (first) conn.reply(corr, { type: "SubscribeOk", subId: 1 });
      else conn.reply(corr, { type: "Error", code: 404, message: "consumer 'c' not found", detail: null });
      first = false;
    };
    client = await ExspeedClient.connect({ port: server.port, keepaliveMs: 0, reconnect: { initialDelayMs: 10 } });
    const sub = await client.subscribe("c");
    const reconnected = new Promise((r) => client!.once("reconnect", r));
    server.conns[0]!.socket.destroy();
    await reconnected;
    expect(await sub.next({ timeoutMs: 1000 })).toBeNull();
    expect(sub.endReason).toEqual({ code: 404, message: "consumer 'c' not found" });
  });

  it("gives up after maxAttempts and closes", async () => {
    server = await FakeServer.start();
    client = await ExspeedClient.connect({
      port: server.port,
      keepaliveMs: 0,
      reconnect: { maxAttempts: 2, initialDelayMs: 10 },
    });
    const closed = new Promise<Error>((r) => client!.once("close", r));
    await server.close();
    const err = await closed;
    expect(err).toBeInstanceOf(ConnectionError);
    expect(client.connected).toBe(false);
    await expect(client.ping()).rejects.toThrow(/closed/);
  });
});

describe("keepalive", () => {
  it("pings on the configured interval", async () => {
    await setup({ keepaliveMs: 30 });
    await server.until(() => server.last.of("Ping").length >= 2, 1000);
  });
});

describe("cluster leader discovery", () => {
  it("connects to the leader by following hints from a seed", async () => {
    let leaderPort = 0;
    const metadata = (conn: import("./fake-server.js").FakeConn, corr: number, isLeader: boolean, leader: string | null) =>
      conn.reply(corr, {
        type: "Json",
        json: Buffer.from(JSON.stringify({ node_id: "x", is_leader: isLeader, leader, server_version: "t" })),
      });
    const follower = await FakeServer.start((conn, corr, req) => {
      if (req.type === "Connect") {
        conn.reply(corr, { type: "ConnectOk", serverVersion: "t", nodeId: "f", leader: `127.0.0.1:${leaderPort}` });
        return true;
      }
      if (req.type === "Metadata") {
        metadata(conn, corr, false, `127.0.0.1:${leaderPort}`);
        return true;
      }
    });
    const leader = await FakeServer.start((conn, corr, req) => {
      if (req.type === "Connect") {
        conn.reply(corr, { type: "ConnectOk", serverVersion: "t", nodeId: "l", leader: null });
        return true;
      }
      if (req.type === "Metadata") {
        metadata(conn, corr, true, null);
        return true;
      }
    });
    leaderPort = leader.port;
    server = follower; // closed by afterEach
    try {
      client = await ExspeedClient.connect({
        servers: [`127.0.0.1:${follower.port}`],
        keepaliveMs: 0,
        reconnect: { initialDelayMs: 10, maxDelayMs: 20, maxAttempts: 20 },
      });
      expect(client.serverInfo.nodeId).toBe("l");
    } finally {
      await leader.close();
    }
  });
});
