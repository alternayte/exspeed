import { describe, it, expect, afterEach, vi } from "vitest";
import * as net from "node:net";
import { ExspeedClient } from "../src/client.js";
import {
  OpCode, PROTOCOL_VERSION, encodeFrame, decodeFrame,
} from "../src/protocol/index.js";
import { decodePublish } from "../src/protocol/publish.js";
import { decodeCreateStream } from "../src/protocol/stream.js";
import { encodeRecord } from "../src/protocol/record.js";
import { Connection } from "../src/connection.js";
import { encodeErrorFrame } from "../src/protocol/error-frame.js";
import { KeyCollisionError, DedupMapFullError } from "../src/errors.js";

function createTestServer(
  handler: (frame: { opcode: OpCode; correlationId: number; payload: Buffer }, socket: net.Socket) => void,
) {
  const connections: net.Socket[] = [];
  const server = net.createServer((socket) => {
    connections.push(socket);
    let buf = Buffer.alloc(0);
    socket.on("data", (data) => {
      buf = Buffer.concat([buf, data]);
      let offset = 0;
      while (true) {
        const result = decodeFrame(buf, offset);
        if (!result) break;
        offset += result.bytesConsumed;
        handler(result.frame, socket);
      }
      if (offset > 0) buf = buf.subarray(offset);
    });
  });
  return {
    server,
    connections,
    start: () => new Promise<number>((resolve) => {
      server.listen(0, "127.0.0.1", () => resolve((server.address() as net.AddressInfo).port));
    }),
    stop: () => new Promise<void>((resolve) => {
      connections.forEach((c) => c.destroy());
      server.close(() => resolve());
    }),
  };
}

function replyOk(socket: net.Socket, correlationId: number, payload = Buffer.alloc(0)) {
  socket.write(encodeFrame({
    version: PROTOCOL_VERSION,
    opcode: OpCode.Ok,
    correlationId,
    payload,
  }));
}

describe("ExspeedClient", () => {
  let testServer: ReturnType<typeof createTestServer>;

  afterEach(async () => {
    if (testServer) await testServer.stop();
  });

  it("publish() sends data as JSON and returns offset", async () => {
    let capturedPublish: any;
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        replyOk(socket, frame.correlationId);
      } else if (frame.opcode === OpCode.Publish) {
        capturedPublish = decodePublish(frame.payload);
        const payload = Buffer.alloc(8);
        payload.writeBigUInt64LE(42n, 0);
        replyOk(socket, frame.correlationId, payload);
      }
    });
    const port = await testServer.start();

    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test", reconnect: false });
    await client.connect();

    const result = await client.publish("orders", {
      subject: "orders.created",
      data: { id: 1 },
      headers: [["source", "test"]],
    });

    expect(result.offset).toBe(42n);
    expect(capturedPublish.stream).toBe("orders");
    expect(capturedPublish.subject).toBe("orders.created");
    expect(JSON.parse(capturedPublish.value.toString())).toEqual({ id: 1 });
    expect(capturedPublish.headers).toEqual([["source", "test"]]);
    expect(result.toJSON()).toEqual({ offset: "42" });

    await client.close();
  });

  it("publish() sends raw Buffer when value is provided", async () => {
    let capturedValue: Buffer;
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        replyOk(socket, frame.correlationId);
      } else if (frame.opcode === OpCode.Publish) {
        capturedValue = decodePublish(frame.payload).value;
        const payload = Buffer.alloc(8);
        payload.writeBigUInt64LE(0n, 0);
        replyOk(socket, frame.correlationId, payload);
      }
    });
    const port = await testServer.start();

    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test", reconnect: false });
    await client.connect();

    const raw = Buffer.from([0xde, 0xad]);
    await client.publish("s", { subject: "s", value: raw });
    expect(Buffer.compare(capturedValue!, raw)).toBe(0);

    await client.close();
  });

  it("createStream() sends correct protocol message", async () => {
    let captured: any;
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        replyOk(socket, frame.correlationId);
      } else if (frame.opcode === OpCode.CreateStream) {
        captured = decodeCreateStream(frame.payload);
        replyOk(socket, frame.correlationId);
      }
    });
    const port = await testServer.start();

    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test", reconnect: false });
    await client.connect();
    await client.createStream("orders", { maxAgeSecs: 86400 });

    expect(captured.name).toBe("orders");
    expect(captured.maxAgeSecs).toBe(86400n);
    expect(captured.maxBytes).toBe(0n);

    await client.close();
  });

  it("subscribe() receives pushed records via async iterator", async () => {
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        replyOk(socket, frame.correlationId);
      } else if (frame.opcode === OpCode.Subscribe) {
        replyOk(socket, frame.correlationId);
        setTimeout(() => {
          const recordPayload = encodeRecord({
            consumerName: "my-consumer",
            offset: 0n,
            timestamp: 1000n,
            subject: "orders.created",
            deliveryAttempt: 1,
            value: Buffer.from('{"id":99}'),
            headers: [],
          });
          socket.write(encodeFrame({
            version: PROTOCOL_VERSION,
            opcode: OpCode.Record,
            correlationId: 0,
            payload: recordPayload,
          }));
        }, 50);
      } else if (frame.opcode === OpCode.Ack) {
        replyOk(socket, frame.correlationId);
      } else if (frame.opcode === OpCode.Unsubscribe) {
        replyOk(socket, frame.correlationId);
      }
    });
    const port = await testServer.start();

    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test", reconnect: false });
    await client.connect();

    const sub = await client.subscribe("my-consumer");
    const messages: any[] = [];

    for await (const msg of sub) {
      messages.push(msg);
      expect(msg.json()).toEqual({ id: 99 });
      expect(msg.subject).toBe("orders.created");
      await msg.ack();
      await sub.unsubscribe();
      break;
    }

    expect(messages.length).toBe(1);
    await client.close();
  });

  it("ping() returns latency", async () => {
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        replyOk(socket, frame.correlationId);
      } else if (frame.opcode === OpCode.Ping) {
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.Pong,
          correlationId: frame.correlationId,
          payload: Buffer.alloc(0),
        }));
      }
    });
    const port = await testServer.start();

    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test", reconnect: false });
    await client.connect();

    const latency = await client.ping();
    expect(typeof latency).toBe("number");
    expect(latency).toBeGreaterThanOrEqual(0);

    await client.close();
  });
});

describe("subscribe() ack/nack routing", () => {
  afterEach(() => vi.restoreAllMocks());

  it("ackFn binds to subscription's own connection (not mainConn)", async () => {
    const client = new ExspeedClient({
      brokers: [{ host: "127.0.0.1", port: 9 }], // discard port — we stub connect
      clientId: "ack-routing-test",
      reconnect: false,
    });

    vi.spyOn(Connection.prototype as any, "connect").mockResolvedValue(undefined);
    const reqSpy = vi
      .spyOn(Connection.prototype as any, "request")
      .mockResolvedValue({ opcode: 0x10, payload: Buffer.alloc(0), correlationId: 0, version: 1 });

    const sub = await client.subscribe("c");
    const entry = (client as any).subscriptions.get("c");
    expect(entry).toBeDefined();
    expect(entry.conn).not.toBe((client as any).mainConn);

    // Drive a synthetic Record push onto the subscription's connection.
    const { OpCode } = await import("../src/protocol/index.js");
    const { encodeRecord } = await import("../src/protocol/record.js");
    entry.conn.emit("push", {
      opcode: OpCode.Record, correlationId: 0, version: 1,
      payload: encodeRecord({
        consumerName: "c", offset: 5n, timestamp: 0n,
        subject: "s", deliveryAttempt: 1, key: undefined,
        value: Buffer.from(""), headers: [],
      }),
    });

    const iter = entry.sub[Symbol.asyncIterator]();
    const next = await iter.next();
    expect(next.done).toBe(false);

    reqSpy.mockClear();          // reset history AFTER subscribe's handshake calls
    await next.value!.ack();

    expect(reqSpy).toHaveBeenCalledTimes(1);
    expect(reqSpy.mock.instances[0]).toBe(entry.conn);
    expect(reqSpy.mock.instances[0]).not.toBe((client as any).mainConn);

    await client.close(100);
    vi.restoreAllMocks();
  });

  it("nackFn binds to subscription's own connection (not mainConn)", async () => {
    const client = new ExspeedClient({
      brokers: [{ host: "127.0.0.1", port: 9 }],
      clientId: "nack-routing-test",
      reconnect: false,
    });

    vi.spyOn(Connection.prototype as any, "connect").mockResolvedValue(undefined);
    const reqSpy = vi
      .spyOn(Connection.prototype as any, "request")
      .mockResolvedValue({ opcode: 0x10, payload: Buffer.alloc(0), correlationId: 0, version: 1 });

    const sub = await client.subscribe("c");
    const entry = (client as any).subscriptions.get("c");

    const { OpCode } = await import("../src/protocol/index.js");
    const { encodeRecord } = await import("../src/protocol/record.js");
    entry.conn.emit("push", {
      opcode: OpCode.Record, correlationId: 0, version: 1,
      payload: encodeRecord({
        consumerName: "c", offset: 7n, timestamp: 0n,
        subject: "s", deliveryAttempt: 1, key: undefined,
        value: Buffer.from(""), headers: [],
      }),
    });

    const iter = entry.sub[Symbol.asyncIterator]();
    const next = await iter.next();
    expect(next.done).toBe(false);

    reqSpy.mockClear();
    await next.value!.nack();

    expect(reqSpy).toHaveBeenCalledTimes(1);
    expect(reqSpy.mock.instances[0]).toBe(entry.conn);
    expect(reqSpy.mock.instances[0]).not.toBe((client as any).mainConn);

    await client.close(100);
    vi.restoreAllMocks();
  });

  it("legacy host/port still works", () => {
    const client = new ExspeedClient({
      host: "broker.example.com",
      port: 6000,
      clientId: "legacy-test",
    });
    const endpoints = (client as any).resolveEndpoints();
    expect(endpoints).toEqual([{ host: "broker.example.com", port: 6000 }]);
  });

  it("brokers wins when both host/port and brokers are provided", () => {
    const client = new ExspeedClient({
      host: "old.example.com",
      port: 5000,
      brokers: [{ host: "new.example.com", port: 5933 }],
      clientId: "both-test",
    });
    const endpoints = (client as any).resolveEndpoints();
    expect(endpoints).toEqual([{ host: "new.example.com", port: 5933 }]);
  });

  it("brokers entry without port uses DEFAULT_PORT", () => {
    const client = new ExspeedClient({
      brokers: [{ host: "broker.example.com" }],
      clientId: "default-port-test",
    });
    const endpoints = (client as any).resolveEndpoints();
    expect(endpoints).toEqual([{ host: "broker.example.com", port: 5933 }]);
  });
});

describe("version negotiation", () => {
  let testServer: ReturnType<typeof createTestServer>;

  afterEach(async () => {
    if (testServer) await testServer.stop();
  });

  it("publish() sets ConnectOk serverVersion and returns duplicate flag", async () => {
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        // v2 broker replies with ConnectOk (0x89) + server_version=2
        const payload = Buffer.alloc(1);
        payload.writeUInt8(2, 0);
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.ConnectOk,
          correlationId: frame.correlationId,
          payload,
        }));
      } else if (frame.opcode === OpCode.Publish) {
        // Return PublishOk (9 bytes: offset u64 + duplicate u8)
        const payload = Buffer.alloc(9);
        payload.writeBigUInt64LE(7n, 0);
        payload[8] = 0x01; // duplicate = true
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.PublishOk,
          correlationId: frame.correlationId,
          payload,
        }));
      }
    });
    const port = await testServer.start();

    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test-v2", reconnect: false });
    await client.connect();

    const conn: Connection = (client as any).mainConn;
    expect(conn.getServerVersion()).toBe(2);

    const result = await client.publish("orders", { subject: "orders.created", data: {} });
    expect(result.offset).toBe(7n);
    expect(result.duplicate).toBe(true);

    await client.close();
  });

  it("falls back to x-idempotency-key header against v1 broker", async () => {
    let capturedPublish: any;
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        // v1 broker replies with Ok (0x80)
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.Ok,
          correlationId: frame.correlationId,
          payload: Buffer.alloc(0),
        }));
      } else if (frame.opcode === OpCode.Publish) {
        capturedPublish = decodePublish(frame.payload);
        const payload = Buffer.alloc(8);
        payload.writeBigUInt64LE(1n, 0);
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.Ok,
          correlationId: frame.correlationId,
          payload,
        }));
      }
    });
    const port = await testServer.start();

    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test-v1", reconnect: false });
    await client.connect();

    const conn: Connection = (client as any).mainConn;
    expect(conn.getServerVersion()).toBe(1);

    await client.publish("s", { subject: "s", data: {}, msgId: "my-msg-id" });

    // Flag bit 1 (FLAG_HAS_MSG_ID) should NOT be set
    expect(capturedPublish.msgId).toBeUndefined();
    // x-idempotency-key header should carry the msgId value
    const headers: [string, string][] = capturedPublish.headers;
    const idempotencyHeader = headers.find(([k]) => k === "x-idempotency-key");
    expect(idempotencyHeader).toBeDefined();
    expect(idempotencyHeader![1]).toBe("my-msg-id");

    await client.close();
  });

  it("v1 fallback: explicit msgId replaces any existing x-idempotency-key header", async () => {
    let capturedPublish: any;
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        // v1 broker replies with Ok (0x80)
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.Ok,
          correlationId: frame.correlationId,
          payload: Buffer.alloc(0),
        }));
      } else if (frame.opcode === OpCode.Publish) {
        capturedPublish = decodePublish(frame.payload);
        const payload = Buffer.alloc(8);
        payload.writeBigUInt64LE(2n, 0);
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.Ok,
          correlationId: frame.correlationId,
          payload,
        }));
      }
    });
    const port = await testServer.start();

    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test-v1-replace", reconnect: false });
    await client.connect();

    const conn: Connection = (client as any).mainConn;
    expect(conn.getServerVersion()).toBe(1);

    // Publish with a pre-existing x-idempotency-key header and a msgId that should override it
    await client.publish("s", {
      subject: "s",
      data: {},
      msgId: "new-id",
      headers: [["x-idempotency-key", "old-id"], ["other-header", "value"]],
    });

    // Assert there is exactly one x-idempotency-key header with the new value
    const headers: [string, string][] = capturedPublish.headers;
    const idempotencyHeaders = headers.filter(([k]) => k === "x-idempotency-key");
    expect(idempotencyHeaders.length).toBe(1);
    expect(idempotencyHeaders[0][1]).toBe("new-id");

    // Other headers should be preserved
    const otherHeaders = headers.filter(([k]) => k === "other-header");
    expect(otherHeaders.length).toBe(1);
    expect(otherHeaders[0][1]).toBe("value");

    await client.close();
  });
});

describe("idempotent publish error handling", () => {
  let testServer: ReturnType<typeof createTestServer>;

  afterEach(async () => {
    if (testServer) await testServer.stop();
  });

  function makeErrorPayload(code: number, msg: string, extras?: Buffer): Buffer {
    const msgBytes = Buffer.from(msg, "utf8");
    const extrasLen = extras ? extras.length : 0;
    const buf = Buffer.alloc(2 + 2 + msgBytes.length + extrasLen);
    buf.writeUInt16LE(code, 0);
    buf.writeUInt16LE(msgBytes.length, 2);
    msgBytes.copy(buf, 4);
    if (extras) extras.copy(buf, 4 + msgBytes.length);
    return buf;
  }

  function makeKeyCollisionExtras(storedOffset: bigint): Buffer {
    const extras = Buffer.alloc(1 + 8);
    extras[0] = 0x01;
    extras.writeBigUInt64LE(storedOffset, 1);
    return extras;
  }

  function makeDedupMapFullExtras(retryAfterSecs: number): Buffer {
    const extras = Buffer.alloc(1 + 4);
    extras[0] = 0x02;
    extras.writeUInt32LE(retryAfterSecs, 1);
    return extras;
  }

  it("throws KeyCollisionError (not retried) when server returns 0x1001", async () => {
    let publishCount = 0;
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        // v2 broker
        const payload = Buffer.alloc(1);
        payload.writeUInt8(2, 0);
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.ConnectOk,
          correlationId: frame.correlationId,
          payload,
        }));
      } else if (frame.opcode === OpCode.Publish) {
        publishCount++;
        const errPayload = makeErrorPayload(0x1001, "collision", makeKeyCollisionExtras(42n));
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.Error,
          correlationId: frame.correlationId,
          payload: errPayload,
        }));
      }
    });
    const port = await testServer.start();
    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test", reconnect: false });
    await client.connect();

    await expect(
      client.publish("orders", { subject: "o", data: {}, msgId: "test-id" }),
    ).rejects.toThrow(KeyCollisionError);

    // Must NOT retry — exactly one publish sent
    expect(publishCount).toBe(1);

    await client.close();
  });

  it("throws DedupMapFullError with retryAfterSecs when server returns 0x1002", async () => {
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        const payload = Buffer.alloc(1);
        payload.writeUInt8(2, 0);
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.ConnectOk,
          correlationId: frame.correlationId,
          payload,
        }));
      } else if (frame.opcode === OpCode.Publish) {
        const errPayload = makeErrorPayload(0x1002, "dedup full", makeDedupMapFullExtras(5));
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.Error,
          correlationId: frame.correlationId,
          payload: errPayload,
        }));
      }
    });
    const port = await testServer.start();
    const client = new ExspeedClient({
      host: "127.0.0.1", port, clientId: "test", reconnect: false,
    });
    await client.connect();

    await expect(
      client.publish("orders", { subject: "o", data: {} }),
    ).rejects.toThrow(DedupMapFullError);

    await client.close();
  });

  it("DedupMapFullError has correct retryAfterSecs", async () => {
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        const payload = Buffer.alloc(1);
        payload.writeUInt8(2, 0);
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.ConnectOk,
          correlationId: frame.correlationId,
          payload,
        }));
      } else if (frame.opcode === OpCode.Publish) {
        const errPayload = makeErrorPayload(0x1002, "dedup full", makeDedupMapFullExtras(30));
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.Error,
          correlationId: frame.correlationId,
          payload: errPayload,
        }));
      }
    });
    const port = await testServer.start();
    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test", reconnect: false });
    await client.connect();

    let caughtErr: DedupMapFullError | undefined;
    try {
      await client.publish("orders", { subject: "o", data: {} });
    } catch (err) {
      if (err instanceof DedupMapFullError) {
        caughtErr = err;
      }
    }

    expect(caughtErr).toBeDefined();
    expect(caughtErr!.retryAfterSecs).toBe(30);
    expect(caughtErr!.stream).toBe("orders");

    await client.close();
  });

  it("KeyCollisionError has correct storedOffset and msgId", async () => {
    testServer = createTestServer((frame, socket) => {
      if (frame.opcode === OpCode.Connect) {
        const payload = Buffer.alloc(1);
        payload.writeUInt8(2, 0);
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.ConnectOk,
          correlationId: frame.correlationId,
          payload,
        }));
      } else if (frame.opcode === OpCode.Publish) {
        const errPayload = makeErrorPayload(0x1001, "collision", makeKeyCollisionExtras(99n));
        socket.write(encodeFrame({
          version: PROTOCOL_VERSION,
          opcode: OpCode.Error,
          correlationId: frame.correlationId,
          payload: errPayload,
        }));
      }
    });
    const port = await testServer.start();
    const client = new ExspeedClient({ host: "127.0.0.1", port, clientId: "test", reconnect: false });
    await client.connect();

    let caughtErr: KeyCollisionError | undefined;
    try {
      await client.publish("orders", { subject: "o", data: {}, msgId: "my-msg-id" });
    } catch (err) {
      if (err instanceof KeyCollisionError) {
        caughtErr = err;
      }
    }

    expect(caughtErr).toBeDefined();
    expect(caughtErr!.storedOffset).toBe(99n);
    expect(caughtErr!.msgId).toBe("my-msg-id");

    await client.close();
  });
});
