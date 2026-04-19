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
