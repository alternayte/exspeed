import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as net from "node:net";
import { Connection } from "../src/connection.js";
import {
  OpCode,
  PROTOCOL_VERSION,
  encodeFrame,
  decodeFrame,
} from "../src/protocol/index.js";
import { TimeoutError, ConnectionError } from "../src/errors.js";

function createMockServer(): {
  server: net.Server;
  port: number;
  started: Promise<void>;
  connections: net.Socket[];
} {
  const connections: net.Socket[] = [];
  const server = net.createServer((socket) => {
    connections.push(socket);
  });
  const started = new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", () => resolve());
  });
  return {
    server,
    get port() {
      return (server.address() as net.AddressInfo).port;
    },
    started,
    connections,
  };
}

describe("Connection", () => {
  let mock: ReturnType<typeof createMockServer>;

  beforeEach(async () => {
    mock = createMockServer();
    await mock.started;
  });

  afterEach(async () => {
    for (const conn of mock.connections) {
      conn.destroy();
    }
    await new Promise<void>((resolve) => mock.server.close(() => resolve()));
  });

  it("connects and completes handshake", async () => {
    mock.server.on("connection", (socket) => {
      let buf = Buffer.alloc(0);
      socket.on("data", (data) => {
        buf = Buffer.concat([buf, data]);
        const result = decodeFrame(buf, 0);
        if (result && result.frame.opcode === OpCode.Connect) {
          socket.write(encodeFrame({
            version: PROTOCOL_VERSION,
            opcode: OpCode.Ok,
            correlationId: result.frame.correlationId,
            payload: Buffer.alloc(0),
          }));
        }
      });
    });

    const conn = new Connection({
      host: "127.0.0.1",
      port: mock.port,
      clientId: "test",
      reconnect: false,
      requestTimeout: 2000,
      pingInterval: 0,
    });

    await conn.connect();
    await conn.close();
  });

  it("sends request and receives correlated response", async () => {
    mock.server.on("connection", (socket) => {
      let buf = Buffer.alloc(0);
      socket.on("data", (data) => {
        buf = Buffer.concat([buf, data]);
        let offset = 0;
        while (true) {
          const result = decodeFrame(buf, offset);
          if (!result) break;
          offset += result.bytesConsumed;
          socket.write(encodeFrame({
            version: PROTOCOL_VERSION,
            opcode: OpCode.Ok,
            correlationId: result.frame.correlationId,
            payload: Buffer.alloc(0),
          }));
        }
        if (offset > 0) buf = buf.subarray(offset);
      });
    });

    const conn = new Connection({
      host: "127.0.0.1",
      port: mock.port,
      clientId: "test",
      reconnect: false,
      requestTimeout: 2000,
      pingInterval: 0,
    });
    await conn.connect();

    const response = await conn.request(OpCode.CreateStream, Buffer.from("test-payload"));
    expect(response.opcode).toBe(OpCode.Ok);

    await conn.close();
  });

  it("rejects with TimeoutError when no response", async () => {
    mock.server.on("connection", (socket) => {
      let buf = Buffer.alloc(0);
      let connectDone = false;
      socket.on("data", (data) => {
        buf = Buffer.concat([buf, data]);
        const result = decodeFrame(buf, 0);
        if (result && !connectDone) {
          connectDone = true;
          buf = buf.subarray(result.bytesConsumed);
          socket.write(encodeFrame({
            version: PROTOCOL_VERSION,
            opcode: OpCode.Ok,
            correlationId: result.frame.correlationId,
            payload: Buffer.alloc(0),
          }));
        }
      });
    });

    const conn = new Connection({
      host: "127.0.0.1",
      port: mock.port,
      clientId: "test",
      reconnect: false,
      requestTimeout: 200,
      pingInterval: 0,
    });
    await conn.connect();
    await expect(conn.request(OpCode.Ping, Buffer.alloc(0))).rejects.toThrow(TimeoutError);
    await conn.close();
  });

  it("emits push events for frames with correlationId 0", async () => {
    mock.server.on("connection", (socket) => {
      let buf = Buffer.alloc(0);
      socket.on("data", (data) => {
        buf = Buffer.concat([buf, data]);
        const result = decodeFrame(buf, 0);
        if (result) {
          buf = buf.subarray(result.bytesConsumed);
          socket.write(encodeFrame({
            version: PROTOCOL_VERSION,
            opcode: OpCode.Ok,
            correlationId: result.frame.correlationId,
            payload: Buffer.alloc(0),
          }));
          setTimeout(() => {
            socket.write(encodeFrame({
              version: PROTOCOL_VERSION,
              opcode: OpCode.Record,
              correlationId: 0,
              payload: Buffer.from("push-data"),
            }));
          }, 50);
        }
      });
    });

    const conn = new Connection({
      host: "127.0.0.1",
      port: mock.port,
      clientId: "test",
      reconnect: false,
      requestTimeout: 2000,
      pingInterval: 0,
    });

    const pushReceived = new Promise<Buffer>((resolve) => {
      conn.on("push", (frame: any) => resolve(frame.payload));
    });

    await conn.connect();
    const payload = await pushReceived;
    expect(payload.toString()).toBe("push-data");
    await conn.close();
  });

  it("rejects pending requests on disconnect", async () => {
    mock.server.on("connection", (socket) => {
      let buf = Buffer.alloc(0);
      let connectDone = false;
      socket.on("data", (data) => {
        buf = Buffer.concat([buf, data]);
        const result = decodeFrame(buf, 0);
        if (result && !connectDone) {
          connectDone = true;
          buf = buf.subarray(result.bytesConsumed);
          socket.write(encodeFrame({
            version: PROTOCOL_VERSION,
            opcode: OpCode.Ok,
            correlationId: result.frame.correlationId,
            payload: Buffer.alloc(0),
          }));
          setTimeout(() => socket.destroy(), 50);
        }
      });
    });

    const conn = new Connection({
      host: "127.0.0.1",
      port: mock.port,
      clientId: "test",
      reconnect: false,
      requestTimeout: 5000,
      pingInterval: 0,
    });
    await conn.connect();

    await new Promise((r) => setTimeout(r, 100));
    await expect(conn.request(OpCode.Ping, Buffer.alloc(0))).rejects.toThrow(ConnectionError);
  });
});
