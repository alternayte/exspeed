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
      endpoints: [{ host: "127.0.0.1", port: mock.port }],
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
      endpoints: [{ host: "127.0.0.1", port: mock.port }],
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
      endpoints: [{ host: "127.0.0.1", port: mock.port }],
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
      endpoints: [{ host: "127.0.0.1", port: mock.port }],
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
      endpoints: [{ host: "127.0.0.1", port: mock.port }],
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

describe("Connection failover", () => {
  async function pickPort(): Promise<number> {
    return new Promise((resolve) => {
      const srv = net.createServer();
      srv.listen(0, "127.0.0.1", () => {
        const p = (srv.address() as net.AddressInfo).port;
        srv.close(() => resolve(p));
      });
    });
  }

  interface EchoServer {
    server: net.Server;
    /** All sockets currently connected to this server. */
    sockets: Set<net.Socket>;
    /** Destroy all connected sockets and close the listening server. */
    kill(): Promise<void>;
  }

  function startEchoServer(port: number): Promise<EchoServer> {
    return new Promise((resolve) => {
      const sockets = new Set<net.Socket>();
      const srv = net.createServer((socket) => {
        sockets.add(socket);
        socket.once("close", () => sockets.delete(socket));
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
      srv.listen(port, "127.0.0.1", () =>
        resolve({
          server: srv,
          sockets,
          kill(): Promise<void> {
            for (const s of sockets) s.destroy();
            return new Promise<void>((r) => srv.close(() => r()));
          },
        }),
      );
    });
  }

  it("falls through to the second endpoint when the first refuses", async () => {
    const deadPort = await pickPort();
    const livePort = await pickPort();
    const liveSrv = await startEchoServer(livePort);

    const conn = new Connection({
      endpoints: [
        { host: "127.0.0.1", port: deadPort },
        { host: "127.0.0.1", port: livePort },
      ],
      clientId: "failover-test",
      reconnect: false,
      requestTimeout: 1000,
      pingInterval: 0,
    });

    await conn.connect();
    expect(conn.currentEndpoint()).toEqual({ host: "127.0.0.1", port: livePort });

    await conn.close();
    await liveSrv.kill();
  });

  it("rejects when no endpoints are reachable", async () => {
    const p1 = await pickPort();
    const p2 = await pickPort();
    const conn = new Connection({
      endpoints: [
        { host: "127.0.0.1", port: p1 },
        { host: "127.0.0.1", port: p2 },
      ],
      clientId: "all-dead-test",
      reconnect: false,
      requestTimeout: 500,
      pingInterval: 0,
    });

    await expect(conn.connect()).rejects.toThrow(ConnectionError);
  });

  it("emits endpoint_changed when reconnect rotates to next endpoint", async () => {
    const p1 = await pickPort();
    const p2 = await pickPort();

    // Start both echo servers (srv2 stays up so reconnect can land there).
    const srv1 = await startEchoServer(p1);
    const srv2 = await startEchoServer(p2);

    const conn = new Connection({
      endpoints: [
        { host: "127.0.0.1", port: p1 },
        { host: "127.0.0.1", port: p2 },
      ],
      clientId: "endpoint-changed-test",
      reconnect: true,
      maxReconnectAttempts: 5,
      reconnectBaseDelay: 10,
      reconnectMaxDelay: 100,
      requestTimeout: 1000,
      pingInterval: 0,
    });

    // Connect — should land on p1.
    await conn.connect();
    expect(conn.currentEndpoint().port).toBe(p1);

    // Arm the listener before killing so we don't miss the event.
    const endpointChangedPromise = new Promise<{
      from: { host: string; port: number };
      to: { host: string; port: number };
    }>((resolve) => {
      conn.once("endpoint_changed", (payload: any) => resolve(payload));
    });

    // Kill srv1: destroy all connected sockets (so the client socket gets a
    // close event immediately) then stop accepting.
    await srv1.kill();

    // Wait up to 2 s for the endpoint_changed event.
    const changed = await Promise.race([
      endpointChangedPromise,
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("timeout waiting for endpoint_changed")), 2000),
      ),
    ]);

    expect(changed.from.port).toBe(p1);
    expect(changed.to.port).toBe(p2);

    await conn.close();
    await srv2.kill();
  });
});
