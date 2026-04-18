import { describe, it, expect, vi } from "vitest";
import * as net from "node:net";

// Mock node:tls *before* importing Connection so that Connection's
// `import * as tls from "node:tls"` picks up our stub. vi.mock is hoisted
// by vitest, so declare it at the top of the file.
//
// NOTE: vi.spyOn cannot redefine ESM namespace bindings (the plan's original
// approach fails at runtime with "Cannot redefine property: connect"). vi.mock
// replaces the whole module for this file's scope, which is the supported way
// to intercept ESM imports in vitest.
const tlsConnectMock = vi.fn();
const netCreateConnectionMock = vi.fn();

vi.mock("node:tls", async () => {
  const actual = await vi.importActual<typeof import("node:tls")>("node:tls");
  return {
    ...actual,
    connect: (...args: unknown[]) => {
      tlsConnectMock(...args);
      // Emulate immediate secureConnect to short-circuit openSocket().
      const s = new net.Socket();
      setImmediate(() => s.emit("secureConnect"));
      return s as unknown as import("node:tls").TLSSocket;
    },
  };
});

vi.mock("node:net", async () => {
  const actual = await vi.importActual<typeof import("node:net")>("node:net");
  return {
    ...actual,
    createConnection: ((...args: unknown[]) => {
      netCreateConnectionMock(...args);
      return (actual as any).createConnection(...args);
    }) as typeof net.createConnection,
  };
});

// Import Connection *after* the mocks so its module-level `import * as tls/net`
// resolves against the mocked modules.
const { Connection } = await import("../src/connection.js");

describe("Connection TLS", () => {
  it("tls: true invokes tls.connect, not net.createConnection", async () => {
    tlsConnectMock.mockClear();
    netCreateConnectionMock.mockClear();

    const conn = new Connection({
      host: "127.0.0.1",
      port: 5933,
      clientId: "tls-unit",
      reconnect: false,
      requestTimeout: 1000,
      pingInterval: 0,
      tls: true,
    });

    // We won't complete handshake; just verify which API was called.
    try {
      await Promise.race([
        conn.connect(),
        new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 50)),
      ]);
    } catch {
      /* expected: handshake never completes */
    }

    expect(tlsConnectMock).toHaveBeenCalled();
    expect(netCreateConnectionMock).not.toHaveBeenCalled();

    await conn.close().catch(() => {});
  });

  it("tls undefined uses net.createConnection (plain TCP)", async () => {
    tlsConnectMock.mockClear();
    netCreateConnectionMock.mockClear();

    // Stand up a trivial plain-TCP server so openSocket() succeeds.
    const server = net.createServer((socket) => {
      socket.on("data", () => {}); // swallow
      socket.on("error", () => {});
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    const port = (server.address() as net.AddressInfo).port;

    const conn = new Connection({
      host: "127.0.0.1",
      port,
      clientId: "plain-unit",
      reconnect: false,
      requestTimeout: 100,
      pingInterval: 0,
      // tls omitted
    });

    // openSocket will succeed, handshake() will time out — we don't care.
    try {
      await conn.connect();
    } catch {
      /* expected: handshake never completes */
    }

    expect(netCreateConnectionMock).toHaveBeenCalled();
    expect(tlsConnectMock).not.toHaveBeenCalled();

    await conn.close().catch(() => {});
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("tls: { rejectUnauthorized: false } passes through to tls.connect", async () => {
    tlsConnectMock.mockClear();
    netCreateConnectionMock.mockClear();

    const conn = new Connection({
      host: "127.0.0.1",
      port: 5933,
      clientId: "tls-unit",
      reconnect: false,
      requestTimeout: 1000,
      pingInterval: 0,
      tls: { rejectUnauthorized: false },
    });

    try {
      await Promise.race([
        conn.connect(),
        new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 50)),
      ]);
    } catch {
      /* expected */
    }

    expect(tlsConnectMock).toHaveBeenCalled();
    const callArgs = tlsConnectMock.mock.calls[0]?.[0] as import("node:tls").ConnectionOptions;
    expect(callArgs.rejectUnauthorized).toBe(false);

    await conn.close().catch(() => {});
  });

  it("tls: true enables full verification (rejectUnauthorized: true)", async () => {
    tlsConnectMock.mockClear();

    const conn = new Connection({
      host: "127.0.0.1",
      port: 5933,
      clientId: "tls-unit",
      reconnect: false,
      requestTimeout: 1000,
      pingInterval: 0,
      tls: true,
    });

    try {
      await Promise.race([
        conn.connect(),
        new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 50)),
      ]);
    } catch {
      /* expected */
    }

    expect(tlsConnectMock).toHaveBeenCalled();
    const callArgs = tlsConnectMock.mock.calls[0]?.[0] as import("node:tls").ConnectionOptions;
    expect(callArgs.rejectUnauthorized).toBe(true);

    await conn.close().catch(() => {});
  });
});
