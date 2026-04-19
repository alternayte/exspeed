import { describe, it, expect } from "vitest";
import { encodeConnect, decodeConnect, decodeConnectResponse } from "../../src/protocol/connect.js";
import { AUTH_NONE, AUTH_TOKEN } from "../../src/protocol/types.js";

describe("connect", () => {
  it("round-trips a connect request with no auth", () => {
    const req = { clientId: "my-client", authType: AUTH_NONE, authPayload: Buffer.alloc(0) };
    const buf = encodeConnect(req);
    const decoded = decodeConnect(buf);
    expect(decoded.clientId).toBe("my-client");
    expect(decoded.authType).toBe(AUTH_NONE);
    expect(decoded.authPayload.length).toBe(0);
  });
  it("round-trips a connect request with token auth", () => {
    const token = "secret-token-123";
    const req = { clientId: "service-a", authType: AUTH_TOKEN, authPayload: Buffer.from(token, "utf8") };
    const buf = encodeConnect(req);
    const decoded = decodeConnect(buf);
    expect(decoded.clientId).toBe("service-a");
    expect(decoded.authType).toBe(AUTH_TOKEN);
    expect(decoded.authPayload.toString("utf8")).toBe(token);
  });
  it("encodes client_id with u16 length prefix", () => {
    const req = { clientId: "ab", authType: AUTH_NONE, authPayload: Buffer.alloc(0) };
    const buf = encodeConnect(req);
    expect(buf.readUInt16LE(0)).toBe(2);
    expect(buf.toString("utf8", 2, 4)).toBe("ab");
    expect(buf[4]).toBe(AUTH_NONE);
  });
});

describe("decodeConnectResponse", () => {
  it("decodes server_version from 1-byte payload", () => {
    const payload = Buffer.alloc(1);
    payload.writeUInt8(2, 0);
    expect(decodeConnectResponse(payload)).toEqual({ serverVersion: 2 });
  });

  it("defaults to serverVersion 1 when payload is empty (legacy Ok)", () => {
    expect(decodeConnectResponse(Buffer.alloc(0))).toEqual({ serverVersion: 1 });
  });
});
