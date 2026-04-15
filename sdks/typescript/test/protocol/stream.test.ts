import { describe, it, expect } from "vitest";
import { encodeCreateStream, decodeCreateStream } from "../../src/protocol/stream.js";

describe("stream management", () => {
  it("round-trips a create stream request", () => {
    const req = { name: "orders", maxAgeSecs: 86400n, maxBytes: 1073741824n };
    const decoded = decodeCreateStream(encodeCreateStream(req));
    expect(decoded.name).toBe("orders");
    expect(decoded.maxAgeSecs).toBe(86400n);
    expect(decoded.maxBytes).toBe(1073741824n);
  });
  it("round-trips with zero retention (unlimited)", () => {
    const req = { name: "logs", maxAgeSecs: 0n, maxBytes: 0n };
    const decoded = decodeCreateStream(encodeCreateStream(req));
    expect(decoded.maxAgeSecs).toBe(0n);
    expect(decoded.maxBytes).toBe(0n);
  });
  it("encodes fields in order: name, maxAgeSecs, maxBytes", () => {
    const buf = encodeCreateStream({ name: "s", maxAgeSecs: 1n, maxBytes: 2n });
    expect(buf.readUInt16LE(0)).toBe(1);
    expect(buf.toString("utf8", 2, 3)).toBe("s");
    expect(buf.readBigUInt64LE(3)).toBe(1n);
    expect(buf.readBigUInt64LE(11)).toBe(2n);
  });
});
