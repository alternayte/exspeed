import { describe, it, expect } from "vitest";
import { encodeSeek, decodeSeek } from "../../src/protocol/seek.js";

describe("seek", () => {
  it("round-trips a seek request", () => {
    const req = { consumerName: "my-consumer", timestamp: 1713200000000n };
    const decoded = decodeSeek(encodeSeek(req));
    expect(decoded.consumerName).toBe("my-consumer");
    expect(decoded.timestamp).toBe(1713200000000n);
  });
  it("encodes consumer name then timestamp", () => {
    const buf = encodeSeek({ consumerName: "c", timestamp: 99n });
    expect(buf.readUInt16LE(0)).toBe(1);
    expect(buf.toString("utf8", 2, 3)).toBe("c");
    expect(buf.readBigUInt64LE(3)).toBe(99n);
  });
});
