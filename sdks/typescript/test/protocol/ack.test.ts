import { describe, it, expect } from "vitest";
import { encodeAck, decodeAck } from "../../src/protocol/ack.js";

describe("ack/nack", () => {
  it("round-trips an ack request", () => {
    const req = { consumerName: "my-consumer", offset: 42n };
    const buf = encodeAck(req);
    const decoded = decodeAck(buf);
    expect(decoded.consumerName).toBe("my-consumer");
    expect(decoded.offset).toBe(42n);
  });
  it("handles large offsets", () => {
    const req = { consumerName: "c", offset: 0xffffffffffffffn };
    const buf = encodeAck(req);
    expect(decodeAck(buf).offset).toBe(0xffffffffffffffn);
  });
  it("encodes consumer name with u16 prefix then u64 offset", () => {
    const req = { consumerName: "ab", offset: 1n };
    const buf = encodeAck(req);
    expect(buf.readUInt16LE(0)).toBe(2);
    expect(buf.toString("utf8", 2, 4)).toBe("ab");
    expect(buf.readBigUInt64LE(4)).toBe(1n);
    expect(buf.length).toBe(2 + 2 + 8);
  });
});
