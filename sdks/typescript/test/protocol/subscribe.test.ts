import { describe, it, expect } from "vitest";
import { encodeSubscribe, decodeSubscribe } from "../../src/protocol/subscribe.js";

describe("subscribe/unsubscribe", () => {
  it("round-trips a subscribe request", () => {
    const buf = encodeSubscribe("my-consumer");
    expect(decodeSubscribe(buf)).toBe("my-consumer");
  });
  it("encodes consumer name with u16 prefix", () => {
    const buf = encodeSubscribe("abc");
    expect(buf.readUInt16LE(0)).toBe(3);
    expect(buf.toString("utf8", 2, 5)).toBe("abc");
    expect(buf.length).toBe(5);
  });
  it("round-trips an empty name", () => {
    const buf = encodeSubscribe("");
    expect(decodeSubscribe(buf)).toBe("");
  });
});
