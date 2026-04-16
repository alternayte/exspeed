import { describe, it, expect } from "vitest";
import { encodeSubscribe, encodeUnsubscribe, decodeSubscribe } from "../../src/protocol/subscribe.js";
import { writeString, stringByteLength } from "../../src/protocol/primitives.js";

describe("subscribe/unsubscribe", () => {
  it("round-trips a subscribe request", () => {
    const buf = encodeSubscribe("my-consumer", "sub-id-1");
    const decoded = decodeSubscribe(buf);
    expect(decoded.consumerName).toBe("my-consumer");
    expect(decoded.subscriberId).toBe("sub-id-1");
  });

  it("encodes consumer name with u16 prefix followed by subscriber id", () => {
    const buf = encodeSubscribe("abc", "xyz");
    // First field: 3-byte name "abc" with 2-byte u16 length prefix
    expect(buf.readUInt16LE(0)).toBe(3);
    expect(buf.toString("utf8", 2, 5)).toBe("abc");
    // Second field: 3-byte id "xyz" with 2-byte u16 length prefix
    expect(buf.readUInt16LE(5)).toBe(3);
    expect(buf.toString("utf8", 7, 10)).toBe("xyz");
    expect(buf.length).toBe(10);
  });

  it("round-trips an empty consumer name", () => {
    const buf = encodeSubscribe("", "some-sub-id");
    const decoded = decodeSubscribe(buf);
    expect(decoded.consumerName).toBe("");
    expect(decoded.subscriberId).toBe("some-sub-id");
  });

  it("encodeUnsubscribe produces the same wire format as encodeSubscribe", () => {
    const sub = encodeSubscribe("my-consumer", "sub-id-1");
    const unsub = encodeUnsubscribe("my-consumer", "sub-id-1");
    expect(Buffer.compare(sub, unsub)).toBe(0);
  });

  it("decodeSubscribe backward-compat: single string defaults subscriberId to empty", () => {
    // Simulate old single-field payload (only consumer name encoded)
    const buf = Buffer.alloc(stringByteLength("old-consumer"));
    writeString(buf, 0, "old-consumer");
    const decoded = decodeSubscribe(buf);
    expect(decoded.consumerName).toBe("old-consumer");
    expect(decoded.subscriberId).toBe("");
  });
});
