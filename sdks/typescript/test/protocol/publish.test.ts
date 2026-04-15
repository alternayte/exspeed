import { describe, it, expect } from "vitest";
import { encodePublish, decodePublish, decodePublishOk } from "../../src/protocol/publish.js";

describe("publish", () => {
  it("round-trips a publish with key and headers", () => {
    const req = { stream: "orders", subject: "orders.created", value: Buffer.from('{"id":1}'), key: Buffer.from("customer-1"), headers: [["source", "api"]] as [string, string][] };
    const buf = encodePublish(req);
    const decoded = decodePublish(buf);
    expect(decoded.stream).toBe("orders");
    expect(decoded.subject).toBe("orders.created");
    expect(decoded.value.toString()).toBe('{"id":1}');
    expect(decoded.key!.toString()).toBe("customer-1");
    expect(decoded.headers).toEqual([["source", "api"]]);
  });
  it("round-trips a publish without key", () => {
    const req = { stream: "events", subject: "events.click", value: Buffer.from("data") };
    const buf = encodePublish(req);
    const decoded = decodePublish(buf);
    expect(decoded.key).toBeUndefined();
    expect(decoded.headers).toEqual([]);
  });
  it("sets flag bit 0 when key is present", () => {
    const withKey = encodePublish({ stream: "s", subject: "s", value: Buffer.from("v"), key: Buffer.from("k") });
    const withoutKey = encodePublish({ stream: "s", subject: "s", value: Buffer.from("v") });
    const flagsOffset = 2 + 1 + 2 + 1; // after stream(u16+1char) + subject(u16+1char)
    expect(withKey[flagsOffset] & 0x01).toBe(1);
    expect(withoutKey[flagsOffset] & 0x01).toBe(0);
  });
  it("decodes publish OK with offset", () => {
    const buf = Buffer.alloc(8);
    buf.writeBigUInt64LE(42n, 0);
    expect(decodePublishOk(buf).offset).toBe(42n);
  });
  it("round-trips with multiple headers", () => {
    const req = { stream: "s", subject: "sub", value: Buffer.from("val"), headers: [["a", "1"], ["b", "2"], ["c", "3"]] as [string, string][] };
    const buf = encodePublish(req);
    const decoded = decodePublish(buf);
    expect(decoded.headers).toEqual([["a", "1"], ["b", "2"], ["c", "3"]]);
  });
});
