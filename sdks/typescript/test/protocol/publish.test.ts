import { describe, it, expect } from "vitest";
import { encodePublish, encodePublishRequest, decodePublish, decodePublishOk } from "../../src/protocol/publish.js";

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

describe("PublishRequest encoding (msgId)", () => {
  it("sets has_msg_id flag bit when msgId is provided", () => {
    const bytes = encodePublishRequest({
      stream: "orders",
      subject: "orders.created",
      value: new TextEncoder().encode("body"),
      msgId: "abc",
    });
    // stream_len(2) + stream(6) + subject_len(2) + subject(14) = 24 → flags byte at index 24
    expect(bytes[24] & 0x02).toBe(0x02);
  });

  it("omits msg_id block when not provided", () => {
    const bytes = encodePublishRequest({
      stream: "o",
      subject: "s",
      value: new Uint8Array([1, 2, 3]),
    });
    // stream_len(2) + stream(1) + subject_len(2) + subject(1) = 6 → flags at index 6
    expect(bytes[6] & 0x02).toBe(0);
  });

  it("encodes msg_id as u16-length + utf8 bytes", () => {
    const bytes = encodePublishRequest({
      stream: "o",
      subject: "s",
      value: new Uint8Array(),
      msgId: "xyz",
    });
    // stream_len(2) + stream(1) + subject_len(2) + subject(1) + flags(1) = 7
    // msg_id at index 7: u16 length (3) then 3 utf8 bytes 'xyz'
    expect(new DataView(bytes.buffer, bytes.byteOffset + 7, 2).getUint16(0, true)).toBe(3);
    expect(new TextDecoder().decode(bytes.slice(9, 12))).toBe("xyz");
  });
});

describe("PublishOk decoding (duplicate flag)", () => {
  it("reads duplicate bit", () => {
    const payload = Buffer.alloc(9);
    payload.writeBigUInt64LE(42n, 0);
    payload[8] = 0x01;
    expect(decodePublishOk(payload)).toEqual({ offset: 42n, duplicate: true });
  });

  it("duplicate false when flag is zero", () => {
    const payload = Buffer.alloc(9);
    payload.writeBigUInt64LE(50n, 0);
    payload[8] = 0x00;
    expect(decodePublishOk(payload)).toEqual({ offset: 50n, duplicate: false });
  });

  it("duplicate false when payload is only 8 bytes (backward compat)", () => {
    const payload = Buffer.alloc(8);
    payload.writeBigUInt64LE(99n, 0);
    expect(decodePublishOk(payload)).toEqual({ offset: 99n, duplicate: false });
  });
});
