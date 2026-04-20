import { describe, it, expect } from "vitest";
import { decodeErrorFrame } from "../../src/protocol/error-frame.js";

describe("error frame decoding", () => {
  it("decodes KeyCollision extras — stored_offset", () => {
    const msg = "collision!";
    const msgBytes = Buffer.from(msg, "utf8");
    const buf = Buffer.alloc(2 + 2 + msgBytes.length + 1 + 8);
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    view.setUint16(0, 0x1001, true);               // code
    view.setUint16(2, msgBytes.length, true);       // msg_len
    msgBytes.copy(buf, 4);                          // message
    buf[4 + msgBytes.length] = 0x01;               // extras flag
    view.setBigUint64(4 + msgBytes.length + 1, 42n, true); // stored_offset
    const parsed = decodeErrorFrame(buf);
    expect(parsed.code).toBe(0x1001);
    expect(parsed.message).toBe("collision!");
    expect(parsed.storedOffset).toBe(42n);
  });

  it("decodes DedupMapFull extras — retry_after_secs", () => {
    const msg = "dedup full!";
    const msgBytes = Buffer.from(msg, "utf8");
    const buf = Buffer.alloc(2 + 2 + msgBytes.length + 1 + 4);
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    view.setUint16(0, 0x1002, true);
    view.setUint16(2, msgBytes.length, true);
    msgBytes.copy(buf, 4);
    buf[4 + msgBytes.length] = 0x02;
    view.setUint32(4 + msgBytes.length + 1, 30, true);
    const parsed = decodeErrorFrame(buf);
    expect(parsed.code).toBe(0x1002);
    expect(parsed.message).toBe("dedup full!");
    expect(parsed.retryAfterSecs).toBe(30);
  });

  it("falls back to generic error when no extras present (old format: no msg_len prefix)", () => {
    // Old format: 2 bytes code + raw message string (no length prefix, no extras)
    const buf = Buffer.alloc(2 + 7);
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    view.setUint16(0, 0x0001, true);
    buf.write("generic", 2, "utf8");
    const parsed = decodeErrorFrame(buf);
    expect(parsed.code).toBe(0x0001);
    expect(parsed.storedOffset).toBeUndefined();
    expect(parsed.retryAfterSecs).toBeUndefined();
  });

  it("falls back gracefully when no extras in new framing", () => {
    // New format: code + msg_len + msg bytes only, no extras byte
    const msg = "generic error";
    const msgBytes = Buffer.from(msg, "utf8");
    const buf = Buffer.alloc(2 + 2 + msgBytes.length);
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    view.setUint16(0, 0x0001, true);
    view.setUint16(2, msgBytes.length, true);
    msgBytes.copy(buf, 4);
    const parsed = decodeErrorFrame(buf);
    expect(parsed.code).toBe(0x0001);
    expect(parsed.message).toBe("generic error");
    expect(parsed.storedOffset).toBeUndefined();
    expect(parsed.retryAfterSecs).toBeUndefined();
  });
});
