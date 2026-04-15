import { describe, it, expect } from "vitest";
import { encodeFetch, decodeFetch } from "../../src/protocol/fetch.js";

describe("fetch", () => {
  it("round-trips a fetch request with subject filter", () => {
    const req = { stream: "orders", offset: 100n, maxRecords: 50, subjectFilter: "orders.>" };
    const buf = encodeFetch(req);
    const decoded = decodeFetch(buf);
    expect(decoded.stream).toBe("orders");
    expect(decoded.offset).toBe(100n);
    expect(decoded.maxRecords).toBe(50);
    expect(decoded.subjectFilter).toBe("orders.>");
  });
  it("round-trips without subject filter", () => {
    const req = { stream: "events", offset: 0n, maxRecords: 1000 };
    const decoded = decodeFetch(encodeFetch(req));
    expect(decoded.subjectFilter).toBe("");
  });
  it("encodes fields in correct order: stream, offset, maxRecords, filter", () => {
    const buf = encodeFetch({ stream: "s", offset: 1n, maxRecords: 2, subjectFilter: "f" });
    let pos = 0;
    expect(buf.readUInt16LE(pos)).toBe(1); pos += 3;
    expect(buf.readBigUInt64LE(pos)).toBe(1n); pos += 8;
    expect(buf.readUInt32LE(pos)).toBe(2); pos += 4;
    expect(buf.readUInt16LE(pos)).toBe(1); pos += 2;
    expect(buf.toString("utf8", pos, pos + 1)).toBe("f");
  });
});
