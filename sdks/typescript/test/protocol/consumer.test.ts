import { describe, it, expect } from "vitest";
import { encodeCreateConsumer, decodeCreateConsumer, encodeDeleteConsumer, decodeDeleteConsumer } from "../../src/protocol/consumer.js";
import { START_FROM_EARLIEST, START_FROM_LATEST, START_FROM_OFFSET } from "../../src/protocol/types.js";

describe("consumer management", () => {
  it("round-trips a create consumer with group and filter", () => {
    const req = { name: "order-consumer", stream: "orders", group: "order-service", subjectFilter: "orders.created", startFrom: START_FROM_EARLIEST, startOffset: 0n };
    const decoded = decodeCreateConsumer(encodeCreateConsumer(req));
    expect(decoded.name).toBe("order-consumer");
    expect(decoded.stream).toBe("orders");
    expect(decoded.group).toBe("order-service");
    expect(decoded.subjectFilter).toBe("orders.created");
    expect(decoded.startFrom).toBe(START_FROM_EARLIEST);
  });
  it("round-trips a solo consumer (empty group)", () => {
    const req = { name: "solo", stream: "events", group: "", subjectFilter: "", startFrom: START_FROM_LATEST, startOffset: 0n };
    const decoded = decodeCreateConsumer(encodeCreateConsumer(req));
    expect(decoded.group).toBe("");
    expect(decoded.startFrom).toBe(START_FROM_LATEST);
  });
  it("round-trips a consumer starting from offset", () => {
    const req = { name: "c", stream: "s", group: "", subjectFilter: "", startFrom: START_FROM_OFFSET, startOffset: 42n };
    const decoded = decodeCreateConsumer(encodeCreateConsumer(req));
    expect(decoded.startFrom).toBe(START_FROM_OFFSET);
    expect(decoded.startOffset).toBe(42n);
  });
  it("round-trips a delete consumer", () => {
    expect(decodeDeleteConsumer(encodeDeleteConsumer("my-consumer"))).toBe("my-consumer");
  });
});
