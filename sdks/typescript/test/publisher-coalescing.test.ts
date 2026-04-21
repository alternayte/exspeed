import { describe, it, expect } from "vitest";
import { Publisher } from "../src/publisher.js";

describe("Publisher coalescing", () => {
  it("exposes publish, publishBatch, flush, close", () => {
    expect(typeof Publisher.prototype.publish).toBe("function");
    expect(typeof Publisher.prototype.publishBatch).toBe("function");
    expect(typeof Publisher.prototype.flush).toBe("function");
    expect(typeof Publisher.prototype.close).toBe("function");
  });
});
