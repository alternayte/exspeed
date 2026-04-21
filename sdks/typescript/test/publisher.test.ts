import { describe, it, expect } from "vitest";
import { Publisher } from "../src/publisher.js";

describe("Publisher", () => {
  it("exposes publish, flush, close methods", () => {
    expect(typeof Publisher.prototype.publish).toBe("function");
    expect(typeof Publisher.prototype.flush).toBe("function");
    expect(typeof Publisher.prototype.close).toBe("function");
  });
});
