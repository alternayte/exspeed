import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // The e2e suite starts real servers; give it room on slow CI machines.
    testTimeout: 30_000,
    hookTimeout: 60_000,
  },
});
