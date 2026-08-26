import { defineConfig } from "vitest/config";

// Every suite reshapes the one shared tenant (`PUT /services/raw`), so the
// files MUST run one after another: `fileParallelism: false` pins that, and
// `sequence.concurrent: false` keeps tests inside a file sequential too.
// Parallelism across suites comes from running several hosts on different
// ports (see README "The port rule"), never from vitest workers.
export default defineConfig({
  test: {
    include: ["*.test.ts"],
    fileParallelism: false,
    sequence: { concurrent: false, shuffle: false },
    globalSetup: ["./src/global-setup.ts"],
    testTimeout: 30_000,
    hookTimeout: 120_000,
    // No retries: a flaky conformance assertion is a finding, not noise.
    retry: 0,
  },
});
