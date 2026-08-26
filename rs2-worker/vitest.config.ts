import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./test/wrangler.test.jsonc" },
      miniflare: {
        compatibilityFlags: ["nodejs_compat"],
        // Canned upstream for the SDK-corpus engine tests
        // (`test/engines.test.ts`): a static auxiliary worker — a dynamic
        // worker's own entrypoints cannot serve as another dynamic
        // worker's `globalOutbound` — bound as `MOCK_UPSTREAM`.
        serviceBindings: { MOCK_UPSTREAM: "rs2-mock-upstream" },
        workers: [
          {
            name: "rs2-mock-upstream",
            modules: true,
            compatibilityDate: "2026-08-22",
            scriptPath: "test/mock-upstream.js",
          },
        ],
      },
    }),
  ],
  test: {
    include: ["test/**/*.test.ts"],
  },
});
