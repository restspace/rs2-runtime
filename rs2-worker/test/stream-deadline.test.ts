// The wall clock over a *streamed* response (issue #2 item 8). A guest that
// calls `beginStream` and then never finishes used to keep its invocation
// record, its `TransformStream`, and the client's response open until the
// isolate was evicted — the engine's timer only guarded the phase before the
// stream began. Rust's watchdog bounds the whole handler run; so does this.

import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import type { DynamicWorkerEngine, Invocations } from "../src/engines/dynamic-worker";
import { NullLogStore } from "../src/runtime/logging";
import { Message } from "../src/runtime/message";

const loader = (env as { LOADER?: WorkerLoader }).LOADER;

/// The engine as the tenant DO wires it — its own invocations map and its
/// real `HostApi`/`EgressSockets` entrypoints, so the guest's `beginStream`
/// and `write` travel the production RPC path.
interface TenantInternals {
  buildEngine(tenant: string): DynamicWorkerEngine | undefined;
  invocations: Invocations;
}

describe("streamed responses are bounded by the wall clock", () => {
  it("a guest that begins and then stalls loses its record and its stream (issue #2 item 8)", async () => {
    expect(loader, "worker_loaders binding (wrangler.test.jsonc)").toBeDefined();
    const stub = env.TENANTS.get(env.TENANTS.idFromName("t"));
    await runInDurableObject(stub, async (instance) => {
      const self = instance as unknown as TenantInternals;
      const engine = self.buildEngine("t")!;
      const invocations = self.invocations;

      const source = `export default async (msg, ctx) => {
      const s = await ctx.beginStream({ status: 200, mediaType: "text/plain" });
      await s.write("first");
      // A long sleep, not a never-settling promise: the runtime cancels the
      // latter by itself, and the point here is the deadline, not that.
      await new Promise((r) => setTimeout(r, 60000));
    };`;

      const msg = Message.request("GET", "/stream", "t");
      const resp = await engine.invoke({
        codeId: "t:/svc:stall@v1",
        source,
        msg,
        config: { responseStreaming: true },
        grants: new Map(),
        serviceRef: "stall@v1",
        logCtx: {
          sink: new NullLogStore(),
          tenant: "t",
          mount: "/svc",
          service: "stall@v1",
          traceId: "trace",
          spanId: "span",
        },
        outboundBudget: 0,
        materializeCap: 1024 * 1024,
        wallClockMs: 500,
        cpuMs: 5_000,
      });

      // The response came back as soon as the guest began streaming.
      expect(resp.status).toBe(200);
      const reader = resp.body!.intoStream().getReader();
      const first = await reader.read();
      expect(new TextDecoder().decode(first.value)).toBe("first");

      // …and the deadline still applies: the stream errors rather than hanging.
      await expect(reader.read()).rejects.toThrow();
      expect(invocations.size, "the invocation record is released").toBe(0);
    });
  });
});
