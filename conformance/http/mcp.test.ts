// The `mcp` service over HTTP (rs2-core/tests/mcp.rs): the agent surface as
// a Model Context Protocol server. Tools come from the caller's own agent
// surface, and every call re-enters dispatch as the caller.

import { afterAll, beforeAll, describe, expect, test } from "vitest";

import type { Rs2Client } from "./src/client.ts";
import { Seed } from "./src/seed.ts";

const GREET = "/mpipes/.pipelines/greet";

async function rpc(client: Rs2Client, method: string, params: unknown = {}) {
  const res = await client.post("/mcp", { json: { jsonrpc: "2.0", id: 7, method, params } });
  expect(res.status, `${method}: ${res.describe()}`).toBe(200);
  const body = res.json();
  expect(body.jsonrpc).toBe("2.0");
  expect(body.id).toBe(7);
  return body;
}

function toolNames(list: { result: { tools: { name: string }[] } }): string[] {
  return list.result.tools.map((t) => t.name);
}

describe("mcp service (mcp.rs)", () => {
  let seed: Seed | undefined;
  let anon: Rs2Client;
  let asA: Rs2Client;
  let asU: Rs2Client;

  beforeAll(async () => {
    seed = await Seed.create();
    await seed.applyMounts([
      {
        path: "/mcp",
        service: "mcp",
        config: { access: { read: "all", invoke: "all" }, instructions: "Order desk tools." },
      },
      { path: "/mdata", service: "data", config: { access: { read: "U A", write: "A" } } },
      {
        path: "/mhidden",
        service: "data",
        config: { access: { read: "all", write: "A" }, "x-expose": ["ui"] },
      },
      { path: "/mpipes", service: "pipeline", config: { access: { read: "all", invoke: "all", write: "A" } } },
    ]);
    await seed.createPrincipals([{ email: "u@conf.test", password: "u-pw", roles: "U" }]);
    anon = seed.anon;
    asA = seed.admin;
    asU = await seed.clientAs({ email: "u@conf.test", password: "u-pw" });
    seed.trackDataset("/data", "mcporders");
    const res = await asA.put(GREET, {
      json: {
        description: "Greet someone by name",
        effect: "pure",
        input: { type: "object", properties: { name: { type: "string" } }, required: ["name"] },
        pipeline: [{ greeting: '"Hello"', who: "name" }],
      },
    });
    expect(res.status, res.describe()).toBe(201);
  });

  afterAll(async () => {
    await asA?.delete(GREET);
    await seed?.restore();
  });

  test("initialize negotiates a version and introduces the server", async () => {
    let init = await rpc(anon, "initialize", {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: { name: "conformance", version: "0" },
    });
    expect(init.result.protocolVersion).toBe("2025-06-18");
    expect(init.result.capabilities.tools.listChanged).toBe(false);
    expect(init.result.serverInfo.name).toBe("rs2");
    expect(init.result.instructions).toBe("Order desk tools.");
    init = await rpc(anon, "initialize", { protocolVersion: "1999-01-01" });
    expect(init.result.protocolVersion).toBe("2025-11-25");
    expect((await rpc(anon, "ping")).result).toEqual({});
    expect((await rpc(anon, "resources/list")).error.code).toBe(-32601);
  });

  test("transport rules: notifications, GET, batches, protocol header", async () => {
    let res = await anon.post("/mcp", { json: { jsonrpc: "2.0", method: "notifications/initialized" } });
    expect(res.status, res.describe()).toBe(202);
    res = await anon.get("/mcp");
    expect(res.status, res.describe()).toBe(405);
    expect(res.headers.get("allow")).toBe("POST");
    res = await anon.post("/mcp", { json: [] });
    expect(res.status, res.describe()).toBe(400);
    expect(res.json().error.code).toBe(-32600);
    res = await anon.post("/mcp", { body: "{nope", contentType: "application/json" });
    expect(res.status, res.describe()).toBe(400);
    expect(res.json().error.code).toBe(-32700);
    res = await anon.post("/mcp", {
      json: { jsonrpc: "2.0", id: 1, method: "ping" },
      headers: { "mcp-protocol-version": "1999-01-01" },
    });
    expect(res.status, res.describe()).toBe(400);
  });

  test("tools follow the caller's surface", async () => {
    const anonTools = toolNames(await rpc(anon, "tools/list"));
    expect(anonTools).toContain("mpipes_greet");
    expect(anonTools).not.toContain("mdata_get");
    // `x-expose: ["ui"]` keeps a mount off the mcp surface for everyone.
    expect(anonTools).not.toContain("mhidden_get");

    const user = await rpc(asU, "tools/list");
    const names = toolNames(user);
    for (const n of ["mdata_list", "mdata_get", "mdata_put", "mdata_delete", "mpipes_greet"]) {
      expect(names).toContain(n);
    }
    expect(names).not.toContain("mhidden_get");
    const greet = user.result.tools.find((t: { name: string }) => t.name === "mpipes_greet");
    expect(greet.inputSchema.required).toEqual(["name"]);
    expect(greet.annotations.readOnlyHint).toBe(true);
    expect(greet.description).toContain("Greet someone by name");
    const put = user.result.tools.find((t: { name: string }) => t.name === "mdata_put");
    expect(put.inputSchema.required).toEqual(["path", "record"]);
    expect(put.annotations).toEqual({ readOnlyHint: false, idempotentHint: true });
  });

  test("calls run as the caller", async () => {
    let call = await rpc(anon, "tools/call", { name: "mpipes_greet", arguments: { name: "Ada" } });
    expect(call.result.isError, JSON.stringify(call)).toBe(false);
    expect(call.result.structuredContent.who).toBe("Ada");

    // U reads /mdata but may not write it: the store's 403 is a tool error.
    const args = { path: "mcporders/1", record: { sku: "x" } };
    call = await rpc(asU, "tools/call", { name: "mdata_put", arguments: args });
    expect(call.result.isError, JSON.stringify(call)).toBe(true);
    expect(call.result.content[0].text).toContain("403");

    call = await rpc(asA, "tools/call", { name: "mdata_put", arguments: args });
    expect(call.result.isError, JSON.stringify(call)).toBe(false);
    call = await rpc(asU, "tools/call", { name: "mdata_get", arguments: { path: "mcporders/1" } });
    expect(call.result.structuredContent.sku, JSON.stringify(call)).toBe("x");

    // Anonymous callers never see /mdata, so its tools are unknown to them.
    call = await rpc(anon, "tools/call", { name: "mdata_get", arguments: { path: "mcporders/1" } });
    expect(call.error.code).toBe(-32602);

    call = await rpc(asU, "tools/call", { name: "mdata_get", arguments: { path: "a?b=1" } });
    expect(call.result.isError).toBe(true);
  });

  test("the mount advertises the mcp facet", async () => {
    const res = await anon.request("OPTIONS", "/mcp");
    expect(res.status, res.describe()).toBe(200);
    expect(res.json().pattern).toBe("api");
    expect(res.json().facets).toEqual(["mcp"]);
  });
});
