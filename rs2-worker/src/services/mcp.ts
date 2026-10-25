// `mcp` service: the tenant's agent surface as a Model Context Protocol
// server, over the Streamable HTTP transport (JSON responses, no sessions).
// Port of `rs2-core/src/services/mcp.rs` — the two must agree on tool names,
// schemas, annotations and every error code.
//
// The tool list is derived per request from the caller's own
// `/.well-known/rs2/agent-surface?surface=<surface>` (default `mcp`), so it
// is already filtered by read permission and `x-expose`. A tool call becomes
// one ordinary internal request carrying the caller's principal, so every
// access check, limit and log applies exactly as over HTTP.

import { base64 } from "../runtime/body";
import { WELL_KNOWN_PREFIX } from "../runtime/discovery";
import { codes, RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { Body } from "../runtime/body";
import { Message, clonePrincipal } from "../runtime/message";
import { RS2_VERSION } from "../runtime/version";
import type { Service, ServiceContext } from "./context";

/// Protocol revisions this server speaks, newest first.
export const MCP_PROTOCOL_VERSIONS = ["2025-11-25", "2025-06-18", "2025-03-26"];

/// `_meta` key a client uses to send an `Idempotency-Key` with a tool call.
export const IDEMPOTENCY_META_KEY = "rs2/idempotencyKey";

const PARSE_ERROR = -32700;
const INVALID_REQUEST = -32600;
const METHOD_NOT_FOUND = -32601;
const INVALID_PARAMS = -32602;
const INTERNAL_ERROR = -32603;

const MAX_TOOL_NAME = 64;

class RpcError {
  constructor(readonly code: number, readonly message: string) {}
  toJson(): JsonObject {
    return { code: this.code, message: this.message };
  }
}

type EntityVerb = "list" | "get" | "put" | "delete";

type Op =
  | { kind: "action"; path: string; wrapped: boolean }
  | { kind: "query"; path: string }
  | { kind: "entity"; base: string; verb: EntityVerb };

interface Tool {
  name: string;
  description: string;
  inputSchema: JsonObject;
  annotations: JsonObject;
  op: Op;
}

export class McpService implements Service {
  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const atRoot = msg.url.servicePath === "" || msg.url.servicePath === "/";
    if (!atRoot) throw RsError.notFound("an MCP endpoint answers only at its mount root");
    if (msg.method !== "POST") {
      const resp = msg.errorResponse(
        new RsError(
          405,
          codes.BAD_REQUEST,
          "Method Not Allowed",
          "this MCP endpoint takes JSON-RPC by POST (no SSE stream, no sessions)",
        ),
      );
      resp.setHeader("allow", "POST");
      return resp;
    }
    const version = msg.header("mcp-protocol-version");
    if (version !== undefined && !MCP_PROTOCOL_VERSIONS.includes(version)) {
      return rpcHttpError(
        msg,
        400,
        INVALID_REQUEST,
        `unsupported MCP-Protocol-Version '${version}' (supported: ${MCP_PROTOCOL_VERSIONS.join(", ")})`,
      );
    }

    const raw = msg.body ? await msg.body.materialize(ctx.limits.materializedBodyBytes) : new Uint8Array();
    let request: unknown;
    try {
      request = JSON.parse(new TextDecoder().decode(raw));
    } catch (e) {
      return rpcHttpError(msg, 400, PARSE_ERROR, `body is not JSON: ${(e as Error).message}`);
    }
    if (request === null || typeof request !== "object" || Array.isArray(request)) {
      const detail = Array.isArray(request)
        ? "JSON-RPC batches are not supported"
        : "a JSON-RPC message is an object";
      return rpcHttpError(msg, 400, INVALID_REQUEST, detail);
    }
    const req = request as JsonObject;

    // Notifications and client responses carry no reply.
    if (typeof req.method !== "string" || !("id" in req) || req.id === undefined) {
      return msg.response(202, undefined);
    }
    const method = req.method;
    const id = req.id as Json;
    const params = (req.params ?? {}) as Json;

    let result: Json;
    try {
      switch (method) {
        case "initialize":
          result = initialize(params, ctx);
          break;
        case "ping":
          result = {};
          break;
        case "tools/list":
          result = { tools: (await listTools(ctx, msg)).map(describe) };
          break;
        case "tools/call":
          result = await callTool(params, ctx, msg);
          break;
        default:
          throw new RpcError(METHOD_NOT_FOUND, `method '${method}' is not supported`);
      }
    } catch (e) {
      if (e instanceof RpcError) {
        return msg.okJson({ jsonrpc: "2.0", id, error: e.toJson() });
      }
      throw e;
    }
    return msg.okJson({ jsonrpc: "2.0", id, result });
  }
}

/// A transport-level failure: an HTTP error status carrying a JSON-RPC error
/// with no id.
function rpcHttpError(msg: Message, status: number, code: number, detail: string): Message {
  return msg.response(
    status,
    Body.fromJson({ jsonrpc: "2.0", id: null, error: { code, message: detail } }),
  );
}

function initialize(params: Json, ctx: ServiceContext): JsonObject {
  const requested = isObject(params) ? params.protocolVersion : undefined;
  const version =
    typeof requested === "string" && MCP_PROTOCOL_VERSIONS.includes(requested)
      ? requested
      : MCP_PROTOCOL_VERSIONS[0]!;
  const name = typeof ctx.config.name === "string" ? ctx.config.name : "rs2";
  const result: JsonObject = {
    protocolVersion: version,
    capabilities: { tools: { listChanged: false } },
    serverInfo: { name, version: RS2_VERSION },
  };
  if (typeof ctx.config.instructions === "string") result.instructions = ctx.config.instructions;
  return result;
}

function describe(tool: Tool): JsonObject {
  return {
    name: tool.name,
    description: tool.description,
    inputSchema: tool.inputSchema,
    annotations: tool.annotations,
  };
}

function internalRequest(caller: Message, method: string, path: string): Message {
  const req = Message.request(method, path, caller.tenant);
  req.source = "internal";
  req.principal = clonePrincipal(caller.principal);
  req.trace = caller.trace.child();
  req.depth = caller.depth + 1;
  return req;
}

function requesterOf(ctx: ServiceContext) {
  if (!ctx.requester) {
    throw new RpcError(INTERNAL_ERROR, "this host gives the mcp service no internal requester");
  }
  return ctx.requester;
}

async function agentSurface(ctx: ServiceContext, caller: Message): Promise<Json> {
  const surface = typeof ctx.config.surface === "string" ? ctx.config.surface : "mcp";
  const path = `${WELL_KNOWN_PREFIX}agent-surface?surface=${encodeQuery(surface)}`;
  const resp = await requesterOf(ctx).request(internalRequest(caller, "GET", path));
  if (!resp.isOk()) {
    throw new RpcError(INTERNAL_ERROR, `reading the agent surface failed with ${resp.status}`);
  }
  if (!resp.body) return {};
  const bytes = await resp.body.materialize(ctx.limits.materializedBodyBytes);
  return JSON.parse(new TextDecoder().decode(bytes)) as Json;
}

async function listTools(ctx: ServiceContext, caller: Message): Promise<Tool[]> {
  return toolsFromSurface(await agentSurface(ctx, caller));
}

/// Derive the tool set from an agent-surface document: entities, then
/// queries, then actions, numeric suffixes resolving name collisions.
export function toolsFromSurface(surface: Json): Tool[] {
  const tools: Tool[] = [];
  const names = new Set<string>();
  const entries = (key: string): JsonObject[] => {
    const v = isObject(surface) ? surface[key] : undefined;
    return Array.isArray(v) ? (v.filter(isObject) as JsonObject[]) : [];
  };

  for (const entry of entries("entities")) {
    const base = entry.path;
    if (typeof base !== "string") continue;
    const slug = slugOf(base);
    const hint = describeEntry(entry);
    const pathDoc =
      entry.schemaUrlPattern !== undefined
        ? "Path under the mount: '' lists datasets, '<dataset>/' lists a dataset, " +
          "'<dataset>/<key>' is one record, '<dataset>/.schema.json' is its schema."
        : "Path under the mount ('' is the mount root; a trailing '/' lists a directory).";
    const verbs: [EntityVerb, string][] = [
      ["list", "List entries"],
      ["get", "Read one entry"],
      ["put", "Create or replace one entry"],
      ["delete", "Delete one entry"],
    ];
    for (const [verb, summary] of verbs) {
      const properties: JsonObject = { path: { type: "string", description: pathDoc } };
      const required: string[] = [];
      if (verb !== "list") required.push("path");
      if (verb === "put") {
        properties.record = { description: "The entry to store (JSON)." };
        required.push("record");
      }
      const annotations: JsonObject =
        verb === "list" || verb === "get"
          ? { readOnlyHint: true }
          : verb === "put"
            ? { readOnlyHint: false, idempotentHint: true }
            : { readOnlyHint: false, destructiveHint: true, idempotentHint: true };
      tools.push({
        name: uniqueName(names, `${slug}_${verb}`),
        description: `${summary} in the store at ${base}.${hint}`,
        inputSchema: { type: "object", properties, required },
        annotations,
        op: { kind: "entity", base, verb },
      });
    }
  }

  for (const entry of entries("queries")) {
    const path = entry.path;
    if (typeof path !== "string") continue;
    const params = entry.params;
    tools.push({
      name: uniqueName(names, slugOf(path)),
      description: `Run the query at GET ${path}.${describeEntry(entry)}`,
      inputSchema: isObjectSchema(params) ? (params as JsonObject) : { type: "object" },
      annotations: effectAnnotations("pure"),
      op: { kind: "query", path },
    });
  }

  for (const entry of entries("actions")) {
    const path = entry.path;
    if (typeof path !== "string") continue;
    const effect = typeof entry.effect === "string" ? entry.effect : "unsafe";
    const schema = entry.inputSchema;
    let inputSchema: JsonObject;
    let wrapped = false;
    if (schema === undefined) {
      inputSchema = { type: "object" };
    } else if (isObjectSchema(schema)) {
      inputSchema = schema as JsonObject;
    } else {
      inputSchema = { type: "object", properties: { body: schema }, required: ["body"] };
      wrapped = true;
    }
    tools.push({
      name: uniqueName(names, slugOf(path)),
      description: `Run the action at POST ${path} (effect: ${effect}).${describeEntry(entry)}`,
      inputSchema,
      annotations: effectAnnotations(effect),
      op: { kind: "action", path, wrapped },
    });
  }
  return tools;
}

function describeEntry(entry: JsonObject): string {
  let text: unknown = entry.description;
  if (typeof text !== "string") {
    const agent = entry["x-agent"];
    text = isObject(agent) ? agent.description : undefined;
  }
  return typeof text === "string" && text.trim() !== "" ? ` ${text.trim()}` : "";
}

function effectAnnotations(effect: string): JsonObject {
  switch (effect) {
    case "pure":
      return { readOnlyHint: true };
    case "idempotent":
      return { readOnlyHint: false, idempotentHint: true };
    default:
      return { readOnlyHint: false, idempotentHint: false };
  }
}

function isObject(v: unknown): v is JsonObject {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function isObjectSchema(schema: unknown): boolean {
  if (!isObject(schema)) return false;
  return schema.type === "object" || (schema.type === undefined && schema.properties !== undefined);
}

/// A path as a tool name: `/orders/create` → `orders_create`, root → `root`.
export function slugOf(path: string): string {
  const slug = path.replace(/^\/+|\/+$/g, "").replace(/[^A-Za-z0-9_-]/g, "_");
  return (slug === "" ? "root" : slug).slice(0, MAX_TOOL_NAME);
}

function uniqueName(taken: Set<string>, base: string): string {
  let name = base;
  for (let n = 2; taken.has(name); n++) name = `${base}_${n}`;
  taken.add(name);
  return name;
}

/// RFC 3986 unreserved characters pass; everything else is percent-encoded
/// (matching the Rust host's encode set, which also escapes `!'()*`).
function encodeQuery(value: string): string {
  return encodeURIComponent(value).replace(
    /[!'()*]/g,
    (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}

async function callTool(params: Json, ctx: ServiceContext, caller: Message): Promise<JsonObject> {
  const p = isObject(params) ? params : {};
  if (typeof p.name !== "string") throw new RpcError(INVALID_PARAMS, "tools/call needs a string 'name'");
  let args: JsonObject;
  if (p.arguments === undefined || p.arguments === null) args = {};
  else if (isObject(p.arguments)) args = { ...p.arguments };
  else throw new RpcError(INVALID_PARAMS, "'arguments' must be an object");

  const tools = await listTools(ctx, caller);
  const tool = tools.find((t) => t.name === p.name);
  if (!tool) throw new RpcError(INVALID_PARAMS, `unknown tool '${p.name}'`);

  const built = buildCall(tool.op, args, caller);
  // An argument the tool can't use is reported to the model, which can
  // correct it — a tool execution error, not a protocol error.
  if (typeof built === "string") return toolError(built);
  const meta = p._meta;
  const key = isObject(meta) ? meta[IDEMPOTENCY_META_KEY] : undefined;
  if (typeof key === "string") built.setHeader("idempotency-key", key);
  const resp = await requesterOf(ctx).request(built);
  return toolResult(resp, ctx.limits.materializedBodyBytes);
}

function buildCall(op: Op, args: JsonObject, caller: Message): Message | string {
  switch (op.kind) {
    case "action": {
      const body = op.wrapped ? (args.body ?? null) : args;
      return internalRequest(caller, "POST", op.path).withJson(body as Json);
    }
    case "query": {
      const parts: string[] = [];
      for (const [k, v] of Object.entries(args)) {
        if (v === null || v === undefined) continue;
        const value = typeof v === "string" ? v : JSON.stringify(v);
        parts.push(`${encodeQuery(k)}=${encodeQuery(value)}`);
      }
      const query = parts.length ? `?${parts.join("&")}` : "";
      return internalRequest(caller, "GET", `${op.path}${query}`);
    }
    case "entity": {
      const relArg = args.path;
      let rel: string;
      if (relArg === undefined || relArg === null) rel = "";
      else if (typeof relArg === "string") rel = relArg.replace(/^\/+/, "");
      else return "'path' must be a string";
      if (rel.includes("?") || rel.includes("#")) return "'path' may not carry a query or fragment";
      const base = op.base.replace(/\/+$/, "");
      let path = rel === "" ? `${base}/` : `${base}/${rel}`;
      if (op.verb !== "list" && rel === "") return "'path' names the entry and may not be empty";
      let method: string;
      switch (op.verb) {
        case "list":
          if (!path.endsWith("/")) path += "/";
          method = "GET";
          break;
        case "get":
          method = "GET";
          break;
        case "put":
          method = "PUT";
          break;
        case "delete":
          method = "DELETE";
          break;
      }
      const req = internalRequest(caller, method, path);
      return op.verb === "put" ? req.withJson((args.record ?? null) as Json) : req;
    }
  }
}

function toolError(detail: string): JsonObject {
  return { content: [{ type: "text", text: detail }], isError: true };
}

/// An internal response as a `CallToolResult`. A 4xx/5xx is a tool execution
/// error (`isError`) carrying the problem document.
async function toolResult(resp: Message, cap: number): Promise<JsonObject> {
  const status = resp.status ?? 200;
  const location = resp.header("location");
  const content: JsonObject[] = [];
  let structured: Json | undefined;
  if (resp.body) {
    const media = resp.body.mediaType;
    let bytes: Uint8Array | undefined;
    try {
      bytes = await resp.body.materialize(cap);
    } catch (e) {
      const detail = e instanceof RsError ? e.detail : String(e);
      content.push({ type: "text", text: `response body unreadable: ${detail}` });
    }
    if (bytes) {
      const text = () => new TextDecoder().decode(bytes);
      if (media.isJson()) {
        try {
          const v = JSON.parse(text()) as Json;
          content.push({ type: "text", text: JSON.stringify(v) });
          if (isObject(v)) structured = v;
        } catch {
          content.push({ type: "text", text: text() });
        }
      } else if (media.isText()) {
        content.push({ type: "text", text: text() });
      } else if (media.essence().startsWith("image/")) {
        content.push({ type: "image", data: base64(bytes), mimeType: media.essence() });
      } else if (bytes.byteLength > 0) {
        content.push({
          type: "text",
          text: `${bytes.byteLength} bytes of ${media.essence()} (base64): ${base64(bytes)}`,
        });
      }
    }
  }
  if (content.length === 0) content.push({ type: "text", text: `HTTP ${status}` });
  if (location !== undefined) content.push({ type: "text", text: `Location: ${location}` });
  const result: JsonObject = { content, isError: status >= 400 };
  if (structured !== undefined) result.structuredContent = structured;
  return result;
}
