// `log` reader service (PRD §14). Port of `rs2-core/src/services/log_reader.rs`.

import { Body } from "../runtime/body";
import { RsError, codes } from "../runtime/error";
import type { LogQuery } from "../runtime/logging";
import { parseSeverity, toOtlpJson } from "../runtime/logging";
import { MediaType } from "../runtime/media-type";
import type { Message } from "../runtime/message";
import type { Service, ServiceContext } from "./context";

/// Parse a `since`/`until` value: Unix milliseconds or RFC 3339, to nanoseconds.
function parseTime(s: string): bigint | undefined {
  if (/^\d+$/.test(s)) return BigInt(s) * 1_000_000n;
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/.test(s)) return undefined;
  const t = Date.parse(s);
  if (Number.isNaN(t)) return undefined;
  return BigInt(Math.max(t, 0)) * 1_000_000n;
}

export class LogReaderService implements Service {
  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    if (msg.method !== "GET") {
      throw new RsError(405, codes.BAD_REQUEST, "Method Not Allowed", "the log reader is read-only (GET)");
    }
    const store = ctx.logStore;
    if (!store) throw RsError.capabilityDenied("logStore");
    if (!store.isQueryable()) {
      throw RsError.engineUnavailable("logs are exported to an external sink, not locally queryable on this node");
    }
    const takeParam = msg.url.queryParam("$take");
    const take = Math.min(takeParam !== undefined && /^\+?\d+$/.test(takeParam) ? Number(takeParam) : 100, 10_000);
    const q: LogQuery = { take };
    const first = msg.url.serviceSegments()[0];
    if (first !== undefined) q.traceId = first;
    const traceParam = msg.url.queryParam("traceId");
    if (traceParam !== undefined) q.traceId = traceParam;
    const sev = msg.url.queryParam("severity");
    if (sev !== undefined) {
      const parsed = parseSeverity(sev);
      if (parsed === undefined) throw RsError.badRequest(`unknown severity '${sev}' (debug|info|warn|error)`);
      q.minSeverity = parsed;
    }
    q.service = msg.url.queryParam("service");
    q.contains = msg.url.queryParam("q");
    const since = msg.url.queryParam("since");
    if (since !== undefined) q.since = parseTime(since);
    const until = msg.url.queryParam("until");
    if (until !== undefined) q.until = parseTime(until);

    const accept = msg.header("accept");
    const wantsText = accept !== undefined && accept.includes("text/plain") && !accept.includes("application/json");
    const records = await store.query(msg.tenant, q);
    const total = records.length;
    const body = wantsText
      ? Body.fromString(records.map((r) => JSON.stringify(toOtlpJson(r))).join("\n"), new MediaType("text/plain; charset=utf-8"))
      : Body.fromString(JSON.stringify(records.map(toOtlpJson)), MediaType.json());
    const resp = msg.response(200, body);
    resp.setHeader("x-total-count", String(total));
    return resp;
  }
}
