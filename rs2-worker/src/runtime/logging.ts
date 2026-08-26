// OTel-compatible structured logging (PRD §14). Port of
// `rs2-core/src/logging/mod.rs`: the OTLP-ish flat record, severities,
// the query filter, and the mount-bound `ServiceLogger`.

import type { Json, JsonObject } from "./error";
import type { TraceContext } from "./message";

/// Log severity, ordered Debug < Info < Warn < Error.
export type Severity = 0 | 1 | 2 | 3;
export const Severity = { Debug: 0 as Severity, Info: 1 as Severity, Warn: 2 as Severity, Error: 3 as Severity };

/// OTLP `severityNumber`: DEBUG=5, INFO=9, WARN=13, ERROR=17.
export function severityNumber(s: Severity): number {
  return [5, 9, 13, 17][s]!;
}

export function severityText(s: Severity): string {
  return ["DEBUG", "INFO", "WARN", "ERROR"][s]!;
}

/// Parse a severity name; case-insensitive; accepts `warning`.
export function parseSeverity(s: string): Severity | undefined {
  switch (s.trim().toLowerCase()) {
    case "debug":
      return Severity.Debug;
    case "info":
      return Severity.Info;
    case "warn":
    case "warning":
      return Severity.Warn;
    case "error":
      return Severity.Error;
    default:
      return undefined;
  }
}

/// Recover a severity from a stored OTLP `severityNumber`.
export function severityFromNumber(n: number): Severity {
  if (n <= 8) return Severity.Debug;
  if (n <= 12) return Severity.Info;
  if (n <= 16) return Severity.Warn;
  return Severity.Error;
}

/// One log entry, modeled on the OTLP LogRecord. `timeUnixNano` is a
/// decimal string (ms precision × 1_000_000 on this host).
export interface LogRecord {
  timeUnixNano: bigint;
  severity: Severity;
  body: string;
  tenant: string;
  traceId: string;
  spanId: string;
  attributes: Array<[string, Json]>;
}

export function nowUnixNano(): bigint {
  return BigInt(Date.now()) * 1_000_000n;
}

export function recordNow(severity: Severity, tenant: string, trace: TraceContext, body: string): LogRecord {
  return {
    timeUnixNano: nowUnixNano(),
    severity,
    body,
    tenant,
    traceId: trace.traceId,
    spanId: trace.spanId,
    attributes: [],
  };
}

export function attr(rec: LogRecord, key: string, value: Json): LogRecord {
  rec.attributes.push([key, value]);
  return rec;
}

export function attrStr(rec: LogRecord, key: string): string | undefined {
  const hit = rec.attributes.find(([k]) => k === key);
  return hit && typeof hit[1] === "string" ? hit[1] : undefined;
}

/// Serialize to one OTLP-shaped JSON object; `rs2.tenant` is the first attribute.
export function toOtlpJson(rec: LogRecord): JsonObject {
  const attrs: JsonObject = { "rs2.tenant": rec.tenant };
  for (const [k, v] of rec.attributes) attrs[k] = v;
  return {
    timeUnixNano: rec.timeUnixNano.toString(),
    severityNumber: severityNumber(rec.severity),
    severityText: severityText(rec.severity),
    body: rec.body,
    traceId: rec.traceId,
    spanId: rec.spanId,
    attributes: attrs,
  };
}

/// A logging sink: fire-and-forget emit.
export interface LogSink {
  emit(record: LogRecord): void;
  enabled(): boolean;
}

/// Reader-side filter for `LogStore.query`.
export interface LogQuery {
  take: number;
  since?: bigint;
  until?: bigint;
  minSeverity?: Severity;
  traceId?: string;
  /// Mount filter, matched against `rs2.mount` or the `url.path` prefix.
  service?: string;
  /// Case-sensitive substring on the message body.
  contains?: string;
}

/// Whether a record passes every set filter (the in-memory reference; the
/// SQLite store reproduces it in SQL).
export function queryMatches(q: LogQuery, r: LogRecord): boolean {
  if (q.since !== undefined && r.timeUnixNano < q.since) return false;
  if (q.until !== undefined && r.timeUnixNano > q.until) return false;
  if (q.minSeverity !== undefined && r.severity < q.minSeverity) return false;
  if (q.traceId !== undefined && r.traceId !== q.traceId) return false;
  if (q.service !== undefined) {
    const byMount = attrStr(r, "rs2.mount") === q.service;
    const p = attrStr(r, "url.path");
    const byPath = p !== undefined && (p === q.service || p.startsWith(`${q.service.replace(/\/+$/, "")}/`));
    if (!byMount && !byPath) return false;
  }
  if (q.contains !== undefined && !r.body.includes(q.contains)) return false;
  return true;
}

/// A queryable log store: a `LogSink` that can also read back.
export interface LogStore extends LogSink {
  query(tenant: string, q: LogQuery): Promise<LogRecord[]>;
  isQueryable(): boolean;
}

/// A no-op store: drops emits, reports not-queryable.
export class NullLogStore implements LogStore {
  emit(_record: LogRecord): void {}
  enabled(): boolean {
    return false;
  }
  async query(): Promise<LogRecord[]> {
    return [];
  }
  isQueryable(): boolean {
    return false;
  }
}

/// A mount-bound emit handle stored on `ServiceContext`; the host stamps
/// identity so a service cannot forge another's logs.
export class ServiceLogger {
  constructor(
    private readonly sink_: LogStore,
    private readonly tenant: string,
    private readonly mount: string,
    private readonly service: string,
  ) {}

  sink(): LogStore {
    return this.sink_;
  }

  log(trace: TraceContext, severity: Severity, body: string): void {
    if (!this.sink_.enabled()) return;
    this.sink_.emit({
      timeUnixNano: nowUnixNano(),
      severity,
      body,
      tenant: this.tenant,
      traceId: trace.traceId,
      spanId: trace.spanId,
      attributes: [
        ["rs2.mount", this.mount],
        ["rs2.service", this.service],
        ["rs2.source", "service"],
      ],
    });
  }

  debug(trace: TraceContext, body: string): void {
    this.log(trace, Severity.Debug, body);
  }
  info(trace: TraceContext, body: string): void {
    this.log(trace, Severity.Info, body);
  }
  warn(trace: TraceContext, body: string): void {
    this.log(trace, Severity.Warn, body);
  }
  error(trace: TraceContext, body: string): void {
    this.log(trace, Severity.Error, body);
  }
}
