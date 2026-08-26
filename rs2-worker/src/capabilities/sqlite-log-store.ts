// DO SQLite `LogStore` (cloudflare.md §C.3): `emit` inserts synchronously;
// after every 256 inserts rows below `MAX(id) - 50_000` are trimmed;
// `query` reproduces `LogQuery.matches` as `WHERE` clauses, newest first.

import type { Json, JsonObject } from "../runtime/error";
import type { LogQuery, LogRecord, LogStore } from "../runtime/logging";
import { attrStr, severityFromNumber, severityNumber } from "../runtime/logging";

export const LOG_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  time_unix_nano TEXT NOT NULL,
  severity INTEGER NOT NULL,
  body TEXT NOT NULL, trace_id TEXT NOT NULL, span_id TEXT NOT NULL,
  mount TEXT, url_path TEXT,
  attributes TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS logs_trace ON logs(trace_id);
CREATE INDEX IF NOT EXISTS logs_time ON logs(time_unix_nano);
`;

const TRIM_EVERY = 256;
const ROW_CAP = 50_000;

/// Fixed-width decimal so `time_unix_nano` compares chronologically as TEXT.
function nanoKey(n: bigint): string {
  return n.toString().padStart(24, "0");
}

interface Row extends Record<string, SqlStorageValue> {
  time_unix_nano: string;
  severity: number;
  body: string;
  trace_id: string;
  span_id: string;
  attributes: string;
}

export class SqliteLogStore implements LogStore {
  private inserts = 0;

  constructor(
    private readonly sql: SqlStorage,
    private readonly tenant: string,
    private readonly enabled_: boolean,
  ) {}

  emit(record: LogRecord): void {
    if (!this.enabled_) return;
    const attrs: JsonObject = {};
    for (const [k, v] of record.attributes) attrs[k] = v;
    this.sql.exec(
      "INSERT INTO logs (time_unix_nano, severity, body, trace_id, span_id, mount, url_path, attributes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      nanoKey(record.timeUnixNano),
      severityNumber(record.severity),
      record.body,
      record.traceId,
      record.spanId,
      attrStr(record, "rs2.mount") ?? null,
      attrStr(record, "url.path") ?? null,
      JSON.stringify(attrs),
    );
    if (++this.inserts % TRIM_EVERY === 0) {
      this.sql.exec("DELETE FROM logs WHERE id < (SELECT COALESCE(MAX(id), 0) FROM logs) - ?", ROW_CAP);
    }
  }

  enabled(): boolean {
    return this.enabled_;
  }

  isQueryable(): boolean {
    return true;
  }

  async query(_tenant: string, q: LogQuery): Promise<LogRecord[]> {
    if (q.take === 0) return [];
    const where: string[] = [];
    const args: Array<string | number> = [];
    if (q.since !== undefined) {
      where.push("time_unix_nano >= ?");
      args.push(nanoKey(q.since));
    }
    if (q.until !== undefined) {
      where.push("time_unix_nano <= ?");
      args.push(nanoKey(q.until));
    }
    if (q.minSeverity !== undefined) {
      where.push("severity >= ?");
      args.push(severityNumber(q.minSeverity));
    }
    if (q.traceId !== undefined) {
      where.push("trace_id = ?");
      args.push(q.traceId);
    }
    if (q.service !== undefined) {
      where.push("(mount = ? OR url_path = ? OR url_path LIKE ? || '/%')");
      args.push(q.service, q.service, q.service.replace(/\/+$/, ""));
    }
    if (q.contains !== undefined) {
      where.push("instr(body, ?) > 0");
      args.push(q.contains);
    }
    const sql = `SELECT time_unix_nano, severity, body, trace_id, span_id, attributes FROM logs${
      where.length ? ` WHERE ${where.join(" AND ")}` : ""
    } ORDER BY id DESC LIMIT ?`;
    args.push(q.take);
    const rows = this.sql.exec<Row>(sql, ...args).toArray();
    return rows.map((r) => {
      const attrs = JSON.parse(r.attributes) as JsonObject;
      const attributes: Array<[string, Json]> = Object.entries(attrs).filter(([k]) => k !== "rs2.tenant");
      return {
        timeUnixNano: BigInt(r.time_unix_nano),
        severity: severityFromNumber(r.severity),
        body: r.body,
        tenant: this.tenant,
        traceId: r.trace_id,
        spanId: r.span_id,
        attributes,
      };
    });
  }
}
