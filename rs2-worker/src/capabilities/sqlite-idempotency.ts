// DO SQLite `IdempotencyStore` (cloudflare.md §C.3). `begin` is one
// `transactionSync`; expired `done` rows are swept every 4096 begins; the
// row cap is 100 000 with oldest-done eviction (an eighth per sweep),
// in-flight never evicted — `MemIdempotencyStore` semantics — except that
// an in-flight row older than `inFlightMs` is *abandoned*: a DO reset
// mid-request (an unhandled rejection resets the whole object) would
// otherwise leave the row in-flight forever and answer 409 for that
// scope+key until the end of time. Rust's in-memory store forgets on
// restart; this is the durable equivalent.

import type { Begin, IdempotencyStore, StoredResponse } from "../runtime/idempotency";
import { DEFAULT_REPLAY_WINDOW_MS } from "../runtime/idempotency";

export const IDEMPOTENCY_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS idempotency (
  scope TEXT NOT NULL, key TEXT NOT NULL,
  payload_hash TEXT,
  state INTEGER NOT NULL,
  status INTEGER, headers TEXT,
  body BLOB, media_type TEXT,
  completed_at INTEGER,
  started_at INTEGER,
  PRIMARY KEY (scope, key));
CREATE INDEX IF NOT EXISTS idem_done ON idempotency(completed_at);
`;

/// Bring a pre-`started_at` table up to date (idempotent; safe to run on
/// every construction). Rows that predate the column have no start time
/// and are treated as abandoned on their next `begin`.
export function migrateIdempotencySchema(sql: SqlStorage): void {
  const cols = sql
    .exec<{ name: string }>("SELECT name FROM pragma_table_info('idempotency')")
    .toArray()
    .map((r) => r.name);
  if (!cols.includes("started_at")) {
    sql.exec("ALTER TABLE idempotency ADD COLUMN started_at INTEGER");
  }
}

const SWEEP_EVERY = 4096;
const MAX_ENTRIES = 100_000;

/// Default in-flight lifetime: a generous multiple of the longest request
/// the host will run (`wallClockServiceMs` 30 s, dispatch-raced), so a live
/// request is never mistaken for an abandoned one.
export const DEFAULT_IN_FLIGHT_MS = 5 * 60_000;

interface Row extends Record<string, SqlStorageValue> {
  payload_hash: string | null;
  state: number;
  status: number | null;
  headers: string | null;
  body: ArrayBuffer | null;
  media_type: string | null;
  completed_at: number | null;
  started_at: number | null;
}

export class SqliteIdempotencyStore implements IdempotencyStore {
  private begins = 0;

  constructor(
    private readonly storage: DurableObjectStorage,
    private readonly windowMs = DEFAULT_REPLAY_WINDOW_MS,
    private readonly maxEntries = MAX_ENTRIES,
    private readonly inFlightMs = DEFAULT_IN_FLIGHT_MS,
  ) {}

  private sweep(now: number, forceEvict: boolean): void {
    const sql = this.storage.sql;
    sql.exec("DELETE FROM idempotency WHERE state = 1 AND completed_at < ?", now - this.windowMs);
    // Abandoned in-flight rows (a reset mid-request); never a live one.
    sql.exec(
      "DELETE FROM idempotency WHERE state = 0 AND (started_at IS NULL OR started_at < ?)",
      now - this.inFlightMs,
    );
    if (!forceEvict) return;
    const n = sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM idempotency").one().n;
    if (n >= this.maxEntries) {
      sql.exec(
        "DELETE FROM idempotency WHERE rowid IN (SELECT rowid FROM idempotency WHERE state = 1 ORDER BY completed_at ASC LIMIT ?)",
        Math.floor(this.maxEntries / 8) + 1,
      );
    }
  }

  async begin(scope: string, key: string, payloadHash: string | undefined): Promise<Begin> {
    const n = this.begins++;
    return this.storage.transactionSync(() => {
      const sql = this.storage.sql;
      const now = Date.now();
      if (n % SWEEP_EVERY === 0) this.sweep(now, true);
      else {
        const count = sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM idempotency").one().n;
        if (count >= this.maxEntries) this.sweep(now, true);
      }
      // Opportunistic expiry of the addressed slot: a done row past the
      // replay window, or an in-flight row past its lifetime (abandoned).
      sql.exec(
        "DELETE FROM idempotency WHERE scope = ? AND key = ? AND ((state = 1 AND completed_at < ?) OR (state = 0 AND (started_at IS NULL OR started_at < ?)))",
        scope,
        key,
        now - this.windowMs,
        now - this.inFlightMs,
      );
      const rows = sql
        .exec<Row>(
          "SELECT payload_hash, state, status, headers, body, media_type, completed_at, started_at FROM idempotency WHERE scope = ? AND key = ?",
          scope,
          key,
        )
        .toArray();
      if (rows.length === 0) {
        sql.exec(
          "INSERT INTO idempotency (scope, key, payload_hash, state, started_at) VALUES (?, ?, ?, 0, ?)",
          scope,
          key,
          payloadHash ?? null,
          now,
        );
        return { kind: "fresh" } as Begin;
      }
      const row = rows[0]!;
      if (row.payload_hash !== null && payloadHash !== undefined && row.payload_hash !== payloadHash) {
        return { kind: "payloadMismatch" } as Begin;
      }
      if (row.state === 0) return { kind: "inFlight" } as Begin;
      const stored: StoredResponse = {
        status: row.status ?? 200,
        headers: row.headers ? (JSON.parse(row.headers) as Array<[string, string]>) : [],
        body: row.body && row.media_type !== null ? [new Uint8Array(row.body), row.media_type] : undefined,
      };
      return { kind: "replay", stored } as Begin;
    });
  }

  async complete(scope: string, key: string, response: StoredResponse): Promise<void> {
    this.storage.sql.exec(
      "UPDATE idempotency SET state = 1, status = ?, headers = ?, body = ?, media_type = ?, completed_at = ? WHERE scope = ? AND key = ?",
      response.status,
      JSON.stringify(response.headers),
      response.body ? response.body[0] : null,
      response.body ? response.body[1] : null,
      Date.now(),
      scope,
      key,
    );
  }

  async abandon(scope: string, key: string): Promise<void> {
    this.storage.sql.exec("DELETE FROM idempotency WHERE scope = ? AND key = ?", scope, key);
  }
}
