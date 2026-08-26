// `SqliteIdempotencyStore` (cloudflare.md §C.3) against a real DO SQLite:
// the begin/complete/abandon state machine, and — issue #2 item 3 — an
// in-flight row left behind by a mid-request reset expires instead of
// answering 409 forever.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import {
  IDEMPOTENCY_SCHEMA_SQL,
  SqliteIdempotencyStore,
  migrateIdempotencySchema,
} from "../src/capabilities/sqlite-idempotency";

/// Run `fn` inside a fresh tenant DO (its constructor applies the schema).
function withStorage<T>(name: string, fn: (storage: DurableObjectStorage) => Promise<T>): Promise<T> {
  const id = env.TENANTS.idFromName(name);
  const stub = env.TENANTS.get(id);
  return runInDurableObject(stub, async (_instance, state) => fn(state.storage));
}

describe("SqliteIdempotencyStore", () => {
  it("fresh → in-flight → replay, with payload-mismatch detection", async () => {
    await withStorage("idem-basic", async (storage) => {
      const store = new SqliteIdempotencyStore(storage);
      expect((await store.begin("s", "k", "h1")).kind).toBe("fresh");
      expect((await store.begin("s", "k", "h1")).kind).toBe("inFlight");
      expect((await store.begin("s", "k", "h2")).kind).toBe("payloadMismatch");
      await store.complete("s", "k", { status: 201, headers: [["location", "/x"]], body: undefined });
      const replay = await store.begin("s", "k", "h1");
      expect(replay.kind).toBe("replay");
      if (replay.kind === "replay") expect(replay.stored.status).toBe(201);
      await store.abandon("s", "k");
      expect((await store.begin("s", "k", "h1")).kind).toBe("fresh");
    });
  });

  it("an in-flight row older than its lifetime is abandoned, a live one is not", async () => {
    await withStorage("idem-inflight", async (storage) => {
      const short = new SqliteIdempotencyStore(storage, undefined, undefined, 50);
      expect((await short.begin("s", "k", "h")).kind).toBe("fresh");
      expect((await short.begin("s", "k", "h")).kind).toBe("inFlight");
      // The request died with the object (no `complete`/`abandon`).
      await new Promise((r) => setTimeout(r, 80));
      expect((await short.begin("s", "k", "h")).kind).toBe("fresh");
      // A row within its lifetime still blocks.
      expect((await short.begin("s", "k", "h")).kind).toBe("inFlight");
    });
  });

  it("rows from before the started_at column count as abandoned", async () => {
    await withStorage("idem-migrate", async (storage) => {
      const sql = storage.sql;
      // Recreate the pre-migration table shape and seed a stuck row.
      sql.exec("DROP TABLE idempotency");
      sql.exec(
        "CREATE TABLE idempotency (scope TEXT NOT NULL, key TEXT NOT NULL, payload_hash TEXT, state INTEGER NOT NULL, status INTEGER, headers TEXT, body BLOB, media_type TEXT, completed_at INTEGER, PRIMARY KEY (scope, key))",
      );
      sql.exec("INSERT INTO idempotency (scope, key, payload_hash, state) VALUES ('s', 'stuck', 'h', 0)");
      sql.exec(IDEMPOTENCY_SCHEMA_SQL);
      migrateIdempotencySchema(sql);
      migrateIdempotencySchema(sql); // idempotent
      const store = new SqliteIdempotencyStore(storage);
      expect((await store.begin("s", "stuck", "h")).kind).toBe("fresh");
    });
  });
});
