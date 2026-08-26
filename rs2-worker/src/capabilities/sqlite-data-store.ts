// DO SQLite as `DataStore` (cloudflare.md §C.3): `records` + `datasets`
// tables, namespaced by `ns` (`""` for the node default, a `store.root`
// for an explicit `builtin:file` mount, `mem` for `builtin:mem`). Key order
// is SQLite `ORDER BY key` — the BTreeMap semantics of the Rust adapters.

import { RsError } from "../runtime/error";
import type { Json } from "../runtime/error";
import type { ListSpec } from "../runtime/listing";
import { compareRecords, compareUtf8, parseJsonPreservingBigInts, project, stringifyJson } from "../runtime/listing";
import { sha256Hex } from "../runtime/crypto";
import type { DataStore } from "./types";

export const DATA_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS records (
  ns TEXT NOT NULL, dataset TEXT NOT NULL, key TEXT NOT NULL,
  value TEXT NOT NULL,
  etag TEXT NOT NULL,
  PRIMARY KEY (ns, dataset, key));
CREATE TABLE IF NOT EXISTS datasets (
  ns TEXT NOT NULL, dataset TEXT NOT NULL, schema TEXT,
  PRIMARY KEY (ns, dataset));
`;

const SCAN_PAGE = 500;

export class SqliteDataStore implements DataStore {
  constructor(
    private readonly sql: SqlStorage,
    private readonly ns: string,
  ) {}

  private datasetExists(dataset: string): boolean {
    const r = this.sql
      .exec(
        "SELECT 1 AS x FROM datasets WHERE ns = ? AND dataset = ? UNION ALL SELECT 1 FROM records WHERE ns = ? AND dataset = ? LIMIT 1",
        this.ns,
        dataset,
        this.ns,
        dataset,
      )
      .toArray();
    return r.length > 0;
  }

  async get(_tenant: string, dataset: string, key: string): Promise<Json> {
    const rows = this.sql
      .exec<{ value: string }>("SELECT value FROM records WHERE ns = ? AND dataset = ? AND key = ?", this.ns, dataset, key)
      .toArray();
    if (rows.length === 0) throw RsError.notFound(`no record '${key}' in dataset '${dataset}'`);
    return parseJsonPreservingBigInts(rows[0]!.value);
  }

  async put(_tenant: string, dataset: string, key: string, value: Json): Promise<boolean> {
    const text = stringifyJson(value);
    const etag = (await sha256Hex(text)).slice(0, 16);
    const existed =
      this.sql
        .exec("SELECT 1 AS x FROM records WHERE ns = ? AND dataset = ? AND key = ?", this.ns, dataset, key)
        .toArray().length > 0;
    this.sql.exec(
      "INSERT INTO records (ns, dataset, key, value, etag) VALUES (?, ?, ?, ?, ?) ON CONFLICT(ns, dataset, key) DO UPDATE SET value = excluded.value, etag = excluded.etag",
      this.ns,
      dataset,
      key,
      text,
      etag,
    );
    return !existed;
  }

  async delete(_tenant: string, dataset: string, key: string): Promise<void> {
    const cur = this.sql.exec("DELETE FROM records WHERE ns = ? AND dataset = ? AND key = ?", this.ns, dataset, key);
    if (cur.rowsWritten === 0) throw RsError.notFound(`no record '${key}' in dataset '${dataset}'`);
  }

  async listKeys(_tenant: string, dataset: string, take: number, skip: number): Promise<[string[], number]> {
    if (!this.datasetExists(dataset)) throw RsError.notFound(`no dataset '${dataset}'`);
    const total = this.sql
      .exec<{ n: number }>("SELECT COUNT(*) AS n FROM records WHERE ns = ? AND dataset = ?", this.ns, dataset)
      .one().n;
    const keys = this.sql
      .exec<{ key: string }>(
        "SELECT key FROM records WHERE ns = ? AND dataset = ? ORDER BY key LIMIT ? OFFSET ?",
        this.ns,
        dataset,
        clampLimit(take),
        skip,
      )
      .toArray()
      .map((r) => r.key);
    return [keys, total];
  }

  async listDatasets(_tenant: string, take: number, skip: number): Promise<[string[], number]> {
    const names = this.sql
      .exec<{ dataset: string }>(
        "SELECT dataset FROM datasets WHERE ns = ? UNION SELECT DISTINCT dataset FROM records WHERE ns = ? ORDER BY dataset",
        this.ns,
        this.ns,
      )
      .toArray()
      .map((r) => r.dataset);
    return [names.slice(skip, skip + take), names.length];
  }

  async getSchema(_tenant: string, dataset: string): Promise<Json | undefined> {
    const rows = this.sql
      .exec<{ schema: string | null }>("SELECT schema FROM datasets WHERE ns = ? AND dataset = ?", this.ns, dataset)
      .toArray();
    if (rows.length === 0 || rows[0]!.schema === null) return undefined;
    return JSON.parse(rows[0]!.schema) as Json;
  }

  async putSchema(_tenant: string, dataset: string, schema: Json): Promise<void> {
    this.sql.exec(
      "INSERT INTO datasets (ns, dataset, schema) VALUES (?, ?, ?) ON CONFLICT(ns, dataset) DO UPDATE SET schema = excluded.schema",
      this.ns,
      dataset,
      JSON.stringify(schema),
    );
  }

  async deleteDataset(_tenant: string, dataset: string): Promise<void> {
    if (!this.datasetExists(dataset)) throw RsError.notFound(`no dataset '${dataset}'`);
    this.sql.exec("DELETE FROM records WHERE ns = ? AND dataset = ?", this.ns, dataset);
    this.sql.exec("DELETE FROM datasets WHERE ns = ? AND dataset = ?", this.ns, dataset);
  }

  private *walk(dataset: string): Generator<[string, Json]> {
    let after: string | undefined;
    for (;;) {
      const rows =
        after === undefined
          ? this.sql
              .exec<{ key: string; value: string }>(
                "SELECT key, value FROM records WHERE ns = ? AND dataset = ? ORDER BY key LIMIT ?",
                this.ns,
                dataset,
                SCAN_PAGE,
              )
              .toArray()
          : this.sql
              .exec<{ key: string; value: string }>(
                "SELECT key, value FROM records WHERE ns = ? AND dataset = ? AND key > ? ORDER BY key LIMIT ?",
                this.ns,
                dataset,
                after,
                SCAN_PAGE,
              )
              .toArray();
      for (const r of rows) yield [r.key, parseJsonPreservingBigInts(r.value)];
      if (rows.length < SCAN_PAGE) return;
      after = rows[rows.length - 1]!.key;
    }
  }

  async scanMatching(_tenant: string, dataset: string, keep: (v: Json) => boolean): Promise<Array<[string, Json]>> {
    if (!this.datasetExists(dataset)) throw RsError.notFound(`no dataset '${dataset}'`);
    const out: Array<[string, Json]> = [];
    for (const [k, v] of this.walk(dataset)) if (keep(v)) out.push([k, v]);
    return out;
  }

  async listRecords(_tenant: string, dataset: string, spec: ListSpec): Promise<[Array<[string, Json]>, number]> {
    if (!this.datasetExists(dataset)) throw RsError.notFound(`no dataset '${dataset}'`);
    const total = this.sql
      .exec<{ n: number }>("SELECT COUNT(*) AS n FROM records WHERE ns = ? AND dataset = ?", this.ns, dataset)
      .one().n;
    if (spec.sort.length === 0) {
      const rows = this.sql
        .exec<{ key: string; value: string }>(
          "SELECT key, value FROM records WHERE ns = ? AND dataset = ? ORDER BY key LIMIT ? OFFSET ?",
          this.ns,
          dataset,
          clampLimit(spec.take),
          spec.skip,
        )
        .toArray();
      return [rows.map((r) => [r.key, project(parseJsonPreservingBigInts(r.value), spec.fields)]), total];
    }
    const all = [...this.walk(dataset)];
    all.sort(([ka, a], [kb, b]) => {
      const ord = compareRecords(a, b, spec.sort);
      return ord !== 0 ? ord : compareUtf8(ka, kb);
    });
    const page: Array<[string, Json]> = all
      .slice(spec.skip, spec.skip + spec.take)
      .map(([k, r]) => [k, project(r, spec.fields)]);
    return [page, total];
  }

  listingPushdown(): boolean {
    return true;
  }
}

/// SQLite `LIMIT` takes a signed 64-bit integer; `Number.MAX_SAFE_INTEGER` is fine, `Infinity` is not.
function clampLimit(n: number): number {
  return Number.isFinite(n) ? n : Number.MAX_SAFE_INTEGER;
}
