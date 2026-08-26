// Types for mock-mongo.mjs (plain JS; see that file for behavior).
export function startMockMongo(opts?: { port?: number; host?: string }): Promise<{
  port: number;
  store: Map<string, Map<string, Record<string, unknown>>>;
  close(): Promise<void>;
}>;
