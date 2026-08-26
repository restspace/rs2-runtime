// Types for mock-redis.mjs (plain JS; see that file for behavior).
export function startMockRedis(opts?: { port?: number; host?: string; delayMs?: number }): Promise<{
  port: number;
  store: Map<string, string>;
  connections(): number;
  close(): Promise<void>;
}>;
