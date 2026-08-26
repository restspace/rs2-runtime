// Retry policies and effect classes (PRD §7.1, §7.3). Port of
// `rs2-core/src/retry.rs`.

import type { Json } from "./error";
import type { RsError } from "./error";
import type { Message } from "./message";

export type EffectClass = "pure" | "idempotent" | "keyed" | "unsafe";

/// Default inference from method (PRD §7.1).
export function effectForMethod(method: string): EffectClass {
  switch (method) {
    case "GET":
    case "HEAD":
    case "OPTIONS":
      return "pure";
    case "PUT":
    case "DELETE":
      return "idempotent";
    default:
      return "unsafe";
  }
}

/// Whether auto-retry is permitted for this effect class (PRD §7.3).
export function permitsRetry(effect: EffectClass, hasIdempotencyKey: boolean): boolean {
  switch (effect) {
    case "pure":
    case "idempotent":
      return true;
    case "keyed":
      return hasIdempotencyKey;
    case "unsafe":
      return false;
  }
}

export type Jitter = "none" | "full";

export interface RetryPolicy {
  enabled: boolean;
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  backoffMultiplier: number;
  jitter: Jitter;
  retryStatuses: number[];
  retryOnNetworkError: boolean;
  respectRetryAfter: boolean;
}

export function defaultRetryPolicy(): RetryPolicy {
  return {
    enabled: true,
    maxAttempts: 4,
    baseDelayMs: 250,
    maxDelayMs: 5000,
    backoffMultiplier: 2.0,
    jitter: "full",
    retryStatuses: [408, 429, 500, 502, 503, 504],
    retryOnNetworkError: true,
    respectRetryAfter: true,
  };
}

/// A policy that never retries (the runtime default when nothing is configured).
export function noRetry(): RetryPolicy {
  return { ...defaultRetryPolicy(), enabled: false, maxAttempts: 1 };
}

/// Resolve the effective policy from the override chain: first present wins.
export function resolveRetry(chain: Array<RetryPolicy | undefined>): RetryPolicy {
  for (const p of chain) if (p) return { ...p, retryStatuses: [...p.retryStatuses] };
  return noRetry();
}

/// Parse a policy from a JSON config value; `undefined` on null/invalid
/// (serde `from_value(..).ok()` semantics; unknown keys are ignored).
export function retryFromConfig(value: Json | undefined): RetryPolicy | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object" || Array.isArray(value)) return undefined;
  const p = defaultRetryPolicy();
  const v = value;
  const bool = (k: keyof RetryPolicy): boolean => {
    if (v[k] === undefined) return true;
    if (typeof v[k] !== "boolean") return false;
    (p as unknown as Record<string, unknown>)[k] = v[k];
    return true;
  };
  const num = (k: keyof RetryPolicy, integer: boolean, min: number): boolean => {
    if (v[k] === undefined) return true;
    const x = v[k];
    if (typeof x !== "number" || (integer && !Number.isInteger(x)) || x < min) return false;
    (p as unknown as Record<string, unknown>)[k] = x;
    return true;
  };
  if (!bool("enabled") || !bool("retryOnNetworkError") || !bool("respectRetryAfter")) return undefined;
  if (!num("maxAttempts", true, 0) || !num("baseDelayMs", true, 0) || !num("maxDelayMs", true, 0)) return undefined;
  if (!num("backoffMultiplier", false, -Infinity)) return undefined;
  if (v.jitter !== undefined) {
    if (v.jitter !== "none" && v.jitter !== "full") return undefined;
    p.jitter = v.jitter;
  }
  if (v.retryStatuses !== undefined) {
    if (!Array.isArray(v.retryStatuses) || !v.retryStatuses.every((s) => typeof s === "number" && Number.isInteger(s)))
      return undefined;
    p.retryStatuses = v.retryStatuses as number[];
  }
  return p;
}

export function retryableStatus(p: RetryPolicy, status: number): boolean {
  return p.enabled && p.retryStatuses.includes(status);
}

/// Backoff delay (ms) before retrying after `failedAttempt` (1-based).
export function retryDelayMs(p: RetryPolicy, failedAttempt: number, retryAfterMs: number | undefined): number {
  if (p.respectRetryAfter && retryAfterMs !== undefined) return Math.min(retryAfterMs, p.maxDelayMs);
  const raw = p.baseDelayMs * Math.pow(p.backoffMultiplier, Math.max(failedAttempt - 1, 0));
  const capped = Math.min(Math.floor(raw), p.maxDelayMs);
  if (p.jitter === "none") return capped;
  return Math.floor(Math.random() * (capped + 1));
}

/// Parse a `Retry-After` header value (seconds or HTTP date) into ms.
export function parseRetryAfter(value: string): number | undefined {
  const trimmed = value.trim();
  if (/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(trimmed)) {
    const secs = Number(trimmed);
    if (Number.isFinite(secs) && secs >= 0) return Math.floor(secs * 1000);
    return undefined;
  }
  const t = Date.parse(trimmed);
  if (Number.isNaN(t)) return undefined;
  const now = Date.now();
  return t > now ? t - now : 0;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/// Drive an attempt function under a retry policy, gated on the effect class.
export async function retryRequest(
  policy: RetryPolicy,
  effect: EffectClass,
  hasIdempotencyKey: boolean,
  attemptFn: (attempt: number) => Promise<Message>,
): Promise<Message> {
  const maxAttempts = policy.enabled && permitsRetry(effect, hasIdempotencyKey) ? Math.max(policy.maxAttempts, 1) : 1;
  let attempt = 1;
  for (;;) {
    try {
      const resp = await attemptFn(attempt);
      const status = resp.status ?? 200;
      if (attempt >= maxAttempts || !retryableStatus(policy, status)) return resp;
      const ra = resp.header("retry-after");
      await sleep(retryDelayMs(policy, attempt, ra !== undefined ? parseRetryAfter(ra) : undefined));
    } catch (e) {
      const err = e as RsError;
      const transportRetryable = policy.enabled && policy.retryOnNetworkError && err.retryable === true;
      if (attempt >= maxAttempts || !transportRetryable) throw e;
      await sleep(retryDelayMs(policy, attempt, undefined));
    }
    attempt += 1;
  }
}
