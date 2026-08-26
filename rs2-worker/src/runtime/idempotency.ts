// Idempotency keys and response replay (PRD §7.2). Port of
// `rs2-core/src/idempotency.rs`; the store itself is the SQLite adapter in
// `capabilities/sqlite-idempotency.ts`.

import { Body } from "./body";
import { sha256Hex, toHex, sha256 } from "./crypto";
import type { RsError } from "./error";
import { MediaType } from "./media-type";
import type { Message } from "./message";

/// Max accepted key length (PRD §7.2: opaque ≤256 chars).
export const MAX_KEY_LEN = 256;

/// Default replay window (24 h), in ms.
export const DEFAULT_REPLAY_WINDOW_MS = 24 * 60 * 60 * 1000;

/// Default cap on stored response bodies; larger bodies are not replayed.
export const DEFAULT_BODY_CAP = 1024 * 1024;

/// A recorded response: status, headers, body bytes up to the size cap.
export interface StoredResponse {
  status: number;
  headers: Array<[string, string]>;
  body: [Uint8Array, string] | undefined;
}

/// Rebuild a response `Message` from the stored form, marked
/// `Idempotency-Replayed: true`.
export function storedIntoMessage(stored: StoredResponse, template: Message): Message {
  const body = stored.body ? Body.fromBytes(stored.body[0], MediaType.parse(stored.body[1])) : undefined;
  const resp = template.response(stored.status, body);
  for (const [k, v] of stored.headers) resp.setHeader(k, v);
  resp.setHeader("idempotency-replayed", "true");
  return resp;
}

/// Outcome of registering a key before executing.
export type Begin =
  | { kind: "fresh" }
  | { kind: "inFlight" }
  | { kind: "replay"; stored: StoredResponse }
  | { kind: "payloadMismatch" };

export interface IdempotencyStore {
  begin(scope: string, key: string, payloadHash: string | undefined): Promise<Begin>;
  complete(scope: string, key: string, response: StoredResponse): Promise<void>;
  abandon(scope: string, key: string): Promise<void>;
}

/// Scope a key per tenant + mount + method + path (PRD §7.2).
export function scopeFor(msg: Message, mountBase: string): string {
  return `${msg.tenant}|${mountBase}|${msg.method}|${msg.url.path}`;
}

/// SHA-256 hex of a materialized request payload; `"empty"` for none;
/// `undefined` for streams.
export async function payloadHash(msg: Message): Promise<string | undefined> {
  if (!msg.body) return "empty";
  if (msg.body.payload.kind === "bytes") return sha256Hex(msg.body.payload.bytes);
  return undefined;
}

/// Deterministic per-segment idempotency key (PRD §7.3):
/// `sha256(invocationId ‖ 0x00 ‖ u64le(segmentIndex))`.
export async function segmentKey(invocationId: string, segmentIndex: number): Promise<string> {
  const id = new TextEncoder().encode(invocationId);
  const buf = new Uint8Array(id.byteLength + 1 + 8);
  buf.set(id, 0);
  buf[id.byteLength] = 0;
  new DataView(buf.buffer).setBigUint64(id.byteLength + 1, BigInt(segmentIndex), true);
  return toHex(await sha256(buf));
}

/// Capture a response into stored form, materializing its body up to
/// `bodyCap`. Returns the (possibly materialized) response and the stored
/// form, or `undefined` when the body is too large to record.
export async function captureResponse(
  resp: Message,
  bodyCap: number,
): Promise<[Message, StoredResponse | undefined]> {
  const headers: Array<[string, string]> = [];
  resp.headers.forEach((v, k) => headers.push([k, v]));
  const status = resp.status ?? 200;
  let body: [Uint8Array, string] | undefined;
  if (resp.body) {
    const mt = resp.body.mediaType.toString();
    try {
      const bytes = await resp.body.materialize(bodyCap);
      body = [bytes, mt];
    } catch (_e: unknown) {
      // Too large (or unreadable) to record: skip storage so a duplicate
      // re-executes rather than replaying wrongly.
      void (_e as RsError);
      return [resp, undefined];
    }
  }
  return [resp, { status, headers, body }];
}
