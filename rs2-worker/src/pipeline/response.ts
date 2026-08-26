// `$response` — success-path response shaping from a transform. A
// transform result is a response directive iff it is an object whose
// single key is `$response`. Port of `rs2-core/src/pipeline/response.rs`.

import { Body } from "../runtime/body";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { MediaType } from "../runtime/media-type";
import type { Message } from "../runtime/message";
import { isValidHeaderName } from "./spec";

/// Match a transform output as a response directive: an object whose only
/// key is `$response`, holding an object. Anything else is a plain body.
export function detectResponse(out: Json): JsonObject | undefined {
  if (!out || typeof out !== "object" || Array.isArray(out)) return undefined;
  const keys = Object.keys(out);
  if (keys.length !== 1) return undefined;
  const inner = out.$response;
  if (!inner || typeof inner !== "object" || Array.isArray(inner)) return undefined;
  return inner;
}

/// Apply the directive to the response message. Invalid directives are
/// structured 400s.
export function applyResponse(inner: JsonObject, msg: Message): void {
  for (const key of Object.keys(inner)) {
    if (!["status", "headers", "mediaType", "body"].includes(key)) {
      throw RsError.badRequest(`$response: unknown field '${key}' (have: status, headers, mediaType, body)`);
    }
  }

  let status = 200;
  if (inner.status !== undefined) {
    const v = inner.status;
    if (typeof v !== "number" || !Number.isInteger(v) || v < 0 || v > 65535) {
      throw RsError.badRequest(`$response.status must be an integer, got ${JSON.stringify(v)}`);
    }
    if (v < 100 || v > 999) throw RsError.badRequest(`$response.status ${v} is not a valid HTTP status`);
    status = v;
  }

  let mediaType: MediaType | undefined;
  if (inner.mediaType !== undefined) {
    if (typeof inner.mediaType !== "string") {
      throw RsError.badRequest(`$response.mediaType must be a string, got ${JSON.stringify(inner.mediaType)}`);
    }
    mediaType = new MediaType(inner.mediaType);
  }

  const body = inner.body;
  if (typeof body === "string") {
    // A string body is raw text (v1 `to-text`), not a JSON-encoded string.
    msg.body = Body.fromString(body, mediaType ?? new MediaType("text/plain"));
  } else if (body !== undefined) {
    const b = Body.fromJson(body);
    if (mediaType) b.mediaType = mediaType;
    msg.body = b;
  } else if (mediaType && msg.body) {
    // No body: shape in place (v1 `set-status`), retyping if asked.
    msg.body.mediaType = mediaType;
  }

  if (inner.headers !== undefined) {
    const headers = inner.headers;
    if (!headers || typeof headers !== "object" || Array.isArray(headers)) {
      throw RsError.badRequest(`$response.headers must be an object, got ${JSON.stringify(headers)}`);
    }
    for (const [name, value] of Object.entries(headers)) {
      let text: string;
      if (typeof value === "string") text = value;
      else if (typeof value === "number" || typeof value === "boolean") text = String(value);
      else throw RsError.badRequest(`$response.headers.${name} must be a scalar, got ${JSON.stringify(value)}`);
      if (!isValidHeaderName(name)) throw RsError.badRequest(`$response: invalid header name '${name}'`);
      try {
        msg.headers.set(name, text);
      } catch {
        throw RsError.badRequest(`$response: invalid value for header '${name}'`);
      }
    }
  }

  msg.status = status;
}
