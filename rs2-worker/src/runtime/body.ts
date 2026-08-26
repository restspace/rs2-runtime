// Body: stream or bytes, always media-typed, carrying provenance (PRD
// §6.2–6.3). Port of `rs2-core/src/message/body.rs` over `ReadableStream`
// and `Uint8Array`.

import { RsError } from "./error";
import { MediaType } from "./media-type";
import type { Json } from "./error";

/// How a body could be reproduced (metadata only in v1).
export type Provenance =
  | { kind: "materialized" }
  | { kind: "replayable"; url: string; version: string }
  | { kind: "ephemeral" };

export const MATERIALIZED: Provenance = { kind: "materialized" };
export const EPHEMERAL: Provenance = { kind: "ephemeral" };

export type Payload =
  | { kind: "bytes"; bytes: Uint8Array }
  | { kind: "stream"; stream: ReadableStream<Uint8Array> };

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export class Body {
  payload: Payload;
  mediaType: MediaType;
  size: number | undefined;
  lastModified: Date | undefined;
  provenance: Provenance;

  private constructor(payload: Payload, mediaType: MediaType, size: number | undefined, provenance: Provenance) {
    this.payload = payload;
    this.mediaType = mediaType;
    this.size = size;
    this.lastModified = undefined;
    this.provenance = provenance;
  }

  static fromBytes(bytes: Uint8Array, mediaType: MediaType): Body {
    return new Body({ kind: "bytes", bytes }, mediaType, bytes.byteLength, MATERIALIZED);
  }

  static fromString(text: string, mediaType: MediaType): Body {
    return Body.fromBytes(encoder.encode(text), mediaType);
  }

  static fromJson(value: Json): Body {
    return Body.fromBytes(encoder.encode(JSON.stringify(value)), MediaType.json());
  }

  static fromStream(
    stream: ReadableStream<Uint8Array>,
    mediaType: MediaType,
    size: number | undefined,
    provenance: Provenance,
  ): Body {
    return new Body({ kind: "stream", stream }, mediaType, size, provenance);
  }

  withLastModified(when: Date): Body {
    this.lastModified = when;
    return this;
  }

  withSchema(schemaUrl: string): Body {
    this.mediaType = this.mediaType.withSchema(schemaUrl);
    return this;
  }

  isStream(): boolean {
    return this.payload.kind === "stream";
  }

  /// Materialize the payload into bytes, enforcing the host size limit
  /// (PRD §9.3): rejects early on the declared size and while reading.
  async materialize(maxBytes: number): Promise<Uint8Array> {
    if (this.payload.kind === "stream") {
      if (this.size !== undefined && this.size > maxBytes) {
        throw RsError.limitExceeded("materialized_body_bytes", this.size, maxBytes);
      }
      const chunks: Uint8Array[] = [];
      let total = 0;
      const reader = this.payload.stream.getReader();
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          if (!value) continue;
          if (total + value.byteLength > maxBytes) {
            throw RsError.limitExceeded("materialized_body_bytes", total + value.byteLength, maxBytes);
          }
          chunks.push(value);
          total += value.byteLength;
        }
      } catch (e) {
        // Abandon the rest of the stream so the producer is released.
        await reader.cancel().catch(() => undefined);
        if (e instanceof RsError) throw e;
        throw RsError.internal(`body stream error: ${e instanceof Error ? e.message : String(e)}`);
      }
      const buf = new Uint8Array(total);
      let off = 0;
      for (const c of chunks) {
        buf.set(c, off);
        off += c.byteLength;
      }
      this.size = total;
      this.payload = { kind: "bytes", bytes: buf };
      this.provenance = MATERIALIZED;
    }
    const bytes = this.payload.bytes;
    // The cap applies uniformly: bytes already in memory still may not cross
    // an engine boundary above the host limit.
    if (bytes.byteLength > maxBytes) {
      throw RsError.limitExceeded("materialized_body_bytes", bytes.byteLength, maxBytes);
    }
    return bytes;
  }

  /// Capture the payload as bytes **without consuming the body** when it
  /// does not fit. Under `maxBytes` this is `materialize` (the body ends up
  /// materialized and the bytes are returned); over it the body is left
  /// readable — a fresh stream of the buffered prefix followed by the
  /// untouched remainder — and `undefined` comes back. Idempotency capture
  /// needs exactly this: a response too large to record must still reach
  /// the client (issue #2 item 4).
  async capture(maxBytes: number): Promise<Uint8Array | undefined> {
    if (this.payload.kind === "bytes") {
      return this.payload.bytes.byteLength <= maxBytes ? this.payload.bytes : undefined;
    }
    if (this.size !== undefined && this.size > maxBytes) return undefined;
    const reader = this.payload.stream.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    let overflow = false;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        if (!value || value.byteLength === 0) continue;
        chunks.push(value);
        total += value.byteLength;
        if (total > maxBytes) {
          overflow = true;
          break;
        }
      }
    } catch (e) {
      await reader.cancel().catch(() => undefined);
      if (e instanceof RsError) throw e;
      throw RsError.internal(`body stream error: ${e instanceof Error ? e.message : String(e)}`);
    }
    if (overflow) {
      // Put the read prefix back in front of the rest of the stream. The
      // body stays exactly as long as it was; only the recording is lost.
      this.payload = { kind: "stream", stream: resumeStream(chunks, reader) };
      return undefined;
    }
    const buf = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) {
      buf.set(c, off);
      off += c.byteLength;
    }
    this.size = total;
    this.payload = { kind: "bytes", bytes: buf };
    this.provenance = MATERIALIZED;
    return buf;
  }

  /// Materialize and parse as JSON (only for JSON-family media types).
  async asJson(maxBytes: number): Promise<Json> {
    if (!this.mediaType.isJson()) {
      throw RsError.badRequest(`expected a JSON body, got '${this.mediaType.essence()}'`);
    }
    let bytes = await this.materialize(maxBytes);
    // Strip a UTF-8 BOM, which may appear on bodies read from files.
    if (bytes.byteLength >= 3 && bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf) {
      bytes = bytes.subarray(3);
    }
    try {
      return JSON.parse(decoder.decode(bytes)) as Json;
    } catch (e) {
      throw RsError.badRequest(`invalid JSON body: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  /// Consume the body as a stream regardless of representation.
  intoStream(): ReadableStream<Uint8Array> {
    if (this.payload.kind === "stream") return this.payload.stream;
    const bytes = this.payload.bytes;
    return new ReadableStream<Uint8Array>({
      start(controller) {
        if (bytes.byteLength > 0) controller.enqueue(bytes);
        controller.close();
      },
    });
  }
}

/// A stream of `chunks` already pulled from `reader`, then whatever
/// `reader` has left. Cancelling it cancels the underlying stream.
function resumeStream(
  chunks: Uint8Array[],
  reader: ReadableStreamDefaultReader<Uint8Array>,
): ReadableStream<Uint8Array> {
  let i = 0;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (i < chunks.length) {
        controller.enqueue(chunks[i++]!);
        return;
      }
      const { done, value } = await reader.read();
      if (done) {
        controller.close();
        return;
      }
      if (value) controller.enqueue(value);
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  });
}

export function utf8Decode(bytes: Uint8Array): string {
  return decoder.decode(bytes);
}

export function utf8Encode(text: string): Uint8Array {
  return encoder.encode(text);
}
