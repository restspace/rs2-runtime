// `Body.capture` and the idempotency capture built on it (issue #2 item 4):
// a response too large to record must still reach the client intact.
import { describe, expect, it } from "vitest";

import { Body, EPHEMERAL, utf8Decode } from "../src/runtime/body";
import { captureResponse } from "../src/runtime/idempotency";
import { MediaType } from "../src/runtime/media-type";
import { Message } from "../src/runtime/message";

function chunked(chunks: string[], size?: number): Body {
  const enc = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const c of chunks) controller.enqueue(enc.encode(c));
      controller.close();
    },
  });
  return Body.fromStream(stream, new MediaType("text/plain"), size, EPHEMERAL);
}

async function drain(body: Body): Promise<string> {
  const reader = body.intoStream().getReader();
  let out = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) out += utf8Decode(value);
  }
  return out;
}

describe("Body.capture", () => {
  it("materializes a stream that fits and returns its bytes", async () => {
    const body = chunked(["abc", "def"]);
    const bytes = await body.capture(1024);
    expect(bytes && utf8Decode(bytes)).toBe("abcdef");
    expect(body.isStream()).toBe(false);
    expect(await drain(body)).toBe("abcdef");
  });

  it("leaves an over-cap stream readable end to end", async () => {
    const body = chunked(["aaaa", "bbbb", "cccc"]);
    expect(await body.capture(6)).toBeUndefined();
    // The prefix already pulled is replayed in front of the remainder.
    expect(await drain(body)).toBe("aaaabbbbcccc");
  });

  it("declines a declared size over the cap without touching the stream", async () => {
    const body = chunked(["hello"], 5000);
    expect(await body.capture(10)).toBeUndefined();
    expect(await drain(body)).toBe("hello");
  });

  it("returns in-memory bytes under the cap and declines them over it", async () => {
    const small = Body.fromString("hi", new MediaType("text/plain"));
    expect(await small.capture(16)).toBeDefined();
    const big = Body.fromString("hello world", new MediaType("text/plain"));
    expect(await big.capture(4)).toBeUndefined();
    expect(await drain(big)).toBe("hello world");
  });
});

describe("captureResponse", () => {
  const template = Message.request("POST", "/x", "t");

  it("records a small body", async () => {
    const resp = template.response(201, Body.fromString("done", new MediaType("text/plain")));
    const [out, stored] = await captureResponse(resp, 1024);
    expect(stored?.status).toBe(201);
    expect(stored?.body && utf8Decode(stored.body[0])).toBe("done");
    expect(await drain(out.body!)).toBe("done");
  });

  it("skips the record but hands back a live body over the cap (issue #2 item 4)", async () => {
    const resp = template.response(200, chunked(["1234", "5678"]));
    const [out, stored] = await captureResponse(resp, 5);
    expect(stored, "too large to replay").toBeUndefined();
    expect(await drain(out.body!), "the client still gets every byte").toBe("12345678");
  });
});
