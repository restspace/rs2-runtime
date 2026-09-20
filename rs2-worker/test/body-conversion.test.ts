// Media-type-directed body conversion, the Worker half of the Rust host's
// `rs2-core/tests/body_conversion.rs`. Restspace v1 converted an incoming
// body for a transform by media type (`MessageBody.asJson`): JSON parsed,
// text became a string, binary became base64. Both hosts must agree, so
// these assertions mirror the Rust ones case for case.
import { describe, expect, it } from "vitest";

import { Executor, defaultPipelineLimits } from "../src/pipeline/executor";
import { specFromJson } from "../src/pipeline/spec";
import type { Requester } from "../src/services/context";
import { Body } from "../src/runtime/body";
import { MediaType } from "../src/runtime/media-type";
import { Message } from "../src/runtime/message";
import { defaultRetryPolicy } from "../src/runtime/retry";

const CAP = 10 * 1024 * 1024;

const notCalled: Requester = {
  request(msg: Message): Promise<Message> {
    throw new Error(`unexpected call to ${msg.url.path}`);
  },
};

/// Serves back a fixed body, so a `call | transform` pipeline sees a
/// non-JSON upstream response.
function serves(mediaType: string, bytes: Uint8Array): Requester {
  return {
    request(msg: Message): Promise<Message> {
      return Promise.resolve(msg.response(200, Body.fromBytes(bytes, new MediaType(mediaType))));
    },
  };
}

function executor(requester: Requester): Executor {
  return new Executor(requester, defaultPipelineLimits(), defaultRetryPolicy());
}

function input(mediaType: string, bytes: Uint8Array): Message {
  const msg = Message.request("POST", "/run", "demo");
  msg.body = Body.fromBytes(bytes, new MediaType(mediaType));
  return msg;
}

const enc = (s: string) => new TextEncoder().encode(s);

async function outJson(msg: Message) {
  if (!msg.body) throw new Error("no body");
  return await msg.body.asJson(CAP);
}

describe("media-type-directed body conversion", () => {
  it("gives a transform over a text body the string, not a 400", async () => {
    const spec = specFromJson({ steps: [{ transform: { page: "$", len: "$length($)" } }] });
    const out = await executor(notCalled).run(spec, input("text/html", enc("<p>hi</p>")), undefined);
    expect(out.status).toBe(200);
    const v = await outJson(out);
    expect(v).toMatchObject({ page: "<p>hi</p>", len: 9 });
  });

  it("lets a transform parse a CSV body", async () => {
    const spec = specFromJson({ steps: [{ transform: { rows: "$count($split($, '\n'))" } }] });
    const out = await executor(notCalled).run(spec, input("text/csv", enc("a,b\n1,2\n3,4")), undefined);
    expect((await outJson(out)) as { rows: number }).toMatchObject({ rows: 3 });
  });

  it("passes a binary body through as lossless base64", async () => {
    const png = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0xff, 0xd8]);
    const spec = specFromJson({ steps: [{ transform: { b64: "$" } }] });
    const out = await executor(notCalled).run(spec, input("image/png", png), undefined);
    const { b64 } = (await outJson(out)) as { b64: string };
    // Byte-for-byte, and the exact encoding the Rust host produces.
    expect(b64).toBe("iVBORw0KGgr/2A==");
    expect(Uint8Array.from(atob(b64), (c) => c.charCodeAt(0))).toEqual(png);
  });

  it("still parses a JSON body to a value", async () => {
    const spec = specFromJson({ steps: [{ transform: { doubled: "a * 2" } }] });
    const out = await executor(notCalled).run(spec, input("application/json", enc('{"a":21}')), undefined);
    expect((await outJson(out)) as { doubled: number }).toMatchObject({ doubled: 42 });
  });

  it("still rejects a body that lies about being JSON", async () => {
    const spec = specFromJson({ steps: [{ transform: { x: "$" } }] });
    await expect(
      executor(notCalled).run(spec, input("application/json", enc("not json")), undefined),
    ).rejects.toMatchObject({ status: 400 });
  });

  it("captures a text response as a string instead of null", async () => {
    const spec = specFromJson({
      steps: [
        { call: { method: "GET", url: "/page" }, as: "$page" },
        { transform: { captured: "$page" } },
      ],
    });
    const out = await executor(serves("text/html", enc("<h1>x</h1>"))).run(
      spec,
      input("application/json", enc("{}")),
      undefined,
    );
    expect((await outJson(out)) as { captured: string }).toMatchObject({ captured: "<h1>x</h1>" });
  });
});

describe("$_rawBody", () => {
  it("is byte-faithful for a non-UTF-8 payload", async () => {
    // A lossy decode here would silently break HMAC verification over a
    // binary webhook payload.
    const bytes = new Uint8Array([0x7b, 0xff, 0x7d]);
    const spec = specFromJson({ steps: [{ transform: { raw: "$_rawBody" } }] });
    const out = await executor(notCalled).run(spec, input("application/octet-stream", bytes), undefined);
    const { raw } = (await outJson(out)) as { raw: string };
    expect(Uint8Array.from(atob(raw), (c) => c.charCodeAt(0))).toEqual(bytes);
  });

  it("keeps a UTF-8 payload verbatim", async () => {
    const spec = specFromJson({ steps: [{ transform: { raw: "$_rawBody" } }] });
    const out = await executor(notCalled).run(spec, input("application/json", enc('{"id":"evt_1"}')), undefined);
    expect((await outJson(out)) as { raw: string }).toMatchObject({ raw: '{"id":"evt_1"}' });
  });
});
