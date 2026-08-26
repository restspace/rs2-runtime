// Outbound HTTP adapter over the platform `fetch`. Port of
// `rs2-core/src/adapters/http_out.rs`: a 30 s timeout, non-2xx is a
// response not an error, transport failure → 502 `internal` retryable,
// request/response caps 32 MiB.

import { Body } from "../runtime/body";
import { RsError, codes } from "../runtime/error";
import { MediaType } from "../runtime/media-type";
import type { Message } from "../runtime/message";
import type { HttpOut } from "./types";

const BODY_CAP = 32 * 1024 * 1024;

export class FetchHttpOut implements HttpOut {
  async request(msg: Message): Promise<Message> {
    const url = msg.url.query === "" ? msg.url.path : `${msg.url.path}?${msg.url.query}`;
    if (!(url.startsWith("http://") || url.startsWith("https://"))) {
      throw RsError.badRequest(`not an absolute http(s) URL: '${url}'`);
    }
    const headers = new Headers();
    msg.headers.forEach((v, k) => headers.append(k, v));
    const bodyBytes = msg.body ? await msg.body.materialize(BODY_CAP) : undefined;
    const template = msg.response(200, undefined);
    let upstream: Response;
    try {
      upstream = await fetch(url, {
        method: msg.method,
        headers,
        body: bodyBytes && bodyBytes.byteLength > 0 ? bodyBytes : undefined,
        signal: AbortSignal.timeout(30_000),
        redirect: "manual",
      });
    } catch (e) {
      const err = new RsError(502, codes.INTERNAL, "Upstream Error", e instanceof Error ? e.message : String(e));
      err.retryable = true;
      throw err;
    }
    const bytes = new Uint8Array(await upstream.arrayBuffer());
    if (bytes.byteLength > BODY_CAP) {
      throw RsError.internal("upstream body read failed: response exceeds the 32 MiB cap");
    }
    const mediaType = upstream.headers.get("content-type") ?? "";
    const body = bytes.byteLength === 0 ? undefined : Body.fromBytes(bytes, MediaType.parse(mediaType));
    const resp = template.response(upstream.status, body);
    upstream.headers.forEach((v, k) => resp.setHeader(k, v));
    return resp;
  }
}
