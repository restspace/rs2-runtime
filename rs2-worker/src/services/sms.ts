// `sms` service: the canonical HTTP surface over the SMS gateway capability
// (PRD §9.2). Port of `rs2-core/src/services/sms.rs`. The provider is
// swappable per mount via `store.adapter`; on this host every adapter form
// is refused at build until P4b (cloudflare.md §C.1), so the routes below
// only run once resident adapters arrive — they are ported now so the
// service surface matches Rust byte for byte.
//
// - `POST /<mount>/send` with `{ "to": "+1…", "body": "…" }` → `201 { "id": … }`
// - `GET  /<mount>/status/<id>` → `200` provider-shaped delivery status.

import { Body } from "../runtime/body";
import { RsError } from "../runtime/error";
import type { Message } from "../runtime/message";
import type { Service, ServiceContext } from "./context";

export class SmsService implements Service {
  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const sms = ctx.sms;
    if (!sms) throw RsError.capabilityDenied("sms");

    const parts = msg.url.serviceSegments();

    if (msg.method === "POST" && parts.length === 1 && parts[0] === "send") {
      if (!msg.body) throw RsError.badRequest("POST /send requires a JSON body {to, body}");
      const payload = await msg.body.asJson(ctx.limits.materializedBodyBytes);
      const obj = payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {};
      const to = obj.to;
      if (typeof to !== "string") throw RsError.badRequest("'to' (string) is required");
      const text = obj.body;
      if (typeof text !== "string") throw RsError.badRequest("'body' (string) is required");
      const id = await sms.send(to, text);
      return msg.response(201, Body.fromJson({ id }));
    }
    if (msg.method === "GET" && parts.length === 2 && parts[0] === "status") {
      const status = await sms.status(parts[1]!);
      return msg.okJson(status);
    }
    throw RsError.badRequest("sms endpoint: POST /send {to, body}, GET /status/{id}");
  }
}
