// `message` service: the canonical HTTP surface over the message gateway
// capability (PRD §9.2). Port of `rs2-core/src/services/message.rs`. One
// endpoint for every delivery channel; the provider is swappable per mount via
// `store.adapter` (one channel) or `store.adapters` (a channel -> adapter map),
// so a tenant mails through one provider and texts through another at the same
// mount.
//
// - `POST /<mount>/send` {channel, to, …} -> `201 {id?, channel, provider}`
// - `GET  /<mount>/status/<id>` -> `200` provider-shaped, or `501` when the
//   configured provider does not report delivery status at all
// - `GET  /<mount>/channels` -> what this mount can actually do
//
// Sending is a non-idempotent, externally visible effect: a retried POST must
// not send twice, which the host's idempotency layer handles from the mount's
// declared effect class. The service stays a pure function on the message.

import type { ScopedMessageGateway } from "../capabilities/scoped";
import { outboundFromJson, receiptJson } from "../capabilities/message";
import { Body } from "../runtime/body";
import { RsError } from "../runtime/error";
import type { Message } from "../runtime/message";
import type { Service, ServiceContext } from "./context";

export class MessageService implements Service {
  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const gateway: ScopedMessageGateway | undefined = ctx.messaging;
    if (!gateway) throw RsError.capabilityDenied("message");

    const parts = msg.url.serviceSegments();

    if (msg.method === "POST" && parts.length === 1 && parts[0] === "send") {
      if (!msg.body) throw RsError.badRequest("POST /send requires a JSON body {channel, to, …}");
      const payload = await msg.body.asJson(ctx.limits.materializedBodyBytes);
      const out = outboundFromJson(payload);

      // Refuse an unserved channel here rather than at the provider: the
      // mount's own configuration is the answer, and it is a 400 that names
      // what this mount can do.
      const served = gateway.channels();
      if (!served.includes(out.channel)) {
        throw RsError.badRequest(
          `this mount has no adapter for the '${out.channel}' channel (configured: ${
            served.length === 0 ? "none" : served.join(", ")
          })`,
        );
      }
      const receipt = await gateway.send(out);
      return msg.response(201, Body.fromJson(receiptJson(receipt)));
    }

    if (msg.method === "GET" && parts.length === 2 && parts[0] === "status") {
      if (!gateway.deliveryStatus()) {
        throw RsError.providerUnavailable(
          `provider '${gateway.provider()}' does not report per-message delivery status`,
        );
      }
      return msg.okJson(await gateway.status(parts[1]!));
    }

    if (msg.method === "GET" && parts.length === 1 && parts[0] === "channels") {
      return msg.okJson({
        channels: gateway.channels(),
        deliveryStatus: gateway.deliveryStatus(),
        provider: gateway.provider(),
      });
    }

    throw RsError.badRequest(
      "message endpoint: POST /send {channel, to, …}, GET /status/{id}, GET /channels",
    );
  }
}
