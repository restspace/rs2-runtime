// Cloudflare Email Service as a `MessageGateway` — the `builtin:cf-email`
// provider adapter for the `email` channel. Port of
// `rs2-core/src/adapters/cf_email.rs`.
//
// Uses the REST API rather than the Workers `send_email` binding. The binding
// needs no token, but its permitted senders are fixed in `wrangler.jsonc` at
// deploy time (wrong for per-tenant sending domains) and it does not exist off
// Workers — one REST implementation serves both hosts and takes its bearer
// token from operator infra, so the same mount config works on either.
//
// Two provider facts shape what this can promise:
//
// - The REST send answers synchronously with per-recipient delivery
//   (`delivered` / `queued` / `permanent_bounces`) and mints no message id. So
//   the receipt carries `detail` and no `id`, and `deliveryStatus()` is false —
//   there is nothing to look up afterwards, not because the provider is
//   deficient but because it already told us.
// - Sending is Workers-Paid-only and quota'd; a provider rejection keeps its
//   own message rather than being flattened to a generic 502.

import { Body } from "../runtime/body";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { Message } from "../runtime/message";
import { CredentialInjector } from "./credential";
import type { Addr, Channel, MessageGateway, Outbound, Receipt } from "./message";
import { rfc5322 } from "./message";
import type { HttpOut } from "./types";

const SEND_PATH = "/email/sending/send";
/// Provider responses are small JSON documents; this bounds a hostile one.
const MAX_RESPONSE_BYTES = 256 * 1024;

export class CfEmailGateway implements MessageGateway {
  private constructor(
    private readonly http: HttpOut,
    private readonly injector: CredentialInjector,
    private readonly accountId: string,
    private readonly defaultFrom: Addr | undefined,
    private readonly apiBase: string,
  ) {}

  /// Build from an (already infra-expanded) `store` block:
  ///
  ///   { "adapter": "builtin:cf-email", "accountId": "…",
  ///     "from": "noreply@example.com", "fromName": "Example",
  ///     "auth": "bearer", "token": "<from infra/secret>" }
  static fromConfig(config: Json, http: HttpOut): CfEmailGateway {
    const cfg: JsonObject = config && typeof config === "object" && !Array.isArray(config) ? config : {};
    const accountId = cfg.accountId;
    if (typeof accountId !== "string") {
      throw RsError.badRequest(
        "message adapter 'builtin:cf-email' requires 'accountId' (the Cloudflare account the sending domain belongs to)",
      );
    }
    const injector = CredentialInjector.fromConfig(cfg);
    if (!injector) {
      throw RsError.badRequest(
        "message adapter 'builtin:cf-email' requires an 'auth' credential — supply it through 'infra:<name>' so the token never lands in tenant config",
      );
    }
    let from: Addr | undefined;
    if (typeof cfg.from === "string") {
      from = typeof cfg.fromName === "string" ? { email: cfg.from, name: cfg.fromName } : { email: cfg.from };
    }
    // Overridable only so tests can point at a local stub; operators never set
    // it, and the default is the real API.
    const apiBase = (typeof cfg.apiBase === "string" ? cfg.apiBase : "https://api.cloudflare.com/client/v4").replace(
      /\/+$/,
      "",
    );
    return new CfEmailGateway(http, injector, accountId, from, apiBase);
  }

  /// The provider's JSON body for one email.
  private payload(out: Outbound): JsonObject {
    if (out.channel !== "email") {
      // The service and the routing gateway both check first; this is the
      // belt-and-braces case for a directly constructed gateway.
      throw RsError.badRequest("the cf-email adapter serves the 'email' channel only");
    }
    const sender = out.from ?? this.defaultFrom;
    if (!sender) {
      throw RsError.badRequest(
        "no sender: give the message a 'from', or configure the adapter with a default 'from' on a verified sending domain",
      );
    }
    const body: JsonObject = { from: rfc5322(sender), to: out.to.map(rfc5322) };
    if (out.cc.length > 0) body.cc = out.cc.map(rfc5322);
    if (out.bcc.length > 0) body.bcc = out.bcc.map(rfc5322);
    if (out.replyTo) body.replyTo = rfc5322(out.replyTo);
    body.subject = out.subject;
    if (out.text !== undefined) body.text = out.text;
    if (out.html !== undefined) body.html = out.html;
    if (out.attachments.length > 0) {
      body.attachments = out.attachments.map((a) => {
        const m: JsonObject = { filename: a.filename, contentType: a.contentType, content: a.content };
        if (a.contentId !== undefined) m.contentId = a.contentId;
        return m;
      });
    }
    if (Object.keys(out.headers).length > 0) body.headers = { ...out.headers };
    return body;
  }

  async send(tenant: string, out: Outbound): Promise<Receipt> {
    const payload = this.payload(out);
    const url = `${this.apiBase}/accounts/${this.accountId}${SEND_PATH}`;
    const req = Message.request("POST", url, tenant);
    req.body = Body.fromJson(payload);
    await this.injector.apply(req, MAX_RESPONSE_BYTES);

    const resp = await this.http.request(req);
    const status = resp.status ?? 0;
    let doc: Json = null;
    if (resp.body) {
      try {
        doc = await resp.body.asJson(MAX_RESPONSE_BYTES);
      } catch {
        doc = null;
      }
    }
    const success = doc && typeof doc === "object" && !Array.isArray(doc) ? doc.success : undefined;
    if (status < 200 || status >= 300 || success === false) throw providerError(status, doc);

    const result = doc && typeof doc === "object" && !Array.isArray(doc) ? doc.result : undefined;
    const receipt: Receipt = { channel: "email", provider: "cf-email" };
    // The REST send mints no id (see the module note); if a future response
    // carries one, pass it through rather than discard it.
    if (result && typeof result === "object" && !Array.isArray(result)) {
      const id = result.message_id ?? result.messageId;
      if (typeof id === "string") receipt.id = id;
      receipt.detail = result;
    }
    return receipt;
  }

  async status(_tenant: string, _id: string): Promise<Json> {
    throw RsError.providerUnavailable("cf-email reports delivery in the send response, not by later lookup");
  }

  channels(): Channel[] {
    return ["email"];
  }

  deliveryStatus(): boolean {
    return false;
  }

  provider(): string {
    return "cf-email";
  }
}

/// Keep the provider's own words: a quota rejection and a bad sending domain
/// are different operator problems, and flattening both to "502" hides which.
function providerError(status: number, doc: Json): RsError {
  let detail = "";
  if (doc && typeof doc === "object" && !Array.isArray(doc) && Array.isArray(doc.errors)) {
    detail = doc.errors
      .map((e) => (e && typeof e === "object" && !Array.isArray(e) && typeof e.message === "string" ? e.message : ""))
      .filter((m) => m !== "")
      .join("; ");
  }
  if (detail === "") detail = `provider returned ${status}`;
  const err =
    status >= 400 && status < 500
      ? RsError.badRequest(`cf-email rejected the message: ${detail}`)
      : RsError.providerUnavailable(`cf-email send failed: ${detail}`);
  err.extra = { provider: "cf-email", status };
  return err;
}
