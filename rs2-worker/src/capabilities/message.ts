// Outbound messaging: the typed provider capability (PRD §9.2) covering every
// delivery channel behind one interface. Port of
// `rs2-core/src/capabilities/message.rs` — the 400 wordings are Rust's, so the
// conformance suite reads one answer from both hosts.
//
// Two channels that do not agree shaped the interface:
//
// - Payload. Email has a subject, HTML and attachments; SMS has a string.
//   `Outbound` is a discriminated union rather than one bag of optionals, so
//   `subject` on an SMS is not representable. The wire form is one flat tagged
//   object (`{"channel": "email", …}`).
// - Delivery status is not universal (AWS SNS has none; Cloudflare answers at
//   send time and mints no id). `deliveryStatus()` declares it and `Receipt.id`
//   is optional — the same feature-detected-facet pattern as
//   `DataStore.listingPushdown`.

import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";

export type Channel = "email" | "sms";

/// Every channel this build knows, in the order they are advertised.
export const CHANNELS: readonly Channel[] = ["email", "sms"] as const;

export function parseChannel(s: string): Channel | undefined {
  return (CHANNELS as readonly string[]).includes(s) ? (s as Channel) : undefined;
}

/// An email participant. `"a@b.com"` or `{email, name}` on the wire.
export interface Addr {
  email: string;
  name?: string;
}

/// RFC 5322 display form: `Name <a@b.com>`, or the bare address.
export function rfc5322(a: Addr): string {
  return a.name ? `${a.name} <${a.email}>` : a.email;
}

export interface Attachment {
  filename: string;
  contentType: string;
  /// Base64-encoded contents.
  content: string;
  /// When set, the attachment is inline and referenced as `cid:<id>`.
  contentId?: string;
}

/// The combined to+cc+bcc cap: the lowest common ceiling across the providers
/// we target, so exceeding it is one 400 here rather than a different error
/// from each provider.
export const MAX_RECIPIENTS = 50;

export type Outbound =
  | {
      channel: "email";
      to: Addr[];
      cc: Addr[];
      bcc: Addr[];
      from?: Addr;
      replyTo?: Addr;
      subject: string;
      text?: string;
      html?: string;
      attachments: Attachment[];
      headers: Record<string, string>;
    }
  | { channel: "sms"; to: string; from?: string; text: string };

/// What the provider gives back for an accepted message. `id` is optional
/// because a message id is not universal: SNS mints one and has no status
/// endpoint; Cloudflare's REST send reports per-recipient delivery inline and
/// mints no id, so there is nothing left to look up. Rather than fake an id no
/// caller could use, the adapter leaves it absent and puts the provider's own
/// answer in `detail`.
export interface Receipt {
  id?: string;
  channel: Channel;
  provider: string;
  detail?: Json;
}

export function receiptJson(r: Receipt): JsonObject {
  const out: JsonObject = {};
  if (r.id !== undefined) out.id = r.id;
  out.channel = r.channel;
  out.provider = r.provider;
  if (r.detail !== undefined && r.detail !== null) out.detail = r.detail;
  return out;
}

/// Outbound messaging behind a swappable provider adapter. `tenant` is supplied
/// by the host scoping wrapper, never by service code.
export interface MessageGateway {
  send(tenant: string, out: Outbound): Promise<Receipt>;
  /// Only called when `deliveryStatus()` is true.
  status(tenant: string, id: string): Promise<Json>;
  /// Which channels this adapter serves; a send on any other is a 400 before
  /// the provider is called.
  channels(): Channel[];
  /// Whether the provider can answer `status` at all.
  deliveryStatus(): boolean;
  provider(): string;
}

// ---------------------------------------------------------------- parsing

function addrFromJson(v: Json, field: string): Addr {
  if (typeof v === "string") return checkedAddr(v, undefined, field);
  if (v && typeof v === "object" && !Array.isArray(v)) {
    const email = v.email;
    if (typeof email !== "string") throw RsError.badRequest(`'${field}' object requires 'email' (string)`);
    const name = typeof v.name === "string" && v.name !== "" ? v.name : undefined;
    return checkedAddr(email, name, field);
  }
  throw RsError.badRequest(`'${field}' must be an address string or {email, name} object`);
}

/// The shallow syntax check every provider would reject on anyway — done here
/// so it is one 400 at the edge, not a provider-shaped 502 later.
function checkedAddr(email: string, name: string | undefined, field: string): Addr {
  const at = email.indexOf("@");
  const ok = at > 0 && email.slice(at + 1).includes(".") && !/\s/.test(email);
  if (!ok) throw RsError.badRequest(`'${field}' is not an email address: '${email}'`);
  return name === undefined ? { email } : { email, name };
}

/// `to`/`cc`/`bcc`: a single address or an array of them.
function addrList(obj: JsonObject, field: string): Addr[] {
  const v = obj[field];
  if (v === undefined || v === null) return [];
  if (Array.isArray(v)) return v.map((one) => addrFromJson(one, field));
  return [addrFromJson(v, field)];
}

function optStr(obj: JsonObject, field: string): string | undefined {
  const v = obj[field];
  if (v === undefined || v === null) return undefined;
  if (typeof v !== "string") throw RsError.badRequest(`'${field}' must be a string`);
  return v;
}

function attachmentFromJson(v: Json): Attachment {
  if (!v || typeof v !== "object" || Array.isArray(v)) {
    throw RsError.badRequest("each attachment must be an object");
  }
  const req = (key: string): string => {
    const x = v[key];
    if (typeof x !== "string") throw RsError.badRequest(`attachment requires '${key}' (string)`);
    return x;
  };
  const out: Attachment = {
    filename: req("filename"),
    contentType: typeof v.contentType === "string" ? v.contentType : "application/octet-stream",
    content: req("content"),
  };
  if (typeof v.contentId === "string") out.contentId = v.contentId;
  return out;
}

/// Parse and validate a send body. Every failure is a 400 naming the offending
/// field — read by humans writing config and by agents reading the error.
export function outboundFromJson(v: Json): Outbound {
  if (!v || typeof v !== "object" || Array.isArray(v)) {
    throw RsError.badRequest("send body must be a JSON object {channel, to, …}");
  }
  const channel = v.channel;
  if (typeof channel !== "string") {
    throw RsError.badRequest("'channel' (string) is required: 'email' or 'sms'");
  }
  switch (parseChannel(channel)) {
    case "email":
      return emailFromJson(v);
    case "sms":
      return smsFromJson(v);
    default:
      throw RsError.badRequest(`unknown channel '${channel}' (one of: ${CHANNELS.join(", ")})`);
  }
}

function emailFromJson(obj: JsonObject): Outbound {
  const to = addrList(obj, "to");
  if (to.length === 0) throw RsError.badRequest("'to' must name at least one recipient");
  const cc = addrList(obj, "cc");
  const bcc = addrList(obj, "bcc");
  const total = to.length + cc.length + bcc.length;
  if (total > MAX_RECIPIENTS) {
    throw RsError.badRequest(`${total} recipients across to/cc/bcc exceeds the ${MAX_RECIPIENTS} limit`);
  }
  const subject = obj.subject;
  if (typeof subject !== "string") throw RsError.badRequest("'subject' (string) is required for email");
  const text = optStr(obj, "text");
  const html = optStr(obj, "html");
  if (text === undefined && html === undefined) {
    throw RsError.badRequest("an email needs 'text', 'html', or both");
  }
  let attachments: Attachment[] = [];
  const rawAttachments = obj.attachments;
  if (Array.isArray(rawAttachments)) attachments = rawAttachments.map(attachmentFromJson);
  else if (rawAttachments !== undefined && rawAttachments !== null) {
    throw RsError.badRequest("'attachments' must be an array");
  }
  const headers: Record<string, string> = {};
  const rawHeaders = obj.headers;
  if (rawHeaders && typeof rawHeaders === "object" && !Array.isArray(rawHeaders)) {
    for (const [k, val] of Object.entries(rawHeaders)) {
      if (typeof val !== "string") throw RsError.badRequest(`header '${k}' must be a string`);
      headers[k] = val;
    }
  } else if (rawHeaders !== undefined && rawHeaders !== null) {
    throw RsError.badRequest("'headers' must be an object");
  }
  const out: Outbound = { channel: "email", to, cc, bcc, subject, attachments, headers };
  if (obj.from !== undefined && obj.from !== null) out.from = addrFromJson(obj.from, "from");
  if (obj.replyTo !== undefined && obj.replyTo !== null) out.replyTo = addrFromJson(obj.replyTo, "replyTo");
  if (text !== undefined) out.text = text;
  if (html !== undefined) out.html = html;
  return out;
}

function smsFromJson(obj: JsonObject): Outbound {
  const to = obj.to;
  if (typeof to !== "string" || to === "") {
    throw RsError.badRequest("'to' (non-empty string) is required for sms");
  }
  const text = obj.text;
  if (typeof text !== "string" || text === "") {
    throw RsError.badRequest("'text' (non-empty string) is required for sms");
  }
  const out: Outbound = { channel: "sms", to, text };
  const from = optStr(obj, "from");
  if (from !== undefined) out.from = from;
  return out;
}

/// The wire form — the inverse of `outboundFromJson`. Guest (`code:`) adapters
/// receive exactly this, so a bundle and an HTTP caller speak one vocabulary.
export function outboundJson(out: Outbound): JsonObject {
  if (out.channel === "sms") {
    const o: JsonObject = { channel: "sms", to: out.to };
    if (out.from !== undefined) o.from = out.from;
    o.text = out.text;
    return o;
  }
  const addrJson = (a: Addr): Json => (a.name ? { email: a.email, name: a.name } : a.email);
  const o: JsonObject = { channel: "email", to: out.to.map(addrJson) };
  if (out.cc.length > 0) o.cc = out.cc.map(addrJson);
  if (out.bcc.length > 0) o.bcc = out.bcc.map(addrJson);
  if (out.from) o.from = addrJson(out.from);
  if (out.replyTo) o.replyTo = addrJson(out.replyTo);
  o.subject = out.subject;
  if (out.text !== undefined) o.text = out.text;
  if (out.html !== undefined) o.html = out.html;
  if (out.attachments.length > 0) {
    o.attachments = out.attachments.map((a) => {
      const m: JsonObject = { filename: a.filename, contentType: a.contentType, content: a.content };
      if (a.contentId !== undefined) m.contentId = a.contentId;
      return m;
    });
  }
  if (Object.keys(out.headers).length > 0) o.headers = { ...out.headers };
  return o;
}

/// Several single-channel adapters behind one mount: dispatch on the message's
/// channel. This is why the capability is worth unifying — a tenant mails
/// through one provider and texts through another at one endpoint, and that is
/// a config map, not a second service.
export class RoutingGateway implements MessageGateway {
  constructor(private readonly routes: Array<[Channel, MessageGateway]>) {}

  private route(channel: Channel): MessageGateway | undefined {
    return this.routes.find(([c]) => c === channel)?.[1];
  }

  async send(tenant: string, out: Outbound): Promise<Receipt> {
    const g = this.route(out.channel);
    if (!g) {
      throw RsError.badRequest(
        `no adapter is configured for the '${out.channel}' channel (configured: ${this.channels().join(", ")})`,
      );
    }
    return g.send(tenant, out);
  }

  /// The adapter that owns an id cannot be known from the id alone, so ask each
  /// route that reports status, in order, and take the first answer. Providers
  /// mint distinct id shapes, so a wrong-provider hit is a 404 there.
  async status(tenant: string, id: string): Promise<Json> {
    let last: unknown;
    for (const [, g] of this.routes) {
      if (!g.deliveryStatus()) continue;
      try {
        return await g.status(tenant, id);
      } catch (e) {
        last = e;
      }
    }
    if (last !== undefined) throw last;
    throw RsError.providerUnavailable("no configured adapter reports delivery status");
  }

  channels(): Channel[] {
    const seen = new Set<Channel>();
    for (const [c] of this.routes) seen.add(c);
    return CHANNELS.filter((c) => seen.has(c));
  }

  deliveryStatus(): boolean {
    return this.routes.some(([, g]) => g.deliveryStatus());
  }

  provider(): string {
    return "routing";
  }
}
