// AWS SNS as a `MessageGateway` — the `builtin:aws-sns` provider adapter for
// the `sms` channel. Port of `rs2-core/src/adapters/aws_sns.rs`.
//
// SNS is the mirror image of `cf-email`, which is why the two were built
// together: it mints a message id but reports no delivery status (that needs
// separate CloudWatch delivery logging, which is not a per-message lookup). So
// `deliveryStatus()` is false here for the opposite reason — nothing to ask,
// rather than nothing left to ask.
//
// Auth is the `awsSigV4` strategy already implemented and vector-tested in
// `credential.ts`; this adapter adds no cryptography. The Query API is
// form-encoded in and XML out, so the response is scanned for `<MessageId>`
// rather than parsed — one field of one shape, and pulling in an XML parser for
// it would be the larger risk.

import { Body } from "../runtime/body";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { MediaType } from "../runtime/media-type";
import { Message } from "../runtime/message";
import { CredentialInjector } from "./credential";
import type { Channel, MessageGateway, Outbound, Receipt } from "./message";
import type { HttpOut } from "./types";

/// SNS Query API version, fixed by the service.
const API_VERSION = "2010-03-31";
const MAX_RESPONSE_BYTES = 256 * 1024;

/// `application/x-www-form-urlencoded` with AWS's unreserved set left alone.
/// SigV4 signs the hash of this body, so the encoding must be stable and match
/// what the service canonicalizes. `encodeURIComponent` leaves `!'()*` alone,
/// which AWS does percent-encode, so those are finished by hand.
function formEncode(s: string): string {
  return encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

export class AwsSnsGateway implements MessageGateway {
  private constructor(
    private readonly http: HttpOut,
    private readonly injector: CredentialInjector,
    private readonly endpoint: string,
    private readonly senderId: string | undefined,
    private readonly smsType: string,
  ) {}

  /// Build from an (already infra-expanded) `store` block:
  ///
  ///   { "adapter": "builtin:aws-sns", "region": "eu-west-1",
  ///     "senderId": "Example", "smsType": "Transactional",
  ///     "auth": "awsSigV4", "accessKeyId": "…", "secretAccessKey": "…" }
  ///
  /// `service` defaults to `sns` and `region` is shared with the signer, so an
  /// operator writes the region once.
  static fromConfig(config: Json, http: HttpOut): AwsSnsGateway {
    const cfg: JsonObject = config && typeof config === "object" && !Array.isArray(config) ? config : {};
    const region = cfg.region;
    if (typeof region !== "string") {
      throw RsError.badRequest("message adapter 'builtin:aws-sns' requires 'region' (e.g. 'eu-west-1')");
    }
    // Fill in the signer fields this adapter already knows, so the operator
    // does not repeat them and cannot get them inconsistent.
    const signing: JsonObject = { ...cfg, auth: typeof cfg.auth === "string" ? cfg.auth : "awsSigV4", service: "sns", region };
    const injector = CredentialInjector.fromConfig(signing);
    if (!injector) {
      throw RsError.badRequest(
        "message adapter 'builtin:aws-sns' requires AWS credentials ('accessKeyId' and 'secretAccessKey') — supply them through 'infra:<name>' so they never land in tenant config",
      );
    }
    const endpoint =
      typeof cfg.endpoint === "string"
        ? cfg.endpoint.replace(/\/+$/, "")
        : `https://sns.${region}.amazonaws.com`;
    return new AwsSnsGateway(
      http,
      injector,
      endpoint,
      typeof cfg.senderId === "string" ? cfg.senderId : undefined,
      typeof cfg.smsType === "string" ? cfg.smsType : "Transactional",
    );
  }

  formBody(out: Outbound): string {
    if (out.channel !== "sms") throw RsError.badRequest("the aws-sns adapter serves the 'sms' channel only");
    const pairs: Array<[string, string]> = [
      ["Action", "Publish"],
      ["Version", API_VERSION],
      ["PhoneNumber", out.to],
      ["Message", out.text],
    ];
    // Message attributes are positional in the Query API; the per-message
    // `from` wins over the adapter default.
    const sender = out.from ?? this.senderId;
    let n = 0;
    if (sender !== undefined) attribute(pairs, ++n, "AWS.SNS.SMS.SenderID", sender);
    attribute(pairs, ++n, "AWS.SNS.SMS.SMSType", this.smsType);
    return pairs.map(([k, v]) => `${formEncode(k)}=${formEncode(v)}`).join("&");
  }

  async send(tenant: string, out: Outbound): Promise<Receipt> {
    const req = Message.request("POST", this.endpoint, tenant);
    req.body = Body.fromString(this.formBody(out), new MediaType("application/x-www-form-urlencoded"));
    await this.injector.apply(req, MAX_RESPONSE_BYTES);

    const resp = await this.http.request(req);
    const status = resp.status ?? 0;
    const text = resp.body ? new TextDecoder().decode(await resp.body.materialize(MAX_RESPONSE_BYTES)) : "";
    if (status < 200 || status >= 300) throw providerError(status, text);
    const id = xmlField(text, "MessageId");
    if (id === undefined) {
      throw RsError.contractViolation("aws-sns accepted the message but returned no MessageId");
    }
    return { id, channel: "sms", provider: "aws-sns" };
  }

  async status(_tenant: string, _id: string): Promise<Json> {
    throw RsError.providerUnavailable(
      "aws-sns has no per-message status API — enable SMS delivery-status logging and read it from CloudWatch",
    );
  }

  channels(): Channel[] {
    return ["sms"];
  }

  deliveryStatus(): boolean {
    return false;
  }

  provider(): string {
    return "aws-sns";
  }
}

function attribute(pairs: Array<[string, string]>, n: number, name: string, value: string): void {
  pairs.push([`MessageAttributes.entry.${n}.Name`, name]);
  pairs.push([`MessageAttributes.entry.${n}.Value.DataType`, "String"]);
  pairs.push([`MessageAttributes.entry.${n}.Value.StringValue`, value]);
}

/// Pull one element's text out of the Query API's XML response. Deliberately
/// not a parser: the documents we read have exactly one interesting field.
export function xmlField(body: string, tag: string): string | undefined {
  const open = `<${tag}>`;
  const close = `</${tag}>`;
  const start = body.indexOf(open);
  if (start < 0) return undefined;
  const from = start + open.length;
  const end = body.indexOf(close, from);
  if (end < 0) return undefined;
  return body.slice(from, end).trim();
}

/// SNS puts its reason in `<Code>` / `<Message>`; keep both, because
/// `InvalidParameter` on a phone number and `Throttling` are different operator
/// problems.
export function providerError(status: number, body: string): RsError {
  const code = xmlField(body, "Code") ?? "";
  const message = xmlField(body, "Message") ?? "";
  const detail = message === "" ? `provider returned ${status}` : code === "" ? message : `${code}: ${message}`;
  const err =
    status >= 400 && status < 500
      ? RsError.badRequest(`aws-sns rejected the message: ${detail}`)
      : RsError.providerUnavailable(`aws-sns send failed: ${detail}`);
  err.extra = { provider: "aws-sns", status };
  return err;
}
