// Port of the `#[cfg(test)]` modules in `rs2-core/src/capabilities/message.rs`,
// `adapters/cf_email.rs` and `adapters/aws_sns.rs`. The 400 wordings are
// asserted verbatim: they are the cross-host contract the conformance suite
// reads from both hosts.
import { describe, expect, it } from "vitest";
import { AwsSnsGateway, providerError, xmlField } from "../src/capabilities/aws-sns";
import { CfEmailGateway } from "../src/capabilities/cf-email";
import { CHANNELS, MAX_RECIPIENTS, RoutingGateway, outboundFromJson, outboundJson } from "../src/capabilities/message";
import type { Channel, MessageGateway, Outbound, Receipt } from "../src/capabilities/message";
import { Body } from "../src/runtime/body";
import { RsError } from "../src/runtime/error";
import type { Json } from "../src/runtime/error";
import { Message } from "../src/runtime/message";
import type { HttpOut } from "../src/capabilities/types";

function caught(fn: () => unknown): RsError {
  try {
    fn();
  } catch (e) {
    return e as RsError;
  }
  throw new Error("expected a throw");
}

async function caughtAsync(p: Promise<unknown>): Promise<RsError> {
  return p.then(
    () => {
      throw new Error("expected a rejection");
    },
    (e: unknown) => e as RsError,
  );
}

describe("Outbound parsing", () => {
  it("an email round-trips through the wire form", () => {
    const out = outboundFromJson({
      channel: "email",
      to: [{ email: "a@b.com", name: "A" }, "c@d.com"],
      subject: "Hi",
      html: "<p>hi</p>",
      headers: { "X-Tag": "welcome" },
    });
    expect(out.channel).toBe("email");
    expect(outboundFromJson(outboundJson(out))).toEqual(out);
  });

  it("an email without a body is rejected", () => {
    const err = caught(() => outboundFromJson({ channel: "email", to: "a@b.com", subject: "Hi" }));
    expect(err.detail).toBe("an email needs 'text', 'html', or both");
  });

  it("recipients are capped across all three fields", () => {
    const to = Array.from({ length: 40 }, (_, i) => `u${i}@b.com`);
    const cc = Array.from({ length: 11 }, (_, i) => `c${i}@b.com`);
    const err = caught(() => outboundFromJson({ channel: "email", to, cc, subject: "Hi", text: "hi" }));
    expect(err.detail).toBe(`51 recipients across to/cc/bcc exceeds the ${MAX_RECIPIENTS} limit`);
  });

  it("sms takes a bare string and has nowhere to put a subject", () => {
    const out = outboundFromJson({ channel: "sms", to: "+447700900000", text: "hi", subject: "ignored" });
    expect(out).toEqual({ channel: "sms", to: "+447700900000", text: "hi" });
    expect(outboundJson(out).subject).toBeUndefined();
  });

  it("an unknown channel names the known ones", () => {
    const err = caught(() => outboundFromJson({ channel: "carrier-pigeon", to: "x" }));
    expect(err.detail).toBe(`unknown channel 'carrier-pigeon' (one of: ${CHANNELS.join(", ")})`);
  });

  it("a malformed address is a 400 at the edge", () => {
    const err = caught(() => outboundFromJson({ channel: "email", to: "not-an-address", subject: "s", text: "t" }));
    expect(err.detail).toBe("'to' is not an email address: 'not-an-address'");
  });
});

/// Captures the outbound request and replies with a canned response, so the
/// tests assert the exact wire shape without reaching a provider.
class StubHttp implements HttpOut {
  seen: { url: string; body: string } | undefined;
  constructor(
    private readonly status: number,
    private readonly reply: Json | string,
  ) {}

  async request(msg: Message): Promise<Message> {
    const bytes = msg.body ? await msg.body.materialize(1 << 20) : new Uint8Array();
    this.seen = { url: msg.url.path, body: new TextDecoder().decode(bytes) };
    const resp = msg.response(
      this.status,
      typeof this.reply === "string" ? Body.fromString(this.reply, msg.body!.mediaType) : Body.fromJson(this.reply),
    );
    resp.status = this.status;
    return resp;
  }
}

const cfConfig = {
  accountId: "acct123",
  from: "noreply@example.com",
  fromName: "Example",
  auth: "bearer",
  token: "tok",
};

function welcome(): Outbound {
  return {
    channel: "email",
    to: [{ email: "a@b.com", name: "A" }],
    cc: [],
    bcc: [],
    subject: "Welcome",
    text: "hi",
    html: "<p>hi</p>",
    attachments: [],
    headers: {},
  };
}

describe("cf-email adapter", () => {
  it("hits the account endpoint with the provider's field names and mints no id", async () => {
    const http = new StubHttp(200, {
      success: true,
      errors: [],
      result: { delivered: ["a@b.com"], queued: [], permanent_bounces: [] },
    });
    const g = CfEmailGateway.fromConfig(cfConfig, http);
    const receipt = await g.send("t", welcome());

    expect(http.seen!.url).toMatch(/\/accounts\/acct123\/email\/sending\/send$/);
    const body = JSON.parse(http.seen!.body) as Record<string, unknown>;
    expect(body.from).toBe("Example <noreply@example.com>");
    expect(body.to).toEqual(["A <a@b.com>"]);
    expect(body.subject).toBe("Welcome");
    expect(body.html).toBe("<p>hi</p>");

    // No id to give, and the provider's own answer preserved verbatim.
    expect(receipt.id).toBeUndefined();
    expect(receipt.provider).toBe("cf-email");
    expect((receipt.detail as Record<string, unknown>).delivered).toEqual(["a@b.com"]);
    expect(g.deliveryStatus()).toBe(false);
  });

  it("a message sender overrides the configured default", async () => {
    const http = new StubHttp(200, { success: true, result: {} });
    const g = CfEmailGateway.fromConfig(cfConfig, http);
    const out: Outbound = { ...welcome(), from: { email: "billing@example.com" } } as Outbound;
    await g.send("t", out);
    expect((JSON.parse(http.seen!.body) as Record<string, unknown>).from).toBe("billing@example.com");
  });

  it("a provider rejection keeps the provider's own words", async () => {
    const http = new StubHttp(403, {
      success: false,
      errors: [{ code: 1001, message: "sending domain not verified" }],
    });
    const g = CfEmailGateway.fromConfig(cfConfig, http);
    const err = await caughtAsync(g.send("t", welcome()));
    expect(err.status).toBe(400);
    expect(err.detail).toContain("sending domain not verified");
  });

  it("a success:false body is a failure even with a 200", async () => {
    const http = new StubHttp(200, { success: false, errors: [{ message: "quota exceeded" }] });
    const g = CfEmailGateway.fromConfig(cfConfig, http);
    const err = await caughtAsync(g.send("t", welcome()));
    expect(err.detail).toContain("quota exceeded");
  });

  it("an adapter without a credential is a config error", () => {
    const err = caught(() => CfEmailGateway.fromConfig({ accountId: "a" }, new StubHttp(200, {})));
    expect(err.detail).toContain("requires an 'auth' credential");
  });

  it("status says the answer already arrived at send time", async () => {
    const g = CfEmailGateway.fromConfig(cfConfig, new StubHttp(200, {}));
    const err = await caughtAsync(g.status("t", "x"));
    expect(err.status).toBe(501);
  });
});

const snsConfig = { region: "eu-west-1", accessKeyId: "AKIAEXAMPLE", secretAccessKey: "secret" };

describe("aws-sns adapter", () => {
  it("the publish form carries the number, the text and the SMS type", () => {
    const g = AwsSnsGateway.fromConfig(snsConfig, new StubHttp(200, ""));
    const form = g.formBody({ channel: "sms", to: "+447700900000", text: "your code is 123" });
    expect(form).toContain("Action=Publish");
    expect(form).toContain("PhoneNumber=%2B447700900000");
    // Spaces are %20, never '+': a '+' would decode back as a space and
    // corrupt an E.164 number if the same encoder were used on one.
    expect(form).toContain("Message=your%20code%20is%20123");
    expect(form).toContain("StringValue=Transactional");
  });

  it("a per-message sender beats the adapter default", () => {
    const g = AwsSnsGateway.fromConfig({ ...snsConfig, senderId: "Default" }, new StubHttp(200, ""));
    const form = g.formBody({ channel: "sms", to: "+447700900000", from: "Override", text: "hi" });
    expect(form).toContain("StringValue=Override");
    expect(form).not.toContain("StringValue=Default");
  });

  it("an email handed to the sms adapter is refused", () => {
    const g = AwsSnsGateway.fromConfig(snsConfig, new StubHttp(200, ""));
    const err = caught(() => g.formBody(welcome()));
    expect(err.detail).toBe("the aws-sns adapter serves the 'sms' channel only");
  });

  it("reads the message id out of the Query API response", async () => {
    const xml =
      "<PublishResponse><PublishResult><MessageId>abc-123</MessageId></PublishResult></PublishResponse>";
    expect(xmlField(xml, "MessageId")).toBe("abc-123");
    expect(xmlField(xml, "Nope")).toBeUndefined();

    const g = AwsSnsGateway.fromConfig(snsConfig, new StubHttp(200, xml));
    expect(await g.send("t", { channel: "sms", to: "+1555", text: "hi" })).toEqual({
      id: "abc-123",
      channel: "sms",
      provider: "aws-sns",
    });
  });

  it("a provider rejection keeps its code and message", () => {
    const xml =
      "<ErrorResponse><Error><Code>InvalidParameter</Code><Message>Invalid phone number</Message></Error></ErrorResponse>";
    const err = providerError(400, xml);
    expect(err.status).toBe(400);
    expect(err.detail).toContain("InvalidParameter: Invalid phone number");
  });

  it("status names the limitation rather than faking an answer", async () => {
    const g = AwsSnsGateway.fromConfig(snsConfig, new StubHttp(200, ""));
    const err = await caughtAsync(g.status("t", "abc"));
    expect(err.status).toBe(501);
    expect(err.detail).toContain("no per-message status API");
  });
});

describe("RoutingGateway", () => {
  class Fake implements MessageGateway {
    sent: Outbound | undefined;
    constructor(
      private readonly ch: Channel,
      private readonly reports: boolean,
    ) {}
    async send(_t: string, out: Outbound): Promise<Receipt> {
      this.sent = out;
      return { id: `${this.ch}-1`, channel: this.ch, provider: `fake-${this.ch}` };
    }
    async status(_t: string, id: string): Promise<Json> {
      if (!id.startsWith(this.ch)) throw RsError.notFound(`no message '${id}'`);
      return { id, status: "delivered" };
    }
    channels(): Channel[] {
      return [this.ch];
    }
    deliveryStatus(): boolean {
      return this.reports;
    }
    provider(): string {
      return `fake-${this.ch}`;
    }
  }

  it("the channel picks the route, and the union is what the mount advertises", async () => {
    const email = new Fake("email", true);
    const sms = new Fake("sms", false);
    const g = new RoutingGateway([
      ["email", email],
      ["sms", sms],
    ]);
    expect(g.channels()).toEqual(["email", "sms"]);
    // One route reports status, so the mount does.
    expect(g.deliveryStatus()).toBe(true);

    expect((await g.send("t", welcome())).provider).toBe("fake-email");
    expect(sms.sent).toBeUndefined();
    expect((await g.send("t", { channel: "sms", to: "+1", text: "x" })).provider).toBe("fake-sms");
  });

  it("an unrouted channel names what is configured", async () => {
    const g = new RoutingGateway([["email", new Fake("email", true)]]);
    const err = await caughtAsync(g.send("t", { channel: "sms", to: "+1", text: "x" }));
    expect(err.detail).toBe("no adapter is configured for the 'sms' channel (configured: email)");
  });

  it("status asks only the routes that report it", async () => {
    const g = new RoutingGateway([
      ["email", new Fake("email", true)],
      ["sms", new Fake("sms", false)],
    ]);
    expect(await g.status("t", "email-1")).toEqual({ id: "email-1", status: "delivered" });
    // The sms route is skipped entirely, so this is the email route's 404.
    const err = await caughtAsync(g.status("t", "sms-1"));
    expect(err.status).toBe(404);
  });
});
