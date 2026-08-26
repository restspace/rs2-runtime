// `template` — compiled JSX bundles rendered to HTML, store-patterned.
// Port of `rs2-core/src/services/template.rs` over the Dynamic Worker
// engine (cloudflare.md §D): the compiled bundle runs in a locked sandbox
// (`globalOutbound: null`, `env: {}`, `limits: {cpuMs: 1000}`), keyed
// `tpl:` + sha256(source)[0..16], invoked with the resident envelope
// `{method: "POST", url: "/", body: props, mediaType: "application/json"}`.
// A non-string body from the guest is a 502; guest headers are discarded.

import type { DynamicWorkerEngine } from "../engines/dynamic-worker";
import { Body } from "../runtime/body";
import { sha256Hex } from "../runtime/crypto";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { MediaType } from "../runtime/media-type";
import { utf8Encode } from "../runtime/body";
import type { Message } from "../runtime/message";
import { TEMPLATE_SUBTREE } from "./context";
import { urlParams } from "./query-template";
import { ROOT_SPEC } from "./spec-store";
import type { SpecStore, SpecValidator } from "./spec-store";
import type { Service, ServiceContext } from "./context";

/// The template sandbox's CPU budget (cloudflare.md decision 17).
const TEMPLATE_CPU_MS = 1_000;

/// Media type used when a template envelope declares no `contentType`.
const DEFAULT_CONTENT_TYPE = "text/html; charset=utf-8";

export class TemplateService implements Service {
  constructor(
    private readonly store: SpecStore,
    private readonly engine: DynamicWorkerEngine | undefined,
  ) {}

  static fromConfig(_config: JsonObject, store: SpecStore, engine: DynamicWorkerEngine | undefined): TemplateService {
    return new TemplateService(store, engine);
  }

  /// The write-time validator: the envelope must carry a non-empty compiled
  /// `source` string (bundles that fail to evaluate surface at first render).
  static validator(): SpecValidator {
    return (doc: Json) => {
      const source = doc && typeof doc === "object" && !Array.isArray(doc) ? doc.source : undefined;
      if (typeof source === "string" && source.trim() !== "") return doc;
      throw RsError.badRequest(
        'template envelope must be a JSON object with a non-empty "source" string (the compiled bundle from `rs2 template build`)',
      );
    };
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    if (this.store.isAuthoring(msg)) return this.store.handleAuthoring(msg);

    // ---- render: any verb, longest stored prefix ----
    const segments = msg.url.serviceSegments();
    const resolved = await this.store.resolve(segments);
    if (!resolved) {
      throw RsError.notFound(
        `no template matches '${msg.url.servicePath}' (author one at ${msg.url.basePath}${TEMPLATE_SUBTREE}/…)`,
      );
    }
    const [doc, split] = resolved;
    const source = doc && typeof doc === "object" && !Array.isArray(doc) ? doc.source : undefined;
    if (typeof source !== "string") {
      throw RsError.internal("stored template is missing its compiled 'source'");
    }
    const contentTypeRaw = doc && typeof doc === "object" && !Array.isArray(doc) ? doc.contentType : undefined;
    const contentType = typeof contentTypeRaw === "string" ? contentTypeRaw : DEFAULT_CONTENT_TYPE;

    // Props: positional URL segments, then query-string pairs, then the
    // JSON body (object = named props) — later wins.
    const props: JsonObject = {};
    segments.slice(split).forEach((seg, i) => {
      props[String(i)] = seg;
    });
    Object.assign(props, urlParams(msg.url.query, undefined));
    if (msg.body) {
      const named = await msg.body.asJson(ctx.limits.materializedBodyBytes);
      if (named && typeof named === "object" && !Array.isArray(named)) Object.assign(props, named);
      else if (named !== null) throw RsError.badRequest("template props must be a JSON object");
    }

    const engine = this.engine;
    if (!engine) {
      throw RsError.engineUnavailable(
        "template rendering needs the worker loader binding (wrangler.jsonc worker_loaders)",
      );
    }
    const key = split === 0 ? ROOT_SPEC : segments.slice(0, split).join("/");
    void key; // isolate identity is the content hash, not the name
    const version = (await sha256Hex(utf8Encode(source))).slice(0, 16);
    const { status, body } = await engine.invokeResident(
      `tpl:${version}`,
      source,
      { method: "POST", url: "/", body: props, mediaType: "application/json" },
      {},
      TEMPLATE_CPU_MS,
      ctx.limits.wallClockMs,
    );
    if (typeof body !== "string") {
      throw RsError.contractViolation("template render did not return a string body");
    }
    const st = status >= 100 && status <= 999 ? status : 200;
    return msg.response(st, Body.fromString(body, MediaType.parse(contentType)));
  }
}
