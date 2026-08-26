// `wrapper` — a single inline pipeline fronting another mount (PRD §10.3).
// Runs its config's one spec for every verb and every sub-path; declares
// its discovery `pattern`/`facets` in config; `${url.rest}` forwards the
// exact request path; `inputSchema` is enforced on bodied verbs. Access is
// host-enforced against the mount's own `access`. Port of
// `rs2-core/src/services/wrapper_service.rs`.

import type { Validator } from "@cfworker/json-schema";

import { convert as convertDsl } from "../pipeline/dsl";
import { specFromValue } from "../pipeline/spec";
import type { PipelineSpec } from "../pipeline/spec";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import type { Message } from "../runtime/message";
import type { Service, ServiceContext } from "./context";
import { runInline } from "./pipeline-service";
import { compileSchema, validateInstance } from "./schema";

export class WrapperService implements Service {
  private constructor(
    private readonly spec: PipelineSpec,
    /// Compiled validator for the declared `inputSchema`; `outputSchema` is
    /// advisory (compile-checked at build, read by `discovery`).
    private readonly inputValidator: Validator | undefined,
  ) {}

  /// Parse and validate the inline pipeline from `config.pipeline` and
  /// compile the declared schemas — all 400s at build time.
  static fromConfig(config: JsonObject): WrapperService {
    const specValue = config.pipeline;
    if (specValue === undefined) throw RsError.badRequest("a wrapper mount requires an inline 'pipeline' spec");
    const spec = specFromValue(specValue, convertDsl);
    const inputValidator = compileOptional(config.inputSchema, "inputSchema");
    compileOptional(config.outputSchema, "outputSchema");
    return new WrapperService(spec, inputValidator);
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    // Enforce the declared input schema on verbs that carry a body, before
    // the pipeline runs (`asJson` materializes in place for the pipeline).
    if (this.inputValidator && (msg.method === "PUT" || msg.method === "POST" || msg.method === "PATCH") && msg.body) {
      const value = await msg.body.asJson(ctx.limits.materializedBodyBytes);
      const errors = validateInstance(this.inputValidator, value);
      if (errors.length > 0) {
        throw RsError.validationFailed("request body does not conform to the wrapper's inputSchema", errors);
      }
    }

    // The inline spec governs the whole mount, so the entire sub-path is
    // the URL plane; `rest` is the byte-exact suffix.
    const peeled = msg.url.serviceSegments();
    const baseSegs = msg.url.baseSegments();
    const urlName = peeled.length ? peeled[peeled.length - 1] : undefined;
    return runInline(this.spec, msg, ctx, {
      peeled,
      baseSegs,
      urlName,
      urlQuery: msg.url.query,
      rest: msg.url.servicePath,
      envelopeRetry: undefined,
    });
  }
}

/// Compile an optional JSON Schema: `undefined` when absent, 400 when malformed.
function compileOptional(schema: Json | undefined, label: string): Validator | undefined {
  if (schema === undefined) return undefined;
  return compileSchema(schema, (detail) => RsError.badRequest(`wrapper '${label}' is not a valid JSON Schema: ${detail}`));
}
