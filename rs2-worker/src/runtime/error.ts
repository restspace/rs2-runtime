// Structured errors (PRD §12): every failure maps to an RFC 9457
// problem-details body with a machine-readable `code`. Port of
// `rs2-core/src/error.rs`; the `codes` strings are the contract.

export const codes = {
  BAD_REQUEST: "bad_request",
  UNAUTHORIZED: "unauthorized",
  FORBIDDEN: "forbidden",
  NOT_FOUND: "not_found",
  CONFLICT: "conflict",
  PRECONDITION_FAILED: "precondition_failed",
  PAYLOAD_TOO_LARGE: "payload_too_large",
  VALIDATION_FAILED: "validation_failed",
  IDEMPOTENCY_KEY_REUSE: "idempotency_key_reuse",
  LIMIT_EXCEEDED: "limit_exceeded",
  CAPABILITY_DENIED: "capability_denied",
  CONTRACT_VIOLATION: "contract_violation",
  ENGINE_UNAVAILABLE: "engine_unavailable",
  PATH_UNSAFE: "path_unsafe",
  INTERNAL: "internal",
} as const;

export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };
export type JsonObject = { [k: string]: Json };

/// The single error type crossing module boundaries.
export class RsError extends Error {
  status: number;
  code: string;
  title: string;
  detail: string;
  retryable: boolean;
  retryAfterMs?: number;
  /// Optional structured extras (e.g. which limit, observed value, cap).
  extra?: JsonObject;

  constructor(status: number, code: string, title: string, detail: string) {
    super(`${status} ${code}: ${detail}`);
    this.status = status;
    this.code = code;
    this.title = title;
    this.detail = detail;
    this.retryable = false;
  }

  static badRequest(detail: string): RsError {
    return new RsError(400, codes.BAD_REQUEST, "Bad Request", detail);
  }
  static unauthorized(detail: string): RsError {
    return new RsError(401, codes.UNAUTHORIZED, "Unauthorized", detail);
  }
  static forbidden(detail: string): RsError {
    return new RsError(403, codes.FORBIDDEN, "Forbidden", detail);
  }
  static notFound(detail: string): RsError {
    return new RsError(404, codes.NOT_FOUND, "Not Found", detail);
  }
  static conflict(detail: string): RsError {
    return new RsError(409, codes.CONFLICT, "Conflict", detail);
  }
  /// An `If-Match`/`If-None-Match` precondition on a store write was not met.
  static preconditionFailed(detail: string): RsError {
    return new RsError(412, codes.PRECONDITION_FAILED, "Precondition Failed", detail);
  }
  static payloadTooLarge(detail: string): RsError {
    return new RsError(413, codes.PAYLOAD_TOO_LARGE, "Payload Too Large", detail);
  }
  static validationFailed(detail: string, errors: Json): RsError {
    const e = new RsError(422, codes.VALIDATION_FAILED, "Validation Failed", detail);
    e.extra = { errors };
    return e;
  }
  /// Same `Idempotency-Key`, different request payload (PRD §7.2).
  static idempotencyKeyReuse(detail: string): RsError {
    return new RsError(422, codes.IDEMPOTENCY_KEY_REUSE, "Idempotency Key Reuse", detail);
  }
  static pathUnsafe(detail: string): RsError {
    return new RsError(400, codes.PATH_UNSAFE, "Unsafe Path", detail);
  }
  static capabilityDenied(capability: string): RsError {
    const e = new RsError(
      403,
      codes.CAPABILITY_DENIED,
      "Capability Denied",
      `capability '${capability}' is not granted to this service`,
    );
    e.extra = { capability };
    return e;
  }
  static contractViolation(detail: string): RsError {
    return new RsError(502, codes.CONTRACT_VIOLATION, "Contract Violation", detail);
  }
  /// A resource limit was breached. Attributed to the tenant; retryable.
  static limitExceeded(limit: string, observed: Json, cap: Json): RsError {
    const e = new RsError(503, codes.LIMIT_EXCEEDED, "Limit Exceeded", `limit '${limit}' exceeded`);
    e.retryable = true;
    e.retryAfterMs = 2000;
    e.extra = { limit, observed, cap };
    return e;
  }
  static engineUnavailable(detail: string): RsError {
    return new RsError(501, codes.ENGINE_UNAVAILABLE, "Engine Unavailable", detail);
  }
  static internal(detail: string): RsError {
    return new RsError(500, codes.INTERNAL, "Internal Error", detail);
  }

  asRetryable(): RsError {
    this.retryable = true;
    return this;
  }

  /// RFC 9457 problem-details body for this error. `extra` keys are
  /// flattened at the top level after `retryAfterMs`.
  toProblemJson(tenant: string, traceId: string): JsonObject {
    const problem: JsonObject = {
      type: `https://rs2.dev/errors#${this.code}`,
      title: this.title,
      status: this.status,
      code: this.code,
      detail: this.detail,
      tenant,
      traceId,
      retryable: this.retryable,
    };
    if (this.retryAfterMs !== undefined) problem.retryAfterMs = this.retryAfterMs;
    if (this.extra) for (const [k, v] of Object.entries(this.extra)) problem[k] = v;
    return problem;
  }
}

/// Coerce any thrown value into an `RsError` (unexpected exceptions become
/// 500 `internal`, the analogue of Rust's `From<io::Error>` catch-all).
export function toRsError(e: unknown): RsError {
  if (e instanceof RsError) return e;
  const detail = e instanceof Error ? e.message : String(e);
  return RsError.internal(detail);
}
