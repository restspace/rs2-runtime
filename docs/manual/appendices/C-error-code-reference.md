# Appendix C — Error code reference

Every failure is `application/problem+json` with a machine-readable `code` (9.2).
Branch on `code`, respect `retryable` and `Retry-After`, and capture `traceId`.

## The body

```json
{ "type": "https://rs2.dev/errors#<code>", "title": "…", "status": <int>,
  "code": "<code>", "detail": "…", "tenant": "…", "traceId": "…",
  "retryable": <bool>, "errors": [ { "path": "…", "error": "…" } ] }
```

`errors` appears for validation failures. Pipeline failures add a `pipeline`
object (`failedStep` + per-step `steps`), 7.9.

## Codes

| code | status | retryable | notes |
| --- | --- | --- | --- |
| `bad_request` | 400 | no | Malformed request; missing required query param |
| `path_unsafe` | 400 | no | Router rejected the path (traversal/encoding/null/backslash/drive) — every service (3.2) |
| `unauthorized` | 401 | no | Missing/invalid/expired credentials; lockout adds `Retry-After` (5.3) |
| `forbidden` | 403 | no | Principal lacks the required role (5.4) |
| `capability_denied` | 403 | no | Sandboxed code used an ungranted capability; extra `capability` field (8.5) |
| `not_found` | 404 | no | No such resource / mount / deployed code ref |
| `conflict` | 409 | maybe | `If-Match` version mismatch on the **config** (10.4); in-flight idempotent duplicate (9.3); delete of a referenced code version (10.5) |
| `precondition_failed` | 412 | maybe | A **store** write's or delete's `If-Match` didn't match the current ETag, or `If-None-Match: *` hit an existing resource (4.2) — re-read and retry |
| `payload_too_large` | 413 | no | Body exceeds the materialization cap |
| `validation_failed` | 422 | no | Schema validation; extra `errors` array (4.5, 6.5) |
| `idempotency_key_reuse` | 422 | no | Same `Idempotency-Key`, different payload (9.3) |
| `internal` | 500 | maybe | Unexpected host error |
| `engine_unavailable` | 501 | no | Build lacks the engine for this code type, or no HTTP adapter for an `httpOut` grant (8.8, 10.1) |
| `contract_violation` | 502 | no | Custom service broke the contract — bad output, crash, or compile failure (8.8) |
| `limit_exceeded` | 503 | **yes** | Resource breach; extras `limit`, `observed`, `cap`, `retryAfterMs` (9.5) |

## Headers worth reading on errors

| Header | When | Meaning |
| --- | --- | --- |
| `X-Trace-Id` | always | Correlate to logs (9.6) |
| `Retry-After` | 401 lockout, 409 in-flight, 503 breaker/limit | Wait this long before retrying |
| `Idempotency-Replayed: true` | replayed success | Body is a stored prior result, not a fresh run (9.3) |

## Handling guidance

- **`limit_exceeded` / `503`** — retryable; back off per `retryAfterMs`/
  `Retry-After`. Don't hammer.
- **`401` with an expired token** — strip the token, refresh or retry
  anonymously; don't resend the stale one (5.3).
- **`capability_denied`** — check the `capability` field against your `grants`
  (8.5).
- **`validation_failed`** — map `errors[].path` (JSON Pointers) to fields.
- **`409` on config or code** — re-read, reapply, retry (optimistic concurrency).

---

← [Appendix B — Tenant config reference](B-tenant-config-reference.md) · [Manual home](../README.md) · [Next: Appendix D — Default limits →](D-default-limits.md)
