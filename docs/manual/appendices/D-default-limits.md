# Appendix D — Default limits and how to change them

The runtime enforces resource limits on every invocation (9.5). The defaults
below are operator-configurable; a breach returns `limit_exceeded` (503) naming
the limit.

## Invocation / tenant limits

| Limit | Default | On breach |
| --- | --- | --- |
| Wall clock — service | 30 s | `limit_exceeded` `wall_clock_ms`; invocation terminated |
| Wall clock — pipeline | 120 s | as above |
| Memory | 128 MB | `limit_exceeded` `memory_bytes`; isolate killed (no process impact) |
| Materialized body | 100 MB | `payload_too_large` / `limit_exceeded` |
| Concurrent invocations / tenant | 64 | Excess fails fast `503` — no queueing |
| Outbound calls / invocation | 64 | `limit_exceeded` |
| Call depth | 16 | `limit_exceeded` |
| Pipeline fan-out | 1000 | `limit_exceeded` |

## Circuit breaker

| Parameter | Default |
| --- | --- |
| Breach threshold | 8 within 10 s |
| Cooldown | 5 s (with `Retry-After`) |
| Trip key | `limit: "tenant_breaker"` |

Only genuine wall-clock/memory/materialization breaches feed the breaker;
admission (concurrency) rejections do not (9.5).

## Idempotency

| Parameter | Default |
| --- | --- |
| Replay window | 24 h |
| Stored body cap | 1 MB |
| Key length cap | 256 chars |

## Store pagination

| Parameter | Default | Cap |
| --- | --- | --- |
| `$take` (listings) | 1000 | 10000 |
| `$take` (query results) | varies by service | 10000 |
| `$take` (log reader) | 100 | 10000 |

## Pipeline defaults

| Parameter | Default |
| --- | --- |
| Subpipeline/split concurrency | 12 |
| Segment snapshot threshold (streaming opt-out) | 1 MB |
| Retry | none (until a policy is set) |

## Changing them

These are **operator-level** settings, tuned where the node and its limits are
configured (not in tenant config — a tenant can't widen its own limits, by
design). Adjust them at the deployment/embedding layer; the trait and config
surfaces are stable. As an application author, treat the defaults as the contract
and design within them (8.8): finish inside the wall-clock budget, bound your
memory and fan-out, and respect `Retry-After` on `limit_exceeded`.

---

← [Appendix C — Error code reference](C-error-code-reference.md) · [Manual home](../README.md) · [Next: Appendix E — The rs2 CLI reference →](E-cli-reference.md)
