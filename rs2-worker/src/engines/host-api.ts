// Engine-neutral service contract, host side (PRD §5.4). Port of
// `rs2-core/src/contract/mod.rs`: `GrantedHost` enforces capability grants
// (default deny), the outbound-call budget (counted **before** dispatch),
// and trace propagation. Guest `ctx.state` is backed by DO KV
// (`state:<service>:<key>`) — durable, an intentional upgrade over Rust's
// in-memory map (cloudflare.md decision 12).

import { RsError } from "../runtime/error";
import { Severity, attr, recordNow } from "../runtime/logging";
import type { LogStore } from "../runtime/logging";
import { TraceContext } from "../runtime/message";
import type { Message } from "../runtime/message";

export type LogLevel = "debug" | "info" | "warn" | "error";

/// Rust `level_from_str`: unknown levels are info.
export function severityFromLevel(level: string): Severity {
  switch (level) {
    case "debug":
      return Severity.Debug;
    case "warn":
      return Severity.Warn;
    case "error":
      return Severity.Error;
    default:
      return Severity.Info;
  }
}

/// Resolves a named capability grant to an executable target.
export type CapabilityTarget = (msg: Message) => Promise<Message>;

/// Identity stamped onto a sandbox service's own logs (`HostApi::log`).
export interface LogContext {
  sink: LogStore;
  tenant: string;
  mount: string;
  service: string;
  traceId: string;
  spanId: string;
}

/// The invocation-external state store (invariant 4), keyed per service
/// instance. On this host: DO KV under `state:<service>:<key>`.
export interface GuestStateKv {
  get(key: string): Promise<string | undefined>;
  put(key: string, value: string): Promise<void>;
}

/// The host side handed to engines: capability grants (default deny), the
/// outbound budget, and trace propagation.
export class GrantedHost {
  private outboundUsed = 0;
  private logCtx: LogContext | undefined;

  constructor(
    private readonly grants: Map<string, CapabilityTarget>,
    private readonly outboundBudget: number,
    private readonly state: GuestStateKv | undefined,
    private readonly serviceName: string,
  ) {}

  withLogContext(ctx: LogContext): GrantedHost {
    this.logCtx = ctx;
    return this;
  }

  /// A host with no grants at all — the default-deny baseline.
  static denyAll(serviceName: string): GrantedHost {
    return new GrantedHost(new Map(), 0, undefined, serviceName);
  }

  async request(capability: string, msg: Message): Promise<Message> {
    const target = this.grants.get(capability);
    if (!target) throw RsError.capabilityDenied(capability);
    const used = ++this.outboundUsed;
    if (used > this.outboundBudget) {
      throw RsError.limitExceeded("outbound_calls", used, this.outboundBudget);
    }
    msg.trace = msg.trace.child();
    msg.depth = Math.min(msg.depth + 1, 0xffff);
    return target(msg);
  }

  log(level: string, text: string): void {
    // Sandbox `console.log` / guest `ctx.log` land here, stamped with this
    // invocation's identity (PRD §14). No context drops them.
    const c = this.logCtx;
    if (!c) return;
    const trace = new TraceContext(c.traceId, c.spanId);
    const rec = recordNow(severityFromLevel(level), c.tenant, trace, text);
    attr(rec, "rs2.mount", c.mount);
    attr(rec, "rs2.service", c.service);
    attr(rec, "rs2.source", "custom");
    c.sink.emit(rec);
  }

  async stateGet(key: string): Promise<string | undefined> {
    if (!this.state) return undefined;
    return this.state.get(`state:${this.serviceName}:${key}`);
  }

  async statePut(key: string, value: string): Promise<void> {
    if (!this.state) return;
    await this.state.put(`state:${this.serviceName}:${key}`, value);
  }
}
