// Services that arrive in later phases (cloudflare.md §H): the mount builds
// (so a tenant config that references them dry-builds and hot-reloads) and
// every request answers 501 `engine_unavailable`. Spec-store-backed
// services keep their authoring subtree live so the store contract holds.

import { RsError } from "../runtime/error";
import type { Message } from "../runtime/message";
import type { Service, ServiceContext } from "./context";
import type { SpecStore } from "./spec-store";

/// `query`/`template`: authoring works, execution is P3 (`pipeline` runs).
export class SpecBackedStub implements Service {
  constructor(
    private readonly kind: string,
    readonly store: SpecStore,
  ) {}

  async handle(msg: Message, _ctx: ServiceContext): Promise<Message> {
    if (this.store.isAuthoring(msg)) return this.store.handleAuthoring(msg);
    throw RsError.engineUnavailable(`${this.kind} execution is not available on this host yet (cloudflare.md §H P3)`);
  }
}

/// `proxy`/`sms`: P3.
export class NotYetService implements Service {
  constructor(private readonly kind: string) {}

  async handle(_msg: Message, _ctx: ServiceContext): Promise<Message> {
    throw RsError.engineUnavailable(`the ${this.kind} service is not available on this host yet (cloudflare.md §H P3)`);
  }
}
