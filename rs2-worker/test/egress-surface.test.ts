// The guest's `env.RS2` binding is a `WorkerEntrypoint`; workerd exposes
// EVERY prototype method over RPC, so anything that is not an intended op
// (a helper returning the tenant stub, say) is a capability leak. Pin the
// exact surface so an added helper fails the build rather than shipping.
import { describe, expect, it } from "vitest";
import { Egress, HostApi } from "../src/egress";

const HOST_API_OPS = [
  "request",
  "log",
  "stateGet",
  "statePut",
  "bodyRead",
  "streamBegin",
  "bodyWrite",
  "socketCheck",
  "fetchOut",
].sort();

function ownMethods(cls: { prototype: object }): string[] {
  return Object.getOwnPropertyNames(cls.prototype)
    .filter((n) => n !== "constructor")
    .sort();
}

describe("guest-boundary entrypoints expose only the op table", () => {
  it("HostApi has exactly the documented ops (§E.3)", () => {
    expect(ownMethods(HostApi)).toEqual(HOST_API_OPS);
  });

  it("Egress exposes only fetch", () => {
    expect(ownMethods(Egress)).toEqual(["fetch"]);
  });
});
