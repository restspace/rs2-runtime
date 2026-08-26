// DO-side guest ops that carry a security or contract invariant: the
// single-use socket approvals behind the egress `connect` hook (issue #2
// item 11) and call-depth accounting for a guest hop (issue #2 item 9).
import { describe, expect, it } from "vitest";

import {
  SOCKET_DIAL_SUFFIX,
  consumeSocketApproval,
  guestSocketCheckOp,
  messageFromRequest,
  recordSocketApproval,
} from "../src/engines/dynamic-worker";
import type { InvocationRecord, Invocations, SocketApprovals } from "../src/engines/dynamic-worker";
import { GrantedHost } from "../src/engines/host-api";
import type { JsonObject } from "../src/runtime/error";
import { Message } from "../src/runtime/message";

function record(allowlist: string[]): InvocationRecord {
  return {
    host: GrantedHost.denyAll("svc@v1"),
    tenant: "t",
    depth: 3,
    principal: undefined,
    materializeCap: 1024,
    hostError: undefined,
    socketAllowlist: allowlist,
    bodyReader: undefined,
    streamedIn: 0,
    sink: undefined,
  };
}

function nonceOf(check: unknown): string {
  const dial = (check as { dial?: string }).dial ?? "";
  expect(dial.endsWith(SOCKET_DIAL_SUFFIX)).toBe(true);
  return dial.slice(0, dial.length - SOCKET_DIAL_SUFFIX.length);
}

describe("socket approvals", () => {
  it("an allowed check mints a dial name whose nonce redeems the real target", () => {
    const invocations: Invocations = new Map([["i1", record(["redis.example.com:6379"])]]);
    const approvals: SocketApprovals = new Map();
    const check = guestSocketCheckOp(invocations, approvals, "i1", "redis.example.com", 6379, false);
    const approval = consumeSocketApproval(approvals, nonceOf(check));
    expect(approval).toMatchObject({ host: "redis.example.com", port: 6379, tls: false });
  });

  it("is single-use: a replay of the same nonce gets nothing", () => {
    const approvals: SocketApprovals = new Map();
    const dial = recordSocketApproval(approvals, "db.example.com", 27017, true);
    const nonce = dial.slice(0, dial.length - SOCKET_DIAL_SUFFIX.length);
    expect(consumeSocketApproval(approvals, nonce)).toMatchObject({ tls: true });
    expect(consumeSocketApproval(approvals, nonce)).toBeUndefined();
  });

  it("an approval cannot be redeemed by anything but its own nonce", () => {
    // The point of the nonce (issue #2 item 11): knowing the target is not
    // enough — another mount racing the same `host:port` has no way in.
    const approvals: SocketApprovals = new Map();
    recordSocketApproval(approvals, "redis.example.com", 6379, false);
    expect(consumeSocketApproval(approvals, "redis.example.com:6379")).toBeUndefined();
    expect(consumeSocketApproval(approvals, "deadbeef".repeat(4))).toBeUndefined();
    expect(consumeSocketApproval(approvals, "")).toBeUndefined();
  });

  it("expired approvals are dropped, not redeemed", () => {
    const approvals: SocketApprovals = new Map();
    const dial = recordSocketApproval(approvals, "h", 1, false);
    const nonce = dial.slice(0, dial.length - SOCKET_DIAL_SUFFIX.length);
    approvals.set(nonce, { host: "h", port: 1, tls: false, expires: Date.now() - 1 });
    expect(consumeSocketApproval(approvals, nonce)).toBeUndefined();
    expect(approvals.size).toBe(0);
  });

  it("a denied check mints nothing and fails the guest", () => {
    const invocations: Invocations = new Map([["i1", record(["only.example.com:6379"])]]);
    const approvals: SocketApprovals = new Map();
    const out = guestSocketCheckOp(invocations, approvals, "i1", "evil.example.com", 6379, false) as JsonObject;
    expect(out.__rs2_error).toBe(true);
    expect(out.code).toBe("capability_denied");
    expect(approvals.size).toBe(0);
    expect(invocations.get("i1")!.hostError?.code).toBe("capability_denied");
  });
});

describe("call depth over a guest hop (issue #2 item 9)", () => {
  it("advances by exactly one — in GrantedHost, not twice", async () => {
    const call = messageFromRequest({ url: "/inner" }, "t", 3, undefined);
    expect(call.depth, "the request message carries the caller's depth").toBe(3);

    let seen = -1;
    const host = new GrantedHost(
      new Map([
        [
          "data",
          async (msg: Message) => {
            seen = msg.depth;
            return msg.response(200, undefined);
          },
        ],
      ]),
      4,
      undefined,
      "svc@v1",
    );
    await host.request("data", call);
    expect(seen).toBe(4);
  });
});
