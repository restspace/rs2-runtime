// The `apikeys` seal is shared with the Rust host (`rs2-core/src/services/
// apikeys.rs`): the same record must seal to the same value on both, since
// `rs2 sync` copies records between them.

import { describe, expect, it } from "vitest";

import { canonical, splitKey } from "../src/services/apikeys";
import { base64UrlEncode, hmacSha256 } from "../src/runtime/crypto";

/// The vector pinned in the Rust unit test `seal_matches_the_worker_host`.
const VECTOR_SEAL = "tIOq7m8FMkSZWRuhiE2bb7Je4g3M3XRfBDVPyS8OE1U";

describe("apikeys", () => {
  it("canonical JSON sorts keys at every level", () => {
    const v = { b: 1, a: { y: [1, { q: 2, p: 1 }], x: "s" } };
    expect(canonical(v)).toBe('{"a":{"x":"s","y":[1,{"p":1,"q":2}]},"b":1}');
  });

  it("splits a key into hex id and secret", () => {
    expect(splitKey("rs2k_0a1b_sec_ret")).toEqual(["0a1b", "sec_ret"]);
    expect(splitKey("rs2k_xyz_secret")).toBeUndefined();
    expect(splitKey("rs2k__secret")).toBeUndefined();
    expect(splitKey("rs2k_0a1b_")).toBeUndefined();
    expect(splitKey("token")).toBeUndefined();
  });

  it("seals the shared test vector exactly as the Rust host does", async () => {
    const input = canonical([
      "rs2-apikey-v1", "00ff", "s3cret", "ann@example.com", "U W",
      { accountId: "acme", tier: { n: 2, a: "é\n" } }, 1700000000, 1707776000, "ci key",
    ]);
    const seal = base64UrlEncode(await hmacSha256("vector-secret", input));
    expect(seal).toBe(VECTOR_SEAL);
  });
});
