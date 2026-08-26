// Port of the `#[cfg(test)]` module in `rs2-core/src/adapters/credential.rs`
// (incl. the AWS SigV4 `get-vanilla` golden vector) and the crypto vectors.
import { describe, expect, it } from "vitest";
import { CredentialInjector, canonicalPath, canonicalQueryString, signAwsSigV4 } from "../src/capabilities/credential";
import { Body } from "../src/runtime/body";
import { fromHex, hmacBytes, sha256Hex, toHex } from "../src/runtime/crypto";
import { MediaType } from "../src/runtime/media-type";
import { Message } from "../src/runtime/message";

function req(): Message {
  return Message.request("POST", "https://api.example.com/v1/send?b=2&a=1", "t");
}

describe("credential", () => {
  it("bearer sets authorization", async () => {
    const inj = CredentialInjector.fromConfig({ auth: "bearer", token: "sk-123" })!;
    const msg = req();
    await inj.apply(msg, 1 << 20);
    expect(msg.header("authorization")).toBe("Bearer sk-123");
  });

  it("header and basic and query", async () => {
    let msg = req();
    await CredentialInjector.fromConfig({ auth: "header", name: "X-Api-Key", value: "k" })!.apply(msg, 1 << 20);
    expect(msg.header("x-api-key")).toBe("k");

    msg = req();
    await CredentialInjector.fromConfig({ auth: "basic", username: "u", password: "p" })!.apply(msg, 1 << 20);
    expect(msg.header("authorization")).toBe("Basic dTpw");
  });

  it("query param appends", async () => {
    const msg = req();
    await CredentialInjector.fromConfig({ auth: "query", name: "api_key", value: "a b" })!.apply(msg, 1 << 20);
    expect(msg.url.query).toBe("b=2&a=1&api_key=a%20b");
  });

  it("hmac signs body", async () => {
    const msg = req().withBody(Body.fromString("payload", MediaType.json()));
    await CredentialInjector.fromConfig({ auth: "hmac", algorithm: "sha256", secret: "Jefe", header: "X-Sig" })!.apply(
      msg,
      1 << 20,
    );
    const expected = toHex((await hmacBytes("sha256", "Jefe", "payload"))!);
    expect(msg.header("x-sig")).toBe(expected);
  });

  it("no auth key yields none", () => {
    expect(CredentialInjector.fromConfig({ adapter: "x" })).toBeUndefined();
  });

  it("unknown strategy and missing field are 400s", () => {
    expect(() => CredentialInjector.fromConfig({ auth: "magic" })).toThrow(
      "unknown auth strategy 'magic' (one of: bearer, header, basic, query, hmac, awsSigV4)",
    );
    expect(() => CredentialInjector.fromConfig({ auth: "bearer" })).toThrow("auth 'bearer' requires 'token'");
  });

  it("canonical query recanonicalizes without double encoding", () => {
    expect(canonicalQueryString("v=a%20b&k=1")).toBe("k=1&v=a%20b");
    expect(canonicalQueryString("q=a%2Fb")).toBe("q=a%2Fb");
    expect(canonicalQueryString("")).toBe("");
  });

  it("canonical path double encodes for non-s3 only", () => {
    expect(canonicalPath("/example%20space/", "execute-api")).toBe("/example%2520space/");
    expect(canonicalPath("/example%20space/", "s3")).toBe("/example%20space/");
    expect(canonicalPath("/a b", "execute-api")).toBe("/a%2520b");
    expect(canonicalPath("/a b", "s3")).toBe("/a%20b");
    expect(canonicalPath("/v1/send", "execute-api")).toBe("/v1/send");
  });

  it("sigv4 matches get-vanilla vector", async () => {
    const [authorization, amzDate] = await signAwsSigV4(
      "GET",
      "example.amazonaws.com",
      "/",
      "",
      new Uint8Array(0),
      "AKIDEXAMPLE",
      "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
      "us-east-1",
      "service",
      new Date(Date.UTC(2015, 7, 30, 12, 36, 0)),
    );
    expect(amzDate).toBe("20150830T123600Z");
    expect(authorization).toBe(
      "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
    );
  });

  it("crypto known vectors", async () => {
    expect(toHex((await hmacBytes("sha256", "Jefe", "what do ya want for nothing?"))!)).toBe(
      "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
    );
    expect(await sha256Hex("abc")).toBe("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    const bytes = new Uint8Array([0, 1, 15, 16, 255]);
    expect(fromHex(toHex(bytes))).toEqual(bytes);
    expect(fromHex("xyz")).toBeUndefined();
    expect(fromHex("abc")).toBeUndefined();
  });
});
