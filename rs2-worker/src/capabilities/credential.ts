// Host-side outbound credential injection. Port of
// `rs2-core/src/adapters/credential.rs`: strategies bearer|header|basic|
// query|hmac|awsSigV4 with the exact 400 wordings; SigV4 byte-exact.

import { base64Encode, hmacBytes, hmacSha256, sha256Hex, toHex } from "../runtime/crypto";
import { RsError } from "../runtime/error";
import type { Json } from "../runtime/error";
import type { Message } from "../runtime/message";

export type AuthStrategy =
  | { kind: "bearer"; token: string }
  | { kind: "header"; name: string; value: string }
  | { kind: "basic"; username: string; password: string }
  | { kind: "query"; name: string; value: string }
  | { kind: "hmac"; algorithm: string; secret: string; header: string }
  | { kind: "awsSigV4"; accessKey: string; secretKey: string; region: string; service: string };

const encoder = new TextEncoder();

/// RFC 3986 unreserved set (`A-Z a-z 0-9 - _ . ~`) — everything else is
/// percent-encoded, byte-wise over UTF-8.
function encUnreserved(s: string, keepSlash: boolean): string {
  let out = "";
  for (const b of encoder.encode(s)) {
    const c = String.fromCharCode(b);
    if (/[A-Za-z0-9\-_.~]/.test(c) || (keepSlash && c === "/")) out += c;
    else out += `%${b.toString(16).toUpperCase().padStart(2, "0")}`;
  }
  return out;
}

function enc(s: string): string {
  return encUnreserved(s, false);
}

function percentDecodeLossy(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    return s.replace(/%[0-9a-fA-F]{2}/g, (m) => {
      try {
        return decodeURIComponent(m);
      } catch {
        return "�";
      }
    });
  }
}

/// Re-canonicalize an already wire-encoded component: decode, then re-encode.
function recanon(s: string, keepSlash: boolean): string {
  return encUnreserved(percentDecodeLossy(s), keepSlash);
}

export class CredentialInjector {
  constructor(readonly strategy: AuthStrategy) {}

  /// Build an injector from an already infra-merged config value. `undefined`
  /// when there is no `auth` key; throws on a malformed strategy.
  static fromConfig(merged: Json): CredentialInjector | undefined {
    if (!merged || typeof merged !== "object" || Array.isArray(merged)) return undefined;
    const auth = merged.auth;
    if (typeof auth !== "string") return undefined;
    const s = (key: string): string => {
      const v = merged[key];
      if (typeof v !== "string") throw RsError.badRequest(`auth '${auth}' requires '${key}'`);
      return v;
    };
    const opt = (key: string, dflt: string): string => (typeof merged[key] === "string" ? (merged[key] as string) : dflt);
    let strategy: AuthStrategy;
    switch (auth) {
      case "bearer":
        strategy = { kind: "bearer", token: s("token") };
        break;
      case "header":
        strategy = { kind: "header", name: s("name"), value: s("value") };
        break;
      case "basic":
        strategy = { kind: "basic", username: s("username"), password: s("password") };
        break;
      case "query":
        strategy = { kind: "query", name: s("name"), value: s("value") };
        break;
      case "hmac":
        strategy = { kind: "hmac", algorithm: opt("algorithm", "sha256"), secret: s("secret"), header: opt("header", "X-Signature") };
        break;
      case "awsSigV4":
        strategy = {
          kind: "awsSigV4",
          accessKey: s("accessKeyId"),
          secretKey: s("secretAccessKey"),
          region: s("region"),
          service: s("service"),
        };
        break;
      default:
        throw RsError.badRequest(
          `unknown auth strategy '${auth}' (one of: bearer, header, basic, query, hmac, awsSigV4)`,
        );
    }
    return new CredentialInjector(strategy);
  }

  /// Apply the credential to `msg` in place. `maxBodyBytes` bounds body
  /// materialization for signing strategies.
  async apply(msg: Message, maxBodyBytes: number): Promise<void> {
    const st = this.strategy;
    switch (st.kind) {
      case "bearer":
        return setHeader(msg, "authorization", `Bearer ${st.token}`);
      case "header":
        return setHeader(msg, st.name, st.value);
      case "basic":
        return setHeader(msg, "authorization", `Basic ${base64Encode(encoder.encode(`${st.username}:${st.password}`))}`);
      case "query": {
        const pair = `${enc(st.name)}=${enc(st.value)}`;
        msg.url.query = msg.url.query === "" ? pair : `${msg.url.query}&${pair}`;
        return;
      }
      case "hmac": {
        const body = await materialize(msg, maxBodyBytes);
        const mac = await hmacBytes(st.algorithm, st.secret, body);
        if (!mac) throw RsError.badRequest(`hmac: unsupported algorithm '${st.algorithm}'`);
        return setHeader(msg, st.header, toHex(mac));
      }
      case "awsSigV4": {
        const body = await materialize(msg, maxBodyBytes);
        const [host, path] = splitUrl(msg.url.path);
        const [authorization, amzDate] = await signAwsSigV4(
          msg.method,
          host,
          path,
          msg.url.query,
          body,
          st.accessKey,
          st.secretKey,
          st.region,
          st.service,
          new Date(),
        );
        setHeader(msg, "x-amz-date", amzDate);
        return setHeader(msg, "authorization", authorization);
      }
    }
  }
}

function setHeader(msg: Message, name: string, value: string): void {
  if (!/^[A-Za-z0-9!#$%&'*+.^_`|~-]+$/.test(name)) throw RsError.badRequest(`invalid header name '${name}'`);
  try {
    msg.headers.set(name, value);
  } catch {
    throw RsError.badRequest("credential value is not a valid header value");
  }
}

async function materialize(msg: Message, maxBodyBytes: number): Promise<Uint8Array> {
  return msg.body ? msg.body.materialize(maxBodyBytes) : new Uint8Array(0);
}

/// Split an absolute URL into `[host, path]` — `host` is the authority
/// (incl. any port), `path` the absolute path (`/` when empty).
function splitUrl(url: string): [string, string] {
  const i = url.indexOf("://");
  if (i < 0) throw RsError.badRequest(`not an absolute URL: '${url}'`);
  const rest = url.slice(i + 3);
  const slash = rest.indexOf("/");
  const authority = slash < 0 ? rest : rest.slice(0, slash);
  let path = slash < 0 ? "/" : rest.slice(slash);
  path = path.split(/[?#]/)[0] ?? "/";
  return [authority, path === "" ? "/" : path];
}

function pad(n: number, w: number): string {
  return String(n).padStart(w, "0");
}

/// Compute an AWS SigV4 `Authorization` header (signing `host` +
/// `x-amz-date`). Returns `[authorization, x-amz-date]`.
export async function signAwsSigV4(
  method: string,
  host: string,
  path: string,
  query: string,
  body: Uint8Array,
  accessKey: string,
  secretKey: string,
  region: string,
  service: string,
  datetime: Date,
): Promise<[string, string]> {
  const amzDate = `${pad(datetime.getUTCFullYear(), 4)}${pad(datetime.getUTCMonth() + 1, 2)}${pad(datetime.getUTCDate(), 2)}T${pad(datetime.getUTCHours(), 2)}${pad(datetime.getUTCMinutes(), 2)}${pad(datetime.getUTCSeconds(), 2)}Z`;
  const dateStamp = amzDate.slice(0, 8);

  const canonicalUri = canonicalPath(path, service);
  const canonicalQuery = canonicalQueryString(query);
  const payloadHash = await sha256Hex(body);
  const canonicalHeaders = `host:${host}\nx-amz-date:${amzDate}\n`;
  const signedHeaders = "host;x-amz-date";
  const canonicalRequest = `${method}\n${canonicalUri}\n${canonicalQuery}\n${canonicalHeaders}\n${signedHeaders}\n${payloadHash}`;

  const scope = `${dateStamp}/${region}/${service}/aws4_request`;
  const stringToSign = `AWS4-HMAC-SHA256\n${amzDate}\n${scope}\n${await sha256Hex(canonicalRequest)}`;

  const kDate = await hmacSha256(`AWS4${secretKey}`, dateStamp);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, service);
  const kSigning = await hmacSha256(kService, "aws4_request");
  const signature = toHex(await hmacSha256(kSigning, stringToSign));

  const authorization = `AWS4-HMAC-SHA256 Credential=${accessKey}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  return [authorization, amzDate];
}

/// Canonical URI path for SigV4: recanonicalized; non-S3 services
/// double-encode (`%20` → `%2520`), S3 signs the single-encoded path.
export function canonicalPath(path: string, service: string): string {
  const single = recanon(path, true);
  return service === "s3" ? single : encUnreserved(single, true);
}

/// Canonical query string: params sorted by encoded key (then value), each
/// key and value re-canonicalized, joined by `&`.
export function canonicalQueryString(query: string): string {
  if (query === "") return "";
  const pairs: Array<[string, string]> = query
    .split("&")
    .filter((p) => p !== "")
    .map((p) => {
      const eq = p.indexOf("=");
      const k = eq < 0 ? p : p.slice(0, eq);
      const v = eq < 0 ? "" : p.slice(eq + 1);
      return [recanon(k, false), recanon(v, false)];
    });
  pairs.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0));
  return pairs.map(([k, v]) => `${k}=${v}`).join("&");
}
