// Small shared crypto helpers over WebCrypto. Port of `rs2-core/src/crypto.rs`.

const encoder = new TextEncoder();

/// Lowercase-hex encode bytes.
export function toHex(bytes: Uint8Array | ArrayBuffer): string {
  const b = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let out = "";
  for (const x of b) out += x.toString(16).padStart(2, "0");
  return out;
}

/// Decode lowercase/uppercase hex; `undefined` on odd length or non-hex digits.
export function fromHex(s: string): Uint8Array | undefined {
  const t = s.trim();
  if (t.length === 0 || t.length % 2 !== 0 || !/^[0-9a-fA-F]+$/.test(t)) return undefined;
  const out = new Uint8Array(t.length / 2);
  for (let i = 0; i < t.length; i += 2) out[i / 2] = parseInt(t.slice(i, i + 2), 16);
  return out;
}

function bytesOf(v: string | Uint8Array): Uint8Array {
  return typeof v === "string" ? encoder.encode(v) : v;
}

/// Copy into a fresh ArrayBuffer-backed view (WebCrypto rejects views over
/// SharedArrayBuffer / offset views in some runtimes).
function asBuffer(v: Uint8Array): ArrayBuffer {
  const out = new ArrayBuffer(v.byteLength);
  new Uint8Array(out).set(v);
  return out;
}

/// HMAC of `message` under `key` for `sha256`/`sha512`; `undefined` on an
/// unknown algorithm.
export async function hmacBytes(
  algorithm: string,
  key: string | Uint8Array,
  message: string | Uint8Array,
): Promise<Uint8Array | undefined> {
  const hash = algorithm === "sha256" ? "SHA-256" : algorithm === "sha512" ? "SHA-512" : undefined;
  if (!hash) return undefined;
  const k = await crypto.subtle.importKey("raw", asBuffer(bytesOf(key)), { name: "HMAC", hash }, false, ["sign"]);
  const sig = await crypto.subtle.sign("HMAC", k, asBuffer(bytesOf(message)));
  return new Uint8Array(sig);
}

/// Constant-time HMAC verification against a hex signature.
export async function hmacVerify(
  algorithm: string,
  key: string | Uint8Array,
  message: string | Uint8Array,
  signatureHex: string,
): Promise<boolean> {
  const provided = fromHex(signatureHex);
  if (!provided) return false;
  const expected = await hmacBytes(algorithm, key, message);
  if (!expected) return false;
  return constantTimeEqual(expected, provided);
}

/// HMAC-SHA256 raw bytes (the SigV4 signing primitive).
export async function hmacSha256(key: string | Uint8Array, message: string | Uint8Array): Promise<Uint8Array> {
  return (await hmacBytes("sha256", key, message))!;
}

/// SHA-256 digest bytes.
export async function sha256(bytes: string | Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", asBuffer(bytesOf(bytes))));
}

/// SHA-256 digest, lowercase hex.
export async function sha256Hex(bytes: string | Uint8Array): Promise<string> {
  return toHex(await sha256(bytes));
}

/// Constant-time byte equality. Unequal lengths fail fast — the length isn't
/// the secret.
export function constantTimeEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  const subtle = crypto.subtle as unknown as { timingSafeEqual?: (a: ArrayBuffer, b: ArrayBuffer) => boolean };
  if (typeof subtle.timingSafeEqual === "function") {
    return subtle.timingSafeEqual(asBuffer(a), asBuffer(b));
  }
  let diff = 0;
  for (let i = 0; i < a.byteLength; i++) diff |= a[i]! ^ b[i]!;
  return diff === 0;
}

export function base64Encode(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}

export function base64Decode(s: string): Uint8Array {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export function base64UrlEncode(bytes: Uint8Array): string {
  return base64Encode(bytes).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/// Strict base64url (no padding) decode; `undefined` on malformed input.
export function base64UrlDecode(s: string): Uint8Array | undefined {
  if (!/^[A-Za-z0-9_-]*$/.test(s)) return undefined;
  const pad = s.length % 4;
  if (pad === 1) return undefined;
  const b64 = s.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat(pad === 0 ? 0 : 4 - pad);
  try {
    return base64Decode(b64);
  } catch {
    return undefined;
  }
}
