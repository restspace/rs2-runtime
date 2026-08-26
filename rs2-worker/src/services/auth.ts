// `auth` — authentication & RBAC (PRD §10.5). Port of
// `rs2-core/src/services/auth.rs`: HS512 JWTs over WebCrypto, argon2id via
// hash-wasm (§I.22), lockout in DO memory. Token verification
// (`principalFromToken`) is what the P2 dispatch order needs; the login/
// refresh/logout/user endpoints ride along because they are small and the
// conformance seeding logs in.

import { argon2id, argon2Verify, bcryptVerify } from "../vendor/hash-wasm";
import { base64UrlDecode, base64UrlEncode, constantTimeEqual, hmacBytes } from "../runtime/crypto";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import type { Message, Principal } from "../runtime/message";
import { isSameOrigin, originMatches } from "../runtime/wrapper";
import type { Service, ServiceContext } from "./context";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export interface AuthSettings {
  jwtSecret: string;
  sessionMinutes: number;
  maxAttempts: number;
  lockMinutes: number;
  userDataset: string;
  allowedLoginOrigins: string[];
  jwtUserProps: string[];
}

export function defaultAuthSettings(): AuthSettings {
  return {
    jwtSecret: "",
    sessionMinutes: 60,
    maxAttempts: 5,
    lockMinutes: 10,
    userDataset: "users",
    allowedLoginOrigins: [],
    jwtUserProps: [],
  };
}

/// serde-style parse of the `auth` object (camelCase, defaults, typed).
export function parseAuthSettings(value: Json): AuthSettings {
  const s = defaultAuthSettings();
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw RsError.badRequest("invalid auth settings: expected an object");
  }
  const str = (k: string): string | undefined => {
    const v = value[k];
    if (v === undefined) return undefined;
    if (typeof v !== "string") throw RsError.badRequest(`invalid auth settings: '${k}' must be a string`);
    return v;
  };
  const uint = (k: string): number | undefined => {
    const v = value[k];
    if (v === undefined) return undefined;
    if (typeof v !== "number" || !Number.isInteger(v) || v < 0) {
      throw RsError.badRequest(`invalid auth settings: '${k}' must be a non-negative integer`);
    }
    return v;
  };
  const list = (k: string): string[] | undefined => {
    const v = value[k];
    if (v === undefined) return undefined;
    if (!Array.isArray(v) || !v.every((x) => typeof x === "string")) {
      throw RsError.badRequest(`invalid auth settings: '${k}' must be an array of strings`);
    }
    return v as string[];
  };
  s.jwtSecret = str("jwtSecret") ?? s.jwtSecret;
  s.sessionMinutes = uint("sessionMinutes") ?? s.sessionMinutes;
  s.maxAttempts = uint("maxAttempts") ?? s.maxAttempts;
  s.lockMinutes = uint("lockMinutes") ?? s.lockMinutes;
  s.userDataset = str("userDataset") ?? s.userDataset;
  s.allowedLoginOrigins = list("allowedLoginOrigins") ?? s.allowedLoginOrigins;
  s.jwtUserProps = list("jwtUserProps") ?? s.jwtUserProps;
  return s;
}

export interface Claims {
  sub: string;
  roles: string;
  kind: string;
  iat: number;
  exp: number;
  extra: JsonObject;
}

function nowSecs(): number {
  return Math.floor(Date.now() / 1000);
}

/// Sign claims as a HS512 JWT (header bytes literally `{"alg":"HS512","typ":"JWT"}`).
export async function sign(claims: Claims, secret: string): Promise<string> {
  const header = base64UrlEncode(encoder.encode('{"alg":"HS512","typ":"JWT"}'));
  const payloadObj: JsonObject = { sub: claims.sub, roles: claims.roles, kind: claims.kind, iat: claims.iat, exp: claims.exp };
  if (Object.keys(claims.extra).length > 0) payloadObj.extra = claims.extra;
  const payload = base64UrlEncode(encoder.encode(JSON.stringify(payloadObj)));
  const signingInput = `${header}.${payload}`;
  const mac = await hmacBytes("sha512", secret, signingInput);
  return `${signingInput}.${base64UrlEncode(mac!)}`;
}

/// Verify a HS512 JWT (constant-time MAC comparison) and return its claims.
export async function verify(token: string, secret: string): Promise<Claims> {
  const parts = token.split(".");
  if (parts.length !== 3) throw RsError.unauthorized("malformed token");
  const [header, payload, sig] = parts as [string, string, string];
  const headerBytes = base64UrlDecode(header);
  if (!headerBytes) throw RsError.unauthorized("malformed token header");
  let headerJson: Json;
  try {
    headerJson = JSON.parse(decoder.decode(headerBytes)) as Json;
  } catch {
    throw RsError.unauthorized("malformed token header");
  }
  const alg = headerJson && typeof headerJson === "object" && !Array.isArray(headerJson) ? headerJson.alg : undefined;
  if (alg !== "HS512") throw RsError.unauthorized("unsupported token algorithm");
  const sigBytes = base64UrlDecode(sig);
  if (!sigBytes) throw RsError.unauthorized("malformed signature");
  const expected = await hmacBytes("sha512", secret, `${header}.${payload}`);
  if (!constantTimeEqual(expected!, sigBytes)) throw RsError.unauthorized("invalid token signature");
  const payloadBytes = base64UrlDecode(payload);
  if (!payloadBytes) throw RsError.unauthorized("malformed token payload");
  let claimsJson: Json;
  try {
    claimsJson = JSON.parse(decoder.decode(payloadBytes)) as Json;
  } catch {
    throw RsError.unauthorized("malformed token payload");
  }
  const c = claimsJson && typeof claimsJson === "object" && !Array.isArray(claimsJson) ? claimsJson : undefined;
  if (
    !c ||
    typeof c.sub !== "string" ||
    typeof c.roles !== "string" ||
    typeof c.kind !== "string" ||
    typeof c.iat !== "number" ||
    typeof c.exp !== "number"
  ) {
    throw RsError.unauthorized("malformed token payload");
  }
  const extra = c.extra && typeof c.extra === "object" && !Array.isArray(c.extra) ? c.extra : {};
  if (c.exp < nowSecs()) throw RsError.unauthorized("token expired");
  return { sub: c.sub, roles: c.roles, kind: c.kind, iat: c.iat, exp: c.exp, extra };
}

/// Extract the token from `Authorization: Bearer ` (case-sensitive prefix)
/// or the `rs-auth` cookie.
export function extractToken(msg: Message): string | undefined {
  const auth = msg.header("authorization");
  if (auth !== undefined && auth.startsWith("Bearer ")) return auth.slice("Bearer ".length).trim();
  const cookies = msg.header("cookie");
  if (cookies === undefined) return undefined;
  for (const c of cookies.split(";")) {
    const t = c.trim();
    const eq = t.indexOf("=");
    if (eq < 0) continue;
    if (t.slice(0, eq) === "rs-auth") return t.slice(eq + 1);
  }
  return undefined;
}

/// Verify the request's token (if any) into a `Principal`.
export async function principalFromToken(msg: Message, secret: string): Promise<Principal | undefined> {
  const token = extractToken(msg);
  if (token === undefined) return undefined;
  const claims = await verify(token, secret);
  return {
    id: claims.sub,
    roles: claims.roles.split(/\s+/).filter((r) => r !== ""),
    kind: claims.kind,
    extra: claims.extra,
  };
}

/// Hash a password with argon2id (PHC output, `m=19456,t=2,p=1`).
export async function hashPassword(password: string): Promise<string> {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  return argon2id({ password, salt, parallelism: 1, iterations: 2, memorySize: 19456, hashLength: 32, outputType: "encoded" });
}

/// Verify against an argon2 PHC string or a legacy bcrypt hash.
export async function verifyPassword(password: string, hash: string): Promise<boolean> {
  try {
    if (hash.startsWith("$argon2")) return await argon2Verify({ password, hash });
    if (hash.startsWith("$2")) return await bcryptVerify({ password, hash });
  } catch {
    return false;
  }
  return false;
}

interface LockoutState {
  failures: number;
  lockedUntil: number | undefined;
}

export class AuthService implements Service {
  private readonly lockouts = new Map<string, LockoutState>();

  private constructor(private readonly settings: AuthSettings) {}

  /// Mount config may override tenant-level `auth` settings (minus `access`).
  static fromConfig(mountConfig: JsonObject, tenantAuth: Json | undefined): AuthService {
    const merged: JsonObject =
      tenantAuth && typeof tenantAuth === "object" && !Array.isArray(tenantAuth) ? { ...tenantAuth } : {};
    for (const [k, v] of Object.entries(mountConfig)) if (k !== "access") merged[k] = v;
    const settings = parseAuthSettings(merged);
    if (settings.jwtSecret === "") {
      throw RsError.badRequest("auth requires a 'jwtSecret' in the tenant config's auth settings");
    }
    return new AuthService(settings);
  }

  private checkLockout(user: string): void {
    const state = this.lockouts.get(user);
    if (state?.lockedUntil !== undefined && Date.now() < state.lockedUntil) {
      const err = RsError.unauthorized("account temporarily locked");
      err.retryAfterMs = state.lockedUntil - Date.now();
      throw err;
    }
  }

  private recordFailure(user: string): void {
    if (this.lockouts.size >= 10_000) {
      const now = Date.now();
      for (const [k, s] of this.lockouts) if (!(s.lockedUntil !== undefined && s.lockedUntil > now)) this.lockouts.delete(k);
    }
    let state = this.lockouts.get(user);
    if (!state) {
      state = { failures: 0, lockedUntil: undefined };
      this.lockouts.set(user, state);
    }
    state.failures += 1;
    if (state.failures >= this.settings.maxAttempts) {
      state.lockedUntil = Date.now() + this.settings.lockMinutes * 60 * 1000;
      state.failures = 0;
    }
  }

  private async issue(sub: string, roles: string, kind: string, extra: JsonObject): Promise<[string, number]> {
    const iat = nowSecs();
    const exp = iat + this.settings.sessionMinutes * 60;
    return [await sign({ sub, roles, kind, iat, exp, extra }, this.settings.jwtSecret), exp];
  }

  private checkLoginOrigin(msg: Message): void {
    const origin = msg.header("origin");
    if (origin === undefined) return;
    if (isSameOrigin(origin, msg.header("host"))) return;
    const allowed = this.settings.allowedLoginOrigins;
    if (allowed.length === 0 || allowed.some((p) => originMatches(p, origin))) return;
    throw RsError.forbidden(`login origin '${origin}' is not allowed`);
  }

  private tokenResponse(msg: Message, ctx: ServiceContext, token: string, exp: number): Message {
    const resp = msg.okJson({ token, exp });
    const maxAge = this.settings.sessionMinutes * 60;
    const origin = msg.header("origin");
    let cookie: string | undefined;
    if (origin === undefined || isSameOrigin(origin, msg.header("host"))) {
      cookie = `rs-auth=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${maxAge}`;
    } else if (ctx.cors.isTrusted(origin)) {
      cookie = `rs-auth=${token}; HttpOnly; SameSite=None; Secure; Path=/; Max-Age=${maxAge}`;
    }
    if (cookie !== undefined) resp.headers.set("set-cookie", cookie);
    return resp;
  }

  private async login(msg: Message, ctx: ServiceContext): Promise<Message> {
    this.checkLoginOrigin(msg);
    if (!msg.body) throw RsError.badRequest("login requires a JSON body");
    const body = await msg.body.asJson(ctx.limits.materializedBodyBytes);
    const b = body && typeof body === "object" && !Array.isArray(body) ? body : {};
    if (typeof b.email !== "string") throw RsError.badRequest("login requires 'email'");
    if (typeof b.password !== "string") throw RsError.badRequest("login requires 'password'");
    const email = b.email;
    const password = b.password;
    this.checkLockout(email);
    const data = ctx.data;
    if (!data) throw RsError.internal("auth service has no data capability");
    let user: Json;
    try {
      user = await data.get(this.settings.userDataset, email);
    } catch {
      this.recordFailure(email);
      ctx.logger.warn(msg.trace, `login failed for '${email}' (unknown user)`);
      throw RsError.unauthorized("invalid credentials");
    }
    const u = user && typeof user === "object" && !Array.isArray(user) ? user : {};
    const hash = typeof u.passwordHash === "string" ? u.passwordHash : "";
    if (!(await verifyPassword(password, hash))) {
      this.recordFailure(email);
      ctx.logger.warn(msg.trace, `login failed for '${email}' (bad password)`);
      throw RsError.unauthorized("invalid credentials");
    }
    this.lockouts.delete(email);
    let roles: string;
    if (typeof u.roles === "string") roles = u.roles;
    else if (Array.isArray(u.roles)) roles = u.roles.filter((r): r is string => typeof r === "string").join(" ");
    else roles = "U";
    const kind = typeof u.kind === "string" ? u.kind : "user";
    const extra: JsonObject = {};
    for (const prop of this.settings.jwtUserProps) {
      const v = u[prop];
      if (v !== undefined && v !== null) extra[prop] = v;
    }
    const [token, exp] = await this.issue(email, roles, kind, extra);
    return this.tokenResponse(msg, ctx, token, exp);
  }

  private async refresh(msg: Message, ctx: ServiceContext): Promise<Message> {
    this.checkLoginOrigin(msg);
    const token = extractToken(msg);
    if (token === undefined) throw RsError.unauthorized("refresh requires a token");
    const claims = await verify(token, this.settings.jwtSecret);
    const now = nowSecs();
    const halfway = claims.iat + Math.floor((claims.exp - claims.iat) / 2);
    if (now < halfway) return msg.okJson({ token, exp: claims.exp });
    const [fresh, exp] = await this.issue(claims.sub, claims.roles, claims.kind, claims.extra);
    return this.tokenResponse(msg, ctx, fresh, exp);
  }

  private logout(msg: Message): Message {
    const resp = msg.noContent();
    resp.headers.set("set-cookie", "rs-auth=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0");
    return resp;
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const segs = msg.url.serviceSegments();
    const one = segs.length === 1 ? segs[0] : undefined;
    if (msg.method === "POST" && one === "login") return this.login(msg, ctx);
    if (msg.method === "POST" && one === "refresh") return this.refresh(msg, ctx);
    if (msg.method === "POST" && one === "logout") return this.logout(msg);
    if (msg.method === "GET" && one === "user") {
      const p = msg.principal;
      if (!p) throw RsError.unauthorized("no authenticated principal");
      const body: JsonObject = { id: p.id, roles: p.roles, kind: p.kind };
      for (const [k, v] of Object.entries(p.extra)) if (!(k in body)) body[k] = v;
      return msg.okJson(body);
    }
    throw RsError.notFound(`auth endpoint '${msg.url.servicePath}' (have: POST login/refresh/logout, GET user)`);
  }
}
