// `apikeys`: long-lived, revocable API keys. Port of
// `rs2-core/src/services/apikeys.rs`: same key format, record layout and
// seal, so a record written on one host verifies on the other.
//
// A key is `rs2k_<id>_<secret>`. Its record is stored under `<id>` with an
// HMAC-SHA256 "seal" over the secret and every field, keyed by the tenant's
// `auth.jwtSecret`: the record holds nothing that rebuilds the key, and a
// record written or edited through some other path (a `data` mount on the
// same store) fails verification instead of granting its roles.

import { base64UrlDecode, base64UrlEncode, constantTimeEqual, hmacSha256, toHex } from "../runtime/crypto";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import type { Message, Principal } from "../runtime/message";
import type { AuthOutcome, Authenticator } from "../runtime/authn";
import type { Service, ServiceContext } from "./context";

/// Every key starts with this, so authenticators can tell keys from tokens.
export const API_KEY_PREFIX = "rs2k_";
/// The principal `kind` of a request authenticated by an API key.
export const API_KEY_KIND = "key";

const DAY_SECS = 24 * 60 * 60;
const MAX_NAME_CHARS = 100;

export interface ApiKeysSettings {
  maxDays: number;
  adminRole: string | undefined;
  dataset: string;
}

function nowSecs(): number {
  return Math.floor(Date.now() / 1000);
}

function isObject(v: Json | undefined): v is JsonObject {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/// JSON with object keys sorted at every level (the Rust host's `canonical`).
export function canonical(v: Json | undefined): string {
  if (v === undefined || v === null) return "null";
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  if (typeof v === "object") {
    const keys = Object.keys(v).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`).join(",")}}`;
  }
  return JSON.stringify(v);
}

/// The fields a seal covers, in a fixed order.
function sealInput(id: string, secret: string, record: JsonObject): string {
  return canonical([
    "rs2-apikey-v1",
    id,
    secret,
    record.owner ?? null,
    record.roles ?? null,
    record.extra ?? null,
    record.created ?? null,
    record.expires ?? null,
    record.name ?? null,
  ]);
}

export function splitKey(key: string): [string, string] | undefined {
  if (!key.startsWith(API_KEY_PREFIX)) return undefined;
  const rest = key.slice(API_KEY_PREFIX.length);
  const sep = rest.indexOf("_");
  if (sep < 0) return undefined;
  const id = rest.slice(0, sep);
  const secret = rest.slice(sep + 1);
  if (id === "" || !/^[0-9a-fA-F]+$/.test(id) || secret === "") return undefined;
  return [id, secret];
}

function rolesOf(v: Json | undefined): string[] {
  if (typeof v === "string") return v.split(/\s+/).filter((r) => r !== "");
  if (Array.isArray(v)) {
    return v.flatMap((r) => (typeof r === "string" ? r.split(/\s+/).filter((x) => x !== "") : []));
  }
  return [];
}

/// A record as listed: never the seal.
function publicView(id: string, record: JsonObject): JsonObject {
  return {
    id,
    name: record.name ?? null,
    owner: record.owner ?? null,
    roles: record.roles ?? null,
    created: record.created ?? null,
    expires: record.expires ?? null,
  };
}

export class ApiKeysService implements Service, Authenticator {
  private constructor(
    private readonly settings: ApiKeysSettings,
    /// The tenant's `auth.jwtSecret`: the seal key.
    private readonly sealKey: string,
  ) {}

  static fromConfig(mountConfig: JsonObject, tenantAuth: Json | undefined): ApiKeysService {
    const maxDays = mountConfig.maxDays ?? 90;
    if (typeof maxDays !== "number" || !Number.isInteger(maxDays) || maxDays < 0) {
      throw RsError.badRequest("invalid apikeys config: 'maxDays' is a whole number of days");
    }
    if (maxDays === 0) throw RsError.badRequest("apikeys 'maxDays' must be at least 1");
    const adminRole = mountConfig.adminRole;
    if (adminRole !== undefined && adminRole !== null && typeof adminRole !== "string") {
      throw RsError.badRequest("invalid apikeys config: 'adminRole' is a role name");
    }
    const dataset = mountConfig.dataset ?? "apikeys";
    if (typeof dataset !== "string") throw RsError.badRequest("invalid apikeys config: 'dataset' is a name");
    const secret = isObject(tenantAuth) ? tenantAuth.jwtSecret : undefined;
    if (typeof secret !== "string" || secret === "") {
      throw RsError.badRequest(
        "apikeys requires a 'jwtSecret' in the tenant config's auth settings (it seals key records)",
      );
    }
    return new ApiKeysService({ maxDays, adminRole: adminRole ?? undefined, dataset }, secret);
  }

  private async seal(id: string, secret: string, record: JsonObject): Promise<string> {
    return base64UrlEncode(await hmacSha256(this.sealKey, sealInput(id, secret, record)));
  }

  private async sealMatches(id: string, secret: string, record: JsonObject): Promise<boolean> {
    const sealed = typeof record.seal === "string" ? base64UrlDecode(record.seal) : undefined;
    if (!sealed) return false;
    const expected = await hmacSha256(this.sealKey, sealInput(id, secret, record));
    return constantTimeEqual(expected, sealed);
  }

  private isAdmin(p: Principal): boolean {
    const role = this.settings.adminRole;
    return role !== undefined && p.roles.includes(role);
  }

  private data(ctx: ServiceContext) {
    if (!ctx.data) throw RsError.internal("apikeys has no data capability");
    return ctx.data;
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const segs = msg.url.serviceSegments();
    if (msg.method === "POST" && segs.length === 0) return this.create(msg, ctx);
    if (msg.method === "GET" && segs.length === 0) return this.list(msg, ctx);
    const [only] = segs;
    if (msg.method === "DELETE" && segs.length === 1 && only !== undefined) return this.revoke(msg, only, ctx);
    throw RsError.notFound(
      `apikeys endpoint '${msg.url.servicePath}' (have: POST / to create, GET / to list, DELETE /<id> to revoke)`,
    );
  }

  private async create(msg: Message, ctx: ServiceContext): Promise<Message> {
    const caller = msg.principal;
    if (!caller) throw RsError.unauthorized("creating an API key requires a login");
    if (caller.kind === API_KEY_KIND) {
      throw RsError.forbidden("a request authenticated by an API key cannot create keys");
    }
    const raw = msg.body ? await msg.body.asJson(ctx.limits.materializedBodyBytes) : {};
    const body: JsonObject = isObject(raw) ? raw : {};
    const name = typeof body.name === "string" ? body.name.trim() : "";
    if (name === "") throw RsError.badRequest("an API key needs a 'name'");
    if ([...name].length > MAX_NAME_CHARS) {
      throw RsError.badRequest(`an API key 'name' is at most ${MAX_NAME_CHARS} characters`);
    }
    let roles: string[];
    if (body.roles === undefined || body.roles === null) roles = [...caller.roles];
    else if (typeof body.roles === "string" || Array.isArray(body.roles)) roles = rolesOf(body.roles);
    else throw RsError.badRequest("'roles' is a space-separated string or an array of role names");
    const extra = roles.find((r) => !caller.roles.includes(r));
    if (extra !== undefined) {
      throw RsError.forbidden(`an API key cannot have role '${extra}', which the caller lacks`);
    }
    const maxDays = this.settings.maxDays;
    let days = maxDays;
    if (body.expiresInDays !== undefined && body.expiresInDays !== null) {
      const d = body.expiresInDays;
      if (typeof d !== "number" || !Number.isInteger(d) || d < 1 || d > maxDays) {
        throw RsError.badRequest(`'expiresInDays' is a whole number from 1 to ${maxDays}`);
      }
      days = d;
    }

    const data = this.data(ctx);
    const id = toHex(crypto.getRandomValues(new Uint8Array(8)));
    const secret = base64UrlEncode(crypto.getRandomValues(new Uint8Array(32)));
    const created = nowSecs();
    const record: JsonObject = {
      name,
      owner: caller.id,
      roles: roles.join(" "),
      extra: { ...caller.extra },
      created,
      expires: created + days * DAY_SECS,
    };
    record.seal = await this.seal(id, secret, record);
    await data.put(this.settings.dataset, id, record);

    const view = publicView(id, record);
    view.key = `${API_KEY_PREFIX}${id}_${secret}`;
    const resp = msg.okJson(view);
    resp.status = 201;
    resp.setHeader("location", `${msg.url.basePath.replace(/\/+$/, "")}/${id}`);
    return resp;
  }

  private async list(msg: Message, ctx: ServiceContext): Promise<Message> {
    const caller = msg.principal;
    if (!caller) throw RsError.unauthorized("listing API keys requires a login");
    const data = this.data(ctx);
    const admin = this.isAdmin(caller);
    const keys: Json[] = [];
    let skip = 0;
    for (;;) {
      const [ids, total] = await data.listKeys(this.settings.dataset, 500, skip);
      if (ids.length === 0) break;
      skip += ids.length;
      for (const id of ids) {
        let record: Json;
        try {
          record = await data.get(this.settings.dataset, id);
        } catch {
          continue;
        }
        if (isObject(record) && (admin || record.owner === caller.id)) keys.push(publicView(id, record));
      }
      if (skip >= total) break;
    }
    return msg.okJson(keys);
  }

  private async revoke(msg: Message, id: string, ctx: ServiceContext): Promise<Message> {
    const caller = msg.principal;
    if (!caller) throw RsError.unauthorized("revoking an API key requires a login");
    const data = this.data(ctx);
    const notFound = () => RsError.notFound(`no API key '${id}'`);
    let record: Json;
    try {
      record = await data.get(this.settings.dataset, id);
    } catch {
      throw notFound();
    }
    const owns = isObject(record) && record.owner === caller.id;
    // Someone else's key: indistinguishable from no key.
    if (!owns && !this.isAdmin(caller)) throw notFound();
    await data.delete(this.settings.dataset, id);
    return msg.noContent();
  }

  authenticator(): Authenticator {
    return this;
  }

  /// Keys come only in `Authorization: Bearer`, never a cookie or the
  /// WebSocket subprotocol, so a key is never an ambient credential.
  ambientCredential(_msg: Message): boolean {
    return false;
  }

  async tryAuthenticate(msg: Message, ctx: ServiceContext): Promise<AuthOutcome> {
    const auth = msg.header("authorization");
    if (auth === undefined || !auth.startsWith("Bearer ")) return { kind: "pass" };
    const key = auth.slice("Bearer ".length).trim();
    if (!key.startsWith(API_KEY_PREFIX)) return { kind: "pass" };
    const parts = splitKey(key);
    if (!parts) throw RsError.unauthorized("malformed API key");
    const [id, secret] = parts;
    const refused = () => RsError.unauthorized("unknown or revoked API key");
    let record: Json;
    try {
      record = await this.data(ctx).get(this.settings.dataset, id);
    } catch (e) {
      if (e instanceof RsError && e.status === 404) throw refused();
      throw e;
    }
    if (!isObject(record) || !(await this.sealMatches(id, secret, record))) throw refused();
    const expires = typeof record.expires === "number" ? record.expires : 0;
    if (expires < nowSecs()) throw RsError.unauthorized("API key expired");
    const extra: JsonObject = isObject(record.extra) ? { ...record.extra } : {};
    extra.keyId = id;
    msg.principal = {
      id: typeof record.owner === "string" ? record.owner : "",
      roles: rolesOf(record.roles),
      kind: API_KEY_KIND,
      extra,
    };
    return { kind: "authenticated", expires };
  }
}
