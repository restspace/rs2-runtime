// Engine-backed custom services (`code:<name>@<version>`). Port of the
// name rules and constants from `rs2-core/src/services/code.rs`; the
// Dynamic Worker engine lands in P4 (cloudflare.md §H), so every request
// answers 501 for now.

import { sha256Hex } from "../runtime/crypto";
import { RsError } from "../runtime/error";
import type { Message } from "../runtime/message";
import type { Service, ServiceContext } from "./context";

/// Storage prefix for deployed code in the tenant's file store.
export const CODE_PREFIX = ".rs2-code";
/// Storage prefix for `store` grants (service-private storage).
export const STORE_GRANT_PREFIX = ".rs2-store";
export const BODY_REF_HEADER = "x-rs2-body-ref";
export const BASE_PATH_HEADER = "x-rs2-base-path";

/// Content-addressed version of a bundle: sha256[0..8] hex.
export async function versionOf(bytes: Uint8Array): Promise<string> {
  return (await sha256Hex(bytes)).slice(0, 16);
}

export function codePath(name: string, version: string): string {
  return `${CODE_PREFIX}/${name}/${version}.wasm`;
}

export function codePathJs(name: string, version: string): string {
  return `${CODE_PREFIX}/${name}/${version}.js`;
}

export class CodeService implements Service {
  private constructor(
    readonly name: string,
    readonly version: string,
  ) {}

  /// Parse a `code:<name>@<version>` service reference.
  static fromRef(serviceRef: string): CodeService {
    if (!serviceRef.startsWith("code:")) throw RsError.badRequest("not a code: service reference");
    const rest = serviceRef.slice("code:".length);
    const at = rest.indexOf("@");
    if (at < 0) throw RsError.badRequest(`code reference '${serviceRef}' must be 'code:<name>@<version>'`);
    const name = rest.slice(0, at);
    const version = rest.slice(at + 1);
    if (name === "" || version === "" || /[/\\.]/.test(name)) {
      throw RsError.badRequest(`invalid code reference '${serviceRef}'`);
    }
    return new CodeService(name, version);
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const files = ctx.files;
    if (!files) throw RsError.internal("code service has no file capability");
    void msg;
    // Same load order as Rust: a `.wasm` bundle is never runnable here; a
    // `.js` bundle waits for the Dynamic Worker engine (P4).
    try {
      await files.head(codePath(this.name, this.version));
      throw RsError.engineUnavailable(
        `code:${this.name}@${this.version} is a wasm component but this build has no wasm engine (rebuild with --features wasm)`,
      );
    } catch (e) {
      if (!(e instanceof RsError) || e.status !== 404) throw e;
    }
    try {
      await files.head(codePathJs(this.name, this.version));
    } catch (e) {
      if (e instanceof RsError && e.status === 404) {
        throw RsError.notFound(
          `deployed code '${this.name}@${this.version}' not found — deploy it via PUT /code/${this.name}`,
        );
      }
      throw e;
    }
    throw RsError.engineUnavailable(
      `code:${this.name}@${this.version} is a JS bundle but this host has no JS engine yet (cloudflare.md §H P4)`,
    );
  }
}
