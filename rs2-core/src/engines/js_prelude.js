// RS2 JS compat prelude (PRD §11, G5): the supported web/Node API surface
// for sandboxed JS services. Injected before the service bundle; the raw
// native hooks are captured here and removed from the global scope.
//
// Supported-API list (tied to the npm corpus, tests/npm_compat.rs):
//   console.{log,info,warn,error,debug}
//   fetch / Headers / Request / Response   (via the "fetch" capability grant)
//   setTimeout / clearTimeout / setInterval / clearInterval (virtual time:
//     pending timers fast-forward when the handler is otherwise idle)
//   queueMicrotask, structuredClone
//   TextEncoder / TextDecoder (utf-8)
//   atob / btoa
//   Buffer (subset: from, alloc, concat, isBuffer, toString utf8/base64/hex)
//   URL / URLSearchParams
//   AbortController / AbortSignal (signals never fire: fetch is synchronous)
//   crypto.{getRandomValues, randomUUID}
//   process.{env, nextTick, version, platform}
"use strict";
(() => {
  const nativeFetch = globalThis.__rs2_native_fetch;
  const nativeLog = globalThis.__rs2_native_log;
  const nativeRandom = globalThis.__rs2_native_random;
  delete globalThis.__rs2_native_fetch;
  delete globalThis.__rs2_native_log;
  delete globalThis.__rs2_native_random;

  // ---- console -------------------------------------------------------
  const fmt = (args) =>
    args
      .map((a) => {
        if (typeof a === "string") return a;
        try { return JSON.stringify(a); } catch { return String(a); }
      })
      .join(" ");
  globalThis.console = {
    log: (...a) => nativeLog("info", fmt(a)),
    info: (...a) => nativeLog("info", fmt(a)),
    warn: (...a) => nativeLog("warn", fmt(a)),
    error: (...a) => nativeLog("error", fmt(a)),
    debug: (...a) => nativeLog("debug", fmt(a)),
  };

  // ---- microtasks / clone ---------------------------------------------
  globalThis.queueMicrotask = (fn) => { Promise.resolve().then(fn); };
  globalThis.structuredClone = (v) => (v === undefined ? v : JSON.parse(JSON.stringify(v)));

  // ---- timers (virtual time, fast-forwarded by the engine pump) -------
  let timerSeq = 1;
  let vnow = 0;
  const timers = new Map(); // id -> { due, fn, args, every }
  globalThis.setTimeout = (fn, ms, ...args) => {
    const id = timerSeq++;
    timers.set(id, { due: vnow + Math.max(0, ms | 0), fn, args, every: null });
    return id;
  };
  globalThis.setInterval = (fn, ms, ...args) => {
    const id = timerSeq++;
    const every = Math.max(1, ms | 0);
    timers.set(id, { due: vnow + every, fn, args, every });
    return id;
  };
  globalThis.clearTimeout = (id) => { timers.delete(id); };
  globalThis.clearInterval = (id) => { timers.delete(id); };
  // Engine hook: advance virtual time to the earliest due timer and run
  // everything due. Returns true if any timer ran.
  globalThis.__rs2_runTimers = () => {
    if (timers.size === 0) return false;
    let earliest = Infinity;
    for (const t of timers.values()) earliest = Math.min(earliest, t.due);
    vnow = Math.max(vnow, earliest);
    let ran = false;
    for (const [id, t] of [...timers.entries()]) {
      if (t.due <= vnow) {
        ran = true;
        if (t.every) t.due = vnow + t.every; else timers.delete(id);
        try { t.fn(...t.args); } catch (e) { console.error(`timer threw: ${e}`); }
      }
    }
    return ran;
  };

  // ---- text encoding ---------------------------------------------------
  const utf8Encode = (str) => {
    const out = [];
    for (const ch of str) {
      let cp = ch.codePointAt(0);
      if (cp < 0x80) out.push(cp);
      else if (cp < 0x800) out.push(0xc0 | (cp >> 6), 0x80 | (cp & 0x3f));
      else if (cp < 0x10000)
        out.push(0xe0 | (cp >> 12), 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f));
      else
        out.push(0xf0 | (cp >> 18), 0x80 | ((cp >> 12) & 0x3f),
                 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f));
    }
    return new Uint8Array(out);
  };
  const utf8Decode = (bytes) => {
    let out = "";
    let i = 0;
    const b = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    while (i < b.length) {
      const c = b[i++];
      let cp;
      if (c < 0x80) cp = c;
      else if (c < 0xe0) cp = ((c & 0x1f) << 6) | (b[i++] & 0x3f);
      else if (c < 0xf0)
        cp = ((c & 0x0f) << 12) | ((b[i++] & 0x3f) << 6) | (b[i++] & 0x3f);
      else
        cp = ((c & 0x07) << 18) | ((b[i++] & 0x3f) << 12) |
             ((b[i++] & 0x3f) << 6) | (b[i++] & 0x3f);
      out += String.fromCodePoint(cp);
    }
    return out;
  };
  globalThis.TextEncoder = class TextEncoder {
    encode(str = "") { return utf8Encode(String(str)); }
    get encoding() { return "utf-8"; }
  };
  globalThis.TextDecoder = class TextDecoder {
    decode(bytes) { return bytes == null ? "" : utf8Decode(bytes); }
    get encoding() { return "utf-8"; }
  };

  // ---- base64 ----------------------------------------------------------
  const B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const b64FromBytes = (bytes) => {
    let out = "";
    for (let i = 0; i < bytes.length; i += 3) {
      const [a, b, c] = [bytes[i], bytes[i + 1], bytes[i + 2]];
      out += B64[a >> 2] + B64[((a & 3) << 4) | (b === undefined ? 0 : b >> 4)];
      out += b === undefined ? "=" : B64[((b & 15) << 2) | (c === undefined ? 0 : c >> 6)];
      out += c === undefined ? "=" : B64[c & 63];
    }
    return out;
  };
  const b64ToBytes = (text) => {
    const clean = String(text).replace(/[=\s]+$/, "");
    const out = [];
    let buffer = 0, bits = 0;
    for (const ch of clean) {
      const idx = B64.indexOf(ch);
      if (idx < 0) throw new Error("invalid base64");
      buffer = (buffer << 6) | idx;
      bits += 6;
      if (bits >= 8) { bits -= 8; out.push((buffer >> bits) & 0xff); }
    }
    return new Uint8Array(out);
  };
  globalThis.btoa = (str) => {
    const bytes = [];
    for (const ch of String(str)) {
      const code = ch.charCodeAt(0);
      if (code > 0xff) throw new Error("btoa: character out of latin1 range");
      bytes.push(code);
    }
    return b64FromBytes(bytes);
  };
  globalThis.atob = (b64) => {
    const bytes = b64ToBytes(b64);
    let out = "";
    for (const b of bytes) out += String.fromCharCode(b);
    return out;
  };

  // ---- Buffer (Node subset) -------------------------------------------
  class Buffer extends Uint8Array {
    static isBuffer(v) { return v instanceof Buffer; }
    static from(value, encoding) {
      if (typeof value === "string") {
        if (encoding === "base64") return new Buffer(b64ToBytes(value));
        if (encoding === "hex") {
          const out = new Uint8Array(value.length / 2);
          for (let i = 0; i < out.length; i++)
            out[i] = parseInt(value.substr(i * 2, 2), 16);
          return new Buffer(out);
        }
        return new Buffer(utf8Encode(value));
      }
      return new Buffer(value);
    }
    static alloc(n) { return new Buffer(n); }
    static concat(list) {
      const total = list.reduce((n, b) => n + b.length, 0);
      const out = new Buffer(total);
      let offset = 0;
      for (const b of list) { out.set(b, offset); offset += b.length; }
      return out;
    }
    static byteLength(str) { return utf8Encode(String(str)).length; }
    toString(encoding = "utf8") {
      if (encoding === "base64") return b64FromBytes(this);
      if (encoding === "hex")
        return [...this].map((b) => b.toString(16).padStart(2, "0")).join("");
      return utf8Decode(this);
    }
  }
  globalThis.Buffer = Buffer;

  // ---- URL / URLSearchParams -------------------------------------------
  class URLSearchParams {
    #pairs = [];
    constructor(init) {
      if (typeof init === "string") {
        for (const part of init.replace(/^\?/, "").split("&")) {
          if (!part) continue;
          const eq = part.indexOf("=");
          const k = eq < 0 ? part : part.slice(0, eq);
          const v = eq < 0 ? "" : part.slice(eq + 1);
          this.#pairs.push([decodeURIComponent(k.replace(/\+/g, " ")),
                            decodeURIComponent(v.replace(/\+/g, " "))]);
        }
      } else if (init instanceof URLSearchParams) {
        this.#pairs = [...init.entries()];
      } else if (Array.isArray(init)) {
        this.#pairs = init.map(([k, v]) => [String(k), String(v)]);
      } else if (init && typeof init === "object") {
        this.#pairs = Object.entries(init).map(([k, v]) => [k, String(v)]);
      }
    }
    append(k, v) { this.#pairs.push([String(k), String(v)]); }
    set(k, v) { this.delete(k); this.append(k, v); }
    delete(k) { this.#pairs = this.#pairs.filter(([pk]) => pk !== k); }
    get(k) { const hit = this.#pairs.find(([pk]) => pk === k); return hit ? hit[1] : null; }
    getAll(k) { return this.#pairs.filter(([pk]) => pk === k).map(([, v]) => v); }
    has(k) { return this.#pairs.some(([pk]) => pk === k); }
    entries() { return this.#pairs[Symbol.iterator](); }
    keys() { return this.#pairs.map(([k]) => k)[Symbol.iterator](); }
    values() { return this.#pairs.map(([, v]) => v)[Symbol.iterator](); }
    forEach(fn) { for (const [k, v] of this.#pairs) fn(v, k, this); }
    [Symbol.iterator]() { return this.entries(); }
    get size() { return this.#pairs.length; }
    toString() {
      return this.#pairs
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
        .join("&");
    }
  }
  class URL {
    constructor(input, base) {
      let href = String(input);
      if (base !== undefined && !/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(href)) {
        const b = base instanceof URL ? base : new URL(String(base));
        href = href.startsWith("/")
          ? `${b.origin}${href}`
          : `${b.origin}${b.pathname.replace(/[^/]*$/, "")}${href}`;
      }
      const m = href.match(
        /^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/([^/?#:]*)(?::(\d+))?([^?#]*)(\?[^#]*)?(#.*)?$/
      );
      if (!m) throw new TypeError(`Invalid URL: ${href}`);
      this.protocol = `${m[1]}:`;
      this.hostname = m[2];
      this.port = m[3] ?? "";
      this.pathname = m[4] || "/";
      this.search = m[5] ?? "";
      this.hash = m[6] ?? "";
      this.searchParams = new URLSearchParams(this.search);
    }
    get host() { return this.port ? `${this.hostname}:${this.port}` : this.hostname; }
    get origin() { return `${this.protocol}//${this.host}`; }
    get href() {
      const search = this.searchParams.size ? `?${this.searchParams}` : "";
      return `${this.origin}${this.pathname}${search}${this.hash}`;
    }
    toString() { return this.href; }
  }
  globalThis.URL = URL;
  globalThis.URLSearchParams = URLSearchParams;

  // ---- AbortController (signals never fire: fetch is synchronous) ------
  class AbortSignal {
    aborted = false;
    reason = undefined;
    static timeout() { return new AbortSignal(); }
    static abort(reason) { const s = new AbortSignal(); s.aborted = true; s.reason = reason; return s; }
    addEventListener() {}
    removeEventListener() {}
    throwIfAborted() { if (this.aborted) throw this.reason ?? new Error("aborted"); }
  }
  class AbortController {
    signal = new AbortSignal();
    abort(reason) { this.signal.aborted = true; this.signal.reason = reason; }
  }
  globalThis.AbortSignal = AbortSignal;
  globalThis.AbortController = AbortController;

  // ---- crypto -----------------------------------------------------------
  globalThis.crypto = {
    getRandomValues(arr) {
      const bytes = nativeRandom(arr.byteLength ?? arr.length);
      const view = new Uint8Array(arr.buffer ?? arr, arr.byteOffset ?? 0);
      for (let i = 0; i < view.length; i++) view[i] = bytes[i];
      return arr;
    },
    randomUUID() {
      const b = nativeRandom(16);
      b[6] = (b[6] & 0x0f) | 0x40;
      b[8] = (b[8] & 0x3f) | 0x80;
      const hex = [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    },
  };

  // ---- process ----------------------------------------------------------
  globalThis.process = {
    env: {},
    nextTick: (fn, ...args) => queueMicrotask(() => fn(...args)),
    version: "v22.12.0",
    versions: { node: "22.12.0" },
    platform: "rs2",
  };
  globalThis.global = globalThis;

  // Presence-only WebSocket: SDKs (supabase realtime) sniff for it at
  // construction; actually opening a socket is out of scope for v1.
  globalThis.WebSocket = class WebSocket {
    constructor() {
      throw new Error("WebSocket connections are not available in RS2 v1");
    }
  };

  // Minimal DOM event model: SDK builds for workers extend these (stripe).
  class Event {
    constructor(type, init = {}) {
      this.type = String(type);
      this.bubbles = !!init.bubbles;
      this.cancelable = !!init.cancelable;
      this.defaultPrevented = false;
    }
    preventDefault() { if (this.cancelable) this.defaultPrevented = true; }
    stopPropagation() {}
  }
  class CustomEvent extends Event {
    constructor(type, init = {}) { super(type, init); this.detail = init.detail; }
  }
  class EventTarget {
    #listeners = new Map();
    addEventListener(type, fn) {
      const list = this.#listeners.get(type) ?? [];
      list.push(fn);
      this.#listeners.set(type, list);
    }
    removeEventListener(type, fn) {
      const list = this.#listeners.get(type) ?? [];
      this.#listeners.set(type, list.filter((f) => f !== fn));
    }
    dispatchEvent(event) {
      for (const fn of this.#listeners.get(event.type) ?? []) fn.call(this, event);
      return !event.defaultPrevented;
    }
  }
  globalThis.Event = Event;
  globalThis.CustomEvent = CustomEvent;
  globalThis.EventTarget = EventTarget;

  // Presence-only streams: SDKs reference these for instanceof checks and
  // streaming responses; body streaming is out of scope for v1 (bodies
  // materialize at the engine boundary).
  globalThis.ReadableStream = class ReadableStream {
    constructor() {}
    getReader() { throw new Error("response streaming is not available in RS2 v1"); }
  };
  globalThis.WritableStream = class WritableStream {};
  globalThis.TransformStream = class TransformStream {};

  // Blob / File / FormData: enough for SDK presence checks and text-based
  // payload assembly (binary multipart uploads are out of scope for v1).
  class Blob {
    #text;
    constructor(parts = [], options = {}) {
      this.#text = parts
        .map((p) => (typeof p === "string" ? p : p instanceof Blob ? p.__text : utf8Decode(p)))
        .join("");
      this.type = options.type ?? "";
    }
    get __text() { return this.#text; }
    get size() { return utf8Encode(this.#text).length; }
    text() { return Promise.resolve(this.#text); }
    arrayBuffer() { return Promise.resolve(utf8Encode(this.#text).buffer); }
    slice() { return new Blob([this.#text], { type: this.type }); }
  }
  class File extends Blob {
    constructor(parts, name, options = {}) {
      super(parts, options);
      this.name = String(name);
      this.lastModified = options.lastModified ?? 0;
    }
  }
  class FormData {
    #pairs = [];
    append(k, v, filename) { this.#pairs.push([String(k), v, filename]); }
    set(k, v, filename) { this.delete(k); this.append(k, v, filename); }
    delete(k) { this.#pairs = this.#pairs.filter(([pk]) => pk !== k); }
    get(k) { const hit = this.#pairs.find(([pk]) => pk === k); return hit ? hit[1] : null; }
    getAll(k) { return this.#pairs.filter(([pk]) => pk === k).map(([, v]) => v); }
    has(k) { return this.#pairs.some(([pk]) => pk === k); }
    entries() { return this.#pairs.map(([k, v]) => [k, v])[Symbol.iterator](); }
    forEach(fn) { for (const [k, v] of this.entries()) fn(v, k, this); }
    [Symbol.iterator]() { return this.entries(); }
  }
  globalThis.Blob = Blob;
  globalThis.File = File;
  globalThis.FormData = FormData;

  // ---- fetch ------------------------------------------------------------
  class Headers {
    #map = new Map();
    constructor(init) {
      if (init instanceof Headers) {
        for (const [k, v] of init.entries()) this.#map.set(k, v);
      } else if (Array.isArray(init)) {
        for (const [k, v] of init) this.append(k, v);
      } else if (init && typeof init === "object") {
        for (const [k, v] of Object.entries(init)) this.set(k, v);
      }
    }
    set(k, v) { this.#map.set(String(k).toLowerCase(), String(v)); }
    append(k, v) {
      const key = String(k).toLowerCase();
      const prior = this.#map.get(key);
      this.#map.set(key, prior === undefined ? String(v) : `${prior}, ${v}`);
    }
    get(k) { return this.#map.get(String(k).toLowerCase()) ?? null; }
    has(k) { return this.#map.has(String(k).toLowerCase()); }
    delete(k) { this.#map.delete(String(k).toLowerCase()); }
    entries() { return this.#map.entries(); }
    keys() { return this.#map.keys(); }
    values() { return this.#map.values(); }
    forEach(fn) { for (const [k, v] of this.#map) fn(v, k, this); }
    [Symbol.iterator]() { return this.entries(); }
    toJSON() { return Object.fromEntries(this.#map); }
  }
  class Response {
    constructor(bodyText, { status = 200, headers } = {}) {
      this.status = status;
      this.headers = headers instanceof Headers ? headers : new Headers(headers);
      this.ok = status >= 200 && status < 300;
      this.statusText = "";
      this.bodyUsed = false;
      this.__bodyText = bodyText ?? "";
    }
    text() { this.bodyUsed = true; return Promise.resolve(this.__bodyText); }
    json() { this.bodyUsed = true; return Promise.resolve(JSON.parse(this.__bodyText)); }
    arrayBuffer() {
      this.bodyUsed = true;
      return Promise.resolve(utf8Encode(this.__bodyText).buffer);
    }
    clone() { return new Response(this.__bodyText, { status: this.status, headers: this.headers }); }
  }
  class Request {
    constructor(input, init = {}) {
      this.url = input instanceof Request ? input.url : String(input);
      this.method = (init.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();
      this.headers = new Headers(init.headers ?? (input instanceof Request ? input.headers : undefined));
      this.body = init.body ?? (input instanceof Request ? input.body : null);
      this.signal = init.signal ?? (input instanceof Request ? input.signal : undefined);
    }
    clone() { return new Request(this); }
    text() {
      const b = this.body;
      if (b == null) return Promise.resolve("");
      if (typeof b === "string") return Promise.resolve(b);
      if (b instanceof URLSearchParams) return Promise.resolve(b.toString());
      if (b instanceof Uint8Array) return Promise.resolve(utf8Decode(b));
      return Promise.resolve(JSON.stringify(b));
    }
  }
  globalThis.Headers = Headers;
  globalThis.Response = Response;
  globalThis.Request = Request;
  globalThis.fetch = (input, init = {}) => {
    try {
      const req = new Request(input, init);
      let bodyText = null;
      const body = init.body ?? (input instanceof Request ? input.body : null);
      if (body != null) {
        if (typeof body === "string") bodyText = body;
        else if (body instanceof URLSearchParams) bodyText = body.toString();
        else if (body instanceof Uint8Array) bodyText = utf8Decode(body);
        else bodyText = JSON.stringify(body);
      }
      const out = nativeFetch({
        url: req.url,
        method: req.method,
        headers: req.headers.toJSON(),
        body: bodyText,
      });
      return Promise.resolve(
        new Response(out.body ?? "", { status: out.status, headers: out.headers })
      );
    } catch (e) {
      return Promise.reject(e);
    }
  };
})();
