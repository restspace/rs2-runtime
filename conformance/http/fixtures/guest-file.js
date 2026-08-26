// In-memory `FileStore` adapter — the guest-adapter conformance fixture
// (cloudflare.md P4b), extracted from `rs2-core/tests/guest_adapter.rs` and
// embedded back into it via `include_str!`. Keeps files in a module-level
// map (no backend needed — the socket/pooling path is proven by the Redis
// adapters); implements the store pattern the host's `GuestFileStore`
// speaks: `HEAD`/`GET`/`PUT`/`DELETE`/`MOVE` on `/{path}`, container
// listings on `/{path}/`, content base64 across the JSON boundary.

let FILES = {}; // path -> { data: base64, mediaType }

function b64len(b64) {
  if (!b64) return 0;
  let n = Math.floor((b64.length * 3) / 4);
  if (b64.endsWith("==")) n -= 2;
  else if (b64.endsWith("=")) n -= 1;
  return n;
}

export default async (msg) => {
  const url = String(msg.url);
  const path = url.split("?")[0];
  const qs = new URLSearchParams(url.split("?")[1] || "");
  const isDir = path.endsWith("/");
  const m = msg.method;

  if (m === "HEAD") {
    if (isDir) {
      const exists = path === "/" || Object.keys(FILES).some((k) => k.startsWith(path));
      return exists ? { status: 200, body: { size: 0, isDir: true } } : { status: 404, body: { detail: "no directory" } };
    }
    const f = FILES[path];
    return f
      ? { status: 200, body: { size: b64len(f.data), isDir: false, mediaType: f.mediaType } }
      : { status: 404, body: { detail: "not found" } };
  }

  if (m === "GET" && isDir) {
    const fileSet = new Set(),
      dirSet = new Set();
    for (const k of Object.keys(FILES)) {
      if (path !== "/" && !k.startsWith(path)) continue;
      const rest = path === "/" ? k.slice(1) : k.slice(path.length);
      if (!rest) continue;
      const slash = rest.indexOf("/");
      if (slash === -1) fileSet.add(rest);
      else dirSet.add(rest.slice(0, slash));
    }
    const base = path === "/" ? "/" : path;
    const entries = [];
    for (const d of [...dirSet].sort()) entries.push({ name: d + "/", dir: true, size: 0 });
    for (const f of [...fileSet].sort()) {
      entries.push({ name: f, dir: false, size: b64len(FILES[base + f].data), contentType: FILES[base + f].mediaType });
    }
    const total = entries.length;
    const skip = parseInt(qs.get("$skip") || "0", 10),
      take = parseInt(qs.get("$take") || "1000", 10);
    return { status: 200, body: { entries: entries.slice(skip, skip + take), total } };
  }

  if (m === "GET") {
    const f = FILES[path];
    return f
      ? { status: 200, body: { contentBase64: f.data, mediaType: f.mediaType } }
      : { status: 404, body: { detail: "not found" } };
  }

  if (m === "PUT") {
    const existed = !!FILES[path];
    FILES[path] = { data: msg.body.contentBase64 || "", mediaType: msg.body.mediaType || "application/octet-stream" };
    return { status: existed ? 200 : 201 };
  }

  if (m === "MOVE") {
    const f = FILES[path];
    if (!f) return { status: 404, body: { detail: "source missing" } };
    const existed = !!FILES[msg.body.to];
    FILES[msg.body.to] = f;
    delete FILES[path];
    return { status: existed ? 200 : 201 };
  }

  if (m === "DELETE") {
    if (isDir) {
      const under = Object.keys(FILES).filter((k) => k.startsWith(path));
      if (msg.body && msg.body.recursive) {
        for (const k of under) delete FILES[k];
        return { status: 204 };
      }
      if (under.length > 0) return { status: 409, body: { detail: "directory not empty" } };
      return { status: 204 };
    }
    if (!FILES[path]) return { status: 404, body: { detail: "not found" } };
    delete FILES[path];
    return { status: 204 };
  }

  return { status: 400, body: { detail: "unsupported method" } };
};
