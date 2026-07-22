# Appendix G — How the JavaScript (V8 isolate) engine works

Part 8 told you *how to write* a JavaScript service: a single-file ES module,
a `handle` function, a `ctx` object for talking to the outside world. This
appendix explains *what happens underneath* when that service runs — how RS2
takes your JavaScript and executes it safely next to other tenants' code.

You don't need any of this to ship a service. Read it if you want to understand
why globals don't survive between requests, why `ctx.request` is synchronous,
how a runaway loop gets killed, or how a custom database adapter can keep a
connection open across requests.

The audience is a working front-end developer: if you know what "the browser
runs my JavaScript in a sandbox" means, you already know the core idea.

---

## 1. What a V8 isolate is

**V8** is the JavaScript engine inside Google Chrome and Node.js. It's the
component that takes JavaScript source text, compiles it to machine code, and
runs it. RS2 embeds the *same* engine (via a Rust library called `deno_core`,
more on that below) directly inside the server process.

An **isolate** is V8's unit of isolation — think of it as one completely
self-contained JavaScript world:

- It has its own heap (its own memory for objects, its own garbage collector).
- It has its own set of globals (`globalThis`, the built-in `Array`, `JSON`,
  etc.).
- **Nothing leaks between isolates.** Two isolates can't see each other's
  variables, objects, or memory, even though they live in the same OS process.

This is exactly the mechanism a browser uses to keep one website's JavaScript
from reading another's. A browser tab is (roughly) an isolate. RS2 uses the
same boundary to keep one tenant's service from touching another's.

A crucial property: **a fresh isolate starts with nothing in it.** It has the
JavaScript language built-ins and nothing else — no `fetch`, no `console`, no
file access, no network. Everything your service can do has to be *deliberately
installed* into the isolate by the host. That "nothing by default" starting
point is what makes the capability model (Part 3.5, Part 8.5) enforceable: a
service can only reach what RS2 chose to hand it.

---

## 2. One isolate per request

The headline design decision:

> **RS2 builds a brand-new isolate for every single invocation of a JS service,
> then throws it away when the request finishes.** (The exception is loadable
> adapters and compiled templates — see §10.)

This is the most conservative possible choice, and it's deliberate (PRD open
question 2, resolved in favour of full isolation). The consequences you can
observe from your code:

| Behaviour | Why |
| --- | --- |
| Global variables reset between requests | The isolate that held them was destroyed |
| No data leaks from one request to the next | Each request gets a virgin world |
| Cross-request state must go through `ctx.state` | That's the only thing wired to outlive the isolate (8.2) |
| One tenant can't poison another's JS environment | They never share an isolate |

If you've used serverless functions (AWS Lambda and friends), the mental model
is similar: assume nothing survives between calls, and put anything durable in
explicit storage.

"Build a whole JavaScript engine instance per request" sounds expensive, but
RS2 boots it from a checked-in prelude snapshot rather than re-evaluating all
compat code on every call. That keeps the isolation boundary while avoiding
most setup work. Warm isolates remain reserved for the service types whose
contract calls for residency.

---

## 3. deno_core: the embedding layer

RS2 doesn't talk to raw V8 directly. It uses **`deno_core`**, the foundational
library extracted from the Deno runtime. (Deno is an alternative to Node.js;
`deno_core` is its lowest layer — the part that embeds V8 and provides the
plumbing to connect JavaScript to native code.)

Two pieces of `deno_core` matter here:

- **Ops** — short for "operations." An op is a native (Rust) function that you
  expose to JavaScript. From the guest's side it looks like a magic built-in
  function; on the host side it's regular native code. Ops are the *only*
  doorway from the sandboxed JavaScript back into RS2.
- **The op state / resource table** — a place to hang native-side data that ops
  can read, scoped to one runtime. RS2 uses this to stash the current
  invocation's context (which tenant, which capabilities are granted, open
  sockets, and so on).

Importantly, RS2 uses `deno_core` but **not** the full Deno runtime. It does not
ship Deno's Node-compatibility layer or its large built-in API surface. Instead
RS2 installs a small, deliberately-chosen set of web-style APIs itself (§5).
That keeps the trusted surface small and auditable — every API your service can
call is one RS2 explicitly decided to provide.

---

## 4. Bootstrapping an isolate: from empty world to ready handler

When a request arrives for a JS mount, RS2 assembles the isolate in a fixed
order. Conceptually:

```
[ empty V8 isolate ]
        │
        │  1. install the host ops (the native doorways)
        ▼
[ isolate + ops ]
        │
        │  2. run the BOOTSTRAP script
        ▼
[ + ctx wiring, RS2Socket, __rs2_dispatch ]
        │
        │  3. run the COMPAT PRELUDE
        ▼
[ + console, fetch, TextEncoder, Buffer, URL, timers, … ]
        │
        │  4. load + evaluate YOUR bundle (ES module)
        ▼
[ + your default export captured ]
        │
        │  5. call __rs2_dispatch(yourHandler, msg, config)
        ▼
[ response envelope ]
```

A few of these steps deserve a closer look.

**Step 2 — the bootstrap.** This is a tiny RS2-controlled script that wires the
raw ops into the friendly shapes your code sees. It's what builds the `ctx`
object — `ctx.request`, `ctx.log`, `ctx.state.get/put` — each of which is a thin
wrapper that calls the corresponding op. It also installs `RS2Socket` (§7) and
`__rs2_dispatch`, the internal entry point that locates your handler (whether you
exported a function or a `{ handle }` object) and calls it.

**Step 3 — the compat prelude.** This is the JavaScript file that defines the
web-style APIs (`fetch`, `console`, `Buffer`, etc.) on top of the ops. It's the
implementation behind Appendix... well, behind §5 below and manual §8.3. Because
it runs *before* your bundle, by the time your code executes those globals simply
exist.

In the production path, the stable bootstrap/prelude state is restored from a
generated V8 snapshot. The source and snapshot hash are checked together in the
test suite, so changing the supported API requires regenerating the snapshot;
the diagram above remains the conceptual order.

**Step 4 — your bundle.** Your module is loaded and evaluated *as a module*.
This is why **module top-level code runs at this point, before any request data
is available** — and why doing I/O at the top level fails (manual §8.2). Top-level
is for imports and pure setup; the request hasn't conceptually "happened" yet
when it runs. RS2 then grabs your `default` export and holds onto it.

**Step 5 — dispatch.** RS2 calls `__rs2_dispatch(yourHandler, msg, config)`,
which builds `ctx` and invokes your handler with the incoming message. Whatever
you return (or throw) becomes the response (§6).

---

## 5. The host bridge: how `ctx.request` crosses the boundary

This is the most conceptually interesting part of the engine, because it
reconciles two worlds that don't naturally fit:

- Your JavaScript sees host calls as **synchronous**: `ctx.request("orders", …)`
  returns a value directly; you don't have to `await` it (though awaiting works).
- The RS2 host is **asynchronous**: under the hood, calling another service may
  do real network or disk I/O that the server handles concurrently with
  thousands of other requests.

Here's the arrangement that squares them.

The isolate runs on its **own dedicated thread** (V8 isolates can't be moved
between threads, so each one is pinned to one). The rest of RS2 — the part that
actually performs I/O, re-dispatches internal calls, talks to the network —
runs on the server's **main async runtime**, on other threads.

When your code calls `ctx.request`:

1. The call enters the `op_rs2_request` op (native code) on the isolate's
   thread.
2. The op hands the actual work to the main runtime and **parks the isolate
   thread**, blocking it until the answer comes back over an internal channel.
3. The main runtime does the real work — re-entering the full RS2 dispatch path,
   so the internal call is authorized, rate-limited, traced, and idempotency-
   checked exactly like an external request (this is why a custom service can't
   use internal calls to dodge authorization).
4. The result travels back; the isolate thread un-parks and the op returns the
   value to your JavaScript.

From your code's point of view this is just a function that returned. Under the
hood, the isolate thread was asleep while the server stayed fully responsive to
everything else.

> Why not let the isolate `await` the host future directly? The isolate thread
> is *already inside* an async runtime (it's driving JavaScript's own event
> loop). Trying to block-on a second runtime from inside the first would
> deadlock the engine. The channel hand-off to the main runtime sidesteps that.

Every op follows this pattern — `ctx.log`, `ctx.state.get/put`, `fetch`,
sockets. They are the complete, enumerable list of doorways out of the sandbox.
There is no other way out; an isolate with no granted capabilities is a sealed
box that can compute but cannot touch anything.

### Errors keep their identity across the boundary

When the host refuses a call — say you named a capability you weren't granted —
it produces a structured RS2 error (`capability_denied`, with an HTTP status).
That structure can't pass through V8 natively, so the op returns a small marker
object and the bootstrap re-throws it as a normal JavaScript `Error` whose
`.code` and `.status` properties carry the original meaning:

```js
try {
  ctx.request("payments", { url: "/charge" });
} catch (e) {
  if (e.code === "capability_denied") { /* you can branch on it */ }
}
```

If you *don't* catch it, RS2 still recovers the original structured error on the
host side, so the final HTTP response is the correct `problem+json` (manual §9.2)
rather than a generic "the JS threw." The error's identity is preserved either
way.

---

## 6. Marshaling: how a Message becomes `msg` and back

Your handler doesn't receive RS2's internal message object directly; it receives
a plain JavaScript object. The engine **marshals** (translates) between the two:

**Inbound (default).** Before your code runs, RS2 takes the request message and, subject
to the body-size cap (see §9), materializes the body. If the body is JSON it's
parsed into a real JavaScript value; otherwise it arrives as a string. Headers
become a plain object, and you get:

```js
{ method, url, headers, body, bodySize, mediaType }
```

Three JS mount flags change that crossing. `bodyPassthrough` carries the body
around the isolate unchanged; `requestStreaming` lets the guest pull chunks with
`ctx.readBody`/`ctx.body`; `responseStreaming` lets it open a host-backed writer
with `ctx.beginStream`. The guest never receives a host stream object — chunks
cross through bounded ops — which preserves backpressure and the same byte and
wall-clock limits. See manual §8.2 for the public contract.

**Outbound.** Whatever your handler returns is normalized into a response:

| You return | Becomes |
| --- | --- |
| `{ status?, headers?, body?, mediaType? }` | that response |
| a plain object without `status`/`body` | `200` with that object as a JSON body |
| a string | `200`, `text/plain` |
| `null` / `undefined` | `204 No Content` |

The engine then converts that back into a real RS2 message — a JSON body is
re-serialized, headers are validated and set, the status is applied — and hands
it back to the dispatch path, where the cross-cutting concerns (caching, CORS,
logging) take over.

---

## 7. Timers without a clock, and the event loop

RS2's JS engine has **no real event loop in the Node sense** — there's no
background thread of timers and I/O callbacks firing over wall-clock time. Host
calls are synchronous (§5), so the *only* genuinely asynchronous surface left in
your code is JavaScript promises and timers (`setTimeout` and friends).

The engine handles this with a **virtual-time** trick. After calling your
handler, it repeatedly:

1. Drains all pending microtasks (resolves promises that are ready).
2. Checks whether your handler's returned promise has settled. If yes, done.
3. If not, and the only thing left is pending timers, it **fast-forwards** the
   clock to the earliest due timer and fires it — instantly, with no real
   waiting.

The practical payoff: SDK retry logic that says "wait 2 seconds and try again"
(the classic `429 Too Many Requests` + `Retry-After` backoff loop) **completes
immediately** instead of actually sleeping. Your service stays fast, and real
third-party SDKs (Stripe, OpenAI, etc.) that lean on backoff loops work
unmodified (manual §8.3).

What this also means: code that genuinely needs real elapsed wall-clock time, or
background work that continues after the response, is **out of scope** — there's
no event loop to keep it alive. A handler must drive itself to a settled result.
If a handler's promise can never settle (it's waiting on a timer that will never
come due and has no host work pending), the engine detects the stall and fails
the request rather than hanging.

---

## 8. The supported API surface (the compat prelude)

The web-style globals available to your code are **exactly** the ones the compat
prelude installs — an explicit allowlist, not "whatever Node happens to have."
Manual §8.3 is the user-facing list; here's the engineering framing:

- **`fetch` / `Headers` / `Request` / `Response`** — outbound HTTP, but routed
  through the `fetch` *capability*, which is host-allowlisted per mount
  (`"grants": { "fetch": { "type": "httpOut", "hosts": [...] } }`). A disallowed
  host fails with `capability_denied` *before any network packet leaves*. So
  `fetch` looks standard but is fenced.
- **Timers** — `setTimeout`/`setInterval`/etc., implemented as the virtual-time
  timers of §7.
- **Encoding & utility** — `console`, `TextEncoder`/`TextDecoder`, `atob`/`btoa`,
  `Buffer`, `URL`/`URLSearchParams`, `structuredClone`, `queueMicrotask`,
  `AbortController`/`AbortSignal`.
- **`crypto`** — `getRandomValues` and `randomUUID`, backed by a host op that
  supplies real random bytes.
- **`process`** — a minimal shim: `process.env`, `process.nextTick`,
  `process.version`.

Anything outside this list isn't there. This is a security posture, not an
oversight: the smaller the surface, the smaller the trusted code base, and the
fewer the ways sandboxed code can surprise you. New compat APIs are added only
with a test that proves a real SDK needs them.

---

## 9. Containing untrusted code: the limits

A JS service is untrusted code running in your process next to other tenants.
Three independent mechanisms keep one service from harming the node, each
enforced by the *host*, not trusted to the guest:

**Wall-clock limit (the watchdog).** When dispatch starts, RS2 spawns a small
watchdog thread that watches a deadline. If your handler is still running when
the deadline passes — including a tight infinite loop that never yields — the
watchdog calls V8's `terminate_execution`, which forcibly unwinds the isolate.
The engine maps that kill to a structured `limit_exceeded` (wall clock) error
rather than letting it look like a crash.

**Memory limit (the heap cap).** The isolate is created with a hard heap ceiling
(`InvocationLimits.memory_bytes`, default 128 MiB). RS2 registers a
"near-the-limit" callback: when the isolate approaches the ceiling — for
instance an allocation bomb building an ever-growing array — the callback flags
it and terminates execution. Crucially this yields a clean, structured
`limit_exceeded` (memory) error; it does **not** let V8 abort the whole OS
process the way an out-of-memory normally would. One tenant's allocation bomb is
that tenant's failed request, not a node outage.

**Outbound-call budget.** Every `ctx.request` / `fetch` increments a counter;
past the budget, further calls fail. This bounds how much amplified load one
invocation can generate downstream.

Above all three sits a **per-tenant circuit breaker** (manual §9.5): a tenant
that keeps tripping these limits gets tripped *open* for a cooldown, so RS2 stops
paying to spin up and kill isolates for code that's pathological. This is what
keeps a hostile tenant's infinite loops from degrading a neighbour tenant's
latency (the G3 containment result). The defense is layered: kill inside the
isolate → concurrency admission → circuit breaker.

There's also a **deploy-time compile check**: when you `PUT` a bundle, RS2 spins
up a throwaway isolate and loads/evaluates the module with no request behind it.
A bundle that fails to parse or throws at module top level is rejected at deploy
time, not at first request.

---

## 10. Loadable adapters: the resident-runtime exception

Everything above describes the per-request model: build an isolate, run one
handler, discard it. There is one important variation — **loadable adapters**
(G13), where a deployed JS bundle backs a file/data/query store, an SMS
provider, or a compiled template rather than serving ordinary one-shot requests.

The per-request model has a problem for this use case: if the isolate is
destroyed after every request, a database connection opened during one request
can't survive to the next, so you'd reconnect on every single call. Pooling a
connection requires the isolate to *stay alive*.

So adapters run as **resident runtimes**:

- The bundle is evaluated **once** per resident runtime on its own long-lived
  thread.
- Each runtime then loops, servicing many "jobs" against the **same** isolate.
- Because the isolate persists, a socket the adapter opens during job N is still
  open for job N+1. The adapter caches the connection in an ordinary
  module-level JavaScript variable, and it pools naturally — the proof case
  holds exactly one connection open across an entire test suite.

The engine reuses the same building blocks: the same "build an isolate" step and
the same "dispatch one call" step, just arranged as *build-once, dispatch-many*
instead of *build-once, dispatch-once*. Host socket I/O still hops back to the
main runtime exactly as in §5.

Lifecycle is tied to the tenant. An adapter mount grows a small pool lazily under
concurrent load, up to `store.maxRuntimes` (default 4); each member serializes
its own jobs and dispatch chooses a least-busy member. Members idle beyond
`store.idleMs`/`idleSeconds` (default 60 seconds, `0` disables eviction) are
dropped, closing their sockets. A config change drops the whole pool and lets
the next request start the new version. There is no node-global guest pool.

---

## 11. Putting it together

A single JS request, end to end:

1. Dispatch routes the request to a JS `code:` mount and assembles the granted
   capabilities for it (default-deny).
2. The request body is materialized (capped) and marshaled to a plain `msg`, or
   crosses through the selected passthrough/streaming mode.
3. RS2 builds a fresh isolate on its own thread, installs ops, runs the
   bootstrap + compat prelude, and loads + evaluates your bundle.
4. It calls your handler. Each `ctx.request`/`fetch`/socket op parks the isolate
   thread and runs the real work on the main runtime; promises and timers are
   driven to settle in virtual time.
5. A watchdog and a heap cap stand ready to kill a runaway or memory-bomb
   handler into a structured limit error.
6. Your return value is normalized and marshaled back into a response message,
   or the host-backed response writer completes its stream.
7. The isolate is discarded (or, for a resident adapter, kept warm for the next
   job).

The throughline matches the rest of RS2: **the host owns every doorway and every
limit; the guest gets a sealed world plus exactly the capabilities it was
granted.**

For the WebAssembly engine — the other way to run custom code, with stronger
language choice and stricter isolation — see Appendix H.

---

← [Appendix F — Further reading](F-further-reading.md) · [Manual home](../README.md) · [Next: Appendix H — How the WebAssembly engine works →](H-wasmtime-engine.md)
