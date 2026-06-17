# Appendix H — How the WebAssembly (Wasmtime) engine works

Manual §8.4 told you how to *write* a WebAssembly service: scaffold a Rust
project against the published contract, fill in a `handle` function, compile to a
`.wasm` file, deploy it. This appendix explains *what happens underneath* — what
WebAssembly is, what Wasmtime does with it, and how the same capability and limit
guarantees that govern the JavaScript engine (Appendix G) are enforced across a
very different boundary.

As with Appendix G, you don't need any of this to ship a service. Read it if you
want to understand what a "component" actually is, why bodies are buffered at the
boundary, or how a Wasm service is killed when it runs too long.

The audience is a front-end developer. If you've ever seen "this library is
compiled to WebAssembly for speed" on an npm package, you've met WASM already —
this fills in the rest.

---

## 1. What WebAssembly is

**WebAssembly (WASM)** is a portable, low-level instruction format — a kind of
universal machine code. You don't write it by hand; you write in a normal
language (Rust, C, Go, and others) and *compile* it to WASM, the same way you'd
compile to x86 or ARM. The difference is that WASM isn't tied to any real CPU: a
single `.wasm` file runs unchanged anywhere there's a WASM runtime.

It was created so browsers could run code written in languages other than
JavaScript at near-native speed — video editors, games, CAD tools on the web.
The same technology turned out to be ideal for server-side sandboxing, for two
reasons that matter a lot to RS2:

- **It's fast.** WASM is compiled to real machine code and runs close to native
  speed — typically much faster than interpreted JavaScript for compute-heavy
  work.
- **It's sandboxed by construction.** This is the key property (§2).

The trade-off versus the JavaScript engine: WASM means a compile step and
(usually) a systems language like Rust, rather than "paste in some JS." Manual
§8.4 frames the choice — reach for WASM when you want performance, type safety,
or a non-JS language; reach for JS for quick handlers and the npm ecosystem.

---

## 2. Why WASM is a sandbox

A WASM module runs inside a **linear memory** — think of it as one big array of
bytes that is the *entire* world the module can address. The module can read and
write only within that array. It has:

- **No ambient access to anything.** No file system, no network, no clock, no
  environment variables — none of it, unless the host explicitly provides it.
- **No way to reach outside its memory.** A bug or an attack that tries to read
  past the end of its memory simply traps (aborts safely); it cannot reach the
  host's memory or another module's memory.

This is a *stronger* default than even a V8 isolate. A V8 isolate can compute but
needs the host to install doorways; a WASM module is the same, but its memory
isolation is enforced at the instruction level by the runtime, with a very small
and well-studied trusted core. For running untrusted code next to other tenants',
that's an excellent foundation.

The flip side of "no ambient access" is that **the host must hand the module
every capability it needs, one by one** — which is exactly the capability model
RS2 wants (Part 3.5). The sandbox isn't bolted on; it's the nature of the format.

---

## 3. What Wasmtime is

A `.wasm` file is inert — it's just instructions. Something has to load it,
verify it, compile its portable instructions down to the real machine's
instructions, give it its linear memory, connect its imports to host functions,
and run it. That something is a **WASM runtime**.

RS2 uses **Wasmtime**, a mature, production-grade WASM runtime maintained by the
Bytecode Alliance. RS2 embeds it directly in the server process. Wasmtime is
responsible for:

- **Compiling** a module to native code (once, then caching it — §7).
- **Instantiating** it: giving it fresh memory and wiring its imports.
- **Enforcing limits**: capping its memory, and interrupting it if it runs too
  long (§6).
- **Running** the exported functions and getting results back.

RS2 configures Wasmtime in async mode (so a Wasm call can cooperate with the
server's concurrency), with the component model enabled (§4) and epoch
interruption enabled (§6).

---

## 4. Components and WIT: a typed contract instead of raw bytes

Early WASM had a real ergonomic problem: a module could only pass **numbers**
across the boundary. Strings, lists, records, optional values — none of it
existed natively. Every host and guest had to invent its own scheme for, say,
"pass a string" by agreeing on memory offsets and lengths. Brittle and
language-specific.

The **Component Model** fixes this. A **component** is a WASM module wrapped with
a *typed interface description*: it declares, in a structured way, exactly what
functions it exports, what it imports, and the rich types those functions use
(strings, records, lists, variants/enums, options). The host and guest agree on
types, and the runtime handles the marshaling.

That interface is written in **WIT** (WebAssembly Interface Types) — a small
interface-definition language. RS2's contract lives in `wit/service.wit`, and it
is the single source of truth that *both* the host and your service compile
against. In lightly-paraphrased form, it says:

```wit
// Types that cross the boundary
record message {
  method: string,
  url: string,
  headers: list<header>,
  status: u16,
  body: option<body-data>,   // media type + optional schema + bytes
}

// What the HOST provides to the guest (the capabilities)
interface host {
  request: func(capability: string, msg: message)
             -> result<message, host-error>;   // the sandbox exit
  log: func(level: log-level, text: string);
  state-get: func(key: string) -> option<list<u8>>;
  state-put: func(key: string, value: list<u8>);
}

// What the GUEST (your service) must provide
world service {
  import host;
  export init: func(config: string) -> result<_, string>;
  export handle: func(msg: message, config: string)
                   -> result<message, string>;
}
```

Read it as a two-way contract:

- **Your service exports** `init` (one-time setup from config) and `handle` (the
  request handler). These are the functions RS2 calls.
- **Your service imports** `host.request`, `host.log`, `host.state-get/put`.
  These are the functions RS2 provides — and `host.request` is the *only* exit
  from the sandbox, the direct analogue of `ctx.request` in JavaScript (Appendix
  G §5).

Compare this to the WIT host interface against the `HostApi` the JS engine uses:
they're deliberately the same shape. That's the whole point of an **engine-neutral
contract** — a JS service and a Wasm service see the same capabilities, the same
default-deny grants, the same limits, the same message semantics. The engine is
an implementation detail; the contract is identical. RS2 even runs one shared
conformance test suite against both to keep them honest.

Because the scaffold (`rs2 new`) is generated *from* this WIT, it compiles to a
working component out of the box — you start from something that runs and fill in
your logic.

---

## 5. The linker: wiring imports to host functions

When Wasmtime instantiates your component, it has to connect each function your
component *imports* (`host.request`, `host.log`, …) to actual host code. The
object that performs that wiring is the **linker**.

For each invocation RS2 builds a linker and registers:

- **The RS2 host functions** — implementations of `request`, `log`,
  `state-get`, `state-put`. When your component calls `host.request("orders", …)`,
  control crosses into RS2's native implementation, which re-enters the full
  dispatch path (so the internal call is authorized, limited, traced, and
  idempotency-checked just like any other request — same guarantee as the JS
  engine).
- **A minimal WASI layer.** WASI (the WebAssembly System Interface) is the
  standard set of "operating-system-like" imports a component may expect to
  exist — things compiled-language runtimes assume are present. RS2 wires a
  deliberately bare WASI context: enough to let a standard component instantiate,
  but **not** a grant of real file or network access. Real I/O still only flows
  through the capability functions above.

The host's side of `request` also does the type translation: it converts the
WIT `message` it receives into RS2's internal message, runs the real call, and
converts the response back into a WIT `message` for your component — including
mapping a structured RS2 error onto the WIT `host-error` variant
(`capability-denied` / `limit-exceeded` / `failed`), so error identity is
preserved across the boundary, exactly as in the JS engine.

---

## 6. Containing untrusted code: the limits

Like the JS engine (Appendix G §9), the Wasm engine enforces limits from the
*host* side, so a buggy or hostile component can't escape them. The mechanisms
are different because the substrate is different.

**Wall-clock limit — epoch interruption.** Wasmtime has a feature called *epoch
interruption*: the engine maintains a counter ("epoch"), and a component can be
configured to trap if the epoch advances past a deadline. RS2 sets the
component's deadline to one tick, then spawns a small timer task that, after the
wall-clock budget elapses, increments the engine's epoch. The next time the
component reaches an interruption point — which includes the back-edges of loops
— it traps. This is what lets RS2 kill a **CPU-bound** guest, even a tight
infinite loop with no function calls in it: the loop itself is interruptible. RS2
inspects the trap, recognizes it as an epoch interruption, and maps it to a
structured `limit_exceeded` (wall clock) error. A `tokio` timeout sits behind
this as a backstop for any time spent waiting on the host.

**Memory limit — store limits.** Each invocation runs in a Wasmtime *store* (the
container holding this instance's state and memory). RS2 attaches a memory
ceiling to the store (`InvocationLimits.memory_bytes`). If the component tries to
grow its linear memory past the cap, the growth fails — cleanly, as a normal
error condition, not a process crash. One component's memory appetite is bounded
to its own store.

**Outbound budget and body caps** are enforced host-side in the shared dispatch
machinery, identically to every other engine.

As with JS, a tenant that repeatedly trips these limits is governed by the
**per-tenant circuit breaker** (manual §9.5), so pathological components stop
consuming engine resources and can't degrade a neighbour tenant.

There's also a **deploy-time compile check**: when you `PUT` a component, RS2
compiles it through Wasmtime with no request behind it. A file that isn't a valid
component is rejected at deploy time. (`rs2 test` runs the same check locally
when the CLI is built with the Wasm feature.)

---

## 7. Compilation caching

Compiling a WASM component to native machine code is the expensive part of
running it — far more costly than the actual request. You don't want to pay it on
every request.

So RS2 **caches compiled components by content hash**. The first time a given
component's bytes are seen, Wasmtime compiles it and RS2 stores the result keyed
by a hash of the bytes. Every subsequent invocation of the same component skips
compilation and goes straight to instantiation (which is cheap: fresh memory,
wire the linker, run).

This dovetails with how custom code is deployed: `code:` versions are
**immutable and content-addressed** (manual §8.7). The same version always has
the same bytes, so it compiles exactly once and is reused until it's superseded.
Each *request* still gets a fresh instance (fresh memory, no state carried over
from the previous request) — only the expensive compiled artifact is shared.

---

## 8. Bodies materialize at the boundary

One concrete limitation to be aware of. In this version of the contract, a
message body crosses the WASM boundary **fully materialized** — as a complete
list of bytes — rather than as a stream that the component reads incrementally.

You can see this in the WIT: `body` is an `option<list<u8>>`, a complete byte
array, not a streaming handle. Before calling your component, RS2 reads the whole
body into memory (subject to the materialization cap); your `handle` gets the
finished bytes, and returns finished bytes.

For ordinary request/response handlers this is invisible and exactly what you'd
want. The thing to know is that **streaming a very large body *through* a Wasm
service isn't supported** — a multi-gigabyte upload won't flow chunk-by-chunk
through the sandbox; it would be buffered and is bounded by the materialization
cap. This is a deliberate v1 simplification: the streaming primitives in the
component model were still maturing when the contract was fixed (PRD open
question 1), and the streaming body type is slated to land when those stabilize.
The host-side size cap means the safety guarantee (bounded materialization)
holds regardless.

---

## 9. Putting it together

A single Wasm request, end to end:

1. Dispatch routes the request to a Wasm `code:` mount and assembles the granted
   capabilities for it (default-deny).
2. RS2 fetches the compiled component — compiling and caching it on first sight,
   reusing the cached artifact thereafter.
3. It builds a linker wiring the host capability functions and a bare WASI
   context, creates a fresh store with a memory cap, and arms the epoch
   interruption deadline.
4. The request message is materialized (capped) and translated into the WIT
   `message` type.
5. RS2 instantiates the component and calls its exported `handle`. Each
   `host.request` crosses into native code, re-enters the full dispatch path, and
   returns a translated response — with errors mapped onto the typed
   `host-error` variant.
6. If the component runs past its deadline, the epoch tick traps it into a
   structured wall-clock error; if it overruns memory, the store cap stops it.
7. The component's returned WIT `message` is translated back into an RS2
   response, and the store (with this instance's memory) is discarded.

The guarantees line up exactly with the JavaScript engine (Appendix G), because
both implement the *same* contract: **the host owns every doorway and every
limit; the guest gets a sealed world plus exactly the capabilities it was
granted.** The Wasm engine simply gets a stronger memory sandbox and near-native
speed in exchange for a compile step and (usually) a systems language.

To compare the two engines side by side, see manual §8.4 ("JS or Wasm?"). For
the JavaScript engine internals, see Appendix G.

---

← [Appendix G — How the JavaScript engine works](G-v8-isolate-engine.md) · [Manual home](../README.md)
