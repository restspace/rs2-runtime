# Security Policy

RS2 runs untrusted code in sandboxes (Wasm components, V8 isolates) and handles
authentication (JWT/argon2), capability grants, and tenant isolation, so
security reports are taken seriously.

## Reporting a vulnerability

**Do not open a public GitHub issue.** Instead, email **james@atelyr.com** with
a description, reproduction steps, and the impact. You will receive an
acknowledgement and we will coordinate a fix and disclosure timeline with you.
Please hold off on public disclosure until a patched release is available.

## Scope

In scope: the `rs2-core`, `rs2-server`, and `rs2-cli` crates in this repository —
sandbox escape, capability bypass, tenant-isolation break, authentication
bypass, path traversal, or denial-of-service via the runtime/dispatch path.

Out of scope: the AGPL-3.0 copyleft obligations (see [`LICENSING.md`](LICENSING.md))
and defects in third-party SDKs exercised only by the test corpus under `corpus/`.

## Safe harbor

Good-faith security research is welcomed; we will not pursue legal action
against reporters acting in good faith to identify and responsibly disclose a
real issue.
