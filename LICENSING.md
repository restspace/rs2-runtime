# Licensing

RS2 is **dual-licensed**.

## 1. Open source — AGPL-3.0-only

The contents of this repository are licensed under the GNU Affero General Public
License, version 3.0 (see [`LICENSE`](LICENSE)). You may use, modify, and
redistribute the software under those terms. In particular, the AGPL requires
that if you run a modified version to provide a service over a network, you must
make the corresponding source of your modified version available to the users of
that service.

## 2. Commercial license

The AGPL's obligations are not suitable for everyone. A separate **commercial
license** is available — with no copyleft/network-source obligations — for, e.g.:

- embedding RS2 inside a closed-source product or service;
- offering RS2 (or a derivative) as a hosted/managed service without publishing
  your modifications;
- any use where AGPL compliance is impractical.

The copyright holder retains all rights necessary to grant such licenses.

**To obtain a commercial license, contact:** james@atelyr.com

## What this means for you (plain English)

*Not legal advice, but the practical shape of it. "Modified" means you changed
RS2's own source; building your app **on top of** an unmodified RS2 is not
modifying RS2.*

| You want to… | Under AGPL (free) | Need a commercial license? |
| --- | --- | --- |
| **Use it** — run RS2 in production, internally or to power your own app | ✅ Yes, freely | No |
| **Modify it** for your own use | ✅ Yes | No (until you distribute or network-serve it) |
| **Build your app on top** of an unmodified RS2, talking to it over HTTP (separate process) | ✅ Yes; your app stays yours | No |
| **Embed/link `rs2-core`** into your own (closed-source) binary | Only if you open-source that binary under AGPL | **Yes**, to stay closed |
| **Distribute** RS2 or a modified version | ✅ Yes, but recipients get it under AGPL (source included) | Only to distribute it closed |
| **Host a *modified* RS2 as a service** | ✅ Yes, but you must publish your modifications to its users (§13) | **Yes**, to keep your modifications private |
| **Host *unmodified* RS2 and charge for it** | ✅ Yes — allowed, nothing extra owed | No |

### Common questions

- **Can someone host RS2 and charge others for usage?** Yes. AGPL never
  restricts commercial use or charging. If they run it **unmodified**, they owe
  nothing beyond the already-public source. If they **modify** it and serve it
  over a network, they must make their modified source available to users
  (§13) — they can still charge, they just can't keep changes to RS2 secret.
- **Does AGPL force me to open-source my whole app?** No — only if you *link/embed*
  RS2 into your own program (a combined work) or *modify and serve* RS2 itself.
  Using an unmodified RS2 at arm's length (your app calls it over HTTP) does not
  pull your app under AGPL.
- **What does AGPL *not* protect against?** It does not prevent someone reselling
  hosted access to the unmodified runtime. The copyright holder reserves the
  right to release future versions under different terms if needed.

## Why dual-licensing

This is the standard open-core arrangement: the core runtime is genuinely open
and inspectable, while commercial terms fund continued development and reserve
the right to build proprietary products on top. It also means you can audit
exactly how the runtime handles data and isolates untrusted code — a deliberate
transparency guarantee.
