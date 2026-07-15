# Deploying RS2 on Ubuntu (with Apache)

RS2 is a single binary that listens for **plain HTTP on a loopback port**
(default `127.0.0.1:3100`). Apache terminates TLS and reverse-proxies to it.
This directory holds everything needed to run it as a hardened systemd service.

## One-liner install

```bash
# native + wasm engines:
curl -fsSL https://github.com/atelyr/rs2-runtime/releases/latest/download/install.sh | sudo bash

# with the V8 engine (code: JS services) and an Apache TLS proxy:
curl -fsSL https://github.com/atelyr/rs2-runtime/releases/latest/download/install.sh \
  | sudo bash -s -- --js --apache api.example.com
```

Then issue a certificate (interactive, so it isn't run automatically):

```bash
sudo certbot --apache -d api.example.com
```

## What the installer does

| Path | Purpose |
|------|---------|
| `/usr/bin/rs2-server` | the binary (`rs2-server` or the V8 `rs2-server-js` build) |
| `/etc/rs2/serverConfig.json` | node config — **never overwritten on re-install** |
| `/etc/rs2/tenants/` | per-tenant config (`<tenant>.json`) |
| `/var/lib/rs2/data`, `/var/lib/rs2/data-store` | file + data stores |
| `/var/log/rs2/` | rotated NDJSON logs |
| `/etc/systemd/system/rs2.service` | hardened systemd unit |
| `/etc/apache2/sites-available/rs2.conf` | vhost (only with `--apache`) |

It runs as a dedicated `rs2:rs2` system user, enables the service, and verifies
`/healthz`. Re-running upgrades the binary in place; config is left untouched.

## Two build variants

- **`rs2-server`** — `--features wasm`. Small, fast to build. No JS engine.
- **`rs2-server-js`** — `--features wasm,js`. Statically links V8: large binary,
  heavy build. Required for `code:` JS services and loadable JS adapters.

Both install to the same path, so the systemd unit is identical. Pick with
`--js`. Releases are built on **Ubuntu 22.04** (oldest supported glibc); they
run on 22.04+.

## The Apache proxy — read this

RS2 resolves **tenancy from the `Host` header** (domain map + subdomain), so the
vhost sets `ProxyPreserveHost On`. Without it, every request lands on the wrong
tenant. The template also forwards `X-Forwarded-Proto: https` and disables proxy
buffering so RS2's streaming bodies pass through. WebSockets are out of scope for
v1, so no `mod_proxy_wstunnel` is needed.

Required modules (the installer enables them): `proxy proxy_http headers ssl rewrite`.

## Manual install / files in this directory

- `install.sh` — the installer (also `--uninstall [--purge]`)
- `rs2.service` — systemd unit
- `serverConfig.prod.json` — production config template
- `apache-vhost.conf.tmpl` — vhost; substitute `${DOMAIN}`

Manual steps mirror the installer: copy the binary to `/usr/bin`, create the
`rs2` user and the `/etc/rs2`, `/var/lib/rs2`, `/var/log/rs2` dirs, drop the
config + unit, `systemctl enable --now rs2`, then render the vhost with your
domain and `a2ensite`.

## Building from source instead

The above uses prebuilt release binaries. To **compile on the box** (native +
Wasm + V8/JS) and run it **multi-tenant** behind Apache, follow the step-by-step
runbook in [`build-from-source-ubuntu.md`](build-from-source-ubuntu.md). It
reuses the same systemd unit and vhost, so the two paths converge operationally.

## Upgrading

Re-run the same one-liner. The installer is idempotent: it downloads the new
binary, checksum-verifies it, replaces `/usr/bin/rs2-server`, and **restarts the
service** so the new version is actually running. Your config, tenants, data, and
logs are untouched (the config is only written when absent).

```bash
# upgrade to the latest release (keep the same flags you installed with)
curl -fsSL https://github.com/atelyr/rs2-runtime/releases/latest/download/install.sh \
  | sudo bash -s -- --js

# pin / roll back to a specific release
curl -fsSL https://github.com/atelyr/rs2-runtime/releases/download/v0.2.0/install.sh \
  | sudo bash -s -- --js --version v0.2.0
```

Notes:
- **Switching variants** (add/drop `--js`) is just an upgrade with the other flag
  — same path, the installed binary is swapped.
- **Rollback** is `--version <older-tag>`; nothing is irreversible on disk.
- The restart is brief but **not graceful** (in-flight requests can drop — the
  runtime has no SIGTERM drain yet). For a busy node, drain at the proxy first or
  upgrade in a maintenance window.
- `--apache` is only needed the first time; re-running without it leaves the
  existing vhost in place.

## Operating

```bash
systemctl status rs2
journalctl -u rs2 -f
systemctl restart rs2          # note: no graceful drain yet — in-flight requests drop
curl -fsS http://127.0.0.1:3100/healthz
```

> **Graceful shutdown:** the accept loop currently exits immediately on SIGTERM,
> so a restart can drop in-flight requests. Behind a proxy this is usually fine;
> a `tokio::signal` drain path in `rs2_server::run()` is a worthwhile follow-up
> if you need zero-drop restarts.
