# Runbook — multi-tenant RS2 + MongoDB on Ubuntu ARM64 (from source)

**Audience:** an agent with `sudo` on an **ARM64 (aarch64)** Ubuntu server — an
AWS **t4g** instance (Graviton2). The reference target is a **t4g.medium
(2 vCPU / 4 GB)** running **Ubuntu 26.04** (`resolute`).
**Goal:** compile `rs2-server` from source with **all engines** (native + Wasm +
V8/JS), run **MongoDB locally on the same box** as RS2's data backend, run RS2 as
a boot-started, restart-on-failure systemd service in **multi-tenant** mode, and
put **Apache** in front as a TLS reverse proxy.

This is the *build-from-source* path. The `deploy/install.sh` kit only ships an
`x86_64` release asset, so on ARM you build from source (this doc). The systemd
unit and Apache vhost here match `deploy/rs2.service` / `deploy/apache-vhost.conf.tmpl`.

Work top-to-bottom. Each step is copy-pasteable. Stop and report if any command
fails; do not paper over a failed build, a red `systemctl status`, or a mongod
that won't start.

---

## 0. Facts you must not get wrong

- **You do NOT need to rebuild V8.** This repo pins the `v8` crate `149.x` (via
  `deno_core 0.404`), and rusty_v8 publishes a prebuilt static V8 lib for
  `aarch64-unknown-linux-gnu`. The build downloads it automatically. See §4; the
  from-source fallback and how to size a build host for it are in §4a.
- **The 4 GB box is the binding constraint.** RS2 + MongoDB + Apache + a V8 link
  spike do not all fit in 3.7 GiB at once. §2a adds swap and tunes the kernel;
  §6 **caps MongoDB's cache** so it can't starve RS2. Do not skip either.
- **MongoDB here runs no-auth, loopback-only.** RS2's Mongo adapter is a JS guest
  bundle that speaks the wire protocol but **does not support SCRAM auth**. So
  mongod must run **without** `security.authorization` and bind **only** to
  `127.0.0.1`. That is safe *only* because it's loopback + a closed security
  group. Never expose 27017; never enable auth expecting RS2 to use it.
- **Ubuntu 26.04 has no MongoDB apt repo yet.** MongoDB publishes for `jammy`
  (22.04) and `noble` (24.04). §6 uses the **noble** repo on 26.04 (works in
  practice) with a static-tarball fallback if the packaged binary won't load.
- RS2 listens for **plain HTTP on loopback** (`127.0.0.1:3100`); Apache
  terminates TLS and proxies to it. RS2 never faces the internet directly.
- RS2 resolves **tenancy from the `Host` header** — the proxy MUST pass Host
  through unchanged (`ProxyPreserveHost On`).
- No graceful SIGTERM drain yet: a restart can drop in-flight requests.

**Memory budget (t4g.medium, ~3.7 GiB usable) — the plan:**

| Consumer | Target |
|----------|--------|
| MongoDB WiredTiger cache | **capped at 1.0 GB** (§6) |
| RS2 (`rs2-server` + live V8 isolates) | ~1–1.5 GB under load |
| Apache + OS | ~0.5 GB |
| Swap (OOM safety + build link spike) | **8 GB** (§2a) |

---

## 1. System build dependencies

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential pkg-config libssl-dev \
  lld \
  clang cmake python3 \
  git curl ca-certificates gnupg lsb-release
uname -m        # must print: aarch64
```

`lld` is the memory-efficient linker we use for the V8 link (§2a).
`clang`/`cmake`/`python3` are only the fallback toolchain for a from-source V8
build (§4a) — cheap to keep installed.

## 2. Rust toolchain

```bash
curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
rustc -vV       # host should read aarch64-unknown-linux-gnu
```

## 2a. System tuning — swap, THP, sysctl (needed by BOTH the build and MongoDB)

Do this **before** building and before starting MongoDB.

### 8 GB swap

Absorbs the one-time V8 link spike **and** keeps the OOM killer off mongod/RS2
in steady state. The root volume has ample room (33 GB free on the reference box).

```bash
if ! swapon --show | grep -q /swapfile; then
  sudo fallocate -l 8G /swapfile || sudo dd if=/dev/zero of=/swapfile bs=1M count=8192
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
fi
free -h
```

### sysctl: swappiness + map count (MongoDB guidance)

```bash
sudo tee /etc/sysctl.d/99-rs2-mongo.conf >/dev/null <<'SYSCTL'
# Prefer reclaiming page cache over swapping process memory, but keep swap as a
# last-resort OOM backstop (MongoDB recommends 1, not 0).
vm.swappiness = 1
# MongoDB wants a high max_map_count for many collections/indexes.
vm.max_map_count = 262144
SYSCTL
sudo sysctl --system
```

### Disable Transparent Huge Pages (MongoDB requirement)

The box currently reports `madvise`; MongoDB wants `never`. Make it stick across
reboots with a oneshot unit that runs before mongod.

```bash
sudo tee /etc/systemd/system/disable-thp.service >/dev/null <<'UNIT'
[Unit]
Description=Disable Transparent Huge Pages (for MongoDB)
DefaultDependencies=no
After=sysinit.target local-fs.target
Before=mongod.service

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'echo never > /sys/kernel/mm/transparent_hugepage/enabled; echo never > /sys/kernel/mm/transparent_hugepage/defrag'

[Install]
WantedBy=basic.target
UNIT
sudo systemctl daemon-reload
sudo systemctl enable --now disable-thp.service
cat /sys/kernel/mm/transparent_hugepage/enabled   # expect: always madvise [never]
```

### Link flags for the build shell

```bash
export RUSTFLAGS="-C link-arg=-fuse-ld=lld"   # lower peak link memory + faster
```

## 3. Get the source

```bash
sudo mkdir -p /opt/src && sudo chown "$USER" /opt/src
cd /opt/src
git clone https://github.com/atelyr/rs2-runtime.git
cd rs2-runtime
```

## 4. Compile RS2 (release, all engines) — prebuilt V8, no rebuild

Keep the build on the **prebuilt V8** path — do **not** set `V8_FROM_SOURCE`. The
box only needs outbound HTTPS to GitHub at build time.

```bash
export RUSTFLAGS="-C link-arg=-fuse-ld=lld"          # from §2a
cargo build -p rs2-server --release --features wasm,js
```

Native + Wasm only (no JS engine, no V8, trivial link — but then the Mongo
adapter can't run, since it's a JS guest):

```bash
cargo build -p rs2-server --release --features wasm
```

Verify the artifact:

```bash
file target/release/rs2-server     # expect: ELF 64-bit ... ARM aarch64
```

**If the prebuilt download is blocked:** point the rusty_v8 build script at a
reachable copy via `RUSTY_V8_MIRROR=<base-url>` or `RUSTY_V8_ARCHIVE=/path/to/librusty_v8.a`
(fetched out of band for `aarch64-unknown-linux-gnu`, matching the pinned crate
version); or build on another aarch64 box and `scp` just the binary; or drop
`js`. All three avoid a rebuild.

### 4a. Fallback: building V8 *from source* (avoid on the t4g.medium)

Only if there is genuinely no prebuilt/mirror. A from-source V8 build will
OOM/thrash on 4 GB. Minimum comfortable **t4g** build host:

| Instance | vCPU / RAM | From-source V8 build |
|---|---|---|
| t4g.medium | 2 / 4 GB | ❌ don't — this is the runtime node |
| t4g.large | 2 / 8 GB | ⚠️ works w/ swap, slow, borderline at link |
| **t4g.xlarge** | **4 / 16 GB** | ✅ **practical minimum — comfortable** |
| t4g.2xlarge | 8 / 32 GB | ✅ comfortable + noticeably faster |

Use a **transient** t4g.xlarge (16 GB), set `V8_FROM_SOURCE=1` + swap there,
build, `scp target/release/rs2-server` to the t4g.medium, then terminate it. The
*runtime* runs fine in 4 GB — only the *build* is heavy.

## 5. Install the RS2 binary + service user + directories

```bash
sudo install -m 0755 target/release/rs2-server /usr/bin/rs2-server

id rs2 >/dev/null 2>&1 || sudo useradd --system --no-create-home \
  --shell /usr/sbin/nologin rs2

sudo install -d -o rs2 -g rs2 /var/lib/rs2/data /var/lib/rs2/data-store /var/log/rs2
sudo install -d /etc/rs2/tenants
```

## 6. Install & configure MongoDB (local, loopback, no-auth, capped cache)

### Install (noble repo on 26.04)

MongoDB has no `resolute` (26.04) repo, so use the **noble** (24.04) MongoDB 8.0
repo — the aarch64 noble binaries load on 26.04 in practice.

```bash
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc \
  | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor

echo "deb [ arch=arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" \
  | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

sudo apt-get update
sudo apt-get install -y mongodb-org
```

> t4g is Graviton2 (ARMv8.2-A with LSE atomics) → MongoDB 8.0 aarch64 is
> supported. If `mongod` later fails to start with a libssl/loader error (a
> 26.04-vs-noble mismatch), use the **tarball fallback** at the end of this
> section instead of the apt package.

### Configure — loopback, no-auth, WiredTiger cache capped at 1 GB

Replace `/etc/mongod.conf` wholesale so the cache cap and loopback bind are
explicit:

```bash
sudo tee /etc/mongod.conf >/dev/null <<'MONGOCONF'
storage:
  dbPath: /var/lib/mongodb
  wiredTiger:
    engineConfig:
      # Hard cap so Mongo can't starve RS2 on a 4 GB box. Default would grab
      # ~50% of (RAM-1GB) ≈ 1.5 GB; we hold it to 1.0 GB.
      cacheSizeGB: 1.0
systemLog:
  destination: file
  logAppend: true
  path: /var/log/mongodb/mongod.log
net:
  port: 27017
  # Loopback ONLY. RS2's adapter is no-auth; loopback + a closed security group
  # is the entire security boundary. Do not add other interfaces.
  bindIp: 127.0.0.1
processManagement:
  timeZoneInfo: /usr/share/zoneinfo
# security.authorization is intentionally OMITTED — the RS2 Mongo adapter has no
# SCRAM support and connects with no credentials.
MONGOCONF
```

### Restart-on-failure + boot start

The packaged `mongod.service` may not set a restart policy; add a drop-in so it
matches RS2's restart-on-fail management.

```bash
sudo install -d /etc/systemd/system/mongod.service.d
sudo tee /etc/systemd/system/mongod.service.d/override.conf >/dev/null <<'UNIT'
[Service]
Restart=on-failure
RestartSec=2
UNIT
sudo systemctl daemon-reload
sudo systemctl enable --now mongod
```

Verify it's up and loopback-bound:

```bash
systemctl status mongod --no-pager
ss -ltnp | grep 27017          # must show 127.0.0.1:27017, NOT 0.0.0.0
mongosh --quiet --eval 'db.runCommand({ ping: 1 })'   # {"ok":1}
```

If `mongosh` isn't installed: `sudo apt-get install -y mongodb-mongosh`.

### Tarball fallback (only if the apt binary won't load on 26.04)

```bash
sudo systemctl disable --now mongod 2>/dev/null || true
cd /tmp
# Pick the latest 8.0.x aarch64 ubuntu2404 build from https://www.mongodb.com/try/download/community
VER=8.0.14
curl -fsSLO "https://fastdl.mongodb.org/linux/mongodb-linux-aarch64-ubuntu2404-${VER}.tgz"
tar xzf "mongodb-linux-aarch64-ubuntu2404-${VER}.tgz"
sudo install -m0755 "mongodb-linux-aarch64-ubuntu2404-${VER}/bin/"* /usr/local/bin/
id mongodb >/dev/null 2>&1 || sudo useradd --system --no-create-home --shell /usr/sbin/nologin mongodb
sudo install -d -o mongodb -g mongodb /var/lib/mongodb /var/log/mongodb
sudo tee /etc/systemd/system/mongod.service >/dev/null <<'UNIT'
[Unit]
Description=MongoDB Database Server
After=network-online.target
Wants=network-online.target
Before=rs2.service
[Service]
User=mongodb
Group=mongodb
ExecStart=/usr/local/bin/mongod --config /etc/mongod.conf
Restart=on-failure
RestartSec=2
LimitNOFILE=64000
LimitNPROC=64000
[Install]
WantedBy=multi-user.target
UNIT
sudo systemctl daemon-reload
sudo systemctl enable --now mongod
```

## 7. RS2 node config — multi-tenant

Write `/etc/rs2/serverConfig.json`. Resolution order: `domainMap` first, then
`{tenant}.{mainDomain}`.

```bash
sudo tee /etc/rs2/serverConfig.json >/dev/null <<'JSON'
{
  "listen": "127.0.0.1:3100",
  "tenancy": {
    "mode": "multi",
    "mainDomain": "example.com",
    "domainMap": {
      "api.example.com": "main",
      "acme.example.com": "acme"
    }
  },
  "fileRoot": "/var/lib/rs2/data",
  "dataRoot": "/var/lib/rs2/data-store",
  "tenantsDir": "/etc/rs2/tenants",
  "logging": {
    "sink": "file",
    "level": "info",
    "file": { "path": "/var/log/rs2", "maxBytes": 8388608, "backups": 5 }
  }
}
JSON
```

- `mainDomain: "example.com"` → `beta.example.com` maps to tenant `beta` (single
  subdomain label only).
- `domainMap` pins specific hostnames, overriding the subdomain rule.
- `fileRoot` and `dataRoot` are separate so a `file` mount can't expose records.

### Per-tenant config, and how a tenant reaches MongoDB

Each tenant needs `/etc/rs2/tenants/<tenant>.json`. RS2 reaches Mongo through a
deployed **JS guest adapter** (`guest-adapters/mongo-data.js` /
`mongo-query.js` in this repo). A Mongo-backed `data` mount names the deployed
adapter and grants it a **socket** to loopback 27017 (allowlist format is
`host:port`):

```bash
JWT_MAIN="$(openssl rand -hex 32)"

sudo tee /etc/rs2/tenants/main.json >/dev/null <<JSON
{
  "auth": { "jwtSecret": "${JWT_MAIN}", "userDataset": "users" },
  "operatorRoles": "A",
  "mounts": [
    { "path": "/auth", "service": "auth", "access": "open" },
    { "path": "/services", "service": "services",
      "access": { "readRoles": "A", "writeRoles": "A" } },

    { "path": "/data", "service": "data",
      "access": { "readRoles": "A", "writeRoles": "A" } },

    { "path": "/mongo", "service": "data",
      "store": {
        "adapter": "code:mongo-data@v1",
        "grants": {
          "db": { "type": "socket", "hosts": ["127.0.0.1:27017"] }
        }
      },
      "access": { "readRoles": "A", "writeRoles": "A" } }
  ]
}
JSON
```

> The `code:mongo-data@v1` bundle must be **deployed first** through the
> `services` code API (`PUT /services/code/mongo-data` with
> `guest-adapters/mongo-data.js`) before the `/mongo` mount will build; until
> then that mount errors. The `/mongo` dataset collections live in MongoDB;
> `/data` stays on the local file-backed store (users, secrets). For stored
> Mongo aggregation queries, mount a `query` service the same way with
> `code:mongo-query@v1`.

Give every tenant its **own** `jwtSecret`. Lock down configs (they hold secrets):

```bash
sudo chown -R rs2:rs2 /etc/rs2
sudo chmod 600 /etc/rs2/tenants/*.json
```

### Seed each tenant's admin

`bootstrapAdmin` is single-tenant only (ignored in multi-tenant mode) — seed each
tenant's first `A` user out of band: temporarily run the node single-tenant with
`RS2_ADMIN_EMAIL`/`RS2_ADMIN_PASSWORD` to mint it, or use the `rs2` CLI against
each tenant's `/auth` + `/data`. Don't store passwords in config files.

## 8. RS2 systemd service — boot start + restart on failure

Matches `deploy/rs2.service`, plus an ordering hint so mongod comes up first.

```bash
sudo tee /etc/systemd/system/rs2.service >/dev/null <<'UNIT'
[Unit]
Description=RS2 runtime (sandboxed composable-service runtime)
Documentation=https://github.com/atelyr/rs2-runtime
# Mongo is the data backend; start it first (soft dependency).
After=network-online.target mongod.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/rs2-server /etc/rs2/serverConfig.json
User=rs2
Group=rs2
Restart=on-failure
RestartSec=2

# Host hardening. RS2 sandboxes its guests; this sandboxes the host process.
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictNamespaces=true
RestrictRealtime=true
LockPersonality=true
ReadWritePaths=/var/lib/rs2 /var/log/rs2
# Loopback TCP to Mongo + inbound HTTP are AF_INET; all allowed here.
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable --now rs2.service
```

Verify:

```bash
systemctl status rs2 --no-pager
curl -fsS http://127.0.0.1:3100/healthz && echo "  <- healthy"
journalctl -u rs2 -n 30 --no-pager
```

## 9. Apache reverse proxy (TLS termination)

```bash
sudo apt-get install -y apache2
sudo a2enmod proxy proxy_http headers ssl rewrite

DOMAIN=example.com
sudo tee /etc/apache2/sites-available/rs2.conf >/dev/null <<APACHE
<VirtualHost *:80>
    ServerName ${DOMAIN}
    ServerAlias *.${DOMAIN}
    RewriteEngine On
    RewriteCond %{HTTPS} off
    RewriteRule ^/?(.*) https://%{SERVER_NAME}/\$1 [R=301,L]
</VirtualHost>

<VirtualHost *:443>
    ServerName ${DOMAIN}
    ServerAlias *.${DOMAIN}

    # RS2 resolves tenancy from Host — pass it through unchanged.
    ProxyPreserveHost On
    RequestHeader set X-Forwarded-Proto "https"

    ProxyPass        / http://127.0.0.1:3100/
    ProxyPassReverse / http://127.0.0.1:3100/

    # RS2 streams response bodies; don't let the proxy buffer them whole.
    SetEnv proxy-sendcl 0
    SetEnv proxy-initial-not-pooled 1

    SSLEngine on
    SSLCertificateFile    /etc/letsencrypt/live/${DOMAIN}/fullchain.pem
    SSLCertificateKeyFile /etc/letsencrypt/live/${DOMAIN}/privkey.pem

    ErrorLog  \${APACHE_LOG_DIR}/rs2-error.log
    CustomLog \${APACHE_LOG_DIR}/rs2-access.log combined
</VirtualHost>
APACHE

sudo a2ensite rs2.conf
sudo a2dissite 000-default.conf 2>/dev/null || true
```

> `\$1` and `\${APACHE_LOG_DIR}` are left literal for Apache; only `${DOMAIN}` is
> shell-substituted.

### Certificates

Wildcard needs a DNS-01 challenge:

```bash
sudo apt-get install -y certbot python3-certbot-apache
sudo certbot certonly --manual --preferred-challenges dns -d "${DOMAIN}" -d "*.${DOMAIN}"
```

Or list concrete hosts and let certbot manage the vhost (HTTP-01):

```bash
sudo certbot --apache -d api.example.com -d acme.example.com
```

Then:

```bash
sudo apache2ctl configtest && sudo systemctl reload apache2
```

## 10. End-to-end verification

```bash
# MongoDB up and loopback-only:
systemctl is-active mongod
ss -ltnp | grep 27017 | grep -q 127.0.0.1 && echo "mongo loopback OK"

# RS2 local health:
curl -fsS http://127.0.0.1:3100/healthz && echo OK
curl -fsS http://127.0.0.1:3100/readyz  && echo READY

# Through Apache, per tenant (Host drives tenancy):
curl -fsS https://api.example.com/healthz  && echo "main OK"
curl -fsS https://acme.example.com/healthz && echo "acme OK"

# Memory headroom sanity:
free -h
```

If a tenant host returns the wrong tenant, `ProxyPreserveHost On` is missing.

## 11. Day-2: upgrades & operations

RS2 rebuild-in-place upgrade:

```bash
cd /opt/src/rs2-runtime && git pull
export RUSTFLAGS="-C link-arg=-fuse-ld=lld"
cargo build -p rs2-server --release --features wasm,js
sudo install -m 0755 target/release/rs2-server /usr/bin/rs2-server
sudo systemctl restart rs2      # brief, NOT graceful
```

Routine ops:

```bash
systemctl status rs2 mongod
journalctl -u rs2 -f
tail -f /var/log/mongodb/mongod.log
free -h                                    # watch swap use under load
```

Adding a tenant: extend `domainMap` (or use the subdomain rule), drop
`/etc/rs2/tenants/<name>.json`, ensure the cert covers the host, `systemctl
restart rs2` (server-config changes need a restart; per-tenant mount changes
hot-reload through the `services` API).

---

## Appendix — quick reference

| Path | Purpose |
|------|---------|
| `/usr/bin/rs2-server` | compiled aarch64 RS2 binary |
| `/etc/rs2/serverConfig.json` | node config (listener, tenancy, roots, logging) |
| `/etc/rs2/tenants/<t>.json` | per-tenant config — `chmod 600` |
| `/var/lib/rs2/data`, `/var/lib/rs2/data-store` | RS2 file + data stores |
| `/var/log/rs2/<tenant>.ndjson` | RS2 structured logs |
| `/etc/mongod.conf` | MongoDB config (loopback, cache-capped, no-auth) |
| `/var/lib/mongodb`, `/var/log/mongodb` | Mongo data + log |
| `/etc/systemd/system/disable-thp.service` | THP=never before mongod |
| `/etc/sysctl.d/99-rs2-mongo.conf` | swappiness=1, max_map_count |
| `/swapfile` | 8 GB swap |

**Key sizing decisions:** V8 is prebuilt for aarch64 (no rebuild); WiredTiger
cache capped at 1 GB; 8 GB swap; THP off; swappiness=1. If forced to build V8
from source, use a transient t4g.xlarge (16 GB) and copy the binary back (§4a).

**Security invariant:** MongoDB is no-auth **only** because it's bound to
`127.0.0.1` behind a closed security group. Keep 27017 off every non-loopback
interface and out of every security-group rule.
