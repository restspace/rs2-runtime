#!/usr/bin/env bash
#
# RS2 runtime installer for Ubuntu/Debian.
#
#   curl -fsSL https://get.rs2.example/install.sh | sudo bash
#   curl -fsSL https://get.rs2.example/install.sh | sudo bash -s -- --js --apache api.example.com
#
# Flags:
#   --js                 install the V8-enabled build (rs2-server-js) for code: JS services
#   --apache <domain>    enable Apache as a TLS reverse proxy for <domain>
#   --version <tag>      install a specific release tag (default: latest)
#   --uninstall          remove the service, binary, and vhost (leaves data + config)
#   --purge              with --uninstall, also delete /etc/rs2 and /var/lib/rs2
#
# Re-running upgrades the binary in place; existing config is never overwritten.

set -euo pipefail

# ---- settings ---------------------------------------------------------------
REPO="${RS2_REPO:-atelyr/rs2-runtime}"        # GitHub owner/repo for releases
BASE_URL="${RS2_BASE_URL:-https://github.com/${REPO}/releases}"
RS2_USER="rs2"
BIN_DST="/usr/bin/rs2-server"
ETC_DIR="/etc/rs2"
LIB_DIR="/var/lib/rs2"
LOG_DIR="/var/log/rs2"
UNIT="/etc/systemd/system/rs2.service"
SITE="/etc/apache2/sites-available/rs2.conf"

# ---- args -------------------------------------------------------------------
WANT_JS=0
APACHE_DOMAIN=""
VERSION="latest"
DO_UNINSTALL=0
DO_PURGE=0

while [ $# -gt 0 ]; do
  case "$1" in
    --js)        WANT_JS=1; shift ;;
    --apache)    APACHE_DOMAIN="${2:?--apache needs a domain}"; shift 2 ;;
    --version)   VERSION="${2:?--version needs a tag}"; shift 2 ;;
    --uninstall) DO_UNINSTALL=1; shift ;;
    --purge)     DO_PURGE=1; shift ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m warn:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "run as root (use sudo)."

# ---- uninstall --------------------------------------------------------------
if [ "$DO_UNINSTALL" -eq 1 ]; then
  log "Stopping and disabling service"
  systemctl disable --now rs2.service 2>/dev/null || true
  rm -f "$UNIT"; systemctl daemon-reload || true
  rm -f "$BIN_DST"
  if [ -f "$SITE" ]; then
    a2dissite rs2.conf 2>/dev/null || true
    rm -f "$SITE"
    systemctl reload apache2 2>/dev/null || true
  fi
  if [ "$DO_PURGE" -eq 1 ]; then
    warn "Purging $ETC_DIR and $LIB_DIR"
    rm -rf "$ETC_DIR" "$LIB_DIR" "$LOG_DIR"
    userdel "$RS2_USER" 2>/dev/null || true
  else
    log "Left $ETC_DIR, $LIB_DIR, $LOG_DIR in place (use --purge to remove)."
  fi
  log "Uninstalled."
  exit 0
fi

# ---- preflight --------------------------------------------------------------
ARCH="$(uname -m)"
[ "$ARCH" = "x86_64" ] || die "unsupported arch '$ARCH' (only x86_64 today)."
command -v curl >/dev/null || die "curl is required."

VARIANT="rs2-server"
[ "$WANT_JS" -eq 1 ] && VARIANT="rs2-server-js"
ASSET="${VARIANT}-x86_64-linux"

if [ "$VERSION" = "latest" ]; then
  DL="${BASE_URL}/latest/download"
else
  DL="${BASE_URL}/download/${VERSION}"
fi

# ---- download + verify ------------------------------------------------------
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
log "Downloading $ASSET ($VERSION)"
curl -fsSL "${DL}/${ASSET}" -o "$TMP/rs2-server" \
  || die "download failed: ${DL}/${ASSET}"
if curl -fsSL "${DL}/SHA256SUMS" -o "$TMP/SHA256SUMS" 2>/dev/null; then
  log "Verifying checksum"
  ( cd "$TMP" && grep " ${ASSET}\$" SHA256SUMS | sed "s/${ASSET}/rs2-server/" | sha256sum -c - ) \
    || die "checksum verification failed."
else
  warn "SHA256SUMS not found; skipping checksum verification."
fi
chmod 0755 "$TMP/rs2-server"

# ---- user + dirs ------------------------------------------------------------
if ! id "$RS2_USER" >/dev/null 2>&1; then
  log "Creating system user '$RS2_USER'"
  useradd --system --no-create-home --shell /usr/sbin/nologin "$RS2_USER"
fi
log "Creating directories"
install -d -o "$RS2_USER" -g "$RS2_USER" "$LIB_DIR/data" "$LIB_DIR/data-store" "$LOG_DIR"
install -d "$ETC_DIR/tenants"

# ---- binary -----------------------------------------------------------------
log "Installing binary -> $BIN_DST"
install -m 0755 "$TMP/rs2-server" "$BIN_DST"

# ---- config (never clobber) -------------------------------------------------
CFG="$ETC_DIR/serverConfig.json"
if [ ! -f "$CFG" ]; then
  log "Writing default config -> $CFG"
  cat > "$CFG" <<'JSON'
{
  "listen": "127.0.0.1:3100",
  "tenancy": { "mode": "single", "tenant": "main" },
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
  # A minimal tenant so the node is live on first boot.
  if [ ! -f "$ETC_DIR/tenants/main.json" ]; then
    echo '{ "mounts": [] }' > "$ETC_DIR/tenants/main.json"
  fi
else
  log "Config exists at $CFG — leaving it untouched."
fi
chown -R "$RS2_USER:$RS2_USER" "$ETC_DIR"

# ---- systemd unit -----------------------------------------------------------
log "Installing systemd unit"
cat > "$UNIT" <<'UNITEOF'
[Unit]
Description=RS2 runtime (sandboxed composable-service runtime)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/rs2-server /etc/rs2/serverConfig.json
User=rs2
Group=rs2
Restart=on-failure
RestartSec=2
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
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX

[Install]
WantedBy=multi-user.target
UNITEOF
systemctl daemon-reload
systemctl enable rs2.service >/dev/null
# `restart` both starts a stopped unit and reloads the new binary on upgrade
# (`enable --now` would no-op on an already-running service, stranding the old
# process on the old binary). Restart is brief but not graceful — see README.
systemctl restart rs2.service

# ---- apache (optional) ------------------------------------------------------
if [ -n "$APACHE_DOMAIN" ]; then
  command -v apache2ctl >/dev/null || die "apache2 not installed (apt install apache2)."
  log "Configuring Apache reverse proxy for $APACHE_DOMAIN"
  a2enmod proxy proxy_http headers ssl rewrite >/dev/null
  # Only DOMAIN is substituted; Apache's own ${APACHE_LOG_DIR} etc. survive.
  curl -fsSL "${DL}/apache-vhost.conf.tmpl" -o "$TMP/vhost.tmpl" 2>/dev/null \
    || die "could not fetch vhost template from ${DL}/apache-vhost.conf.tmpl"
  sed "s/\${DOMAIN}/${APACHE_DOMAIN}/g" "$TMP/vhost.tmpl" > "$SITE"
  a2ensite rs2.conf >/dev/null
  if apache2ctl configtest 2>/dev/null; then
    systemctl reload apache2
    log "Apache reloaded. Now obtain a certificate:"
    echo "    certbot --apache -d ${APACHE_DOMAIN}"
  else
    warn "Apache configtest failed; vhost written to $SITE but NOT reloaded."
    warn "Fix the config (likely a missing TLS cert) then: systemctl reload apache2"
  fi
fi

# ---- verify -----------------------------------------------------------------
sleep 1
if curl -fsS http://127.0.0.1:3100/healthz >/dev/null 2>&1; then
  log "RS2 is up and healthy on 127.0.0.1:3100."
else
  warn "Service installed but /healthz did not answer yet. Check: journalctl -u rs2 -n 50"
fi
log "Done."
