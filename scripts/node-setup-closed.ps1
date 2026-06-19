# Create + bootstrap the persistent CLOSED node in .run\closed-node (idempotent).
# Copies the seed-auth example config, then runs seed-auth.rs2 once to enable
# auth + the /users wrapper and lock the node down. Its state then persists, so
# node-closed.ps1 just starts it. Safe to re-run: skips if already bootstrapped.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$rs2  = Join-Path $root 'target\debug\rs2.exe'
$src  = Join-Path $root 'examples\seed-auth'
$dir  = Join-Path $root '.run\closed-node'
$tenant = Join-Path $dir 'tenants\main.json'
if (-not (Test-Path $rs2)) { throw "rs2.exe missing; build it first with: cargo build -p rs2-cli" }

# Already bootstrapped? (tenants/main.json gains a jwtSecret once set up.)
if ((Test-Path $tenant) -and (Select-String -Path $tenant -Pattern 'jwtSecret' -Quiet)) {
    Write-Host "closed node already set up at $dir"
    return
}

New-Item -ItemType Directory -Force -Path (Join-Path $dir 'tenants') | Out-Null
foreach ($f in 'serverConfig.json', 'seed-auth.rs2', 'users.schema.json', 'users.wrapper.mount.json', 'rsconfig.json') {
    Copy-Item (Join-Path $src $f) (Join-Path $dir $f) -Force
}
# Fresh starting tenant: one open /services mount (ASCII, so no UTF-8 BOM that
# would break JSON parsing on load).
$fresh = '{ "mounts": [ { "path": "/services", "service": "services", "config": { "access": { "read": "all", "write": "all" } } } ] }'
Set-Content -Path $tenant -Value $fresh -Encoding ascii

Get-Process -Name rs2 -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Milliseconds 400
$p = Start-Process -FilePath $rs2 -ArgumentList 'dev', 'serverConfig.json' `
    -WorkingDirectory $dir -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 2
Push-Location $dir
& $rs2 run seed-auth.rs2
Pop-Location
Stop-Process -Id $p.Id -Force
Write-Host "closed node bootstrapped at $dir  (admin@demo.local / demo-admin-pw)"
