# Switch to the CLOSED / auth dev node (.run\closed-node) on http://127.0.0.1:3100.
# Stops any running rs2 dev node first; both nodes share port 3100, so only
# one runs at a time. The closed node is bootstrapped once (auth + /users
# wrapper, locked down); its state persists in .run\closed-node between runs.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$rs2  = Join-Path $root 'target\debug\rs2.exe'
$dir  = Join-Path $root '.run\closed-node'
if (-not (Test-Path $rs2)) { throw "rs2.exe missing; build it first with: cargo build -p rs2-cli" }
if (-not (Test-Path $dir)) { throw "closed node not set up at $dir" }

Get-Process -Name rs2 -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Milliseconds 400
$p = Start-Process -FilePath $rs2 -ArgumentList 'dev', 'serverConfig.json' `
    -WorkingDirectory $dir -PassThru -WindowStyle Hidden
Write-Host "CLOSED node up (PID $($p.Id)) - http://127.0.0.1:3100  (auth: admin@demo.local / demo-admin-pw)"
