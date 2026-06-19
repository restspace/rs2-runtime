# Switch to the OPEN / unauth dev node (repo root) on http://127.0.0.1:3100.
# Stops any running rs2 dev node first; both nodes share port 3100, so only
# one runs at a time.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$rs2  = Join-Path $root 'target\debug\rs2.exe'
if (-not (Test-Path $rs2)) { throw "rs2.exe missing; build it first with: cargo build -p rs2-cli" }

Get-Process -Name rs2 -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Milliseconds 400
$p = Start-Process -FilePath $rs2 -ArgumentList 'dev', 'serverConfig.json' `
    -WorkingDirectory $root -PassThru -WindowStyle Hidden
Write-Host "OPEN node up (PID $($p.Id)) - http://127.0.0.1:3100  (all mounts open, no auth)"
