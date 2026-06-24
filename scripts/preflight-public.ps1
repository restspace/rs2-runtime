<#
.SYNOPSIS
  Pre-public-release scanner for rs2-runtime.

  Flags likely secrets, internal absolute paths, internal hostnames, sensitive
  files, and required-but-missing license files BEFORE the repo is made public.
  Read-only: it reports, it never edits or deletes.

.DESCRIPTION
  Scans the *tracked* working tree (what would actually be published) plus a few
  always-check directories. For a truly safe release also scan git *history*
  (see the note printed at the end) - flipping an existing private repo to public
  exposes every past commit, not just the current tree.

.PARAMETER Path
  Repo root to scan. Defaults to the script's parent directory.

.EXAMPLE
  powershell -File scripts/preflight-public.ps1
#>
[CmdletBinding()]
param(
    [string]$Path = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path $Path).Path
Write-Host "Preflight scan of: $root`n" -ForegroundColor Cyan

$findings = [System.Collections.Generic.List[object]]::new()
function Add-Finding($Severity, $Category, $File, $Detail) {
    $findings.Add([pscustomobject]@{
        Severity = $Severity; Category = $Category; File = $File; Detail = $Detail
    })
}

# --- 1. File list: tracked files only (what actually ships) -------------------
Push-Location $root
try { $tracked = @(git ls-files 2>$null) } catch { $tracked = @() }
Pop-Location

if (-not $tracked -or $tracked.Count -eq 0) {
    Write-Host "WARNING: 'git ls-files' returned nothing - scanning filesystem instead." -ForegroundColor Yellow
    $tracked = Get-ChildItem -Path $root -Recurse -File |
        Where-Object { $_.FullName -notmatch '\\(target|node_modules|\.git)\\' } |
        ForEach-Object { ($_.FullName.Substring($root.Length + 1)) -replace '\\','/' }
}

# Text-ish files to grep. Skip binaries / lockfiles / heavy dirs.
$skipExt  = '\.(png|jpg|jpeg|gif|ico|webp|wasm|woff2?|ttf|otf|pdf|zip|gz|lock|snap)$'
$skipPath = '(^|/)(target|node_modules)/'
$textFiles = $tracked | Where-Object { $_ -notmatch $skipExt -and $_ -notmatch $skipPath }

# --- 2. Sensitive paths that probably should NOT be public -------------------
$sensitivePathRules = @(
    @{ Pattern = '^tenants/';                        Detail = 'Tenant config - may carry secrets/keys/domains' },
    @{ Pattern = '^deploy/';                         Detail = 'Deployment config - review for secrets/hosts' },
    @{ Pattern = '^\.github/workflows/';             Detail = 'CI workflow - check for inline secrets/internal refs' },
    @{ Pattern = 'serverConfig\.json$';              Detail = 'Server config - listener/adapter/secret wiring' },
    @{ Pattern = '(^|/)\.env';                       Detail = 'Env file - almost certainly secrets' },
    @{ Pattern = 'node-(open|closed).*\.ps1$';       Detail = 'Open/closed dev-node script - may hardcode internal hosts' },
    @{ Pattern = '(^|/)id_(rsa|ed25519)';            Detail = 'Private key material' },
    @{ Pattern = '(secret|credential)s?\.(json|ya?ml|toml)$'; Detail = 'Likely a secrets file' }
)
foreach ($f in $tracked) {
    foreach ($rule in $sensitivePathRules) {
        if ($f -match $rule.Pattern) { Add-Finding 'HIGH' 'sensitive-file' $f $rule.Detail }
    }
}

# --- 3. Content scans --------------------------------------------------------
# Patterns avoid embedding quote chars (PS-quoting hazard); they match the
# key/value shape regardless of whether the value is quoted.
$contentRules = @(
    # Require a quoted literal value (\x22=" \x27=') so type signatures like
    # `password: Option<String>` and bindings like `let password = x` don't match.
    @{ Sev='HIGH'; Cat='secret';        Rx='(?i)(api[_-]?key|secret[_-]?key|client[_-]?secret|access[_-]?token|auth[_-]?token)\s*[:=]\s*[\x22\x27][^\x22\x27]{6,}'; Detail='Possible hardcoded credential' },
    @{ Sev='HIGH'; Cat='secret';        Rx='(?i)(password|passwd|pwd)\s*[:=]\s*[\x22\x27][^\x22\x27]{4,}'; Detail='Possible hardcoded password' },
    @{ Sev='HIGH'; Cat='secret';        Rx='BEGIN (RSA |EC |OPENSSH |PGP )?PRIVATE KEY';              Detail='Private key block' },
    @{ Sev='HIGH'; Cat='secret';        Rx='\bAKIA[0-9A-Z]{16}\b';                                    Detail='AWS access key id' },
    @{ Sev='HIGH'; Cat='secret';        Rx='\bgh[pousr]_[A-Za-z0-9]{30,}';                            Detail='GitHub token' },
    @{ Sev='HIGH'; Cat='secret';        Rx='\bsk-[A-Za-z0-9]{20,}';                                   Detail='OpenAI/Stripe-style secret key' },
    @{ Sev='HIGH'; Cat='secret';        Rx='\bxox[baprs]-[A-Za-z0-9-]{10,}';                          Detail='Slack token' },
    @{ Sev='HIGH'; Cat='secret';        Rx='\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}'; Detail='JWT (may be a live token)' },
    @{ Sev='MED';  Cat='internal-path'; Rx='[A-Za-z]:\\dev\\';                                        Detail='Internal absolute Windows dev path' },
    @{ Sev='MED';  Cat='internal-path'; Rx='[A-Za-z]:\\Users\\';                                      Detail='Local user-profile path' },
    # Unix home-dir match dropped: PS -match is case-insensitive, so it collided
    # with the common `/users/` API path. The Windows `C:\Users\` rule above
    # covers the real local-leak risk for this repo.
    @{ Sev='LOW';  Cat='internal-host'; Rx='(?i)\batelyr\b';                                          Detail='Internal/company name - confirm it should be public' },
    @{ Sev='LOW';  Cat='private-ip';    Rx='\b(10(\.\d{1,3}){3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.\d{1,3}\.\d{1,3})\b'; Detail='Private network IP' }
)

# Files that legitimately mention secret-ish words (don't flag their content).
$allowFiles = @('LICENSING.md','LICENSE','CONTRIBUTING.md','scripts/preflight-public.ps1')

$scanned = 0
foreach ($rel in $textFiles) {
    $full = Join-Path $root $rel
    if (-not (Test-Path $full)) { continue }
    $scanned++
    $item = Get-Item $full
    if ($item.Length -gt 2MB) {
        Add-Finding 'LOW' 'large-file' $rel ("{0:N0} bytes - confirm it belongs in a public repo" -f $item.Length)
        continue
    }
    $lines = Get-Content -LiteralPath $full -ErrorAction SilentlyContinue
    if (-not $lines) { continue }
    $n = 0
    foreach ($line in $lines) {
        $n++
        foreach ($rule in $contentRules) {
            if (($allowFiles -contains $rel) -and ($rule.Cat -in @('secret','internal-host'))) { continue }
            if ($line -match $rule.Rx) {
                $snippet = $line.Trim()
                if ($snippet.Length -gt 120) { $snippet = $snippet.Substring(0,120) + '...' }
                Add-Finding $rule.Sev $rule.Cat ("{0}:{1}" -f $rel,$n) ("{0} - {1}" -f $rule.Detail,$snippet)
            }
        }
    }
}

# --- 4. Required files present? ----------------------------------------------
foreach ($req in @('LICENSE','LICENSING.md','README.md')) {
    if (-not (Test-Path (Join-Path $root $req))) {
        $sev = if ($req -eq 'LICENSE') { 'HIGH' } else { 'MED' }
        Add-Finding $sev 'missing-file' $req 'Expected before a public release'
    }
}

# --- 5. Report ---------------------------------------------------------------
Write-Host "Scanned $scanned text file(s); $($tracked.Count) tracked total.`n"

$order = @{ HIGH = 0; MED = 1; LOW = 2 }
if ($findings.Count -eq 0) {
    Write-Host "No findings. Still scan git history before going public (see note)." -ForegroundColor Green
} else {
    $sorted = $findings | Sort-Object @{ Expression = { $order[$_.Severity] } }, Category, File
    foreach ($grp in ($sorted | Group-Object Severity | Sort-Object @{ Expression = { $order[$_.Name] } })) {
        $color = switch ($grp.Name) { 'HIGH' {'Red'} 'MED' {'Yellow'} default {'DarkGray'} }
        Write-Host "== $($grp.Name) ($($grp.Count)) ==" -ForegroundColor $color
        foreach ($f in $grp.Group) {
            Write-Host ("  [{0}] {1}" -f $f.Category, $f.File) -ForegroundColor $color
            Write-Host ("      {0}" -f $f.Detail) -ForegroundColor DarkGray
        }
        Write-Host ''
    }
    Write-Host ("Summary: {0} HIGH, {1} MED, {2} LOW" -f `
        ($findings | Where-Object Severity -eq 'HIGH').Count, `
        ($findings | Where-Object Severity -eq 'MED').Count, `
        ($findings | Where-Object Severity -eq 'LOW').Count) -ForegroundColor Cyan
}

Write-Host ''
Write-Host 'NOTE: This scans the current tree only. Flipping an existing private' -ForegroundColor Yellow
Write-Host '      repo to public exposes ALL history. Either scan history, e.g.:' -ForegroundColor Yellow
Write-Host '        gitleaks detect --source . --redact' -ForegroundColor DarkGray
Write-Host '        trufflehog git file://. --only-verified' -ForegroundColor DarkGray
Write-Host '      or publish as a fresh repo with a single squashed commit.' -ForegroundColor Yellow

if (($findings | Where-Object Severity -eq 'HIGH').Count -gt 0) { exit 1 } else { exit 0 }
