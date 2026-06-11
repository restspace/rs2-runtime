# Bundle every corpus entry into a single-file ESM bundle the RS2 JS engine
# can run (same esbuild settings as `rs2 deploy --bundle`).
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot
New-Item -ItemType Directory -Force bundles | Out-Null
$results = @()
Get-ChildItem entries -Filter *.mjs | ForEach-Object {
    $name = $_.BaseName
    Write-Host "== bundling $name"
    & npx esbuild "entries/$name.mjs" --bundle --format=esm --platform=browser `
        --conditions=worker --target=es2022 "--outfile=bundles/$name.js" `
        --define:process.env.NODE_ENV='\"production\"' 2>&1 | ForEach-Object { "$_" }
    $results += [pscustomobject]@{ sdk = $name; bundled = ($LASTEXITCODE -eq 0) }
}
$results | Format-Table
