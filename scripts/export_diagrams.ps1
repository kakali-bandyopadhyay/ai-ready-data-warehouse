param(
  [string]$Mmdc = "npx.cmd",
  [string]$CliVersion = "11.4.2"
)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Source = Join-Path $Root "docs\diagrams"
$Output = Join-Path $Root "docs\img"
New-Item -ItemType Directory -Force -Path $Output | Out-Null
Get-ChildItem -LiteralPath $Source -Filter "*.mmd" | ForEach-Object {
  $Target = Join-Path $Output ($_.BaseName + ".png")
  & $Mmd "--yes" "@mermaid-js/mermaid-cli@$CliVersion" "-i" $_.FullName "-o" $Target "-b" "white"
  if ($LASTEXITCODE -ne 0) { throw "Mermaid export failed for $($_.Name)" }
}
