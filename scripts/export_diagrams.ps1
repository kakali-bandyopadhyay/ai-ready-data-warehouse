param(
  [string]$Npx = "npx"
)
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$sourceDir = Join-Path $repoRoot "docs\diagrams"
$outputDir = Join-Path $repoRoot "docs\img"
$diagrams = @(Get-ChildItem -LiteralPath $sourceDir -Filter "*.mmd" -File | Sort-Object Name)
if ($diagrams.Count -eq 0) {
  throw "No Mermaid diagrams found in $sourceDir"
}

New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
foreach ($diagram in $diagrams) {
  $png = Join-Path $outputDir ($diagram.BaseName + ".png")
  Write-Host "Exporting $($diagram.Name) -> $png"
  & $Npx -y "@mermaid-js/mermaid-cli" -i $diagram.FullName -o $png -b white
  if ($LASTEXITCODE -ne 0) {
    throw "Mermaid export failed for $($diagram.Name) (exit code $LASTEXITCODE)"
  }
}
