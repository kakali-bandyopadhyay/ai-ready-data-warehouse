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
  & $Npx --yes --package "@mermaid-js/mermaid-cli" mmdc -p (Join-Path $repoRoot ".github\puppeteer-config.json") -i $diagram.FullName -o $png -b white
  if ($LASTEXITCODE -ne 0) {
    throw "Mermaid export failed for $($diagram.Name) (exit code $LASTEXITCODE)"
  }
}

$lineageSource = Join-Path $repoRoot "docs\lineage.mmd"
if (Test-Path -LiteralPath $lineageSource -PathType Leaf) {
  $lineagePng = Join-Path $outputDir "dbt_lineage_layers.png"
  Write-Host "Exporting lineage.mmd -> $lineagePng"
  & $Npx --yes --package "@mermaid-js/mermaid-cli" mmdc -p (Join-Path $repoRoot ".github\puppeteer-config.json") -i $lineageSource -o $lineagePng -b white -s 2
  if ($LASTEXITCODE -ne 0) {
    throw "Mermaid export failed for lineage.mmd (exit code $LASTEXITCODE)"
  }
  Copy-Item -LiteralPath $lineagePng -Destination (Join-Path $repoRoot "docs\case-study\img\dbt_lineage_layers.png") -Force
}

$endToEndSource = Join-Path $sourceDir "end_to_end.mmd"
$endToEndPng = Join-Path $outputDir "end_to_end.png"
Write-Host "Re-exporting end_to_end.mmd at scale 2 -> $endToEndPng"
& $Npx --yes --package "@mermaid-js/mermaid-cli" mmdc -p (Join-Path $repoRoot ".github\puppeteer-config.json") -i $endToEndSource -o $endToEndPng -b white -s 2
if ($LASTEXITCODE -ne 0) {
  throw "Mermaid export failed for end_to_end.mmd (exit code $LASTEXITCODE)"
}
Copy-Item -LiteralPath $endToEndPng -Destination (Join-Path $repoRoot "docs\case-study\img\end_to_end.png") -Force
