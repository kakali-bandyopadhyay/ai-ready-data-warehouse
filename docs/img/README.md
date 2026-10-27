# Diagram exports and screenshots

The five Mermaid source diagrams are under `docs/diagrams/`. Run the PowerShell exporter below to create matching PNG files in this directory using the official Mermaid CLI package. PNG generation was attempted in this environment, but npm registry DNS lookup failed (`ENOTFOUND registry.npmjs.org`), so the PNG files are not present yet.

From the repository root on a machine with Node.js/npm and registry access, export PNGs with:

```powershell
Set-Location C:\Projects\ai-ready-data-warehouse
powershell -ExecutionPolicy Bypass -File scripts\export_diagrams.ps1
```

The script invokes `npx -y @mermaid-js/mermaid-cli` for each `.mmd` file. The following evidence screenshots require a successful local pipeline run and a GUI screenshot capture: dbt lineage graph, dbt test results, a mart query result, and one vector-search result. The local pipeline result is recorded in [`FINDINGS.md`](../../FINDINGS.md); screenshots have not been captured here. Suggested local source commands are `dbt docs serve` for lineage, `dbt build`/`pytest -q` for test output, a `SELECT * FROM marts.fct_holdings_daily LIMIT 10` query, and `python -m examples.vector_search_demo "synthetic software company growth" -k 5` for retrieval. Save the resulting PNG captures in this directory after reviewing that they contain only synthetic data.
