# Diagram exports and screenshots

The five Mermaid source diagrams are under `docs/diagrams/`. PNG exports are not checked in yet because Mermaid CLI and Node were unavailable in the build environment, and network DNS could not reach the package registry. No substitute renderer or hand-authored bitmap is used.

From the repository root on a machine with Node.js/npm and registry access, export PNGs with:

```powershell
Set-Location C:\Projects\ai-ready-data-warehouse
powershell -ExecutionPolicy Bypass -File scripts\export_diagrams.ps1
```

The script invokes the official Mermaid CLI package at the pinned version `11.4.2`. The following evidence screenshots also need a local successful pipeline run and a GUI screenshot capture: dbt lineage graph, dbt test results, a mart query result, and one vector-search result. They are intentionally not fabricated. Suggested local source commands are `dbt docs serve` for lineage, `dbt build`/`pytest -q` for test output, a `SELECT * FROM marts.fct_holdings_daily LIMIT 10` query, and `python -m examples.vector_search_demo "synthetic software company growth" -k 5` for retrieval. Save the resulting PNG captures in this directory after reviewing that they contain only synthetic data.
