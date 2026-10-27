# Cloud build findings

**Cloud status:** documented, not run. No Snowflake or Databricks account was available from this environment, so this file contains no measured cloud timings, credits, DBUs, or result claims.

## Phase 1 local PostgreSQL result

The local Phase 1 run completed successfully on **2026-10-27** with **8 funds, 400 companies, and 30 days** of generated data.

The three `dbt build` steps reported:

| Step | Result |
| --- | --- |
| Baseline build | `Done. PASS=53 WARN=0 ERROR=0 SKIP=0 NO-OP=2 REUSED=0 TOTAL=55` |
| Changed-company/SCD2 build | `Done. PASS=53 WARN=0 ERROR=0 SKIP=0 NO-OP=2 REUSED=0 TOTAL=55` |
| Idempotency build | `Done. PASS=53 WARN=0 ERROR=0 SKIP=0 NO-OP=2 REUSED=0 TOTAL=55` |

`SCD2 version check passed: C0001 versions=2, current=1`

`Idempotency check passed: fct_holdings_daily, fct_transactions row counts and checksums identical`

Pytest completed with **64 passed in 39.34s**.

Run [`pipeline.measure_cloud_run`](pipeline/measure_cloud_run.py) from PowerShell using the target-specific steps in [`docs/runbooks/SNOWFLAKE.md`](docs/runbooks/SNOWFLAKE.md) or [`docs/runbooks/DATABRICKS.md`](docs/runbooks/DATABRICKS.md). Each run writes an immutable UTC-dated JSON record to `results/cloud_runs/`, including synthetic data size, generation/load/snapshot/build times, mart and AI-ready row counts, and the best available platform usage metric. Billing system latency or permissions can leave credits/DBUs null; keep the status field and rerun after usage appears rather than filling an estimate.

## Comparison fields

| Target | Data size | Load seconds | Snapshot seconds | Build seconds | Credits | DBUs | Fact/AI row counts | Status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| Snowflake | Pending local run | — | — | — | — | N/A | — | Documented, not run |
| Databricks | Pending local run | — | — | — | N/A | — | — | Documented, not run |
