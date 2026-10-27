## Local PostgreSQL run — 27 Oct 2026 (synthetic: 8 funds, 400 companies, 30 days)
- Clean database (docker compose down -v). Seed PASS=8; three dbt builds, each ERROR=0
  (54 passed, 2 exposures no-op).
- SCD2: changed company C0001 → versions=2, current=1.
- Idempotency: fct_holdings_daily and fct_transactions row counts and checksums identical
  across repeat builds.
- pytest: 81 passed (integration tests on).
- Performance fix: ai_company_profile 76–102 s → ~0.1 s after removing duplicated
  company-staging logic from the KPI staging path.
- CI (GitHub Actions, Linux): full pipeline, pytest and idempotency repeat pass.

## Snowflake run — 27 Oct 2026, 16:43–16:45 UTC (synthetic: 30 days, 400 companies, 1,942 holdings rows)
- Warehouse X-Small, AUTO_SUSPEND 60 s, resource monitor attached (owned by ACCOUNTADMIN).
- Auth: key-pair service user (TYPE = SERVICE) with a non-admin role; session timezone UTC.
- Timings: load 18.5 s; dbt builds 71.1 s; end-to-end window 1 min 49 s.
- Row counts match the local run: raw companies 401 → 400 after latest-wins dedup;
  holdings 1,942 → 1,940; dim_company 401 (400 current + 1 SCD2 history row);
  fct_transactions 800; ai_text_chunks 400.
- SCD2 and idempotency checks passed.
- Credits (ACCOUNT_USAGE, as ACCOUNTADMIN): 0.133 credits in the 16:00–17:00 UTC hour —
  an upper bound, as that hour also includes two debugging runs; 0.464 credits for the
  whole session including debugging. Snowflake meters per warehouse-hour, so one run
  cannot be isolated exactly.

## Databricks run — 27 Oct 2026, 17:35–17:45 UTC (synthetic: 30 days, 400 companies, 1,942 holdings rows)
- Databricks Free Edition: serverless SQL warehouse (2X-Small, the only size Free Edition
  allows), Unity Catalog catalog `workspace`, Delta tables, personal access token auth.
- Timings: load 137.9 s; dbt builds 393.5 s; end-to-end window 9 min 57 s (includes
  serverless warehouse start-up).
- Row counts identical to the Snowflake and local PostgreSQL runs: raw companies 401 → 400
  after latest-wins dedup; holdings 1,942 → 1,940; dim_company 401 (400 current + 1 SCD2
  history row); fct_transactions 800; ai_text_chunks 400.
- SCD2 (C0001 exactly 2 versions, 1 current) and idempotency checks passed.
- DBUs: not captured — no billing rows were visible to the run; Free Edition billing
  visibility is limited and usage can be delayed.
- system.billing.usage is readable on Free Edition but showed no SQL-warehouse rows for this run (only assistant usage), so DBUs were not captured.

## Cross-platform summary (same synthetic data, same dbt models)
| Platform | Tier used | Load | Builds | Outcome |
|---|---|---|---|---|
| PostgreSQL 16 + pgvector | Local Docker | — | 3 builds, ERROR=0 | SCD2 + idempotency pass; 88 tests; CI green |
| Snowflake | Trial, X-Small, auto-suspend 60 s | 18.5 s | 71.1 s | Pass; 0.133 credits (hour upper bound) |
| Databricks | Free Edition, serverless 2X-Small | 137.9 s | 393.5 s | Pass; DBUs not visible |

## Teardown — 27 Oct 2026
- Snowflake: demo database and warehouse dropped with `teardown_cloud.py`; the role,
  key-pair service user and resource monitor were kept, so a rerun needs only
  `snowflake_setup.sql`.
- Databricks: the six demo schemas were dropped manually; the Free Edition SQL warehouse
  was kept (the default teardown now preserves it).

Timings reflect the tiers used (a trial X-Small warehouse vs a Free Edition 2X-Small
serverless warehouse including start-up) and are not a like-for-like platform benchmark.

## Issues found during verification (and fixes)
| Issue | Fix |
|---|---|
| dbt options placed before the command | One shared command builder; test scans pipeline/*.py |
| Snapshot ran before staging existed | Explicit order: seed → freshness → build (DAG) |
| Raw modelled as sources, so DAG did not order seeds first | Seeds loaded as an explicit first step |
| Postgres deadlock rebuilding views concurrently | Single-threaded local target (documented) |
| Profile model 76–102 s | Removed duplicated staging logic |
| Snowflake COPY skipped a reloaded file (load metadata) | Unique staged file names per load |
| Staging kept the oldest row per key | Latest ingested_at wins; test against raw |
| UTC timestamps vs session timezone treated new rows as future-dated | UTC comparison + session timezone UTC |
| SSO user has no password for automation | Key-pair service user |
| Jinja block tags in profiles.yml | Values-only env_var(); YAML-load test |
| UTF-8 BOM in a Python file broke CI | BOM test |
