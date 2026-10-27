# Databricks setup, build, and teardown

The Databricks target uses the separate [cloud dbt project](../../cloud_project/) with `dbt-databricks`. Seeds materialize in Delta, and `fct_holdings_daily` uses merge plus liquid clustering on `date_key` and `company_sk`. The adapter supports serverless SQL warehouses. Unity Catalog comments are emitted through dbt docs; UC tags are opt-in because they require edition support and `APPLY TAG` permissions. All data is synthetic.

## Before starting

1. Select a Unity Catalog catalog where you can create schemas, Delta tables, and views. The build creates only `AI_READY_DEMO_*` schemas; it does not create or drop your catalog.
2. In the Databricks workspace, create a **serverless SQL warehouse**, set the smallest available size (start at 2X-Small), and set auto-stop to 5 minutes. Copy its Server Hostname, HTTP Path, and warehouse ID from Connection Details.
3. Configure account/workspace budget alerts before the run. Serverless compute still incurs DBU charges. Keep the warehouse stopped when idle.
4. Prefer a service principal token for automation. Grant the identity catalog/schema use and create permissions required for dbt. To enable UC tags, grant `APPLY TAG` on the demo schemas or catalog and set `DATABRICKS_ENABLE_TAGS=true`; otherwise leave the option false.

## Build and measure (PowerShell)

Activate the `.venv-cloud` environment created from the Snowflake runbook or create it using the same setup steps there, then set the Databricks connection values:

```powershell
Set-Location C:\Projects\ai-ready-data-warehouse
$cloudProfiles = Join-Path $env:LOCALAPPDATA 'ai-ready-data-warehouse\dbt-profiles'
$env:DBT_PROFILES_DIR = $cloudProfiles
$env:DATABRICKS_HOST = Read-Host 'Databricks workspace host (https://...)'
$env:DATABRICKS_HTTP_PATH = Read-Host 'SQL warehouse HTTP Path'
$env:DATABRICKS_TOKEN = Read-Host 'Databricks service principal token'
$env:DATABRICKS_CATALOG = Read-Host 'Unity Catalog catalog for the demo'
$env:DATABRICKS_WAREHOUSE_ID = Read-Host 'SQL warehouse ID'
$env:DATABRICKS_ENABLE_TAGS = 'false'
python -m pipeline.measure_cloud_run --target databricks --days 30 --companies 400
```

After validating cost and table access, use the larger synthetic run:

```powershell
python -m pipeline.measure_cloud_run --target databricks --days 730 --companies 20000
```

The script seeds generated CSVs into the dedicated raw schema, runs freshness, snapshot, build, docs, row counts, and queries `system.billing.usage` for DBUs associated with `DATABRICKS_WAREHOUSE_ID`. It writes a dated, data-size-labelled JSON result under `results/cloud_runs/`. System billing rows can be delayed or inaccessible; in that case DBUs are `null` with a status instead of a fabricated figure.

## Teardown (PowerShell)

The command prompts for confirmation, drops the six `AI_READY_DEMO_*` schemas in the selected catalog, and deletes the selected SQL warehouse:

```powershell
python -m pipeline.teardown_cloud --target databricks
```

Confirm the SQL warehouse is deleted or stopped in the workspace UI, and verify no `AI_READY_DEMO_*` schema remains. The catalog itself is preserved. Remove the budget policy only if it was created solely for this experiment. Generated local CSVs can be deleted separately with `Remove-Item -Recurse -Force data\generated`.
