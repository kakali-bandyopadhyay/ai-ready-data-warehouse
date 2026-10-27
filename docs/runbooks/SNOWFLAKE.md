# Snowflake setup, build, and teardown

The Snowflake target uses the separate [cloud dbt project](../../cloud_project/), `dbt-snowflake`, an internal named stage, and `PUT` + `COPY INTO` for CSV ingestion. The loader creates/replaces only raw tables in the dedicated `AI_READY_DEMO` database and `AI_READY_DEMO_RAW` schema. All data is synthetic.

## Before starting

1. Use a dedicated Snowflake user and an isolated demo account. Do not run this setup against production resources. Confirm account budget alerts are configured and email notifications are enabled.
2. In Snowsight, run [`cloud_project/ops/snowflake_setup.sql`](../../cloud_project/ops/snowflake_setup.sql) once as `ACCOUNTADMIN`. Snowflake requires `ACCOUNTADMIN` to create resource monitors and attach one to a warehouse. The script creates `AIRDW_ROLE`, provisions the demo objects and 10-credit monthly monitor, then transfers ownership of `AI_READY_DEMO`, `AI_READY_DEMO.AI_READY_DEMO_RAW`, `AI_READY_XS_WH`, and the monitor using `GRANT OWNERSHIP … COPY CURRENT GRANTS`.
3. Grant the role to the user that will run the demo, substituting the Snowflake username:

   ```sql
   GRANT ROLE AIRDW_ROLE TO USER <YOUR_SNOWFLAKE_USER>;
   ```

4. From then on, run dbt and teardown as `AIRDW_ROLE`, never as `ACCOUNTADMIN`. The role owns the demo database, raw schema, warehouse, and monitor, so it can create the dbt schemas/models and remove all demo resources.

## Build and measure (PowerShell)

Create the environment once; the cloud requirement pins do not replace the Postgres CI environment:

```powershell
Set-Location C:\Projects\ai-ready-data-warehouse
py -3.12 -m venv .venv-cloud
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv-cloud\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-cloud.txt
python -m pip install -e .
$cloudProfiles = Join-Path $env:LOCALAPPDATA 'ai-ready-data-warehouse\dbt-profiles'
New-Item -ItemType Directory -Force -Path $cloudProfiles | Out-Null
Copy-Item .\profiles.yml.example (Join-Path $cloudProfiles 'profiles.yml') -Force
$env:DBT_PROFILES_DIR = $cloudProfiles
$env:SNOWFLAKE_ACCOUNT = Read-Host 'Snowflake account identifier'
$env:SNOWFLAKE_USER = Read-Host 'Snowflake user'
$env:SNOWFLAKE_PASSWORD = Read-Host 'Snowflake password'
$env:SNOWFLAKE_ROLE = 'AIRDW_ROLE'
$env:SNOWFLAKE_WAREHOUSE = 'AI_READY_XS_WH'
$env:SNOWFLAKE_DATABASE = 'AI_READY_DEMO'
python -m pipeline.measure_cloud_run --target snowflake --days 30 --companies 400
```

The smoke run is intentionally small. After verifying the first result and cost controls, the larger synthetic run is:

```powershell
python -m pipeline.measure_cloud_run --target snowflake --days 730 --companies 20000
```

The measurement script generates data, loads CSVs through the internal stage, checks source freshness, runs the snapshot and dbt build, generates dbt docs, counts mart/AI-ready rows, and writes `results/cloud_runs/<UTC timestamp>_snowflake_<data size>.json`. It queries `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` for credits. Snowflake account metering can lag; the result records `null` and a status if rows are not yet visible or the role cannot query it. Re-run the measurement query later if needed; preserve each dated result file.

The model applies a clustering key to `fct_holdings_daily` on `date_key` and `company_sk`. The warehouse is XSMALL and auto-suspends after 60 seconds. Do not raise the warehouse size for the demo without setting a new cost limit first.

## Teardown (PowerShell)

The command asks for an exact confirmation string before deleting the dedicated demo database, warehouse, and resource monitor. Keep `SNOWFLAKE_ROLE=AIRDW_ROLE`; teardown checks `CURRENT_ROLE()` and refuses to drop resources if the active role differs. It does not use `ACCOUNTADMIN`.

```powershell
python -m pipeline.teardown_cloud --target snowflake
```

Confirm in Snowsight that the warehouse is suspended and the database, stage storage, and monitor are gone. Generated local CSVs are independent of Snowflake and can be removed with `Remove-Item -Recurse -Force data\generated`.
