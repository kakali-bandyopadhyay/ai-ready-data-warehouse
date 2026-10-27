# Snowflake setup, build, and teardown

The Snowflake target uses the separate [cloud dbt project](../../cloud_project/), `dbt-snowflake`, an internal named stage, and `PUT` + `COPY INTO` for CSV ingestion. The loader creates/replaces only raw tables in the dedicated `AI_READY_DEMO` database and `AI_READY_DEMO_RAW` schema. All data is synthetic.

## Before starting

1. Use an isolated demo account. Do not run this setup against production resources. Confirm account budget alerts are configured and email notifications are enabled.
2. In Snowsight, run [`cloud_project/ops/snowflake_setup.sql`](../../cloud_project/ops/snowflake_setup.sql) once as `ACCOUNTADMIN`. Snowflake requires `ACCOUNTADMIN` to create resource monitors and attach one to a warehouse. The script creates `AIRDW_ROLE`, provisions the demo objects and 10-credit monthly monitor, then transfers ownership of `AI_READY_DEMO`, `AI_READY_DEMO.AI_READY_DEMO_RAW`, `AI_READY_XS_WH`, and the monitor using `GRANT OWNERSHIP … COPY CURRENT GRANTS`.
3. Generate an RSA key pair with Python's `cryptography` package. The private key is saved under `%LOCALAPPDATA%`, outside the repository; the script prompts for an optional encryption passphrase. Cloud requirements install `cryptography` as a Snowflake connector dependency.

   ```powershell
   $keyDirectory = Join-Path $env:LOCALAPPDATA 'ai-ready-data-warehouse\keys'
   New-Item -ItemType Directory -Force -Path $keyDirectory | Out-Null
   $env:AIRDW_KEY_DIRECTORY = $keyDirectory
   @'
   import base64
   import getpass
   import os
   from pathlib import Path
   from cryptography.hazmat.primitives import serialization
   from cryptography.hazmat.primitives.asymmetric import rsa

   key_dir = Path(os.environ["AIRDW_KEY_DIRECTORY"])
   key_dir.mkdir(parents=True, exist_ok=True)
   private_path = key_dir / "airdw_dbt_key.p8"
   passphrase = getpass.getpass("Private-key passphrase (blank for unencrypted key): ")
   encryption = (serialization.BestAvailableEncryption(passphrase.encode()) if passphrase
                 else serialization.NoEncryption())
   private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
   private_path.write_bytes(private_key.private_bytes(
       encoding=serialization.Encoding.PEM,
       format=serialization.PrivateFormat.PKCS8,
       encryption_algorithm=encryption,
   ))
   public_der = private_key.public_key().public_bytes(
       encoding=serialization.Encoding.DER,
       format=serialization.PublicFormat.SubjectPublicKeyInfo,
   )
   print(f"Private key saved to: {private_path}")
   print("Paste this base64 value into RSA_PUBLIC_KEY (omit any PEM header/footer):")
   print(base64.b64encode(public_der).decode("ascii"))
   '@ | python -
   icacls $keyDirectory /inheritance:r /grant:r "$($env:USERNAME):(OI)(CI)F"
   ```

4. In Snowsight as `ACCOUNTADMIN`, create the dedicated service user using the generated base64 public key, then grant the role:

   ```sql
   CREATE USER AIRDW_DBT
     TYPE = SERVICE
     RSA_PUBLIC_KEY = '<paste generated base64 public key>'
     DEFAULT_ROLE = AIRDW_ROLE
     DEFAULT_WAREHOUSE = AI_READY_XS_WH;
   GRANT ROLE AIRDW_ROLE TO USER AIRDW_DBT;
   ALTER USER AIRDW_DBT SET TIMEZONE = 'UTC';
   ```

5. Run dbt and teardown as the service user `AIRDW_DBT` with `AIRDW_ROLE`, never as `ACCOUNTADMIN`. The service user authenticates with the private key and the role owns the demo resources.

## Build and measure (PowerShell)

The loader writes timestamp fields as UTC-naive CSV values matching the target `TIMESTAMP_NTZ` columns. Its initial company load withholds the future-dated C0001 change; after the baseline snapshot it promotes and loads that row with a UTC ingestion time. dbt also sets the Snowflake session timezone to UTC at run start and validates the declared raw-table types after each load.

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
$env:SNOWFLAKE_USER = 'AIRDW_DBT'
$env:SNOWFLAKE_PRIVATE_KEY_PATH = Join-Path $env:LOCALAPPDATA 'ai-ready-data-warehouse\keys\airdw_dbt_key.p8'
# If you encrypted the key, enter the same passphrase used during generation.
$securePassphrase = Read-Host 'Private-key passphrase (leave blank if unencrypted)' -AsSecureString
$passphraseBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassphrase)
try { $env:SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($passphraseBstr) }
finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($passphraseBstr) }
$env:SNOWFLAKE_ROLE = 'AIRDW_ROLE'
$env:SNOWFLAKE_WAREHOUSE = 'AI_READY_XS_WH'
$env:SNOWFLAKE_DATABASE = 'AI_READY_DEMO'
python -m pipeline.measure_cloud_run --target snowflake --days 30 --companies 400
```

`profiles.yml.example` has separate `snowflake` (key-pair) and `snowflake_password` outputs and contains no Jinja block tags, so the file remains valid YAML before dbt renders its value expressions. `pipeline.measure_cloud_run` selects `--target snowflake` when `SNOWFLAKE_PRIVATE_KEY_PATH` is set and `--target snowflake_password` when it is absent. Teardown does not invoke dbt; its direct Snowflake connector selects the same key-pair or password authentication from that environment variable. For the service-user flow, keep the key path set and do not set `SNOWFLAKE_PASSWORD`. For password fallback, unset `SNOWFLAKE_PRIVATE_KEY_PATH`, set `SNOWFLAKE_PASSWORD`, and use the measurement script's normal `--target snowflake` option; it chooses the `snowflake_password` profile output automatically.

The smoke run is intentionally small. After verifying the first result and cost controls, the larger synthetic run is:

```powershell
python -m pipeline.measure_cloud_run --target snowflake --days 730 --companies 20000
```

The measurement script creates each raw table with declared `VARCHAR`, `NUMBER`, `DATE`, and `TIMESTAMP_NTZ` columns before `COPY INTO`. It validates all seven raw table schemas immediately after the load and again after reloading the changed company table. A mismatch stops the run with a `Raw schema validation failed` message before source freshness or dbt builds. For the SCD2 reload, it truncates `AI_READY_DEMO_RAW.COMPANIES`, then PUTs the company CSV under a fresh UUID filename and copies that staged object into the table. This avoids Snowflake load-history deduplication. The script promotes C0001's changed row to a timestamp newer than the captured baseline, then asserts the raw row's sector and timestamp changed before starting build 2. It checks source freshness and runs three `dbt build --exclude resource_type:seed` passes. The first captures baseline history; build 2 captures the changed version and verifies two SCD2 versions; it fingerprints both fact tables, builds once more, and compares row counts/checksums for idempotency. At the start of each measurement, the script drops and recreates the five derived schemas (staging, intermediate, marts, ai_ready, snapshots); raw is loaded separately. It then records row counts and timings in `results/cloud_runs/<UTC timestamp>_snowflake_<data size>.json`. The dbt role intentionally has no `ACCOUNT_USAGE` access. Measure credits separately as `ACCOUNTADMIN`; Snowflake usage history can lag by up to about 3 hours. Preserve each dated result file. Snapshot work runs in DAG order inside each build; it is not a separate `dbt snapshot` step.

As `ACCOUNTADMIN`, query warehouse credits with UTC timestamp bounds (replace the interval with the measurement hour):

```sql
SELECT WAREHOUSE_NAME, SUM(CREDITS_USED) AS CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'AI_READY_XS_WH'
  AND START_TIME >= '2026-10-27 16:00:00 +00:00'::TIMESTAMP_TZ
  AND START_TIME <  '2026-10-27 17:00:00 +00:00'::TIMESTAMP_TZ
GROUP BY WAREHOUSE_NAME;
```

`AIRDW_ROLE` intentionally has no `ACCOUNT_USAGE` access. Run dbt and teardown as `AIRDW_ROLE`, never as `ACCOUNTADMIN`; the credits query is a separate `ACCOUNTADMIN` task. Metering latency can reach approximately 3 hours.

The model applies a clustering key to `fct_holdings_daily` on `date_key` and `company_sk`. The warehouse is XSMALL and auto-suspends after 60 seconds. Do not raise the warehouse size for the demo without setting a new cost limit first.

## Teardown (PowerShell)

The command asks for an exact confirmation string before deleting the dedicated demo database, warehouse, and resource monitor. Keep `SNOWFLAKE_ROLE=AIRDW_ROLE`; teardown checks `CURRENT_ROLE()` and refuses to drop resources if the active role differs. It does not use `ACCOUNTADMIN`.

```powershell
python -m pipeline.teardown_cloud --target snowflake
```

Confirm in Snowsight that the warehouse is suspended and the database, stage storage, and monitor are gone. Generated local CSVs are independent of Snowflake and can be removed with `Remove-Item -Recurse -Force data\generated`.
