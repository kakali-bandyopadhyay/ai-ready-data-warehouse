# AI-ready data warehouse

This personal prototype is designed to turn reproducible synthetic private-markets data into a PostgreSQL star schema and a PII-free retrieval layer. It includes source freshness checks, incremental facts, a dbt snapshot, CI configuration, and pgvector similarity search.

**Boundary:** personal prototype on synthetic data showing data-warehouse and lakehouse engineering; not production software.

## Local run

Prerequisites: Python 3.12, Docker Desktop with Compose, and PowerShell. Network access is needed once to install pinned Python packages and pull the PostgreSQL image.

```powershell
Set-Location C:\Projects\ai-ready-data-warehouse
py -3.12 -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
$env:POSTGRES_PASSWORD = Read-Host "Choose a local PostgreSQL password"
# If host port 5432 is already in use, choose another free port, for example:
# $env:POSTGRES_PORT = '5433'
docker compose up -d
python pipeline/run.py --target postgres
$env:WAREHOUSE_INTEGRATION = "1"
pytest -q
```

The runner creates its temporary dbt `profiles.yml` under the operating-system temp directory. Set `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_HOST`, and `POSTGRES_PORT` only if you use values other than the Compose defaults. `POSTGRES_PASSWORD` is read from the process environment; no credentials file is created. Synthetic data is written to ignored `data/generated/`.

The runner explicitly loads raw seeds first because models read them through `source()`, which provides source freshness metadata but does not create DAG dependencies on dbt seed resources. The initial `dbt seed --full-refresh` withholds the generated future-dated C0001 change, then `dbt source freshness` and `dbt build --exclude resource_type:seed` build staging, the baseline snapshot, marts, and tests. The runner promotes the changed row with a UTC `ingested_at` and appends it to `raw.companies`; the next seed-excluded build rebuilds latest-wins staging and captures the second snapshot version in DAG order. Staging selects the maximum raw `ingested_at` per key and breaks ties with `loaded_at`. The runner verifies two history versions with one current row, fingerprints the fact tables, runs one more seed-excluded build, and fingerprints again for idempotency. The local provider vectors are test-only and not semantic. The optional real provider is `--embeddings openai`; set `OPENAI_API_KEY` in the process environment first. This makes HTTPS calls and can incur API charges.

Local end-to-end validation succeeded on **2026-10-27** with **8 funds, 400 companies, and 30 days** of data. All three dbt builds reported `Done. PASS=53 WARN=0 ERROR=0 SKIP=0 NO-OP=2 REUSED=0 TOTAL=55`. `SCD2 version check passed: C0001 versions=2, current=1`. `Idempotency check passed: fct_holdings_daily, fct_transactions row counts and checksums identical`. Pytest reported **64 passed in 39.34s**. See [FINDINGS.md](FINDINGS.md) for the recorded run results. This run predates the current `ai_company_profile` optimization; the updated query removes a repeated staging-view dependency, and its under-five-second target still needs a fresh PostgreSQL measurement.

If pip fails on Windows with `CERTIFICATE_VERIFY_FAILED`, upgrade certifi and point Python's SSL certificate setting at its CA bundle, then retry the install:

```powershell
python -m pip install --upgrade certifi
$env:SSL_CERT_FILE = (python -c "import certifi; print(certifi.where())")
python -m pip install -r requirements.txt
```

The project pins dbt-core 1.12.5, which is in the supported 1.12 line and works with the pinned Databricks adapter (which caps dbt-core below 1.12.6). The current 1.12.6 core release cannot be paired with that adapter pin.

To search after the run:

```powershell
python -m examples.vector_search_demo "synthetic software company growth" -k 5
```

The demo is implemented to return nearest chunks with source, as-of date, owner, and classification. `--provider openai` selects the optional API provider. `--skip-embeddings` leaves the vector column empty, so the search demo has no results. End-to-end retrieval has not been verified here.

The Compose file is `compose.yaml`; the host port maps from `${POSTGRES_PORT:-5432}` to container port 5432. If 5432 is occupied, set `$env:POSTGRES_PORT = '5433'` before `docker compose up -d`. Keep that same value in the environment while running the pipeline and tests because the runner connects through `POSTGRES_PORT`.

Stop the local database with `docker compose down`. To remove its persisted local volume as well, run `docker compose down -v`; this deletes the local database contents. Generated CSVs can be removed with `Remove-Item -Recurse -Force data\generated` from the repository root.

## Dataset and models

The generator creates eight synthetic funds, 400 synthetic companies by default, parent-group relationships, daily holdings, transactions, valuations, company KPIs, and short synthetic notes. Defaults are sized for a quick local run; use `--days` and `--companies` for larger experiments. Intentional duplicate rows, an orphan key, a delayed transaction, and a changed company version exercise the cleanup and history paths. For instance:

```powershell
python -m generator.generate --output data/generated --seed 20261027 --days 30 --companies 400 --format csv
```

For a larger synthetic cloud experiment, `python -m generator.generate --output data/generated --days 730 --companies 20000 --format csv` produces roughly 2.3 million holdings before deduplication. Increase `--companies` to raise the volume; the generated output remains synthetic. `--format parquet` or `both` additionally needs the official PyArrow package (`python -m pip install pyarrow`).

The dbt project contains raw seeds, typed/deduplicated staging models, enriched intermediate models, a contracted star schema, and AI-ready tables. Facts use PostgreSQL merge with stable keys. The transaction fact has a unique index on its merge key so PostgreSQL can find matched rows without scanning the whole target; the model still reprocesses a 90-day ingestion-time window to capture late arrivals. Company history uses a timestamp snapshot. Marts and AI-ready content contain only synthetic data.

## Cloud platforms (Phase 2)

Separate cloud dbt project targets and runbooks are provided for Snowflake and Databricks. They share the synthetic source generator and expose platform-specific storage and clustering choices. Snowflake has a measured run; Databricks results are pending. See [FINDINGS.md](FINDINGS.md) for the recorded Snowflake measurements.

For Snowflake, use an XS warehouse with 60-second auto-suspend, the internal stage + `PUT`/`COPY INTO` loader, and the holdings-fact clustering key. A resource monitor and account budget alert provide cost guardrails. Start with the 30-day/400-company sample; scale only after reviewing the measured result. See [Snowflake setup, build, and teardown](docs/runbooks/SNOWFLAKE.md).

For Databricks, use a serverless SQL warehouse at the smallest practical size with auto-stop, Delta tables, and liquid clustering on the holdings fact. Unity Catalog tags are opt-in because they need applicable edition support and `APPLY TAG` permissions. Configure workspace budget alerts before building. See [Databricks setup, build, and teardown](docs/runbooks/DATABRICKS.md).

Snowflake key-pair authentication uses a `TYPE = SERVICE` user (`AIRDW_DBT`) with `AIRDW_ROLE`. Follow the [Snowflake runbook](docs/runbooks/SNOWFLAKE.md) to create the service user, generate its RSA key with Python `cryptography`, and store the private key outside the repository under `%LOCALAPPDATA%`. Set `SNOWFLAKE_PRIVATE_KEY_PATH` and optionally `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` in PowerShell; keep the session role as `AIRDW_ROLE` and do not set a service-user password. Cloud credentials are read from environment variables through [profiles.yml.example](profiles.yml.example); copy it to a profile directory outside this repository. `requirements-cloud.txt` holds separate cloud adapter pins so Postgres CI dependencies remain stable. `python -m pipeline.measure_cloud_run --target snowflake` or `--target databricks` writes a UTC-dated, data-size-labelled JSON record under `results/cloud_runs/`. See [cloud findings](FINDINGS.md) and [design decisions](docs/DESIGN_DECISIONS.md).

## Documentation

- [Architecture, diagrams, and lineage](docs/ARCHITECTURE.md)
- [Design decisions](docs/DESIGN_DECISIONS.md)
- [Learning notes](docs/LEARNING_NOTES.md)
- [Phase 1 case study](docs/CASE_STUDY.md)
- [Snowflake runbook](docs/runbooks/SNOWFLAKE.md)
- [Databricks runbook](docs/runbooks/DATABRICKS.md)
- [Cloud vector search notes](docs/CLOUD_VECTOR_SEARCH.md)
- [Cloud build findings](FINDINGS.md)
- [Diagram and screenshot artifact status](docs/img/README.md)

Execution status: the local PostgreSQL pipeline and integration-enabled pytest suite completed successfully as recorded above. CI is configured to use PostgreSQL 16 with pgvector, run the pipeline and integration-enabled pytest suite, and repeat the pipeline for idempotency. CI does not run Snowflake or Databricks; no cloud result is recorded here.
