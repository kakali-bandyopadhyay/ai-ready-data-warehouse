# AI-ready data warehouse

This personal prototype turns reproducible synthetic private-markets data into a tested PostgreSQL star schema and a PII-free retrieval layer. It includes source freshness checks, incremental facts, a dbt snapshot, CI, and pgvector similarity search.

**Boundary:** personal prototype on synthetic data showing data-warehouse and lakehouse engineering; not production software.

## Five-minute local run

Prerequisites: Python 3.12, Docker Desktop with Compose, and PowerShell. Network access is needed once to install pinned Python packages and pull the PostgreSQL image.

```powershell
Set-Location C:\Projects\ai-ready-data-warehouse
py -3.12 -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
$env:POSTGRES_PASSWORD = Read-Host "Choose a local PostgreSQL password"
docker compose up -d
python pipeline/run.py --target postgres
$env:WAREHOUSE_INTEGRATION = "1"
pytest -q
```

The runner creates its temporary dbt `profiles.yml` under the operating-system temp directory. Set `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_HOST`, and `POSTGRES_PORT` only if you use values other than the Compose defaults. `POSTGRES_PASSWORD` is read from the process environment; no credentials file is created. Synthetic data is written to ignored `data/generated/`.

The runner seeds raw tables, checks source freshness, runs the SCD2 snapshot, builds/tests the dbt project, builds the facts a second time and compares row counts/checksums, generates dbt docs, and fills deterministic test vectors. Those vectors are test-only and not semantic. The optional real provider is `--embeddings openai`; set `OPENAI_API_KEY` in the process environment first. This makes HTTPS calls and can incur API charges.

To search after the run:

```powershell
python -m examples.vector_search_demo "synthetic software company growth" -k 5
```

The demo returns the nearest chunks with source, as-of date, owner, and classification. `--provider openai` selects the optional API provider. `--skip-embeddings` leaves the vector column empty, so the search demo has no results.

Stop the local database with `docker compose down`. To remove its persisted local volume as well, run `docker compose down -v`; this deletes the local database contents. Generated CSVs can be removed with `Remove-Item -Recurse -Force data\generated` from the repository root.

## Dataset and models

The generator creates eight synthetic funds, 400 synthetic companies by default, parent-group relationships, daily holdings, transactions, valuations, company KPIs, and short synthetic notes. Defaults are sized for a quick local run; use `--days` and `--companies` for larger experiments. Intentional duplicate rows, an orphan key, a delayed transaction, and a changed company version exercise the cleanup and history paths. For instance:

```powershell
python -m generator.generate --output data/generated --seed 20261027 --days 30 --companies 400 --format csv
```

For a larger synthetic cloud experiment, `python -m generator.generate --output data/generated --days 730 --companies 20000 --format csv` produces roughly 2.3 million holdings before deduplication. Increase `--companies` to raise the volume; the generated output remains synthetic. `--format parquet` or `both` additionally needs the official PyArrow package (`python -m pip install pyarrow`).

The dbt project contains raw seeds, typed/deduplicated staging models, enriched intermediate models, a contracted star schema, and AI-ready tables. Facts use PostgreSQL merge with stable keys. The transaction model reprocesses a 90-day ingestion-time window to capture late arrivals. Company history uses a timestamp snapshot. Marts and AI-ready content contain only synthetic data.

## Cloud platforms (Phase 2 work)

Snowflake and Databricks adapters, target profiles, platform-specific models, setup and teardown runbooks are not part of Phase 1, and no cloud build was run. The repository currently accepts only `--target postgres`. Do not create a warehouse or SQL warehouse until the target-specific Phase 2 implementation and cost controls are reviewed.

For the later Snowflake target, setup requires an account locator, user, role, warehouse, database, and schema; credentials must remain in environment variables or a profile outside this repository. The planned design uses an XS warehouse with 60-second auto-suspend, a staging load, a clustering key for the holdings fact, and a resource monitor if the account plan supports it. Teardown must suspend/drop the warehouse and remove stage/database resources created for the experiment.

For the later Databricks target, setup requires a workspace URL, SQL warehouse identifier, catalog, schema, and supported authentication. The planned design uses Delta tables, serverless SQL where available, liquid clustering (or Z-ORDER) for holdings, and Unity Catalog ownership/classification/AI-readiness metadata where the edition supports it. Teardown must stop or delete the SQL warehouse and remove the experiment schema/catalog as appropriate.

No cloud credentials or account values are stored here. Cloud deployment commands will be added with the Phase 2 target implementation. See [design decisions](docs/DESIGN_DECISIONS.md) for the documented platform trade-offs.

## Documentation

- [Architecture, diagrams, and lineage](docs/ARCHITECTURE.md)
- [Design decisions](docs/DESIGN_DECISIONS.md)
- [Learning notes](docs/LEARNING_NOTES.md)
- [Phase 1 case study](docs/CASE_STUDY.md)
- [Diagram and screenshot artifact status](docs/img/README.md)

CI uses PostgreSQL 16 with pgvector and runs the same pipeline plus pytest. It does not run Snowflake or Databricks.
