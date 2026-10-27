# Case study: synthetic investment portfolio data to retrieval-ready context

## Problem

Investment portfolio reporting often brings together fund reference data, holdings, deal transactions, valuations, company operating indicators, and narrative notes. These sources have different grains and update patterns. A daily holding is not the same kind of event as a transaction, while a company sector can change over time. A downstream analyst needs repeatable totals and a way to trace each number to a consistent model. An AI application also needs concise context with source and date metadata, without exposing direct personal information.

This prototype demonstrates those engineering patterns with generated, fictional records. It does not use real client, company, employer, or fund information and makes no claim of production use. The local execution target is PostgreSQL 16 with pgvector, with dbt managing transformations and tests.

## Data and generation

The generator creates eight funds and a configurable portfolio company set (400 by default). It also creates daily position rows, transactions, dated valuations, revenue-growth and margin KPIs, and short company notes. Company groups are synthetic. A fixed random seed and date make runs repeatable. By default, 30 days of holdings keep the local database modest; `--days` and `--companies` increase the volume for experiments.

The inputs deliberately include a repeated holding, an orphan company reference, one late-ingested transaction, and a company attribute version whose ingestion time is in the future relative to the first run. Staging deduplicates stable source keys and joins valid company/fund references. As the changed company row becomes available on a later date, the timestamp snapshot can retain a prior row and open a new SCD2 version.

## Design

The pipeline uses ELT: generated CSV files are loaded as raw dbt seeds, and SQL models do the type casting, deduplication, enrichment, and dimensional modeling inside Postgres. The staging layer removes invalid references, the intermediate layer enriches holdings and transactions, and the marts expose two facts with fund, company, and date dimensions. Each mart has a dbt model contract. Company history is snapshot-backed and carries parent group identifiers.

The holdings fact uses merge keyed by holding ID and reprocesses a 30-day date overlap. Transactions use merge keyed by transaction ID and re-read a 90-day ingestion overlap so a late record with an old business date can still be incorporated. The pipeline runs the dbt build twice and compares counts and deterministic checksums for both fact tables. This catches changes in output on repeat execution. Changes arriving outside the documented window require a full refresh.

Data controls include dbt source freshness, standard uniqueness/null/relationship/value tests, holdings reconciliation against the latest daily NAV snapshot with a 25% illustrative tolerance, a future valuation check, and a single-current-company check. The pytest suite also checks deterministic generation, defect creation, project contracts, runner order, vector-search behavior, and firm-neutral content. Synthetic records and rules are examples, not investment advice or business controls.

## AI-ready layer

`ai_company_profile` presents selected non-PII attributes and source lineage. `ai_text_chunks` contains short note text with source, as-of date, owner, and classification. PostgreSQL stores an 8-dimensional pgvector column and an HNSW cosine index. The default vectors derive deterministically from text hashes and are **test only; not semantic**. They demonstrate load/index/query wiring but do not establish retrieval quality. An optional OpenAI provider is available behind an explicit CLI flag and an environment variable. It can incur API charges and is not needed for CI.

The search demo returns the top `k` note chunks with provenance metadata. A consuming application could pass that context to a language model, but this prototype does not include an LLM call, prompt, or answer-quality evaluation.

## Platform comparison and results

The runnable Phase 1 target is PostgreSQL. Snowflake and Databricks comparison builds are Phase 2 work and have not been run. No timing, credit, DBU, or cloud row-count measurements are available, so there are no cloud results in this case study.

The planned Snowflake layout uses a small auto-suspending warehouse, staged loads, and holdings clustering. Snowflake offers managed warehouse operations and micro-partition pruning; clustering maintenance and compute can still cost money. The planned Databricks layout uses Delta tables and a serverless SQL warehouse, with liquid clustering or a Z-ORDER fallback. It offers a lakehouse storage approach, while catalog and clustering support can vary by edition. These are design notes, not measured comparisons.

## Trade-offs and limitations

PostgreSQL provides one easy-to-run local target and pgvector keeps retrieval data beside warehouse tables. It does not validate cloud-specific semantics. The small generated dataset is useful for tests, but does not establish performance at millions of rows. The 8D test vectors are deliberately compact and have no semantic quality. The synthetic ownership, NAV tolerance, and classification values are illustrative. The 90-day and 30-day incremental overlaps are bounded. The model uses a current company key for facts rather than a complete as-of dimension lookup. Cloud security, cost, and performance remain unvalidated until Phase 2.

## How to reproduce

From a clean checkout, install the pinned Python dependencies, set `POSTGRES_PASSWORD` in the PowerShell environment, start Compose, run `python pipeline/run.py --target postgres`, and then run `pytest -q`. The README contains the exact commands and shutdown steps. CI follows the same Postgres-only path. The generated CSVs and dbt build artifacts are ignored by Git.
