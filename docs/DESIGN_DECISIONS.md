# Design decisions

These records describe the Phase 1 local implementation and the intended Phase 2 cloud direction. Synthetic data is used throughout.

## ELT over ETL

**Decision:** load generated source rows into PostgreSQL first, then transform with dbt. This keeps raw seed tables available for inspection and places transformation logic in versioned SQL models. Implemented by [`pipeline/run.py`](../pipeline/run.py), `dbt seed`, and `models/`.

## Incremental fact strategy

**Decision:** use PostgreSQL merge materialization keyed by stable holding and transaction IDs. The transaction fact re-reads a 90-day ingestion-time overlap to catch recent late-arriving records. Holdings re-reads a 30-day date overlap. The runner performs a second build and compares counts and checksums. The overlap is a documented lateness bound; older changes require a full refresh. Implemented in [`fct_holdings_daily.sql`](../models/marts/fct_holdings_daily.sql), [`fct_transactions.sql`](../models/marts/fct_transactions.sql), and [`run.py`](../pipeline/run.py).

## SCD Type 2

**Decision:** use a dbt timestamp snapshot keyed by company ID with source ingestion time as the update marker. Preserve changed company versions and derive a surrogate company key from dbt's snapshot key. The current row has no end timestamp. Implemented in [`company_history.sql`](../snapshots/company_history.sql) and [`dim_company.sql`](../models/marts/dim_company.sql).

## Cloud physical design (Phase 2)

Snowflake's planned holdings organization uses a clustering key, which can help prune micro-partitions for repeated date/company filters. The plan is to start with an XS warehouse and 60-second auto-suspend; a resource monitor is account-plan dependent. This target and its costs are not implemented or measured in Phase 1.

Databricks is planned to use Delta tables and a serverless SQL warehouse. Liquid clustering is preferred for evolving filters; Z-ORDER is a fallback where liquid clustering is unavailable. Serverless compute reduces cluster management but can still incur usage charges. Edition-specific Unity Catalog tags and comments will be used only where supported. This target and its costs are not implemented or measured in Phase 1.

## Delta and warehouse tables

Delta Lake is the planned Databricks storage layer and brings transaction log semantics to data files. Snowflake uses managed warehouse tables. PostgreSQL remains the executable source of truth for Phase 1. There is no cloud-specific table code yet.

## pgvector and platform vector search

**Decision:** use pgvector and HNSW cosine distance for the runnable local demo. It keeps retrieval rows and provenance next to relational data. The later platform targets will document Snowflake vector types/similarity and Databricks vector search on Delta; neither has been run or configured in this phase. Implemented locally in [`ai_text_chunks.sql`](../models/ai_ready/ai_text_chunks.sql) and [`vector_search.py`](../pipeline/vector_search.py).

## Cost controls

Local PostgreSQL is containerized and stopped when idle. The cloud plan uses the smallest suitable warehouse, auto-suspend, explicit teardown, and budget alerts before any build. No cloud resources were created or tested in this phase.

## Relational system of record; graph as a later add-on

**Decision:** normalized source keys and dimensional facts are the Phase 1 system of record. Relationship traversal can be added as a separate graph projection in Phase 3 if separately approved. No graph database or graph models are included now.
