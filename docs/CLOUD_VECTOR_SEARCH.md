# Cloud vector search notes

The cloud dbt project materializes the retrieval text and provenance metadata, but leaves `embedding` null. The measurement run deliberately does not call an embedding provider or create a search service. Cloud vector search is therefore documented, not run, and these steps need a real embedding source and target account.

## Snowflake

The cloud model declares `embedding` as `VECTOR(FLOAT, 8)`. Once a compatible eight-dimensional embedding has been populated, Snowflake SQL can rank rows with `VECTOR_COSINE_SIMILARITY`:

```sql
SELECT chunk_id, chunk_text, source_name, as_of_date, owner, classification,
       VECTOR_COSINE_SIMILARITY(embedding, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]::VECTOR(FLOAT, 8)) AS similarity
FROM AI_READY_DEMO.AI_READY_DEMO_AI_READY.AI_TEXT_CHUNKS
WHERE embedding IS NOT NULL
ORDER BY similarity DESC
LIMIT 5;
```

The vector literal is illustrative and is not an embedding. Snowflake documents the vector type and cosine similarity function in [Vector data types](https://docs.snowflake.com/en/sql-reference/data-types-vector) and [VECTOR_COSINE_SIMILARITY](https://docs.snowflake.com/en/sql-reference/functions/vector_cosine_similarity).

## Databricks

The cloud model declares `embedding` as `ARRAY<FLOAT>`. A Databricks AI Search index can be created from a Unity Catalog Delta source table with populated embeddings; index creation and querying require workspace features and permissions, and serverless compute is required for the documented AI Search workflow. Confirm the table meets the index's source requirements (including Change Data Feed for a standard Delta Sync index) before creating the service. See [create AI Search endpoints and indexes](https://docs.databricks.com/aws/en/vector-search/create-vector-search).

For example, after an index named `my_catalog.ai_ready_demo_ai_ready.company_notes_index` exists and the vector column is populated, query it through the Python SDK or REST API from an authorized client. The index, endpoint, embedding model, sync policy, and charges are intentionally not created by this repository's dbt build or measurement script. See [Databricks AI Search](https://docs.databricks.com/aws/en/vector-search/vector-search).

The local PostgreSQL vector-search demo remains the only executable semantic-retrieval interface in this checkout. Its deterministic fixture vectors are test-only and not semantic.
