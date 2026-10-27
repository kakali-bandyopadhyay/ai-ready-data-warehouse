# Learning notes

Examples use synthetic information only.

| Concept | Plain-language explanation | Implementation |
| --- | --- | --- |
| Star schema | Facts store measurable events; dimensions describe the people, things, and dates around them. | [`models/marts/`](../models/marts/) |
| SCD Type 2 | Keep old attribute versions with start/end validity so past changes remain visible. | [`snapshots/company_history.sql`](../snapshots/company_history.sql), [`dim_company.sql`](../models/marts/dim_company.sql) |
| Incremental load | Rebuild only the changing slice and merge by a stable key. | [`fct_holdings_daily.sql`](../models/marts/fct_holdings_daily.sql), [`fct_transactions.sql`](../models/marts/fct_transactions.sql) |
| Idempotent run | Repeating a transformation leaves the same facts, verified here by row counts and content checksums. | [`pipeline/run.py`](../pipeline/run.py) |
| Late-arriving data | A transaction can arrive after its business date; use ingestion time and a lookback so it is not lost. | [`generator/generate.py`](../generator/generate.py), [`fct_transactions.sql`](../models/marts/fct_transactions.sql) |
| Data contract | A model promises column names/types and tests its keys and relationships. | [`models/schema.yml`](../models/schema.yml) and mart SQL contracts |
| ELT | Load first, then transform in the database using versioned SQL. | [`pipeline/run.py`](../pipeline/run.py), [`models/`](../models/) |
| Micro-partitions and clustering | Snowflake stores data in micro-partitions; clustering can improve pruning for commonly filtered columns. | Phase 2 plan in [`DESIGN_DECISIONS.md`](DESIGN_DECISIONS.md) |
| Delta Lake and lakehouse | Delta adds a transaction log and table behavior over data-lake storage. | Phase 2 plan in [`DESIGN_DECISIONS.md`](DESIGN_DECISIONS.md) |
| Serverless compute cost | The provider manages compute startup, but elapsed queries and idle policy still affect charges. | Phase 2 cost controls in [`DESIGN_DECISIONS.md`](DESIGN_DECISIONS.md) |
| Embeddings and vector search | An embedding maps text to numbers; nearest-vector search finds numerically similar items. Deterministic vectors here are test fixtures and not semantic. | [`pipeline/embeddings.py`](../pipeline/embeddings.py), [`pipeline/vector_search.py`](../pipeline/vector_search.py) |
| Relational, graph, and other NoSQL | Relational tables suit consistent facts and joins; graphs suit multi-hop relationships; document/key-value stores fit other access patterns. | Relational model now; graph projection is a separate possible Phase 3 addition. |

The local model uses an 8-dimensional vector to keep the demonstration compact. The optional API provider asks for the same dimension so query and stored vectors remain compatible. A small vector size is for demonstration and should not be treated as a quality benchmark.
