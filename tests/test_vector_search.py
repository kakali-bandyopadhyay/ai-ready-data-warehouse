from __future__ import annotations

import os
from pathlib import Path

import pytest

from pipeline.vector_search import search_similar


@pytest.mark.parametrize("k", [0, -1])
def test_search_requires_positive_k(k: int) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        search_similar("synthetic company", k)


def test_vector_search_integration_returns_top_k_and_metadata() -> None:
    if os.getenv("WAREHOUSE_INTEGRATION") != "1":
        pytest.skip("Set WAREHOUSE_INTEGRATION=1 after pipeline has loaded embeddings")
    matches = search_similar("synthetic company in software", k=3)
    assert len(matches) == 3
    assert all(row["source_name"] and row["as_of_date"] and row["classification"] for row in matches)


def test_vector_model_has_pgvector_column_and_hnsw_index() -> None:
    sql = (Path(__file__).resolve().parents[1] / "models" / "ai_ready" / "ai_text_chunks.sql").read_text(encoding="utf-8")
    assert "vector(8)" in sql
    assert "using hnsw" in sql


def test_search_orders_by_cosine_distance_and_limits_k() -> None:
    import inspect
    source = inspect.getsource(search_similar)
    assert "embedding <=>" in source
    assert "limit %s" in source

