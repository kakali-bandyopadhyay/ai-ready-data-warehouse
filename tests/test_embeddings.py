from __future__ import annotations

import pytest

from pipeline.embeddings import DIMENSIONS, embed, test_embedding


@pytest.mark.parametrize("text", ["", "synthetic", "Company A", "same text", "unicode ☁", "long " * 200])
def test_test_embeddings_are_deterministic_and_normalized(text: str) -> None:
    value = test_embedding(text)
    assert value == test_embedding(text)
    assert len(value) == DIMENSIONS
    assert sum(x * x for x in value) == pytest.approx(1.0)


def test_embedding_provider_validation() -> None:
    with pytest.raises(ValueError):
        embed("q", "unsupported")


def test_openai_provider_requires_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    from pipeline.embeddings import openai_embedding
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        openai_embedding("synthetic")

