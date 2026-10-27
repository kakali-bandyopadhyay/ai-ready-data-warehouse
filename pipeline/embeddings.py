"""Embedding providers for the AI-ready demo.

The default hash-based vectors are deterministic test fixtures, not semantic
embeddings. OpenAI mode is optional and uses its official API over HTTPS.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import urllib.request

DIMENSIONS = 8


def test_embedding(text: str, dimensions: int = DIMENSIONS) -> list[float]:
    """Return a deterministic, normalized pseudo-vector (test only)."""
    raw = hashlib.shake_256(text.encode("utf-8")).digest(dimensions * 4)
    vals = [((int.from_bytes(raw[i:i + 4], "big") / 2**32) * 2.0) - 1.0
            for i in range(0, len(raw), 4)]
    norm = math.sqrt(sum(v * v for v in vals)) or 1.0
    return [v / norm for v in vals]


def openai_embedding(text: str, dimensions: int = DIMENSIONS) -> list[float]:
    """Fetch an embedding from OpenAI text-embedding-3-small."""
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY is required for the openai embedding provider")
    body = json.dumps({"model": "text-embedding-3-small", "input": text,
                       "dimensions": dimensions}).encode("utf-8")
    request = urllib.request.Request("https://api.openai.com/v1/embeddings", data=body,
                                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.load(response)
    return payload["data"][0]["embedding"]


def embed(text: str, provider: str = "test") -> list[float]:
    if provider == "test":
        return test_embedding(text)
    if provider == "openai":
        return openai_embedding(text)
    raise ValueError(f"Unsupported embedding provider: {provider}")

