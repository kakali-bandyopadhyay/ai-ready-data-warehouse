"""Populate the vector column and perform top-k similarity search in Postgres."""
from __future__ import annotations

import os
from typing import Any

from pipeline.embeddings import embed


def _connection():
    try:
        import psycopg  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install requirements.txt before running the pipeline") from exc
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("Set POSTGRES_PASSWORD in the environment before connecting to PostgreSQL")
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"), port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "warehouse"), user=os.getenv("POSTGRES_USER", "warehouse"),
        password=password,
    )


def populate_embeddings(provider: str = "test") -> int:
    """Idempotently replace vectors for every chunk; returns rows updated."""
    with _connection() as conn, conn.cursor() as cur:
        cur.execute("select chunk_id, chunk_text from ai_ready.ai_text_chunks order by chunk_id")
        chunks = cur.fetchall()
        for chunk_id, chunk_text in chunks:
            vector = embed(chunk_text, provider)
            vector_literal = "[" + ",".join(f"{n:.9f}" for n in vector) + "]"
            cur.execute("update ai_ready.ai_text_chunks set embedding = %s::vector where chunk_id = %s",
                        (vector_literal, chunk_id))
        conn.commit()
    return len(chunks)


def search_similar(query: str, k: int = 5, *, provider: str = "test") -> list[dict[str, Any]]:
    """Return nearest chunks, including source, as-of date and classification."""
    if k < 1:
        raise ValueError("k must be at least 1")
    vector = embed(query, provider)
    literal = "[" + ",".join(f"{n:.9f}" for n in vector) + "]"
    with _connection() as conn, conn.cursor() as cur:
        cur.execute("""select chunk_id, company_id, chunk_text, source_name, as_of_date,
                             owner, classification, embedding <=> %s::vector as distance
                      from ai_ready.ai_text_chunks where embedding is not null
                      order by embedding <=> %s::vector limit %s""", (literal, literal, k))
        return [dict(zip(("chunk_id", "company_id", "chunk_text", "source_name", "as_of_date",
                          "owner", "classification", "distance"), row)) for row in cur.fetchall()]

