"""
End-to-end upsert + search round-trip for PgVectorStore.

Skipped unless ``TEST_POSTGRES_URI`` is set in the environment — these tests
need a live pgvector-capable Postgres with the `embeddings` table already
migrated. Useful when developing locally; CI can light up a Postgres service
and set the env var to enable.
"""

from __future__ import annotations

import os
import uuid

import pytest


PG_URI = os.environ.get("TEST_POSTGRES_URI")
pytestmark = pytest.mark.skipif(
    not PG_URI,
    reason="TEST_POSTGRES_URI not set; skipping live pgvector roundtrip",
)


def _vec(n: int, seed: int) -> list[float]:
    # Deterministic 4096-dim vector aligned with the embeddings.embedding column.
    return [((seed + i) % 7) * 0.01 for i in range(n)]


@pytest.fixture
def store():
    from ai_ta_backend.database.vector_store import PgVectorStore

    return PgVectorStore(connection_uri=PG_URI)


def test_upsert_then_search_returns_inserted_row(store):
    course = f"pgtest_{uuid.uuid4().hex[:8]}"
    pid = str(uuid.uuid4())
    vec = _vec(4096, seed=1)
    payload = {
        "page_content": "hello pgvector world",
        "course_name": course,
        "readable_filename": "doc.txt",
        "s3_path": "s3://b/doc.txt",
        "doc_groups": ["g1"],
        "pagenumber": "1",
    }

    store.upsert_batch([pid], [vec], [payload])
    try:
        rows = store.search(
            query_embedding=vec,
            course_name=course,
            doc_groups=[],
            disabled_doc_groups=[],
            public_doc_groups=[],
            top_n=5,
        )
        assert any(r["readable_filename"] == "doc.txt" for r in rows)
        assert rows[0]["score"] > 0.5
    finally:
        store.delete_by_filter("course_name", course)
