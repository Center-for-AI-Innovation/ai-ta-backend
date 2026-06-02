"""
Standalone pgvector store for the ingest worker.
Same schema as ai_ta_backend.database.vector_store so the worker can run
without importing from the main backend service.
"""
import json
import os
import uuid
from typing import Any, Dict, List, Optional, Sequence


def _get_pg_connection_params() -> Dict[str, str]:
    return {
        "host": os.getenv("POSTGRES_ENDPOINT", os.getenv("POSTGRES_HOST", "localhost")),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DATABASE", os.getenv("POSTGRES_DB", "postgres")),
        "user": os.getenv("POSTGRES_USERNAME", os.getenv("POSTGRES_USER", "postgres")),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
    }


def _payload_to_row(payload: Dict[str, Any], vector: List[float], point_id: Optional[str] = None) -> Dict[str, Any]:
    """Convert Qdrant-style payload + vector to pgvector row dict."""
    doc_groups = payload.get("doc_groups")
    if isinstance(doc_groups, list):
        doc_groups_json = json.dumps(doc_groups)
    elif doc_groups is not None and doc_groups != "":
        doc_groups_json = json.dumps([doc_groups] if isinstance(doc_groups, str) else doc_groups)
    else:
        doc_groups_json = "[]"

    pagenumber = payload.get("pagenumber")
    if pagenumber is not None and not isinstance(pagenumber, str):
        pagenumber = str(pagenumber)

    return {
        "qdrant_id": uuid.UUID(point_id) if point_id else None,
        "embedding": vector,
        "page_content": payload.get("page_content") or "",
        "course_name": payload.get("course_name"),
        "s3_path": payload.get("s3_path"),
        "readable_filename": payload.get("readable_filename"),
        "url": payload.get("url"),
        "base_url": payload.get("base_url"),
        "doc_groups": doc_groups_json,
        "chunk_index": payload.get("chunk_index"),
        "pagenumber": pagenumber,
        "timestamp": payload.get("timestamp"),
        "conversation_id": payload.get("conversation_id"),
    }


def _vector_literal(vec: Sequence[float]) -> str:
    return "[" + ",".join(repr(float(x)) for x in vec) + "]"


class PgVectorStore:
    """pgvector implementation for the embeddings table.

    Defaults to host POSTGRES_* env vars; pass ``engine=`` or
    ``connection_uri=`` to bind to a per-project external Postgres.
    """

    TABLE = "embeddings"

    def __init__(
        self,
        engine: Optional[Any] = None,
        connection_uri: Optional[str] = None,
    ):
        import psycopg2

        self._psycopg2 = psycopg2
        self._engine = None
        self._conn_params: Optional[Dict[str, str]] = None

        if connection_uri:
            from sqlalchemy import create_engine

            self._engine = create_engine(connection_uri, pool_pre_ping=True)
        elif engine is not None:
            self._engine = engine
        else:
            self._conn_params = _get_pg_connection_params()

    def _conn(self):
        if self._engine is not None:
            return self._engine.raw_connection()
        return self._psycopg2.connect(**self._conn_params)  # type: ignore[arg-type]

    def upsert_batch(
        self,
        ids: List[str],
        vectors: List[List[float]],
        payloads: List[Dict[str, Any]],
        wait: bool = True,
    ) -> None:
        """Insert or update by qdrant_id. Uses ON CONFLICT (qdrant_id) DO UPDATE."""
        if not ids:
            return
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                for point_id, vector, payload in zip(ids, vectors, payloads):
                    row = _payload_to_row(payload, vector, point_id)
                    cur.execute(
                        """
                        INSERT INTO embeddings (qdrant_id, embedding, page_content, course_name, s3_path,
                          readable_filename, url, base_url, doc_groups, chunk_index, pagenumber, "timestamp", conversation_id)
                        VALUES (%s, %s::vector, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                        ON CONFLICT (qdrant_id) DO UPDATE SET
                          embedding = EXCLUDED.embedding,
                          page_content = EXCLUDED.page_content,
                          course_name = EXCLUDED.course_name,
                          s3_path = EXCLUDED.s3_path,
                          readable_filename = EXCLUDED.readable_filename,
                          url = EXCLUDED.url,
                          base_url = EXCLUDED.base_url,
                          doc_groups = EXCLUDED.doc_groups,
                          chunk_index = EXCLUDED.chunk_index,
                          pagenumber = EXCLUDED.pagenumber,
                          "timestamp" = EXCLUDED."timestamp",
                          conversation_id = EXCLUDED.conversation_id,
                          updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            row["qdrant_id"],
                            str(row["embedding"]),
                            row["page_content"],
                            row["course_name"],
                            row["s3_path"],
                            row["readable_filename"],
                            row["url"],
                            row["base_url"],
                            row["doc_groups"],
                            row["chunk_index"],
                            row["pagenumber"],
                            row["timestamp"],
                            row["conversation_id"],
                        ),
                    )
            if wait:
                conn.commit()
        finally:
            conn.close()

    def delete_by_filter(self, key: str, value: str) -> int:
        """Delete rows where key = value. Returns number of deleted rows.

        Raises ValueError if ``key`` is not one of the supported filter
        columns — silently rewriting unknown keys would delete unrelated rows.
        """
        if key not in ("course_name", "s3_path", "url", "conversation_id", "readable_filename"):
            raise ValueError(
                f"PgVectorStore.delete_by_filter: unsupported key {key!r}. "
                "Allowed: course_name, s3_path, url, conversation_id, readable_filename."
            )
        conn = self._conn()
        try:
            try:
                with conn.cursor() as cur:
                    cur.execute(f'DELETE FROM {self.TABLE} WHERE "{key}" = %s', (value,))
                    n = cur.rowcount
                conn.commit()
                return n or 0
            except Exception:
                conn.rollback()
                raise
        finally:
            conn.close()


def get_vector_store() -> PgVectorStore:
    return PgVectorStore()
