"""
Vector store abstraction for Illinois Chat.
Main collection uses pgvector only (Qdrant removed).
"""
import json
import os
import uuid
from typing import Any, Dict, List, Optional, Tuple

# Optional Qdrant types for filter conversion (only when using Qdrant filter objects)
try:
    from qdrant_client import models as qdrant_models
    from qdrant_client.http import models as qdrant_http_models
except ImportError:
    qdrant_models = None
    qdrant_http_models = None


# Result type compatible with retrieval_service._process_search_results (expects .payload, .score)
class SearchResult:
    __slots__ = ("payload", "score")

    def __init__(self, payload: Dict[str, Any], score: float):
        self.payload = payload
        self.score = score


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


def _row_to_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    """Build Qdrant-style payload dict from pgvector row for retrieval_service."""
    payload = {
        "page_content": row.get("page_content") or "",
        "course_name": row.get("course_name"),
        "s3_path": row.get("s3_path"),
        "readable_filename": row.get("readable_filename"),
        "url": row.get("url"),
        "base_url": row.get("base_url"),
        "doc_groups": row.get("doc_groups") if isinstance(row.get("doc_groups"), (list, str)) else (json.loads(row["doc_groups"]) if row.get("doc_groups") else []),
        "chunk_index": row.get("chunk_index"),
        "pagenumber": row.get("pagenumber"),
        "timestamp": row.get("timestamp"),
        "conversation_id": row.get("conversation_id"),
    }
    return {k: v for k, v in payload.items() if v is not None or k == "page_content"}


def _one_condition_to_sql(cond: Any, params: List[Any]) -> str:
    """Convert a single condition to SQL fragment; append any params to params list."""
    if cond is None:
        return "1=1"
    if hasattr(cond, "key") and hasattr(cond, "match"):
        key = cond.key
        match = getattr(cond, "match", None)
        if match is None:
            return "1=1"
        if hasattr(match, "value"):
            params.append(match.value)
            return f'"{key}" = %s'
        if hasattr(match, "any"):
            any_list = match.any if isinstance(match.any, list) else [match.any]
            if key == "doc_groups":
                params.append(json.dumps(any_list))
                return f'"{key}" && %s::jsonb'
            params.append(any_list)
            return f'"{key}" = ANY(%s)'
        return "1=1"
    if hasattr(cond, "is_empty") and isinstance(cond.is_empty, dict):
        key = cond.is_empty.get("key", "conversation_id")
        return f'("{key}" IS NULL OR "{key}" = \'\')'
    if hasattr(cond, "must") and cond.must:
        inner = [_one_condition_to_sql(m, params) for m in cond.must]
        return "(" + " AND ".join(inner) + ")"
    return "1=1"


def qdrant_filter_to_sql(filter_obj: Any) -> Tuple[str, List[Any]]:
    """
    Convert Qdrant models.Filter to (WHERE clause fragment, params list).
    Handles must, should, must_not; FieldCondition (MatchValue, MatchAny), IsEmptyCondition.
    """
    if filter_obj is None:
        return "1=1", []

    params: List[Any] = []
    parts = []

    if getattr(filter_obj, "must", None):
        must_parts = [_one_condition_to_sql(m, params) for m in filter_obj.must]
        if must_parts:
            parts.append("(" + " AND ".join(must_parts) + ")")
    if getattr(filter_obj, "should", None):
        should_parts = [_one_condition_to_sql(s, params) for s in filter_obj.should]
        if should_parts:
            parts.append("(" + " OR ".join(should_parts) + ")")
    if getattr(filter_obj, "must_not", None):
        for m in filter_obj.must_not:
            parts.append("(NOT (" + _one_condition_to_sql(m, params) + "))")

    where = " AND ".join(parts) if parts else "1=1"
    return where, params


class PgVectorStore:
    """pgvector implementation for main Illinois Chat collection. Same schema/behavior as Qdrant."""

    TABLE = "embeddings"

    def __init__(self):
        import psycopg2
        self._conn_params = _get_pg_connection_params()
        self._psycopg2 = psycopg2

    def _conn(self):
        return self._psycopg2.connect(**self._conn_params)

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

    def search(
        self,
        query_vector: List[float],
        query_filter: Any,
        limit: int,
    ) -> List[SearchResult]:
        """Cosine similarity search with filter. Returns list of SearchResult (payload, score)."""
        where_sql, filter_params = qdrant_filter_to_sql(query_filter)
        params = [str(query_vector), str(query_vector), limit] + filter_params
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT page_content, course_name, s3_path, readable_filename, url, base_url,
                           doc_groups, chunk_index, pagenumber, "timestamp", conversation_id,
                           (1 - (embedding <=> %s::vector)) AS score
                    FROM {self.TABLE}
                    WHERE {where_sql}
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                    """,
                    params,
                )
                rows = cur.fetchall()
            conn.close()
        except Exception:
            conn.close()
            raise

        colnames = [
            "page_content", "course_name", "s3_path", "readable_filename", "url", "base_url",
            "doc_groups", "chunk_index", "pagenumber", "timestamp", "conversation_id", "score",
        ]
        results = []
        for row in rows:
            d = dict(zip(colnames, row))
            score = float(d.pop("score", 0.0))
            doc_groups = d.get("doc_groups")
            if isinstance(doc_groups, str):
                try:
                    d["doc_groups"] = json.loads(doc_groups)
                except Exception:
                    d["doc_groups"] = []
            payload = _row_to_payload(d)
            results.append(SearchResult(payload=payload, score=score))
        return results

    def update_doc_groups(
        self,
        course_name: str,
        s3_path: str,
        url: Optional[str],
        doc_groups: Any,
    ) -> bool:
        """Update doc_groups for points matching course_name, s3_path, and optional url."""
        if isinstance(doc_groups, list):
            doc_groups_json = json.dumps(doc_groups)
        else:
            doc_groups_json = json.dumps([doc_groups] if doc_groups else [])
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                if url is not None and url != "":
                    cur.execute(
                        """
                        UPDATE embeddings SET doc_groups = %s::jsonb, updated_at = CURRENT_TIMESTAMP
                        WHERE course_name = %s AND s3_path = %s AND url = %s
                        """,
                        (doc_groups_json, course_name, s3_path, url),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE embeddings SET doc_groups = %s::jsonb, updated_at = CURRENT_TIMESTAMP
                        WHERE course_name = %s AND s3_path = %s AND (url IS NULL OR url = '')
                        """,
                        (doc_groups_json, course_name, s3_path),
                    )
                n = cur.rowcount
            conn.commit()
            conn.close()
            return n is not None and n > 0
        except Exception:
            conn.rollback()
            conn.close()
            raise

    def delete_by_filter(self, key: str, value: str) -> int:
        """Delete rows where key = value. Returns number of deleted rows."""
        if key not in ("course_name", "s3_path", "url", "conversation_id", "readable_filename"):
            key = "s3_path"
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DELETE FROM {self.TABLE} WHERE "{key}" = %s', (value,))
                n = cur.rowcount
            conn.commit()
            conn.close()
            return n or 0
        except Exception:
            conn.rollback()
            conn.close()
            raise


def get_vector_store() -> PgVectorStore:
    """Return pgvector store for main Illinois Chat collection (Qdrant removed)."""
    return PgVectorStore()
