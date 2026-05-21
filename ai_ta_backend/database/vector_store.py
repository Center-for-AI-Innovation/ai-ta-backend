"""
Vector store abstraction for Illinois Chat.
Main collection uses pgvector only (Qdrant removed).
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
    """pgvector accepts a string of the form '[v1,v2,...]'. Avoids ROW-arg limits."""
    return "[" + ",".join(repr(float(x)) for x in vec) + "]"


class PgVectorStore:
    """pgvector implementation for main Illinois Chat collection.

    Defaults to the host Postgres (POSTGRES_* env vars) when constructed with
    no args. Pass ``engine=`` or ``connection_uri=`` to bind to a per-project
    external Postgres instead.
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

    # ------------------------------------------------------------------
    # Search — port of the frontend vectorSearchWithDrizzle. Same filter
    # semantics: course chunks scoped by doc_groups + public_doc_groups
    # (JSONB @> containment), conversation-specific chunks merged in when
    # conversation_id is supplied, disabled doc_groups excluded.
    # ------------------------------------------------------------------

    def search(
        self,
        query_embedding: Sequence[float],
        course_name: str,
        doc_groups: Optional[Sequence[str]] = None,
        disabled_doc_groups: Optional[Sequence[str]] = None,
        public_doc_groups: Optional[Sequence[Dict[str, Any]]] = None,
        conversation_id: Optional[str] = None,
        top_n: int = 100,
    ) -> List[Dict[str, Any]]:
        """Cosine-distance search. Returns rows with a `score` in [0, 1]."""

        doc_groups = list(doc_groups or [])
        disabled_doc_groups = list(disabled_doc_groups or [])
        public_doc_groups = list(public_doc_groups or [])

        vec_lit = _vector_literal(query_embedding)

        params: List[Any] = [vec_lit]  # %s::vector at position 1
        where_clauses: List[str] = []

        # ---- regular (non-conversation) condition ----
        regular_parts: List[str] = [
            '(conversation_id IS NULL OR conversation_id = \'\')',
        ]
        # SHOULD: own course (optionally narrowed to doc_groups) OR any enabled
        # public doc_group on its origin course.
        should_clauses: List[str] = []
        own_parts: List[str] = ["course_name = %s"]
        params.append(course_name)
        if doc_groups and "All Documents" not in doc_groups:
            overlap, overlap_params = _doc_groups_overlap_sql("doc_groups", doc_groups)
            if overlap:
                own_parts.append(overlap)
                params.extend(overlap_params)
        should_clauses.append("(" + " AND ".join(own_parts) + ")")

        for pdg in public_doc_groups:
            if not pdg.get("enabled"):
                continue
            pdg_course = pdg.get("course_name")
            pdg_name = pdg.get("name")
            if pdg_course is None or pdg_name is None:
                continue
            should_clauses.append(
                "(course_name = %s AND doc_groups @> %s::jsonb)"
            )
            params.append(pdg_course)
            params.append(json.dumps([pdg_name]))

        if len(should_clauses) == 1:
            regular_parts.append(should_clauses[0])
        else:
            regular_parts.append("(" + " OR ".join(should_clauses) + ")")

        if disabled_doc_groups:
            overlap, overlap_params = _doc_groups_overlap_sql(
                "doc_groups", disabled_doc_groups
            )
            if overlap:
                regular_parts.append(f"NOT ({overlap})")
                params.extend(overlap_params)

        regular_sql = "(" + " AND ".join(regular_parts) + ")"

        if conversation_id:
            where_clauses.append(
                f"({regular_sql} OR conversation_id = %s)"
            )
            params.append(conversation_id)
        else:
            where_clauses.append(regular_sql)

        # ORDER BY uses the SAME vector literal as the score expression —
        # bind once and reference it twice in the SQL.
        sql = f"""
            SELECT
              page_content,
              readable_filename,
              course_name,
              s3_path,
              pagenumber,
              url,
              base_url,
              doc_groups,
              conversation_id,
              (1 - (embedding <=> %s::vector)) AS score
            FROM {self.TABLE}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY embedding <=> %s::vector
            LIMIT %s
        """
        # The %s::vector in SELECT consumed the first vec_lit at index 0;
        # we need a second vec_lit for ORDER BY and finally top_n.
        params.append(vec_lit)
        params.append(int(top_n))

        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            conn.close()


def _doc_groups_overlap_sql(column: str, names: Sequence[str]) -> tuple[str, List[str]]:
    """Return (sql, params) matching any of the given doc_group names via JSONB @>."""
    names = list(names)
    if not names:
        return "", []
    if len(names) == 1:
        return f"({column} @> %s::jsonb)", [json.dumps([names[0]])]
    parts = [f"({column} @> %s::jsonb)" for _ in names]
    params = [json.dumps([n]) for n in names]
    return "(" + " OR ".join(parts) + ")", params


def get_vector_store() -> PgVectorStore:
    return PgVectorStore()
