from qdrant_client import QdrantClient
from psycopg2.extras import execute_batch
import psycopg2
import json
import os


COLLECTION_NAME = "illinois_chat"
BATCH_SIZE = 500


# Connect to Qdrant and PostgreSQL
pg_params = {
    "host": os.getenv("POSTGRES_ENDPOINT", os.getenv("POSTGRES_HOST", "localhost")),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DATABASE", os.getenv("POSTGRES_DB", "postgres")),
    "user": os.getenv("POSTGRES_USERNAME", os.getenv("POSTGRES_USER", "postgres")),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}
conn = psycopg2.connect(**pg_params)
conn.autocommit = False
cur = conn.cursor()

qdrant_client = QdrantClient(
    url=os.getenv('QDRANT_URL'),
    api_key=os.getenv('QDRANT_API_KEY'),
)


# Iterate over Qdrant collection and do batch inserts
offset = None

while True:
    points, offset = qdrant_client.scroll(
        collection_name=COLLECTION_NAME,
        limit=BATCH_SIZE, offset=offset,
        with_payload=True, with_vectors=True)
    if not points:
        break

    rows = []
    for point in points:
        payload = point.payload or {}
        row = {
            "qdrant_id": str(point.id),
            "embedding": "[" + ",".join(map(str, point.vector)) + "]",
            "page_content": payload.get("page_content"),
            "course_name": payload.get("course_name"),
            "s3_path": payload.get("s3_path"),
            "readable_filename": payload.get("readable_filename"),
            "url": payload.get("url"),
            "base_url": payload.get("base_url"),
            "doc_groups": json.dumps(payload.get("doc_groups")) if payload.get("doc_groups") is not None else None,
            "chunk_index": payload.get("chunk_index"),
            "pagenumber": payload.get("pagenumber"),
            "timestamp": payload.get("timestamp"),
            "conversation_id": payload.get("conversation_id"),
        }
        rows.append((
            row["qdrant_id"],
            row["embedding"],
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
        ))

    execute_batch(cur,
        """
        INSERT INTO embeddings (
            qdrant_id, embedding, page_content, course_name, s3_path,
            readable_filename, url, base_url, doc_groups,
            chunk_index, pagenumber, "timestamp", conversation_id
        )
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
        rows, page_size=100)

    conn.commit()
    print(f"Inserted batch of {len(rows)}, next offset: {offset}")
    if offset is None:
        break

cur.close()
conn.close()