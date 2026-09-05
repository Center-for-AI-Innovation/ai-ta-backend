"""Shared vectorization primitives.

Used by both Stage 2 entry points:
- batch_vectorize.py    (incremental backlog drain, 50 articles per Postgres page)
- vectorize_pg_driven.py (Postgres-driven catch-up, parallel A/B against
                          the legacy MinIO-scan tool vectorize_minio_direct.py)

Anything that talks to MinIO/Ollama/Qdrant from a Postgres-row work list lives
here, so the two callers stay thin.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import pymupdf
from minio import Minio
from qdrant_client import QdrantClient, models
from langchain_text_splitters.character import RecursiveCharacterTextSplitter

log = logging.getLogger(__name__)


@dataclass
class VectorizeConfig:
    # Database
    db_dsn: str
    db_schema: str

    # MinIO
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_secure: bool
    file_location_prefix: Optional[str]

    # Embedding — embedding_urls is the authoritative list; each call tries
    # them in order. Single-URL deployments populate it from
    # EMBEDDING_BASE_URL; multi-URL deployments populate it from
    # EMBEDDING_URLS (comma-separated).
    embedding_urls: List[str]
    embedding_retry_max: int
    embedding_timeout: int
    embedding_retry_backoff: float

    # Qdrant
    qdrant_url: str
    qdrant_port: int
    qdrant_api_key: Optional[str]
    qdrant_collection: str
    vector_size: int

    # Vectorization
    chunk_size: int
    chunk_overlap: int
    qdrant_upsert_batch: int


def parse_embedding_urls() -> List[str]:
    raw = os.getenv("EMBEDDING_URLS", "").strip()
    if raw:
        urls = [u.strip() for u in raw.split(",") if u.strip()]
        if urls:
            return urls
    base = os.getenv("EMBEDDING_BASE_URL", "").strip()
    return [base] if base else []


# ============================================================================
# MinIO
# ============================================================================

class MinIOReader:
    def __init__(self, config: VectorizeConfig):
        self.config = config
        endpoint = config.minio_endpoint.rstrip("/")
        if "://" in endpoint:
            parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
            host = parsed.hostname or endpoint
            endpoint = f"{host}:{parsed.port}" if parsed.port else host

        self.client = Minio(
            endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=config.minio_secure,
        )
        self.bucket = config.minio_bucket
        log.info("MinIO client initialized: %s (bucket=%s)", endpoint, self.bucket)

    def download_pdf(self, s3_path: str) -> Optional[bytes]:
        try:
            if s3_path.startswith("s3://"):
                key = s3_path.split("/", 3)[3]
            else:
                key = s3_path
            response = self.client.get_object(self.bucket, key)
            try:
                data = response.read()
            finally:
                response.close()
                response.release_conn()
            return data
        except Exception as e:
            log.warning("MinIO download failed for %s: %s", s3_path, e)
            return None


# ============================================================================
# Chunking
# ============================================================================

def chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n\n", "\n", " ", ""],
    )
    return splitter.split_text(text)


# ============================================================================
# Embedding (multi-URL linear fallback)
# ============================================================================

# nomic-embed-text:v1.5 has an 8192-token context. 4000 chars is ~2x safe
# headroom even for dense scientific text, so we fall back to that on a
# 500/"context length" response and retry once.
EMBEDDING_TRUNCATE_CHARS = 4000


def check_embedding_health(url: str, timeout: int = 5) -> bool:
    try:
        resp = requests.post(
            url,
            json={"model": "nomic-embed-text:v1.5", "prompt": "test"},
            timeout=timeout,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        log.warning("Embedding endpoint health check failed for %s: %s", url, e)
        return False


def get_embedding(config: VectorizeConfig, text: str) -> Optional[List[float]]:
    """Get an embedding, falling back across config.embedding_urls in order.

    Strategy:
      - For each URL, attempt config.embedding_retry_max times with exponential
        backoff on timeouts/connection errors.
      - On a 500 with "context length" in the body, truncate the chunk to
        EMBEDDING_TRUNCATE_CHARS and retry without consuming an attempt.
        Truncation is per-text and persists across URL fallbacks.
      - When the budget for a URL is exhausted, advance to the next URL.
      - Returns None only after every URL's budget is exhausted.

    A "content failure" (truncation didn't help) returns None immediately
    without trying further URLs — that's a chunk problem, not an endpoint
    problem.
    """
    urls = config.embedding_urls or []
    if not urls:
        log.error("No embedding URLs configured (set EMBEDDING_URLS or EMBEDDING_BASE_URL)")
        return None

    prompt = text

    for url_idx, url in enumerate(urls):
        is_last_url = url_idx == len(urls) - 1
        for attempt in range(1, config.embedding_retry_max + 1):
            try:
                resp = requests.post(
                    url,
                    json={"model": "nomic-embed-text:v1.5", "prompt": prompt},
                    timeout=config.embedding_timeout,
                )
                if resp.status_code == 500 and "context length" in resp.text:
                    if len(prompt) > EMBEDDING_TRUNCATE_CHARS:
                        log.warning(
                            "Chunk too long (%d chars); truncating to %d and retrying",
                            len(prompt), EMBEDDING_TRUNCATE_CHARS,
                        )
                        prompt = prompt[:EMBEDDING_TRUNCATE_CHARS]
                        continue  # retry without consuming an attempt
                    log.warning(
                        "Embedding context-length error even after truncation (%d chars); giving up on this chunk",
                        len(prompt),
                    )
                    return None  # content failure — don't try further URLs
                resp.raise_for_status()
                emb = resp.json().get("embedding")
                if not isinstance(emb, list) or len(emb) != config.vector_size:
                    # A 200 response with a missing/empty/wrong-size vector is an
                    # endpoint failure (e.g. a degraded Ollama returning []), not
                    # valid data. Raise so the fallback handler below retries this
                    # URL's budget and then advances to the next endpoint, rather
                    # than silently upserting a bad vector into Qdrant.
                    raise ValueError(
                        "endpoint returned invalid embedding (len=%s, expected %d)"
                        % (len(emb) if isinstance(emb, list) else type(emb).__name__,
                           config.vector_size)
                    )
                return emb
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                log.debug(
                    "Embedding transport error (url=%s attempt=%d/%d): %s",
                    url, attempt, config.embedding_retry_max, e,
                )
                if attempt == config.embedding_retry_max:
                    if is_last_url:
                        log.warning(
                            "Embedding transport exhausted on final URL %s after %d attempts",
                            url, config.embedding_retry_max,
                        )
                        return None
                    log.warning(
                        "Embedding transport exhausted on URL %s; falling back to next endpoint",
                        url,
                    )
                    break  # advance to next URL
                backoff = min(config.embedding_retry_backoff * (4 ** (attempt - 1)), 10.0)
                time.sleep(backoff)
            except Exception as e:
                if attempt == config.embedding_retry_max:
                    if is_last_url:
                        log.warning(
                            "Embedding failed on final URL %s after %d retries: %s",
                            url, config.embedding_retry_max, e,
                        )
                        return None
                    log.warning(
                        "Embedding failed on URL %s after %d retries (%s); falling back to next endpoint",
                        url, config.embedding_retry_max, e,
                    )
                    break  # advance to next URL
                backoff = min(config.embedding_retry_backoff * (2 ** (attempt - 1)), 5.0)
                time.sleep(backoff)
    return None


# ============================================================================
# Qdrant
# ============================================================================

class OptimizedQdrant:
    def __init__(self, config: VectorizeConfig):
        self.config = config
        raw_url = config.qdrant_url.rstrip("/")
        if raw_url.startswith("http://") or raw_url.startswith("https://"):
            u = urlparse(raw_url)
            host = u.hostname or "localhost"
            use_https = (u.scheme == "https")
            port = u.port or (443 if use_https else 6333)
        else:
            host = raw_url or "localhost"
            use_https = (config.qdrant_port == 443)
            port = config.qdrant_port or 6333

        client_kwargs = {
            "host": host,
            "port": port,
            "https": use_https,
            "timeout": 60,
            "check_compatibility": False,
        }
        if config.qdrant_api_key:
            client_kwargs["api_key"] = config.qdrant_api_key

        self.client = QdrantClient(**client_kwargs)
        log.info("Qdrant client initialized: %s -> %s:%s https=%s", raw_url, host, port, use_https)

    def set_bulk_mode(self, enable: bool = True):
        threshold = 0 if enable else 1000
        try:
            self.client.update_collection(
                collection_name=self.config.qdrant_collection,
                optimizer_config=models.OptimizersConfigDiff(indexing_threshold=threshold),
            )
            log.info("Qdrant indexing_threshold set to %d", threshold)
        except Exception as e:
            log.warning("Qdrant indexing threshold adjustment skipped (non-critical): %s", e)

    def upsert_batch(self, points: List[models.PointStruct]) -> bool:
        if not points:
            return True
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                self.client.upsert(
                    collection_name=self.config.qdrant_collection,
                    points=points,
                    wait=False,
                )
                time.sleep(2)
                return True
            except Exception as e:
                if attempt == max_attempts:
                    log.error("Qdrant upsert failed after %d attempts: %s", max_attempts, e)
                    return False
                log.warning("Qdrant upsert attempt %d/%d failed: %s", attempt, max_attempts, e)
                time.sleep(1.0 * attempt)
        return False


# ============================================================================
# Per-article worker
# ============================================================================

def vectorize_article(
    article: dict,
    config: VectorizeConfig,
    minio: MinIOReader,
    qdrant: OptimizedQdrant,
    extra_payload: Optional[dict] = None,
) -> Tuple[int, bool, str]:
    """Process one article: download PDF, chunk, embed, upsert. Returns (pmid, success, info).

    The caller is responsible for db.mark_vectorized() — keeping psycopg2 out
    of worker threads.
    """
    pmid = article["pmid"]
    pdf_location = article["pdf_location"]

    pdf_data = minio.download_pdf(pdf_location)
    if not pdf_data:
        return pmid, False, "pdf_download_fail"

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(pdf_data)
            tmp_path = tmp.name

        try:
            doc = pymupdf.open(tmp_path)
        except pymupdf.EmptyFileError:
            return pmid, False, "empty_pdf"

        page_chunks: List[Tuple[int, str]] = []
        try:
            for page_num, page in enumerate(doc, start=1):
                raw = page.get_text().encode("utf8", errors="ignore").decode("utf8", errors="ignore")
                for chunk in chunk_text(raw, config.chunk_size, config.chunk_overlap):
                    page_chunks.append((page_num, chunk))
        finally:
            doc.close()

        if not page_chunks:
            return pmid, False, "empty_pdf"

        points: List[models.PointStruct] = []
        readable = Path(pdf_location).name
        total = len(page_chunks)
        for i, (page_num, chunk) in enumerate(page_chunks):
            emb = get_embedding(config, chunk)
            if not emb:
                return pmid, False, f"embedding_fail_chunk_{i}"
            payload = {
                "page_content": chunk,
                "s3_path": pdf_location,
                "readable_filename": readable,
                "pagenumber": page_num,
                "chunk_index": i,
                "total_chunks": total,
                "pmid": pmid,
            }
            if extra_payload:
                payload.update(extra_payload)
            points.append(models.PointStruct(id=str(uuid.uuid4()), vector=emb, payload=payload))

        if not qdrant.upsert_batch(points):
            return pmid, False, f"qdrant_upsert_fail: {len(points)} points"

        return pmid, True, f"vectorized: {total} chunks"
    except Exception as e:
        log.exception("Unexpected error vectorizing PMID %s", pmid)
        return pmid, False, f"unexpected_error: {e}"
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


# ============================================================================
# Checkpoint helpers (constant-size; one row of state per checkpoint file)
# ============================================================================

def load_checkpoint(path: str, default: Optional[dict] = None) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception as e:
            log.warning("Failed to load checkpoint %s: %s", path, e)
    return default if default is not None else {
        "last_pmid": 0, "processed_count": 0, "success_count": 0, "fail_count": 0,
    }


def save_checkpoint(path: str, data: dict) -> None:
    try:
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        log.warning("Failed to save checkpoint %s: %s", path, e)
