#!/usr/bin/env python3
"""
Re-vectorize PubMed PDFs with chunk_size=1000 and nomic-embed-text:v1.5.

Data sources:
  --source minio    : Scan MinIO bucket for all .pdf objects
  --source supabase : Query vyraid.articles for pdf_location paths
  --source both     : Union of MinIO scan + Supabase articles

Pipeline per object:
  MinIO download → PyMuPDF text extraction → RecursiveCharacterTextSplitter
  → Ollama nomic-embed-text:v1.5 embedding → Qdrant Cloud upsert

Supports:
  - Multi-GPU Ollama round-robin via --embedding-urls
  - Checkpoint/resume via local JSON file
  - Qdrant collection drop/recreate via --drop-collection --confirm-drop
  - Supabase vector_indexed tracking via --mark-supabase
  - Bulk reset via --reset-supabase

Usage:
    # Discovery / test
    python revectorize.py --dry-run --source minio --limit 10
    python revectorize.py --source minio --limit 50 --workers 4

    # Full run on H100 cluster
    python revectorize.py --source both --workers 8 --aggressive-mode --skip-dedup

    # Resume after interruption
    python revectorize.py --source both --workers 8 --aggressive-mode --resume

    # Drop collection and start fresh
    python revectorize.py --drop-collection --confirm-drop

    # Reset Supabase tracking
    python revectorize.py --reset-supabase
"""

import os
import sys
import logging
import time
import argparse
import json
import re
import uuid
import tempfile
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse
import itertools
import threading

import requests
import pymupdf
from minio import Minio
from qdrant_client import QdrantClient, models
from langchain_text_splitters.character import RecursiveCharacterTextSplitter

from dotenv import load_dotenv

# Load .env from current dir, then fall back to .env.production
if os.path.exists('.env'):
    load_dotenv('.env')
elif os.path.exists('.env.production'):
    load_dotenv('.env.production')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

CHECKPOINT_FILE = "revectorize_checkpoint.json"
EMBEDDING_MODEL = "nomic-embed-text:v1.5"
VECTOR_SIZE = 768

# ============================================================================
# Configuration
# ============================================================================

@dataclass
class Config:
    # MinIO
    minio_endpoint: str = ""
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_bucket: str = "pubmed"
    minio_secure: bool = False
    minio_prefix: str = ""

    # Embedding
    embedding_base_url: str = ""
    embedding_urls: List[str] = field(default_factory=list)
    embedding_retry_max: int = 30
    embedding_timeout: int = 300
    embedding_retry_backoff: float = 0.25

    # Qdrant
    qdrant_url: str = ""
    qdrant_port: int = 6333
    qdrant_api_key: Optional[str] = None
    qdrant_collection: str = "pubmed_v2"
    vector_size: int = VECTOR_SIZE

    # Vectorization
    chunk_size: int = 1000
    chunk_overlap: int = 200
    qdrant_upsert_batch: int = 1000

    # Supabase / Postgres
    postgres_dsn: str = ""
    db_schema: str = "vyraid"

    # Execution
    source: str = "minio"  # minio | supabase | both
    workers: int = 4
    limit: int = 0
    dry_run: bool = False
    resume: bool = False
    skip_dedup: bool = False
    mark_supabase: bool = False
    use_manifest: Optional[str] = None


def load_config(args) -> Config:
    aggressive = getattr(args, 'aggressive_mode', False)

    if aggressive:
        retry_max = 5
        timeout = 60
        backoff = 0.1
        upsert_batch = 5000
    else:
        retry_max = 30
        timeout = 300
        backoff = 0.25
        upsert_batch = 1000

    qdrant_url = os.getenv('QDRANT_URL', 'http://localhost')
    qdrant_port = int(os.getenv('QDRANT_PORT', '6333'))

    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    minio_secure = os.getenv('MINIO_SECURE', 'false').lower() in ('1', 'true', 'yes')

    # Normalize MinIO endpoint (strip scheme, extract host:port)
    ep = minio_endpoint
    if ep.startswith("http://") or ep.startswith("https://"):
        parsed = urlparse(ep)
        host = parsed.netloc or parsed.path
        if "/" in host:
            host = host.split("/")[0]
        if "MINIO_SECURE" not in os.environ:
            minio_secure = parsed.scheme == "https"
        ep = host
    ep = ep.rstrip("/")

    embedding_urls = getattr(args, 'embedding_urls', None) or []

    return Config(
        minio_endpoint=ep,
        minio_access_key=os.getenv('MINIO_ACCESS_KEY', ''),
        minio_secret_key=os.getenv('MINIO_SECRET_KEY', ''),
        minio_bucket=os.getenv('MINIO_BUCKET', 'pubmed'),
        minio_secure=minio_secure,
        minio_prefix=getattr(args, 'prefix', '') or '',
        embedding_base_url=os.getenv('EMBEDDING_BASE_URL', 'http://localhost:11434/api/embeddings'),
        embedding_urls=embedding_urls,
        embedding_retry_max=retry_max,
        embedding_timeout=timeout,
        embedding_retry_backoff=backoff,
        qdrant_url=qdrant_url,
        qdrant_port=qdrant_port,
        qdrant_api_key=os.getenv('QDRANT_API_KEY') or None,
        qdrant_collection=getattr(args, 'collection', None) or os.getenv('QDRANT_COLLECTION', 'pubmed_v2'),
        vector_size=VECTOR_SIZE,
        chunk_size=int(os.getenv('CHUNK_SIZE', '1000')),
        chunk_overlap=int(os.getenv('CHUNK_OVERLAP', '200')),
        qdrant_upsert_batch=upsert_batch,
        postgres_dsn=os.getenv('POSTGRES_DSN', ''),
        db_schema=os.getenv('DB_SCHEMA', 'vyraid'),
        source=getattr(args, 'source', 'minio'),
        workers=args.workers,
        limit=args.limit,
        dry_run=args.dry_run,
        resume=args.resume,
        skip_dedup=getattr(args, 'skip_dedup', False),
        mark_supabase=getattr(args, 'mark_supabase', False),
        use_manifest=getattr(args, 'manifest', None),
    )


# ============================================================================
# Checkpoint (local JSON)
# ============================================================================

def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
            data["completed_keys"] = set(data.get("completed_keys") or data.get("completed", []))
            data.pop("completed", None)
            return data
        except Exception as e:
            log.warning(f"Corrupt checkpoint, starting fresh: {e}")
    return {
        "completed_keys": set(),
        "failed": {},
        "stats": {"success": 0, "fail": 0, "chunks_total": 0},
        "last_key": None,
        "timestamp": None,
    }


def save_checkpoint(ckpt: dict):
    ckpt["timestamp"] = datetime.now().isoformat()
    tmp = CHECKPOINT_FILE + ".tmp"
    serializable = dict(ckpt)
    if isinstance(serializable.get("completed_keys"), set):
        serializable["completed_keys"] = sorted(serializable["completed_keys"])
    with open(tmp, 'w') as f:
        json.dump(serializable, f, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)


# ============================================================================
# Supabase / Postgres helpers
# ============================================================================

def _get_pg_conn(dsn: str):
    """Get a psycopg2 connection."""
    import psycopg2
    return psycopg2.connect(dsn)


def query_supabase_articles(cfg: Config) -> List[str]:
    """Query vyraid.articles for MinIO keys of articles with pdf_location."""
    if not cfg.postgres_dsn:
        log.error("POSTGRES_DSN not set — cannot query Supabase articles")
        return []

    import psycopg2
    conn = psycopg2.connect(cfg.postgres_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT pdf_location
                FROM {cfg.db_schema}.articles
                WHERE pdf_location IS NOT NULL
                  AND pdf_location != ''
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    # pdf_location may be a full MinIO URL or a bucket-relative key
    keys = []
    for (loc,) in rows:
        # Strip bucket prefix / URL prefix to get just the object key
        if loc.startswith('http'):
            parsed = urlparse(loc)
            # Path like /pubmed/46/1d/file.pdf → 46/1d/file.pdf
            path = parsed.path.lstrip('/')
            if path.startswith(cfg.minio_bucket + '/'):
                path = path[len(cfg.minio_bucket) + 1:]
            keys.append(path)
        else:
            keys.append(loc)

    log.info(f"Supabase: {len(keys):,} articles with pdf_location")
    return keys


def mark_supabase_vectorized(cfg: Config, minio_keys: List[str]):
    """Mark articles as vector_indexed=true in Supabase for the given MinIO keys."""
    if not cfg.postgres_dsn or not minio_keys:
        return

    import psycopg2
    conn = psycopg2.connect(cfg.postgres_dsn)
    try:
        with conn.cursor() as cur:
            # Build a set of patterns to match pdf_location
            # We match on the key portion
            for batch_start in range(0, len(minio_keys), 500):
                batch = minio_keys[batch_start:batch_start + 500]
                # Use ANY array matching
                cur.execute(f"""
                    UPDATE {cfg.db_schema}.articles
                    SET vector_indexed = true
                    WHERE pdf_location = ANY(%s)
                       OR pdf_location LIKE ANY(
                           SELECT '%%' || unnest(%s::text[])
                       )
                """, (batch, batch))
            conn.commit()
            log.info(f"Marked {len(minio_keys)} articles as vector_indexed=true in Supabase")
    except Exception as e:
        conn.rollback()
        log.error(f"Failed to update Supabase vector_indexed: {e}")
    finally:
        conn.close()


def reset_supabase_vector_indexed(cfg: Config):
    """Reset vector_indexed=false for all articles."""
    if not cfg.postgres_dsn:
        log.error("POSTGRES_DSN not set — cannot reset Supabase")
        sys.exit(1)

    import psycopg2
    conn = psycopg2.connect(cfg.postgres_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {cfg.db_schema}.articles
                SET vector_indexed = false
                WHERE vector_indexed = true
            """)
            count = cur.rowcount
            conn.commit()
            log.info(f"Reset vector_indexed=false for {count:,} articles")
    finally:
        conn.close()


# ============================================================================
# Qdrant collection management
# ============================================================================

def drop_and_recreate_collection(cfg: Config):
    """Drop and recreate the Qdrant collection."""
    kw = {"url": cfg.qdrant_url, "port": cfg.qdrant_port, "timeout": 120, "check_compatibility": False}
    if cfg.qdrant_api_key:
        kw["api_key"] = cfg.qdrant_api_key
    client = QdrantClient(**kw)

    collections = [c.name for c in client.get_collections().collections]
    if cfg.qdrant_collection in collections:
        log.info(f"Deleting collection '{cfg.qdrant_collection}' ...")
        client.delete_collection(cfg.qdrant_collection)
        log.info(f"Deleted.")

    log.info(f"Creating collection '{cfg.qdrant_collection}' (dim={cfg.vector_size}, cosine) ...")
    client.create_collection(
        collection_name=cfg.qdrant_collection,
        vectors_config=models.VectorParams(
            size=cfg.vector_size,
            distance=models.Distance.COSINE,
            on_disk=True,
        ),
        on_disk_payload=True,
        optimizers_config=models.OptimizersConfigDiff(
            indexing_threshold=0,  # start in bulk mode
        ),
    )
    log.info(f"Collection '{cfg.qdrant_collection}' created.")
    client.close()


# ============================================================================
# MinIO helpers
# ============================================================================

def get_minio_client(cfg: Config) -> Minio:
    return Minio(
        cfg.minio_endpoint,
        access_key=cfg.minio_access_key,
        secret_key=cfg.minio_secret_key,
        secure=cfg.minio_secure,
    )


PMCID_RE = re.compile(r'(PMC\d+)')

def extract_pmcid(object_name: str) -> Optional[str]:
    m = PMCID_RE.search(object_name)
    return m.group(1) if m else None


def list_pdf_objects(cfg: Config, minio_client: Minio) -> List[str]:
    """List .pdf objects from MinIO, or load from manifest CSV."""
    if cfg.use_manifest:
        log.info(f"Loading object list from manifest: {cfg.use_manifest}")
        import csv
        objects = []
        with open(cfg.use_manifest, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get('object_name', '')
                if name.endswith('.pdf'):
                    objects.append(name)
        log.info(f"Manifest: {len(objects):,} PDF objects")
        return sorted(objects)

    log.info(f"Scanning MinIO bucket '{cfg.minio_bucket}' prefix='{cfg.minio_prefix}' ...")
    objects = []
    count = 0
    for obj in minio_client.list_objects(cfg.minio_bucket, prefix=cfg.minio_prefix, recursive=True):
        count += 1
        if count % 100_000 == 0:
            log.info(f"  ... scanned {count:,} objects, {len(objects):,} PDFs so far")
        if obj.object_name.endswith('.pdf'):
            objects.append(obj.object_name)
    log.info(f"Found {len(objects):,} PDF objects in MinIO (scanned {count:,} total)")
    return sorted(objects)


def download_pdf(minio_client: Minio, bucket: str, key: str) -> Optional[bytes]:
    try:
        resp = minio_client.get_object(bucket, key)
        raw = resp.read()
        resp.close()
        resp.release_conn()
        return raw
    except Exception as e:
        log.warning(f"MinIO download failed for {key}: {e}")
        return None


# ============================================================================
# Embedding
# ============================================================================

_embed_counter = itertools.count()


def check_embedding_health(url: str, timeout: int = 30) -> bool:
    try:
        resp = requests.post(
            url,
            json={"model": EMBEDDING_MODEL, "prompt": "test"},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        emb = data.get("embedding", [])
        if len(emb) != VECTOR_SIZE:
            log.warning(f"Health check: expected {VECTOR_SIZE}-dim, got {len(emb)}-dim from {url}")
            return False
        return True
    except Exception as e:
        log.warning(f"Embedding health check failed for {url}: {e}")
        return False


def get_embedding(cfg: Config, text: str) -> Optional[List[float]]:
    urls = cfg.embedding_urls or [cfg.embedding_base_url]
    url = urls[next(_embed_counter) % len(urls)]

    for attempt in range(1, cfg.embedding_retry_max + 1):
        try:
            resp = requests.post(
                url,
                json={"model": EMBEDDING_MODEL, "prompt": text},
                timeout=cfg.embedding_timeout,
            )
            resp.raise_for_status()
            return resp.json()["embedding"]
        except requests.exceptions.Timeout:
            if attempt == cfg.embedding_retry_max:
                return None
            time.sleep(min(cfg.embedding_retry_backoff * (4 ** (attempt - 1)), 10.0))
        except requests.exceptions.ConnectionError:
            if attempt == cfg.embedding_retry_max:
                return None
            time.sleep(min(cfg.embedding_retry_backoff * (4 ** (attempt - 1)), 10.0))
        except Exception as e:
            if attempt == cfg.embedding_retry_max:
                log.warning(f"Embedding failed after {cfg.embedding_retry_max} retries: {e}")
                return None
            time.sleep(min(cfg.embedding_retry_backoff * (2 ** (attempt - 1)), 5.0))
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
# Qdrant writer
# ============================================================================

class QdrantWriter:
    def __init__(self, cfg: Config):
        kw = {"url": cfg.qdrant_url, "port": cfg.qdrant_port, "timeout": 120, "check_compatibility": False}
        if cfg.qdrant_api_key:
            kw["api_key"] = cfg.qdrant_api_key
        self.client = QdrantClient(**kw)
        self.collection = cfg.qdrant_collection
        log.info(f"Qdrant: {cfg.qdrant_url}:{cfg.qdrant_port} collection={self.collection}")

    def ensure_collection(self, cfg: Config):
        collections = self.client.get_collections().collections
        names = [c.name for c in collections]
        if self.collection not in names:
            raise RuntimeError(f"Collection '{self.collection}' not found. Available: {names}")
        log.info(f"Collection '{self.collection}' confirmed (in {len(names)} collections)")

    def set_bulk_mode(self, enable: bool):
        threshold = 0 if enable else 10000
        try:
            self.client.update_collection(
                collection_name=self.collection,
                optimizer_config=models.OptimizersConfigDiff(indexing_threshold=threshold),
            )
            log.info(f"Qdrant indexing_threshold -> {threshold}")
        except Exception as e:
            log.warning(f"Indexing threshold change skipped: {e}")

    def upsert_batch(self, points: List[models.PointStruct]) -> bool:
        if not points:
            return True
        for attempt in range(1, 4):
            try:
                self.client.upsert(
                    collection_name=self.collection,
                    points=points,
                    wait=False,
                )
                time.sleep(2)
                return True
            except Exception as e:
                if attempt == 3:
                    log.error(f"Qdrant upsert failed after 3 attempts: {e}")
                    return False
                log.warning(f"Qdrant upsert attempt {attempt}/3 failed: {e}")
                time.sleep(5.0 * attempt)
        return False

    def get_vectorized_paths(self) -> Set[str]:
        log.info(f"Scrolling '{self.collection}' to build dedup set ...")
        paths: Set[str] = set()
        offset = None
        batch_size = 10_000
        scroll_count = 0
        while True:
            try:
                results, next_offset = self.client.scroll(
                    collection_name=self.collection,
                    scroll_filter=None,
                    limit=batch_size,
                    offset=offset,
                    with_payload=["s3_path"],
                    with_vectors=False,
                )
            except Exception as e:
                log.error(f"Qdrant scroll failed at offset {offset}: {e}")
                break

            if not results:
                break

            for point in results:
                s3_path = point.payload.get("s3_path", "")
                if s3_path:
                    paths.add(s3_path)

            scroll_count += len(results)
            if scroll_count % 500_000 == 0:
                log.info(f"  ... scrolled {scroll_count:,} points, {len(paths):,} unique paths")

            offset = next_offset
            if offset is None:
                break

        log.info(f"Dedup scan: {scroll_count:,} points -> {len(paths):,} unique s3_paths")
        return paths


# ============================================================================
# Vectorize one object
# ============================================================================

def vectorize_object(
    key: str,
    cfg: Config,
    minio_client: Minio,
    qdrant: QdrantWriter,
) -> Tuple[str, bool, str, int]:
    """
    Process one MinIO object end-to-end.
    Returns: (key, success, info_message, chunk_count)
    """
    pmcid = extract_pmcid(key) or Path(key).stem.replace('.pdf', '')

    if cfg.dry_run:
        return key, True, f"dry-run: {pmcid}", 0

    # Download PDF from MinIO
    pdf_bytes = download_pdf(minio_client, cfg.minio_bucket, key)
    if not pdf_bytes:
        return key, False, "download_fail", 0

    # Extract text with PyMuPDF
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name

        doc = pymupdf.open(tmp_path)
        page_chunks: List[Tuple[int, str]] = []
        for page_num, page in enumerate(doc, start=1):
            raw = page.get_text().encode("utf8", errors="ignore").decode("utf8", errors="ignore")
            for chunk in chunk_text(raw, cfg.chunk_size, cfg.chunk_overlap):
                page_chunks.append((page_num, chunk))
        doc.close()
        os.unlink(tmp_path)
        tmp_path = None

        if not page_chunks:
            return key, False, "empty_pdf", 0

    except pymupdf.EmptyFileError:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        return key, False, "empty_file", 0
    except Exception as e:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        return key, False, f"pdf_extract_fail: {e}", 0

    # Embed chunks and build Qdrant points
    points: List[models.PointStruct] = []
    for i, (page_num, chunk) in enumerate(page_chunks):
        emb = get_embedding(cfg, chunk)
        if emb is None:
            return key, False, f"embedding_fail_chunk_{i}/{len(page_chunks)}", 0

        points.append(
            models.PointStruct(
                id=str(uuid.uuid4()),
                vector=emb,
                payload={
                    "page_content": chunk,
                    "s3_path": key,
                    "readable_filename": Path(key).name,
                    "pagenumber": page_num,
                    "chunk_index": i,
                    "total_chunks": len(page_chunks),
                    "pmcid": pmcid,
                },
            )
        )

    # Batch upsert to Qdrant
    batch_size = cfg.qdrant_upsert_batch
    for start in range(0, len(points), batch_size):
        batch = points[start:start + batch_size]
        if not qdrant.upsert_batch(batch):
            return key, False, f"qdrant_upsert_fail at chunk {start}", len(points)

    return key, True, f"{len(page_chunks)} chunks", len(page_chunks)


# ============================================================================
# Work list assembly
# ============================================================================

def gather_work_list(cfg: Config, minio_client: Minio) -> List[str]:
    """Build the list of MinIO object keys to process based on --source."""
    minio_keys: Set[str] = set()
    supa_keys: Set[str] = set()

    if cfg.source in ("minio", "both"):
        for key in list_pdf_objects(cfg, minio_client):
            minio_keys.add(key)

    if cfg.source in ("supabase", "both"):
        for key in query_supabase_articles(cfg):
            supa_keys.add(key)

    all_keys = minio_keys | supa_keys
    log.info(f"Work list: minio={len(minio_keys):,}  supabase={len(supa_keys):,}  "
             f"union={len(all_keys):,}")
    return sorted(all_keys)


# ============================================================================
# Progress tracker
# ============================================================================

class ProgressTracker:
    def __init__(self, total: int):
        self.total = total
        self.done = 0
        self.chunks = 0
        self.t0 = time.time()
        self._lock = threading.Lock()

    def update(self, n_chunks: int):
        with self._lock:
            self.done += 1
            self.chunks += n_chunks

    def summary(self) -> str:
        with self._lock:
            elapsed = time.time() - self.t0
            if elapsed < 1:
                return f"{self.done}/{self.total}"
            rate = self.done / elapsed
            chunks_rate = self.chunks / elapsed
            remaining = (self.total - self.done) / rate if rate > 0 else 0
            eta_h = remaining / 3600
            return (f"{self.done:,}/{self.total:,} articles  "
                    f"{self.chunks:,} chunks  "
                    f"{rate:.1f} art/s  {chunks_rate:.0f} ch/s  "
                    f"ETA {eta_h:.1f}h")


# ============================================================================
# Main orchestrator
# ============================================================================

def run(cfg: Config):
    log.info("=" * 80)
    log.info("PUBMED RE-VECTORIZATION")
    log.info(f"  model={EMBEDDING_MODEL}  chunk_size={cfg.chunk_size}  "
             f"chunk_overlap={cfg.chunk_overlap}  vector_dim={cfg.vector_size}")
    log.info(f"  source={cfg.source}  workers={cfg.workers}  limit={cfg.limit}  "
             f"dry_run={cfg.dry_run}  collection={cfg.qdrant_collection}")
    log.info("=" * 80)

    minio_client = get_minio_client(cfg)
    qdrant = QdrantWriter(cfg)
    qdrant.ensure_collection(cfg)

    # Embedding health check
    if not cfg.dry_run:
        urls_to_check = cfg.embedding_urls or [cfg.embedding_base_url]
        log.info(f"Checking {len(urls_to_check)} embedding endpoint(s) ...")
        for url in urls_to_check:
            if not check_embedding_health(url):
                log.error(f"Embedding endpoint not reachable: {url}. Aborting.")
                sys.exit(1)
            log.info(f"  {url} — healthy ({VECTOR_SIZE}-dim)")
        log.info(f"All {len(urls_to_check)} embedding endpoints healthy")

    # Set bulk indexing mode for faster upserts
    if not cfg.dry_run:
        qdrant.set_bulk_mode(True)

    # Gather work list
    all_objects = gather_work_list(cfg, minio_client)

    # Checkpoint resume
    ckpt = load_checkpoint() if cfg.resume else {
        "completed_keys": set(), "failed": {}, "stats": {"success": 0, "fail": 0, "chunks_total": 0},
        "last_key": None, "timestamp": None,
    }
    session_done: Set[str] = ckpt.get("completed_keys", set())
    if not isinstance(session_done, set):
        session_done = set(session_done)
    pending = [k for k in all_objects if k not in session_done]

    # Qdrant dedup
    if not cfg.skip_dedup and not cfg.dry_run:
        vectorized_paths = qdrant.get_vectorized_paths()
        before = len(pending)
        pending = [k for k in pending if k not in vectorized_paths]
        log.info(f"Qdrant dedup: {before:,} -> {len(pending):,} ({before - len(pending):,} already vectorized)")
    else:
        log.info("Skipping Qdrant dedup scan")

    if cfg.limit > 0:
        pending = pending[:cfg.limit]

    log.info(f"Total objects: {len(all_objects):,}  Already done: {len(session_done):,}  Pending: {len(pending):,}")

    if not pending:
        log.info("Nothing to do.")
        if not cfg.dry_run:
            qdrant.set_bulk_mode(False)
        return

    stats = ckpt.get("stats", {"success": 0, "fail": 0, "chunks_total": 0})
    completed_set = set(session_done)
    failed_dict = dict(ckpt.get("failed", {}))
    failure_reasons: dict = {}
    progress = ProgressTracker(len(pending))
    save_every = 25
    supabase_batch: List[str] = []

    try:
        with ThreadPoolExecutor(max_workers=max(1, cfg.workers)) as pool:
            futures = {
                pool.submit(vectorize_object, key, cfg, minio_client, qdrant): key
                for key in pending
            }

            done_count = 0
            for future in as_completed(futures):
                key = futures[future]
                try:
                    _key, success, info, n_chunks = future.result()
                except Exception as e:
                    success, info, n_chunks = False, f"future_error: {e}", 0

                done_count += 1

                if success:
                    stats["success"] += 1
                    stats["chunks_total"] += n_chunks
                    completed_set.add(key)
                    progress.update(n_chunks)
                    supabase_batch.append(key)
                    if done_count % 100 == 0:
                        log.info(f"[{progress.summary()}] OK {key}: {info}")
                    elif done_count <= 10 or done_count % 10 == 0:
                        log.info(f"[{done_count}/{len(pending)}] OK {key}: {info}")
                else:
                    stats["fail"] += 1
                    failed_dict[key] = info
                    reason_bucket = info.split(":")[0] if ":" in info else info
                    failure_reasons[reason_bucket] = failure_reasons.get(reason_bucket, 0) + 1
                    log.warning(f"[{done_count}/{len(pending)}] FAIL {key}: {info}")

                # Periodic checkpoint + Supabase marking
                if done_count % save_every == 0:
                    ckpt.update({
                        "completed_keys": completed_set,
                        "failed": failed_dict,
                        "stats": stats,
                        "last_key": key,
                    })
                    save_checkpoint(ckpt)

                    if cfg.mark_supabase and supabase_batch:
                        mark_supabase_vectorized(cfg, supabase_batch)
                        supabase_batch = []

    finally:
        # Final checkpoint
        ckpt.update({
            "completed_keys": completed_set,
            "failed": failed_dict,
            "stats": stats,
            "last_key": key if pending else None,
        })
        save_checkpoint(ckpt)

        # Final Supabase marking
        if cfg.mark_supabase and supabase_batch:
            mark_supabase_vectorized(cfg, supabase_batch)

        # Disable bulk mode (re-enable indexing)
        if not cfg.dry_run:
            log.info("Re-enabling Qdrant indexing ...")
            qdrant.set_bulk_mode(False)

    elapsed = time.time() - progress.t0
    log.info("=" * 80)
    log.info("COMPLETE")
    log.info("=" * 80)
    log.info(f"Processed: {done_count:,} / {len(pending):,}")
    log.info(f"Success: {stats['success']:,}   Fail: {stats['fail']:,}   "
             f"Chunks: {stats['chunks_total']:,}")
    log.info(f"Elapsed: {elapsed/3600:.1f}h ({elapsed:.0f}s)")
    if stats['success'] > 0:
        log.info(f"Throughput: {stats['success']/elapsed:.1f} articles/s  "
                 f"{stats['chunks_total']/elapsed:.0f} chunks/s")
    if failure_reasons:
        log.info("Failure breakdown:")
        for reason, cnt in sorted(failure_reasons.items(), key=lambda x: -x[1]):
            log.info(f"  {reason}: {cnt}")


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Re-vectorize PubMed PDFs (chunk_size=1000, nomic-embed-text:v1.5)"
    )

    # Data source
    ap.add_argument("--source", choices=["minio", "supabase", "both"], default="minio",
                    help="Data source (default: minio)")

    # Execution
    ap.add_argument("--workers", type=int, default=4,
                    help="Parallel workers (default: 4)")
    ap.add_argument("--limit", type=int, default=0,
                    help="Max objects to process (0 = unlimited)")
    ap.add_argument("--aggressive-mode", action="store_true",
                    help="Fast retry, 5000-chunk upsert batches")
    ap.add_argument("--resume", action="store_true",
                    help="Resume from checkpoint")
    ap.add_argument("--dry-run", action="store_true",
                    help="List objects only, don't embed or upsert")
    ap.add_argument("--skip-dedup", action="store_true",
                    help="Skip Qdrant scroll for dedup (faster startup)")

    # Embedding
    ap.add_argument("--embedding-urls", type=str, nargs='+', default=None,
                    help="Ollama embedding URLs for round-robin "
                         "(e.g. http://host:11434/api/embeddings http://host:11435/api/embeddings)")

    # Qdrant
    ap.add_argument("--collection", type=str, default=None,
                    help="Qdrant collection name (overrides QDRANT_COLLECTION env)")
    ap.add_argument("--drop-collection", action="store_true",
                    help="Drop and recreate collection before running")
    ap.add_argument("--confirm-drop", action="store_true",
                    help="Required safety flag for --drop-collection")

    # Supabase
    ap.add_argument("--mark-supabase", action="store_true",
                    help="Update vector_indexed=true in Supabase after each batch")
    ap.add_argument("--reset-supabase", action="store_true",
                    help="Reset vector_indexed=false for all articles, then exit")

    # MinIO
    ap.add_argument("--prefix", type=str, default="",
                    help="MinIO prefix filter (e.g. '10/' for one subfolder)")
    ap.add_argument("--manifest", type=str, default=None,
                    help="CSV manifest file instead of live MinIO scan")

    args = ap.parse_args()

    # Handle standalone commands
    if args.reset_supabase:
        cfg = load_config(args)
        reset_supabase_vector_indexed(cfg)
        sys.exit(0)

    if args.drop_collection:
        if not args.confirm_drop:
            log.error("--drop-collection requires --confirm-drop safety flag")
            sys.exit(1)
        cfg = load_config(args)
        drop_and_recreate_collection(cfg)
        log.info("Collection recreated. Run again without --drop-collection to start vectorization.")
        sys.exit(0)

    cfg = load_config(args)

    try:
        run(cfg)
    except KeyboardInterrupt:
        log.info("Interrupted. Checkpoint saved — use --resume to continue.")
        sys.exit(130)
    except Exception as e:
        log.exception(f"Fatal: {e}")
        sys.exit(1)
