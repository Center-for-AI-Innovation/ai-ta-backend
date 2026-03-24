#!/usr/bin/env python3
"""
Vectorize PDFs directly from MinIO — no Postgres dependency.

Scans MinIO bucket for .pdf objects, extracts text with PyMuPDF,
embeds with Ollama nomic-embed-text:v1.5, and upserts to Qdrant.

Deduplicates by scrolling the existing Qdrant collection to build a
set of already-vectorized s3_paths before processing.

Tracks progress via a local JSON checkpoint file so runs can be
resumed after interruption.

MinIO object format (from FTP OA mirror):
  {hex2}/{hex2}/{article-name}.PMC{id}.pdf
  e.g. 46/1d/pnas.202319160.PMC10998587.pdf

Usage:
    python vectorize_minio_direct.py --workers 4 --limit 100        # test
    python vectorize_minio_direct.py --workers 4 --aggressive-mode  # full run
    python vectorize_minio_direct.py --resume                       # resume
    python vectorize_minio_direct.py --dry-run --limit 10           # preview
    python vectorize_minio_direct.py --skip-dedup --limit 10        # skip Qdrant scan
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
from dataclasses import dataclass
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

load_dotenv('.env.production')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

CHECKPOINT_FILE = "minio_vectorize_checkpoint.json"

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
    embedding_urls: List[str] = None  # multiple endpoints for round-robin
    embedding_retry_max: int = 30
    embedding_timeout: int = 300
    embedding_retry_backoff: float = 0.25

    # Qdrant
    qdrant_url: str = ""
    qdrant_port: int = 6333
    qdrant_api_key: Optional[str] = None
    qdrant_collection: str = "pubmed"
    vector_size: int = 768

    # Vectorization
    chunk_size: int = 7000
    chunk_overlap: int = 200
    qdrant_upsert_batch: int = 1000

    # Execution
    workers: int = 4
    limit: int = 0
    dry_run: bool = False
    resume: bool = False
    skip_dedup: bool = False
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

    use_local = getattr(args, 'use_local', False)
    local_qdrant = getattr(args, 'local_qdrant', False) or use_local

    if local_qdrant:
        qdrant_url = "http://localhost"
        qdrant_port = 6333
    else:
        qdrant_url = os.getenv('QDRANT_URL', 'http://localhost')
        qdrant_port = int(os.getenv('QDRANT_PORT', '6333'))

    if use_local:
        minio_endpoint = "localhost:9000"
        minio_secure = False
    else:
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

    return Config(
        minio_endpoint=ep,
        minio_access_key=os.getenv('MINIO_ACCESS_KEY', ''),
        minio_secret_key=os.getenv('MINIO_SECRET_KEY', ''),
        minio_bucket=os.getenv('MINIO_BUCKET', 'pubmed'),
        minio_secure=minio_secure,
        minio_prefix=getattr(args, 'prefix', '') or '',
        embedding_base_url=('http://localhost:11434/api/embeddings'
                           if getattr(args, 'local_ollama', False)
                           else os.getenv('EMBEDDING_BASE_URL', '')),
        embedding_urls=getattr(args, 'embedding_urls', None),
        embedding_retry_max=retry_max,
        embedding_timeout=timeout,
        embedding_retry_backoff=backoff,
        qdrant_url=qdrant_url,
        qdrant_port=qdrant_port,
        qdrant_api_key=os.getenv('QDRANT_API_KEY') or None,
        qdrant_collection=getattr(args, 'collection', None) or os.getenv('QDRANT_COLLECTION', 'pubmed'),
        vector_size=int(os.getenv('VECTOR_SIZE', '768')),
        chunk_size=int(os.getenv('CHUNK_SIZE', '7000')),
        chunk_overlap=int(os.getenv('CHUNK_OVERLAP', '200')),
        qdrant_upsert_batch=upsert_batch,
        workers=args.workers,
        limit=args.limit,
        dry_run=args.dry_run,
        resume=args.resume,
        skip_dedup=getattr(args, 'skip_dedup', False),
        use_manifest=getattr(args, 'manifest', None),
    )


# ============================================================================
# Checkpoint (local JSON — no Postgres needed)
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
    """Download PDF object from MinIO."""
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

def check_embedding_health(url: str, timeout: int = 30) -> bool:
    try:
        resp = requests.post(url, json={"model": "nomic-embed-text:v1.5", "prompt": "test"}, timeout=timeout)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.warning(f"Embedding health check failed: {e}")
        return False


def get_embedding(cfg: Config, text: str) -> Optional[List[float]]:
    urls = cfg.embedding_urls or [cfg.embedding_base_url]
    # Thread-safe round-robin across endpoints
    if not hasattr(get_embedding, '_counter'):
        get_embedding._counter = itertools.count()
    url = urls[next(get_embedding._counter) % len(urls)]
    for attempt in range(1, cfg.embedding_retry_max + 1):
        try:
            resp = requests.post(
                url,
                json={"model": "nomic-embed-text:v1.5", "prompt": text},
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
# Qdrant
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
        """Verify collection exists using lightweight list endpoint."""
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
        """Scroll entire collection to build set of already-vectorized s3_paths."""
        log.info(f"Scrolling '{self.collection}' to build dedup set (this may take a while) ...")
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
                log.info(f"  ... scrolled {scroll_count:,} points, {len(paths):,} unique paths so far")

            offset = next_offset
            if offset is None:
                break

        log.info(f"Dedup scan complete: {scroll_count:,} points -> {len(paths):,} unique s3_paths")
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
# Main orchestrator
# ============================================================================

def run(cfg: Config):
    log.info("=" * 80)
    log.info("MINIO-DIRECT VECTORIZATION")
    log.info("=" * 80)
    log.info(f"workers={cfg.workers}  limit={cfg.limit}  dry_run={cfg.dry_run}  "
             f"aggressive={cfg.embedding_retry_max == 5}  prefix='{cfg.minio_prefix}'")

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
            log.info(f"  {url} — healthy")
        log.info(f"All {len(urls_to_check)} embedding endpoints healthy")

    # Gather work list
    all_objects = list_pdf_objects(cfg, minio_client)

    # Load checkpoint and filter already-done keys
    ckpt = load_checkpoint() if cfg.resume else {
        "completed_keys": set(), "failed": {}, "stats": {"success": 0, "fail": 0, "chunks_total": 0},
        "last_key": None, "timestamp": None,
    }
    session_done: Set[str] = ckpt.get("completed_keys", set())
    if not isinstance(session_done, set):
        session_done = set(session_done)
    pending = [k for k in all_objects if k not in session_done]

    # Qdrant dedup: remove objects already vectorized in the collection
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
        return

    stats = ckpt.get("stats", {"success": 0, "fail": 0, "chunks_total": 0})
    completed_set = set(session_done)
    failed_dict = dict(ckpt.get("failed", {}))
    failure_reasons: dict = {}
    t0 = time.time()
    save_every = 25

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
                    log.info(f"[{done_count}/{len(pending)}] OK {key}: {info}")
                else:
                    stats["fail"] += 1
                    failed_dict[key] = info
                    reason_bucket = info.split(":")[0] if ":" in info else info
                    failure_reasons[reason_bucket] = failure_reasons.get(reason_bucket, 0) + 1
                    log.warning(f"[{done_count}/{len(pending)}] FAIL {key}: {info}")

                if done_count % save_every == 0:
                    ckpt.update({
                        "completed_keys": completed_set,
                        "failed": failed_dict,
                        "stats": stats,
                        "last_key": key,
                    })
                    save_checkpoint(ckpt)

    finally:
        # Final checkpoint save
        ckpt.update({
            "completed_keys": completed_set,
            "failed": failed_dict,
            "stats": stats,
            "last_key": key if pending else None,
        })
        save_checkpoint(ckpt)

    elapsed = time.time() - t0
    log.info("=" * 80)
    log.info("COMPLETE")
    log.info("=" * 80)
    log.info(f"Processed: {done_count:,} / {len(pending):,}")
    log.info(f"Success: {stats['success']:,}   Fail: {stats['fail']:,}   "
             f"Chunks: {stats['chunks_total']:,}")
    log.info(f"Elapsed: {elapsed/3600:.1f}h ({elapsed:.0f}s)")
    if failure_reasons:
        log.info("Failure breakdown:")
        for reason, cnt in sorted(failure_reasons.items(), key=lambda x: -x[1]):
            log.info(f"  {reason}: {cnt}")


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Vectorize PDFs directly from MinIO (no Postgres needed)"
    )
    ap.add_argument("--workers", type=int, default=4,
                    help="Parallel workers (default: 4)")
    ap.add_argument("--limit", type=int, default=0,
                    help="Max objects to process (0 = unlimited)")
    ap.add_argument("--aggressive-mode", action="store_true",
                    help="Fast retry, 5000-chunk batches")
    ap.add_argument("--resume", action="store_true",
                    help="Resume from checkpoint")
    ap.add_argument("--dry-run", action="store_true",
                    help="List objects only, don't embed or upsert")
    ap.add_argument("--skip-dedup", action="store_true",
                    help="Skip Qdrant scroll for dedup (faster startup)")
    ap.add_argument("--local-ollama", action="store_true",
                    help="Use local Ollama at localhost:11434 instead of remote")
    ap.add_argument("--embedding-urls", type=str, nargs='+', default=None,
                    help="Multiple Ollama embedding URLs for round-robin (e.g. http://localhost:11434/api/embeddings http://localhost:11435/api/embeddings)")
    ap.add_argument("--local-qdrant", action="store_true",
                    help="Use localhost:6333 for Qdrant (independent of MinIO)")
    ap.add_argument("--use-local", action="store_true",
                    help="Use localhost for Qdrant and MinIO")
    ap.add_argument("--prefix", type=str, default="",
                    help="MinIO prefix filter (e.g. '10/' for one subfolder)")
    ap.add_argument("--collection", type=str, default=None,
                    help="Qdrant collection name (overrides QDRANT_COLLECTION env)")
    ap.add_argument("--manifest", type=str, default=None,
                    help="CSV manifest file to use instead of live MinIO scan")

    args = ap.parse_args()
    cfg = load_config(args)

    try:
        run(cfg)
    except KeyboardInterrupt:
        log.info("Interrupted. Checkpoint saved -- use --resume to continue.")
        sys.exit(130)
    except Exception as e:
        log.exception(f"Fatal: {e}")
        sys.exit(1)
