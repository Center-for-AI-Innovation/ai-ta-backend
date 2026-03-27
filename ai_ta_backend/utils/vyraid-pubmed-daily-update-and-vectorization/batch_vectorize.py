#!/usr/bin/env python3
"""
Optimized batch vectorization for PubMed articles with PDFs.

Processes all articles with pdf_location but vector_indexed=false.
Optimized for throughput with:
- Qdrant indexing_threshold set to 0 during bulk ingestion
- 5000-chunk batch upsets (vs 1000)
- 4-worker parallelization for article processing
- Aggressive embedding retry tuning (5 retries, 0.1s backoff)
- Resume capability from checkpoint

Strategy: This is Stage 2 of two-stage daily pipeline (Stage 1 downloads with --skip-vectorization).
Can run continuously as background job to keep backlog clear.

Usage:
    python batch_vectorize_backlog.py --workers 4 --limit 1000
    python batch_vectorize_backlog.py --aggressive-mode  # Use all optimized settings
    python batch_vectorize_backlog.py --resume  # Resume from checkpoint
"""

import os
import sys
import logging
import time
import argparse
import json
import uuid
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import tempfile
from urllib.parse import urlparse

import psycopg2
from psycopg2 import sql
import requests
import pymupdf
from minio import Minio
from qdrant_client import QdrantClient, models
from langchain_text_splitters.character import RecursiveCharacterTextSplitter

from dotenv import load_dotenv

# Load .env
load_dotenv('.env.production')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

@dataclass
class BatchVectorizeConfig:
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
    
    # Embedding
    embedding_base_url: str
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
    
    # Execution
    workers: int
    limit: int
    dry_run: bool
    resume: bool
    checkpoint_file: str


def load_config(workers: int = 4, limit: int = 0, dry_run: bool = False, 
                resume: bool = False, aggressive: bool = False, use_local: bool = False) -> BatchVectorizeConfig:
    """Load configuration from environment and arguments.
    
    Args:
        use_local: If True, use localhost endpoints for Qdrant/MinIO (for local testing)
    """
    
    # Aggressive mode overrides
    if aggressive:
        embedding_retry_max = 5
        embedding_timeout = 60
        embedding_retry_backoff = 0.1
        qdrant_upsert_batch = 5000
    else:
        embedding_retry_max = 30
        embedding_timeout = 300
        embedding_retry_backoff = 0.25
        qdrant_upsert_batch = 1000
    
    def normalize_minio_endpoint(endpoint: str, secure_flag: bool) -> Tuple[str, bool]:
        if not endpoint:
            return endpoint, secure_flag
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            parsed = urlparse(endpoint)
            host = parsed.netloc or parsed.path
            if "/" in host:
                host = host.split("/")[0]
            if "MINIO_SECURE" in os.environ:
                return host, secure_flag
            return host, parsed.scheme == "https"
        return endpoint.rstrip("/"), secure_flag

    # Local endpoint overrides for testing
    if use_local:
        qdrant_url = "http://localhost"
        qdrant_port = 6333
        minio_endpoint = "localhost:9002"  # Local MinIO API on 9002 (console on 9001)
        minio_secure = False
    else:
        qdrant_url = os.getenv('QDRANT_URL', '')
        qdrant_port = int(os.getenv('QDRANT_PORT', 6333))
        minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        minio_secure = os.getenv('MINIO_SECURE', 'false').lower() in ('1', 'true', 'yes')

    minio_endpoint, minio_secure = normalize_minio_endpoint(minio_endpoint, minio_secure)
    if qdrant_url.endswith("/"):
        qdrant_url = qdrant_url.rstrip("/")

    qdrant_api_key = os.getenv('QDRANT_API_KEY') or None
    
    return BatchVectorizeConfig(
        db_dsn=os.getenv('POSTGRES_DSN') or os.getenv('DB_DSN', ''),
        db_schema=os.getenv('DB_SCHEMA', 'vyraid'),
        minio_endpoint=minio_endpoint,
        minio_access_key=os.getenv('MINIO_ACCESS_KEY', ''),
        minio_secret_key=os.getenv('MINIO_SECRET_KEY', ''),
        minio_bucket=os.getenv('MINIO_BUCKET', 'pubmed'),
        minio_secure=minio_secure,
        file_location_prefix=os.getenv('FILE_LOCATION', '').strip() or None,
        embedding_base_url=os.getenv('EMBEDDING_BASE_URL', ''),
        embedding_retry_max=embedding_retry_max,
        embedding_timeout=embedding_timeout,
        embedding_retry_backoff=embedding_retry_backoff,
        qdrant_url=qdrant_url,
        qdrant_port=qdrant_port,
        qdrant_api_key=qdrant_api_key,
        qdrant_collection=os.getenv('QDRANT_COLLECTION', 'ncbi_pdfs'),
        vector_size=int(os.getenv('VECTOR_SIZE', 768)),
        chunk_size=int(os.getenv('CHUNK_SIZE', 7000)),
        chunk_overlap=int(os.getenv('CHUNK_OVERLAP', 200)),
        qdrant_upsert_batch=qdrant_upsert_batch,
        workers=workers,
        limit=limit,
        dry_run=dry_run,
        resume=resume,
        checkpoint_file='batch_vectorize_checkpoint.json',
    )


# ============================================================================
# Database Access
# ============================================================================

class VectorizeDB:
    def __init__(self, config: BatchVectorizeConfig):
        self.config = config
        self.conn = psycopg2.connect(config.db_dsn)
        log.info("Connected to PostgreSQL")
        self._ensure_batch_vectorize_log_entry()
    
    def close(self):
        if self.conn:
            self.conn.close()

    def _ensure_batch_vectorize_log_entry(self):
        """Ensure a sentinel row exists in xml_processing_log for batch_vectorize failures."""
        query = f"""
        INSERT INTO {self.config.db_schema}.xml_processing_log (xml_filename, processed_all_pmcid)
        VALUES ('batch_vectorize', false)
        ON CONFLICT (xml_filename) DO NOTHING
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
            self.conn.commit()
        except Exception:
            self.conn.rollback()

    def get_non_vectorized_articles(self, batch_size: int, offset: int = 0) -> List[dict]:
        """Fetch articles with PDF but no vectorization."""
        query = f"""
        SELECT pmid, pmcid, pdf_location
        FROM {self.config.db_schema}.articles
        WHERE pdf_location IS NOT NULL
          AND COALESCE(vector_indexed, false) = false
          AND pdf_location NOT LIKE '%%.pdf.gz'
        ORDER BY pmid DESC
        LIMIT %s OFFSET %s
        """
        with self.conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute(query, (batch_size, offset))
            results = cur.fetchall()
            return [
                {"pmid": r[0], "pmcid": r[1], "pdf_location": r[2]}
                for r in results
            ]
    
    def mark_vectorized(self, pmid: int):
        """Mark article as vectorized."""
        query = f"""
        UPDATE {self.config.db_schema}.articles
        SET vector_indexed = true, last_updated = NOW()
        WHERE pmid = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (pmid,))
        self.conn.commit()
    
    def record_vectorization_failure(self, pmid: int, reason: str):
        """Record vectorization failure."""
        # Reset any aborted transaction before attempting the insert
        try:
            self.conn.rollback()
        except Exception:
            pass
        query = f"""
        INSERT INTO {self.config.db_schema}.xml_failed_downloads
        (failed_pmcid, xml_filename, failure_reason, failure_timestamp)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT DO NOTHING
        """
        # Use pmid as failed_pmcid for batch vectorization
        with self.conn.cursor() as cur:
            cur.execute(query, (str(pmid), "batch_vectorize", f"vectorize_fail: {reason}"))
        self.conn.commit()


# ============================================================================
# MinIO Access
# ============================================================================

class MinIOReader:
    def __init__(self, config: BatchVectorizeConfig):
        self.config = config
        # Parse endpoint
        endpoint = config.minio_endpoint.rstrip("/")
        if "://" in endpoint:
            from urllib.parse import urlparse
            parsed = urlparse(f"http://{endpoint}" if "://" not in endpoint else endpoint)
            endpoint = f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname
        
        self.client = Minio(
            endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=config.minio_secure,
        )
        self.bucket = config.minio_bucket
        log.info(f"MinIO client initialized: {endpoint}")
    
    def download_pdf(self, s3_path: str) -> Optional[bytes]:
        """Download PDF from MinIO path (s3://bucket/key format or just key)."""
        try:
            # Extract key from s3:// URI if needed
            if s3_path.startswith('s3://'):
                key = s3_path.split('/', 3)[3]  # s3://bucket/key -> key
            else:
                key = s3_path
            
            response = self.client.get_object(self.bucket, key)
            data = response.read()
            log.debug(f"Downloaded {len(data)} bytes from {key}")
            return data
        except Exception as e:
            log.warning(f"MinIO download failed for {s3_path}: {e}")
            return None


# ============================================================================
# Embedding & Chunking
# ============================================================================

def chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
    """Split text into chunks."""
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n\n", "\n", " ", ""],
    )
    return splitter.split_text(text)


def check_embedding_health(embedding_base_url: str, timeout: int = 5) -> bool:
    """Quick health check for Ollama endpoint."""
    try:
        resp = requests.post(
            embedding_base_url,
            json={"model": "nomic-embed-text:v1.5", "prompt": "test"},
            timeout=timeout
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        log.warning(f"Ollama health check failed: {e}")
        return False


def get_embedding(embedding_base_url: str, text: str, config: BatchVectorizeConfig) -> Optional[List[float]]:
    """Get embedding from Ollama endpoint with adaptive retry (handles service recovery).
    
    If Ollama returns 'input length exceeds context length', the chunk is truncated to
    EMBEDDING_TRUNCATE_CHARS and retried once — no point retrying the original text.
    """
    url = embedding_base_url
    # Truncation fallback: if a chunk exceeds the model's token context window
    # (nomic-embed-text:v1.5 has 8192 tokens), we truncate and retry once.
    # 4000 chars is ~2x safe headroom even for dense scientific text.
    EMBEDDING_TRUNCATE_CHARS = 4000
    prompt = text

    for attempt in range(1, config.embedding_retry_max + 1):
        try:
            resp = requests.post(
                url,
                json={"model": "nomic-embed-text:v1.5", "prompt": prompt},
                timeout=config.embedding_timeout
            )
            # Detect permanent "context length exceeded" — truncate and retry once.
            if resp.status_code == 500 and "context length" in resp.text:
                if len(prompt) > EMBEDDING_TRUNCATE_CHARS:
                    log.warning(f"Chunk too long ({len(prompt)} chars); truncating to {EMBEDDING_TRUNCATE_CHARS} chars and retrying")
                    prompt = prompt[:EMBEDDING_TRUNCATE_CHARS]
                    continue  # retry immediately with shorter text, don't count this as an attempt
                else:
                    # Already at truncated length and still failing — give up
                    log.warning(f"Embedding context-length error even after truncation ({len(prompt)} chars); giving up")
                    return None
            resp.raise_for_status()
            return resp.json()["embedding"]
        except requests.exceptions.Timeout:
            log.debug(f"Embedding timeout attempt {attempt}/{config.embedding_retry_max}")
            if attempt == config.embedding_retry_max:
                return None
            # Longer backoff for timeout (service may be slow to recover)
            backoff = min(config.embedding_retry_backoff * (4 ** (attempt - 1)), 10.0)
            time.sleep(backoff)
        except requests.exceptions.ConnectionError as e:
            log.debug(f"Connection error attempt {attempt}/{config.embedding_retry_max}: {e}")
            if attempt == config.embedding_retry_max:
                return None
            # Longer backoff for connection errors (service may be restarting)
            backoff = min(config.embedding_retry_backoff * (4 ** (attempt - 1)), 10.0)
            time.sleep(backoff)
        except Exception as e:
            if attempt == config.embedding_retry_max:
                log.warning(f"Embedding failed after {config.embedding_retry_max} retries: {e}")
                return None
            # Exponential backoff for other errors
            backoff = config.embedding_retry_backoff * (2 ** (attempt - 1))
            time.sleep(min(backoff, 5.0))
    
    return None


# ============================================================================
# Qdrant Access (with indexing threshold tuning)
# ============================================================================

class OptimizedQdrant:
    def __init__(self, config: BatchVectorizeConfig):
        self.config = config
        # Parse the URL manually to extract host/port/https — QdrantClient does not
        # infer port 443 from an https:// URL; it defaults to gRPC port 6333.
        # This mirrors the approach used in main.py's QdrantHelper.
        raw_url = config.qdrant_url.rstrip("/")
        if raw_url.startswith("http://") or raw_url.startswith("https://"):
            from urllib.parse import urlparse as _up
            u = _up(raw_url)
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
        log.info(f"Qdrant client initialized: {raw_url} → {host}:{port} https={use_https}")
    
    def set_bulk_mode(self, enable: bool = True):
        """Set indexing threshold for bulk ingestion (0 = disabled) or normal (1000)."""
        threshold = 0 if enable else 1000
        try:
            self.client.update_collection(
                collection_name=self.config.qdrant_collection,
                optimizer_config=models.OptimizersConfigDiff(indexing_threshold=threshold)
            )
            log.info(f"Qdrant indexing_threshold set to {threshold}")
        except Exception as e:
            # Qdrant indexing threshold is optimization only, not critical
            # If unreachable, continue anyway (upserts will still work, just slower)
            log.warning(f"Qdrant indexing threshold adjustment skipped (non-critical): {e}")
    
    def upsert_batch(self, points: List[models.PointStruct]) -> bool:
        """Upsert batch of points with retries."""
        if not points:
            return True
        
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                self.client.upsert(
                    collection_name=self.config.qdrant_collection,
                    points=points,
                    wait=False  # Background indexing
                )
                time.sleep(2)  # Small pause after upsert
                return True
            except Exception as e:
                if attempt == max_attempts:
                    log.error(f"Qdrant upsert failed after {max_attempts} attempts: {e}")
                    return False
                log.warning(f"Qdrant upsert attempt {attempt}/{max_attempts} failed: {e}")
                time.sleep(1.0 * attempt)
        
        return False


# ============================================================================
# Checkpoint Management
# ============================================================================

def load_checkpoint(checkpoint_file: str) -> dict:
    """Load checkpoint to resume from last processed article."""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"Failed to load checkpoint: {e}")
    return {"last_pmid": 0, "processed_count": 0, "success_count": 0, "fail_count": 0}


def save_checkpoint(checkpoint_file: str, data: dict):
    """Save checkpoint."""
    try:
        with open(checkpoint_file, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log.warning(f"Failed to save checkpoint: {e}")


# ============================================================================
# Vectorization Worker
# ============================================================================

def vectorize_article(
    article: dict,
    config: BatchVectorizeConfig,
    db: VectorizeDB,
    minio: MinIOReader,
    qdrant: OptimizedQdrant,
) -> Tuple[int, bool, str]:
    """
    Vectorize a single article. Returns: (pmid, success, info_message).
    """
    pmid = article['pmid']
    pmcid = article['pmcid']
    pdf_location = article['pdf_location']
    
    try:
        # Download PDF
        pdf_data = minio.download_pdf(pdf_location)
        if not pdf_data:
            return pmid, False, "pdf_download_fail"
        
        # Extract text from PDF
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                tmp.write(pdf_data)
                tmp_path = tmp.name
            
            doc = pymupdf.open(tmp_path)
            page_chunks = []
            
            for page_num, page in enumerate(doc, start=1):
                raw = page.get_text().encode("utf8", errors="ignore").decode("utf8", errors="ignore")
                for chunk in chunk_text(raw, config.chunk_size, config.chunk_overlap):
                    page_chunks.append((page_num, chunk))
            
            doc.close()
            os.unlink(tmp_path)
            
            if not page_chunks:
                return pmid, False, "empty_pdf"
            
        except pymupdf.EmptyFileError:
            return pmid, False, "empty_pdf"
        except Exception as e:
            return pmid, False, f"pdf_extraction_fail: {str(e)}"
        
        # Embed and prepare points for upsert
        points = []
        for i, (page_num, chunk) in enumerate(page_chunks):
            emb = get_embedding(config.embedding_base_url, chunk, config)
            if not emb:
                return pmid, False, f"embedding_fail_chunk_{i}"
            
            points.append(
                models.PointStruct(
                    id=str(uuid.uuid4()),
                    vector=emb,
                    payload={
                        "page_content": chunk,
                        "s3_path": pdf_location,
                        "readable_filename": Path(pdf_location).name,
                        "pagenumber": page_num,
                        "chunk_index": i,
                        "total_chunks": len(page_chunks),
                        "pmid": pmid,
                    },
                )
            )
        
        # Upsert all points
        if not qdrant.upsert_batch(points):
            return pmid, False, f"qdrant_upsert_fail: {len(points)} points"
        
        # Return success — caller (main thread) is responsible for db.mark_vectorized()
        # to avoid concurrent psycopg2 access from multiple worker threads.
        return pmid, True, f"vectorized: {len(page_chunks)} chunks"
        
    except Exception as e:
        log.exception(f"Unexpected error vectorizing PMID {pmid}: {e}")
        return pmid, False, f"unexpected_error: {str(e)}"


# ============================================================================
# Main Batch Vectorization Loop
# ============================================================================

def run_batch_vectorization(config: BatchVectorizeConfig):
    """Main entry point for batch vectorization."""
    log.info("=" * 80)
    log.info("Starting batch vectorization (backlog + daily articles)")
    log.info("=" * 80)
    log.info(f"Config: workers={config.workers}, limit={config.limit}, aggressive={config.embedding_retry_max == 5}")
    
    db = VectorizeDB(config)
    minio = MinIOReader(config)
    qdrant = OptimizedQdrant(config)
    
    # Load checkpoint if resuming
    checkpoint = load_checkpoint(config.checkpoint_file) if config.resume else {"last_pmid": 0, "processed_count": 0, "success_count": 0, "fail_count": 0}
    
    try:
        # Health check: ensure Ollama is reachable
        log.info("Checking Ollama endpoint health...")
        if not check_embedding_health(config.embedding_base_url, timeout=5):
            log.warning("⚠️  Ollama endpoint not responding - will retry during processing")
        else:
            log.info("✓ Ollama endpoint healthy")
        
        # Enable bulk mode: disable indexing during ingestion
        qdrant.set_bulk_mode(enable=True)
        
        total_processed = 0
        total_successes = 0
        total_failures = 0
        failure_reasons = {}
        batch_size = 50  # Fetch 50 articles at a time
        offset = 0
        
        while True:
            # Fetch batch
            articles = db.get_non_vectorized_articles(batch_size, offset)
            if not articles:
                log.info("No more articles to process")
                break
            
            log.info(f"Fetched batch of {len(articles)} articles")
            
            # Process in parallel with configured workers
            with ThreadPoolExecutor(max_workers=max(1, config.workers)) as executor:
                futures = []
                for article in articles:
                    futures.append(executor.submit(
                        vectorize_article,
                        article,
                        config,
                        db,
                        minio,
                        qdrant,
                    ))
                
                for future in futures:
                    try:
                        pmid, success, info = future.result()
                        total_processed += 1
                        
                        if success:
                            total_successes += 1
                            log.info(f"✓ PMID {pmid}: {info}")
                            if not config.dry_run:
                                db.mark_vectorized(pmid)
                        else:
                            total_failures += 1
                            failure_reasons[info] = failure_reasons.get(info, 0) + 1
                            log.warning(f"✗ PMID {pmid}: {info}")
                            
                            if not config.dry_run:
                                db.record_vectorization_failure(pmid, info)
                        
                        # Check if we've hit the limit
                        if config.limit > 0 and total_processed >= config.limit:
                            log.info(f"Reached limit of {config.limit} articles")
                            break
                    
                    except Exception as e:
                        log.exception(f"Unexpected error processing future: {e}")
                        total_processed += 1
                        total_failures += 1
            
            # Save checkpoint
            checkpoint = {
                "last_pmid": articles[-1]['pmid'],
                "processed_count": total_processed,
                "success_count": total_successes,
                "fail_count": total_failures,
                "timestamp": datetime.now().isoformat(),
            }
            save_checkpoint(config.checkpoint_file, checkpoint)
            
            # Check limit
            if config.limit > 0 and total_processed >= config.limit:
                break
            
            offset += batch_size
        
        # Restore normal indexing mode
        log.info("Restoring Qdrant to normal indexing mode...")
        qdrant.set_bulk_mode(enable=False)
        
        # Summary
        log.info("=" * 80)
        log.info("VECTORIZATION COMPLETE")
        log.info("=" * 80)
        log.info(f"Total processed: {total_processed}")
        log.info(f"Successes: {total_successes} ({100*total_successes/max(1, total_processed):.1f}%)")
        log.info(f"Failures: {total_failures}")
        log.info(f"Failure reasons (top 10):")
        for reason, count in sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True)[:10]:
            log.info(f"  {reason}: {count}")
        log.info("=" * 80)
    
    finally:
        db.close()


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Batch vectorize PubMed articles with PDFs (Stage 2 of two-stage pipeline)"
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4, embedding is sequential per article)"
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max articles to process (0 = unlimited)"
    )
    ap.add_argument(
        "--aggressive-mode",
        action="store_true",
        help="Enable aggressive tuning (fast retry, 5000-chunk batches)"
    )
    ap.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint"
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Log-only mode, don't update database or Qdrant"
    )
    ap.add_argument(
        "--use-local",
        action="store_true",
        help="Use localhost endpoints for Qdrant/MinIO (for local testing on hal-dgx)"
    )
    
    args = ap.parse_args()
    
    config = load_config(
        workers=args.workers,
        limit=args.limit,
        dry_run=args.dry_run,
        resume=args.resume,
        aggressive=args.aggressive_mode,
        use_local=args.use_local,
    )
    
    try:
        run_batch_vectorization(config)
    except Exception as e:
        log.exception(f"Batch vectorization failed: {e}")
        sys.exit(1)
