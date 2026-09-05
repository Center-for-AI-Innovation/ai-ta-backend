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

Strategy: This is Stage 2 of the two-stage daily pipeline (Stage 1 main.py is download-only).
Can run continuously as background job to keep backlog clear.

Usage:
    python batch_vectorize_backlog.py --workers 4 --limit 1000
    python batch_vectorize_backlog.py --aggressive-mode  # Use all optimized settings
    python batch_vectorize_backlog.py --resume  # Resume from checkpoint
"""

import os
import sys
import logging
import argparse
from dataclasses import dataclass
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import urlparse

import psycopg2

from shared.env import load_project_env
load_project_env(__file__)

from shared.vectorize import (
    VectorizeConfig,
    MinIOReader,
    OptimizedQdrant,
    chunk_text,
    check_embedding_health,
    get_embedding,
    vectorize_article,
    load_checkpoint,
    save_checkpoint,
    parse_embedding_urls,
)

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
class BatchVectorizeConfig(VectorizeConfig):
    # Adds the runtime-only fields that the batch_vectorize CLI needs.
    workers: int = 4
    limit: int = 0
    dry_run: bool = False
    resume: bool = False
    checkpoint_file: str = "batch_vectorize_checkpoint.json"


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
    
    embedding_urls = parse_embedding_urls()
    if not embedding_urls:
        log.warning("No EMBEDDING_URLS or EMBEDDING_BASE_URL set; embedding calls will fail.")

    return BatchVectorizeConfig(
        db_dsn=os.getenv('POSTGRES_DSN') or os.getenv('DB_DSN', ''),
        db_schema=os.getenv('DB_SCHEMA', 'vyraid'),
        minio_endpoint=minio_endpoint,
        minio_access_key=os.getenv('MINIO_ACCESS_KEY', ''),
        minio_secret_key=os.getenv('MINIO_SECRET_KEY', ''),
        minio_bucket=os.getenv('MINIO_BUCKET', 'pubmed'),
        minio_secure=minio_secure,
        file_location_prefix=os.getenv('FILE_LOCATION', '').strip() or None,
        embedding_urls=embedding_urls,
        embedding_retry_max=embedding_retry_max,
        embedding_timeout=embedding_timeout,
        embedding_retry_backoff=embedding_retry_backoff,
        qdrant_url=qdrant_url,
        qdrant_port=qdrant_port,
        qdrant_api_key=qdrant_api_key,
        qdrant_collection=os.getenv('QDRANT_COLLECTION', 'ncbi_pdfs'),
        vector_size=int(os.getenv('VECTOR_SIZE', 768)),
        chunk_size=int(os.getenv('CHUNK_SIZE', 1000)),
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

    def get_non_vectorized_articles(self, batch_size: int, before_pmid: int = None) -> List[dict]:
        """Fetch the next batch of articles with a PDF but no vectorization.

        Keyset pagination on pmid (descending), NOT OFFSET. Successful articles
        get vector_indexed=true and leave this result set as they're processed,
        while failures stay (only logged to xml_failed_downloads). An incrementing
        OFFSET against a set that shrinks underneath it skips the unprocessed rows
        that shift into the skipped window, which caused runs to falsely report
        "No more articles" with most of the backlog still pending. Paging by
        `pmid < before_pmid` instead marches strictly downward and never skips a
        row: failures simply sit above the cursor and get retried on the next
        full run (which starts unbounded)."""
        where_cursor = "" if before_pmid is None else "  AND pmid < %s\n"
        query = f"""
        SELECT pmid, pmcid, pdf_location
        FROM {self.config.db_schema}.articles
        WHERE pdf_location IS NOT NULL
          AND COALESCE(vector_indexed, false) = false
          AND pdf_location NOT LIKE '%%.pdf.gz'
        {where_cursor}        ORDER BY pmid DESC
        LIMIT %s
        """
        params = (batch_size,) if before_pmid is None else (before_pmid, batch_size)
        with self.conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute(query, params)
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
        # Health check: ensure at least one embedding URL is reachable.
        log.info("Checking embedding endpoints (%d configured)...", len(config.embedding_urls))
        any_healthy = False
        for url in config.embedding_urls:
            if check_embedding_health(url, timeout=5):
                log.info("[ok] embedding endpoint healthy: %s", url)
                any_healthy = True
            else:
                log.warning("[fail] embedding endpoint not responding: %s", url)
        if not any_healthy:
            log.warning("No embedding endpoints responded healthy - will retry during processing.")
        
        # Enable bulk mode: disable indexing during ingestion
        qdrant.set_bulk_mode(enable=True)
        
        total_processed = 0
        total_successes = 0
        total_failures = 0
        failure_reasons = {}
        batch_size = 50  # Fetch 50 articles at a time
        # Keyset cursor: None = start unbounded (top). On --resume, continue from
        # the smallest pmid we'd reached. (Default checkpoint last_pmid=0 is not a
        # valid cursor for `pmid < 0`, so treat 0/None as unbounded.)
        cursor_pmid = checkpoint.get("last_pmid") or None if config.resume else None

        while True:
            # Fetch batch
            articles = db.get_non_vectorized_articles(batch_size, cursor_pmid)
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

            # Advance keyset cursor to the smallest pmid just seen (results are
            # ORDER BY pmid DESC, so the last row is the smallest). Next fetch
            # pulls pmid < this, never re-reading or skipping rows.
            cursor_pmid = articles[-1]['pmid']

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
        # Restore HNSW indexing even on SIGTERM / crash / Ctrl-C so the
        # collection doesn't get stuck at indexing_threshold=0.
        try:
            log.info("Restoring Qdrant to normal indexing mode...")
            qdrant.set_bulk_mode(enable=False)
        except Exception as e:
            log.warning("Failed to restore Qdrant indexing_threshold: %s", e)
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
