#!/usr/bin/env python3
"""Postgres-driven vectorization of PubMed articles.

Built as a parallel (read: side-by-side, not threaded) re-architecture of
vectorize_minio_direct.py. The legacy tool builds a full in-memory list of
every key in the MinIO bucket and scrolls all of Qdrant for dedup before
processing the first article — fine for a small bucket, painful for ~2M PDFs.

This tool drives the same downstream pipeline (download from MinIO, chunk,
embed, upsert to Qdrant) but reads the work list from Postgres
(vyraid.articles WHERE pdf_location IS NOT NULL AND vector_indexed=false).
Memory is O(1) regardless of bucket size, first article starts within seconds,
and shards trivially via --from-pmid / --to-pmid.

Vetting protocol vs the legacy tool: see plan or README.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Optional

import psycopg2

from shared.env import load_project_env
load_project_env(__file__)

from shared.vectorize import (
    VectorizeConfig,
    MinIOReader,
    OptimizedQdrant,
    check_embedding_health,
    vectorize_article,
    load_checkpoint,
    save_checkpoint,
    parse_embedding_urls,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("vectorize_pg_driven")


# ============================================================================
# Configuration
# ============================================================================

class PgDrivenConfig(VectorizeConfig):
    """VectorizeConfig with run-time CLI fields tacked on. We avoid @dataclass
    inheritance gymnastics by treating these as plain attributes set after
    construction."""
    workers: int
    limit: int
    dry_run: bool
    aggressive: bool
    from_pmid: Optional[int]
    to_pmid: Optional[int]
    label: Optional[str]
    collection_override: Optional[str]
    checkpoint_file: str


def load_config(args) -> PgDrivenConfig:
    if args.aggressive_mode:
        emb_retry_max, emb_timeout, emb_backoff, qd_batch = 5, 60, 0.1, 5000
    else:
        emb_retry_max, emb_timeout, emb_backoff, qd_batch = 30, 300, 0.25, 1000

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_secure = os.getenv("MINIO_SECURE", "false").lower() in ("1", "true", "yes")

    qdrant_url = os.getenv("QDRANT_URL", "").rstrip("/")
    qdrant_collection = args.collection or os.getenv("QDRANT_COLLECTION", "pubmed_v2")

    embedding_urls = parse_embedding_urls()
    if not embedding_urls:
        log.error("No EMBEDDING_URLS or EMBEDDING_BASE_URL set; aborting.")
        sys.exit(2)

    cfg = PgDrivenConfig(
        db_dsn=os.getenv("POSTGRES_DSN") or os.getenv("DB_DSN", ""),
        db_schema=os.getenv("DB_SCHEMA", "vyraid"),
        minio_endpoint=minio_endpoint,
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", ""),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", ""),
        minio_bucket=os.getenv("MINIO_BUCKET", "pubmed"),
        minio_secure=minio_secure,
        file_location_prefix=os.getenv("FILE_LOCATION", "").strip() or None,
        embedding_urls=embedding_urls,
        embedding_retry_max=emb_retry_max,
        embedding_timeout=emb_timeout,
        embedding_retry_backoff=emb_backoff,
        qdrant_url=qdrant_url,
        qdrant_port=int(os.getenv("QDRANT_PORT", 6333)),
        qdrant_api_key=os.getenv("QDRANT_API_KEY") or None,
        qdrant_collection=qdrant_collection,
        vector_size=int(os.getenv("VECTOR_SIZE", 768)),
        chunk_size=int(os.getenv("CHUNK_SIZE", 1000)),
        chunk_overlap=int(os.getenv("CHUNK_OVERLAP", 200)),
        qdrant_upsert_batch=qd_batch,
    )
    cfg.workers = args.workers
    cfg.limit = args.limit
    cfg.dry_run = args.dry_run
    cfg.aggressive = args.aggressive_mode
    cfg.from_pmid = args.from_pmid
    cfg.to_pmid = args.to_pmid
    cfg.label = args.label
    cfg.collection_override = args.collection
    cfg.checkpoint_file = args.checkpoint or "vectorize_pg_driven_checkpoint.json"
    return cfg


# ============================================================================
# Database access (light-touch — work list + status writes only)
# ============================================================================

class PgWorkList:
    def __init__(self, config: PgDrivenConfig):
        self.config = config
        self.conn = psycopg2.connect(config.db_dsn)
        log.info("Connected to PostgreSQL")

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def fetch_batch(self, batch_size: int, last_pmid: Optional[int]) -> List[dict]:
        # Order by pmid DESC and walk via a high-water mark instead of OFFSET.
        # OFFSET on a 2M-row table re-scans the whole prefix every page; a
        # last_pmid filter is index-friendly.
        clauses = [
            "pdf_location IS NOT NULL",
            "COALESCE(vector_indexed, false) = false",
            "pdf_location NOT LIKE %s",
        ]
        params: List = ['%.pdf.gz']
        if last_pmid is not None:
            clauses.append("pmid < %s")
            params.append(last_pmid)
        if self.config.from_pmid is not None:
            clauses.append("pmid >= %s")
            params.append(self.config.from_pmid)
        if self.config.to_pmid is not None:
            clauses.append("pmid <= %s")
            params.append(self.config.to_pmid)
        where = " AND ".join(clauses)
        query = (
            f"SELECT pmid, pmcid, pdf_location FROM {self.config.db_schema}.articles "
            f"WHERE {where} ORDER BY pmid DESC LIMIT %s"
        )
        params.append(batch_size)
        with self.conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute(query, tuple(params))
            return [
                {"pmid": r[0], "pmcid": r[1], "pdf_location": r[2]}
                for r in cur.fetchall()
            ]

    def mark_vectorized(self, pmid: int) -> None:
        if self.config.dry_run:
            return
        query = (
            f"UPDATE {self.config.db_schema}.articles "
            f"SET vector_indexed = true, last_updated = NOW() WHERE pmid = %s"
        )
        with self.conn.cursor() as cur:
            cur.execute(query, (pmid,))
        self.conn.commit()

    def record_failure(self, pmid: int, reason: str) -> None:
        if self.config.dry_run:
            return
        try:
            self.conn.rollback()
        except Exception:
            pass
        query = (
            f"INSERT INTO {self.config.db_schema}.xml_failed_downloads "
            "(failed_pmcid, xml_filename, failure_reason, failure_timestamp) "
            "VALUES (%s, %s, %s, NOW()) ON CONFLICT DO NOTHING"
        )
        with self.conn.cursor() as cur:
            cur.execute(query, (str(pmid), "vectorize_pg_driven", f"vectorize_fail: {reason}"))
        self.conn.commit()


# ============================================================================
# Run loop
# ============================================================================

def run(config: PgDrivenConfig) -> int:
    log.info("=" * 80)
    log.info(
        "vectorize_pg_driven starting (collection=%s, workers=%d, label=%s)",
        config.qdrant_collection, config.workers, config.label or "<none>",
    )
    log.info("=" * 80)

    db = PgWorkList(config)
    minio = MinIOReader(config)
    qdrant = OptimizedQdrant(config)

    cp = load_checkpoint(
        config.checkpoint_file,
        default={"last_pmid": None, "processed_count": 0, "success_count": 0, "fail_count": 0},
    )
    last_pmid = cp.get("last_pmid")

    try:
        for url in config.embedding_urls:
            ok = check_embedding_health(url, timeout=5)
            log.info("[%s] embedding endpoint %s", "ok" if ok else "fail", url)

        qdrant.set_bulk_mode(enable=True)

        total_processed = 0
        total_successes = 0
        total_failures = 0
        failure_reasons: dict = {}
        batch_size = 50
        extra_payload = {"label": config.label} if config.label else None

        while True:
            articles = db.fetch_batch(batch_size, last_pmid)
            if not articles:
                log.info("No more articles to process")
                break
            log.info("Fetched batch of %d articles (last_pmid<=%s)", len(articles), last_pmid)

            with ThreadPoolExecutor(max_workers=max(1, config.workers)) as ex:
                futures = [
                    ex.submit(vectorize_article, a, config, minio, qdrant, extra_payload)
                    for a in articles
                ]
                for fut in futures:
                    try:
                        pmid, success, info = fut.result()
                    except Exception as e:
                        log.exception("worker future raised: %s", e)
                        total_processed += 1
                        total_failures += 1
                        continue
                    total_processed += 1
                    if success:
                        total_successes += 1
                        log.info("[ok] pmid=%s %s", pmid, info)
                        db.mark_vectorized(pmid)
                    else:
                        total_failures += 1
                        failure_reasons[info] = failure_reasons.get(info, 0) + 1
                        log.warning("[fail] pmid=%s %s", pmid, info)
                        db.record_failure(pmid, info)
                    if config.limit and total_processed >= config.limit:
                        break

            last_pmid = articles[-1]["pmid"]
            save_checkpoint(config.checkpoint_file, {
                "last_pmid": last_pmid,
                "processed_count": total_processed,
                "success_count": total_successes,
                "fail_count": total_failures,
                "label": config.label,
                "collection": config.qdrant_collection,
                "timestamp": datetime.now().isoformat(),
            })
            if config.limit and total_processed >= config.limit:
                log.info("reached --limit=%d", config.limit)
                break

        qdrant.set_bulk_mode(enable=False)

        log.info("=" * 80)
        log.info("vectorize_pg_driven complete")
        log.info("processed=%d  success=%d  fail=%d", total_processed, total_successes, total_failures)
        for reason, count in sorted(failure_reasons.items(), key=lambda x: -x[1])[:10]:
            log.info("  %s: %d", reason, count)
        log.info("=" * 80)
        return 0 if total_failures == 0 else 1
    finally:
        db.close()


# ============================================================================
# CLI
# ============================================================================

def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Postgres-driven catch-up vectorizer. Reads work list from "
            "vyraid.articles instead of scanning MinIO. Parallel A/B option "
            "to vectorize_minio_direct.py for vetting before retiring the legacy tool."
        )
    )
    ap.add_argument("--workers", type=int, default=4, help="Parallel worker threads (default: 4)")
    ap.add_argument("--limit", type=int, default=0, help="Max articles to process (0 = unlimited)")
    ap.add_argument("--dry-run", action="store_true", help="Skip db writes; embed + upsert to Qdrant only")
    ap.add_argument("--aggressive-mode", action="store_true", help="Fast-fail embed retry, larger Qdrant batches")
    ap.add_argument("--from-pmid", type=int, default=None, help="Lower bound (inclusive) — for sharded A/B runs")
    ap.add_argument("--to-pmid", type=int, default=None, help="Upper bound (inclusive) — for sharded A/B runs")
    ap.add_argument("--collection", default=None, help="Override QDRANT_COLLECTION (e.g. pubmed_v2_vet_pgdriven)")
    ap.add_argument("--label", default=None, help="Tag added to every Qdrant payload — useful for A/B runs")
    ap.add_argument(
        "--checkpoint", default=None,
        help="Override checkpoint file path (default vectorize_pg_driven_checkpoint.json)",
    )
    args = ap.parse_args()

    cfg = load_config(args)
    return run(cfg)


if __name__ == "__main__":
    sys.exit(main())
