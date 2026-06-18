#!/usr/bin/env python3
"""Read-only audit: is Postgres a faithful index of the MinIO PDF bucket?

Two-way sample-based comparison. Lets us decide whether
vectorize_minio_direct.py (which scans MinIO directly) can be retired in favor
of the Postgres-driven path.

Outputs:
  Postgres: rows with pdf_location ............ N_pg
  MinIO:    objects matching .pdf prefix ...... N_minio (streamed count)
  Postgres -> MinIO sample (1000 keys) ........ X% present
  MinIO -> Postgres sample (1000 keys) ........ Y% present
  Up to 10 example missing keys per direction.

Read-only: no writes to Postgres, MinIO, or Qdrant.

Usage:
  export $(grep -v '^#' /home/dadams/pub-med-daily/.env.production | xargs)
  python scripts/postgres_vs_minio_audit.py
"""
from __future__ import annotations

import os
import random
import sys
from pathlib import Path

# Make the package's shared/ importable when run directly from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv("/home/dadams/pub-med-daily/.env.production")

SAMPLE_SIZE = 1000


def parse_minio_endpoint(endpoint: str) -> str:
    endpoint = endpoint.rstrip("/")
    if "://" in endpoint:
        from urllib.parse import urlparse
        u = urlparse(endpoint)
        host = u.hostname or endpoint
        return f"{host}:{u.port}" if u.port else host
    return endpoint


def main() -> int:
    db_dsn = os.getenv("POSTGRES_DSN") or os.getenv("DB_DSN")
    db_schema = os.getenv("DB_SCHEMA", "vyraid")
    if not db_dsn:
        print("ERROR: POSTGRES_DSN not set", file=sys.stderr)
        return 2

    minio_endpoint = parse_minio_endpoint(os.getenv("MINIO_ENDPOINT", "localhost:9000"))
    minio_secure = os.getenv("MINIO_SECURE", "false").lower() in ("1", "true", "yes")
    minio_bucket = os.getenv("MINIO_BUCKET", "pubmed")

    client = Minio(
        minio_endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", ""),
        secret_key=os.getenv("MINIO_SECRET_KEY", ""),
        secure=minio_secure,
    )

    conn = psycopg2.connect(db_dsn)
    print(f"connected: postgres={db_dsn.split('@')[-1]} minio={minio_endpoint} bucket={minio_bucket}")

    # 1) Postgres totals
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {db_schema}.articles WHERE pdf_location IS NOT NULL")
        n_pg = cur.fetchone()[0]
        cur.execute(f"SELECT count(*) FROM {db_schema}.articles WHERE pdf_location IS NOT NULL AND COALESCE(vector_indexed,false)=false")
        n_pg_unindexed = cur.fetchone()[0]
    print(f"Postgres rows with pdf_location: {n_pg:,}  (vector_indexed=false: {n_pg_unindexed:,})")

    # 2) MinIO total — streaming count (no list materialized)
    print("Counting MinIO objects (streaming)...", flush=True)
    n_minio = 0
    minio_sample: list[str] = []
    rng = random.Random(42)
    for obj in client.list_objects(minio_bucket, recursive=True):
        if not obj.object_name.lower().endswith(".pdf"):
            continue
        n_minio += 1
        # reservoir sample of size SAMPLE_SIZE
        if len(minio_sample) < SAMPLE_SIZE:
            minio_sample.append(obj.object_name)
        else:
            j = rng.randint(0, n_minio - 1)
            if j < SAMPLE_SIZE:
                minio_sample[j] = obj.object_name
        if n_minio % 250_000 == 0:
            print(f"  ... {n_minio:,} objects", flush=True)
    print(f"MinIO .pdf objects: {n_minio:,}  (sampled {len(minio_sample)})")

    # 3) Postgres -> MinIO direction (HEAD a random sample of pdf_locations)
    with conn.cursor() as cur:
        # TABLESAMPLE BERNOULLI keeps memory bounded on huge tables.
        sample_pct = max(0.001, 100.0 * SAMPLE_SIZE / max(n_pg, 1))
        sample_pct = min(sample_pct, 100.0)
        cur.execute(
            f"SELECT pdf_location FROM {db_schema}.articles "
            f"TABLESAMPLE BERNOULLI(%s) WHERE pdf_location IS NOT NULL LIMIT %s",
            (sample_pct, SAMPLE_SIZE),
        )
        pg_sample = [r[0] for r in cur.fetchall()]
    if len(pg_sample) < 100:
        # Fallback: ORDER BY random() if TABLESAMPLE returned too few
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT pdf_location FROM {db_schema}.articles "
                f"WHERE pdf_location IS NOT NULL ORDER BY random() LIMIT %s",
                (SAMPLE_SIZE,),
            )
            pg_sample = [r[0] for r in cur.fetchall()]

    pg_to_minio_present = 0
    pg_to_minio_missing: list[str] = []
    for key in pg_sample:
        try:
            client.stat_object(minio_bucket, key)
            pg_to_minio_present += 1
        except S3Error:
            if len(pg_to_minio_missing) < 10:
                pg_to_minio_missing.append(key)
    pct_pg_in_minio = 100.0 * pg_to_minio_present / max(len(pg_sample), 1)
    print(f"Postgres -> MinIO: {pg_to_minio_present}/{len(pg_sample)} present ({pct_pg_in_minio:.2f}%)")
    if pg_to_minio_missing:
        print("  examples missing in MinIO:")
        for k in pg_to_minio_missing:
            print(f"    {k}")

    # 4) MinIO -> Postgres direction (lookup random MinIO keys)
    if minio_sample:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(minio_sample))
            cur.execute(
                f"SELECT pdf_location FROM {db_schema}.articles "
                f"WHERE pdf_location IN ({placeholders})",
                tuple(minio_sample),
            )
            present_in_pg = {r[0] for r in cur.fetchall()}
        minio_to_pg_present = sum(1 for k in minio_sample if k in present_in_pg)
        minio_to_pg_missing = [k for k in minio_sample if k not in present_in_pg][:10]
        pct_minio_in_pg = 100.0 * minio_to_pg_present / len(minio_sample)
        print(f"MinIO -> Postgres: {minio_to_pg_present}/{len(minio_sample)} present ({pct_minio_in_pg:.2f}%)")
        if minio_to_pg_missing:
            print("  examples missing in Postgres:")
            for k in minio_to_pg_missing:
                print(f"    {k}")

    # 5) Verdict guidance
    print()
    print("Verdict: if both directions are >99% matched, Postgres is a")
    print("faithful index of MinIO and vectorize_minio_direct.py can be retired")
    print("after a final A/B vet using vectorize_pg_driven.py. Below 99%,")
    print("investigate gaps before relying on Postgres-only work lists.")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
