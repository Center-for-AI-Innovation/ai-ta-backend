#!/usr/bin/env python3
"""Test connectivity to all required services before running revectorize.py."""

import os
import sys
import time
import json
from urllib.parse import urlparse

from dotenv import load_dotenv

if os.path.exists('.env'):
    load_dotenv('.env')
elif os.path.exists('.env.production'):
    load_dotenv('.env.production')

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
WARN = "\033[93m!\033[0m"

results = []


def test(name: str, fn):
    t0 = time.time()
    try:
        detail = fn()
        elapsed = (time.time() - t0) * 1000
        print(f"  {PASS} {name} ({elapsed:.0f}ms) — {detail}")
        results.append((name, True))
    except Exception as e:
        elapsed = (time.time() - t0) * 1000
        print(f"  {FAIL} {name} ({elapsed:.0f}ms) — {e}")
        results.append((name, False))


# ── Ollama ────────────────────────────────────────────────────────────────
def test_ollama():
    import requests
    base = os.getenv('EMBEDDING_BASE_URL', 'http://localhost:11434/api/embeddings')
    # Derive version URL from embedding URL
    parsed = urlparse(base)
    version_url = f"{parsed.scheme}://{parsed.netloc}/api/version"

    resp = requests.get(version_url, timeout=10)
    resp.raise_for_status()
    ver = resp.json().get('version', 'unknown')

    # Test actual embedding
    resp = requests.post(
        base,
        json={"model": "nomic-embed-text:v1.5", "prompt": "connectivity test"},
        timeout=30,
    )
    resp.raise_for_status()
    emb = resp.json().get("embedding", [])
    dim = len(emb)
    if dim != 768:
        raise ValueError(f"Expected 768-dim, got {dim}")
    return f"v{ver}, {dim}-dim embeddings OK"


# ── MinIO ─────────────────────────────────────────────────────────────────
def test_minio():
    from minio import Minio

    ep = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    secure = os.getenv('MINIO_SECURE', 'false').lower() in ('1', 'true', 'yes')

    # Normalize endpoint
    if ep.startswith("http://") or ep.startswith("https://"):
        parsed = urlparse(ep)
        host = parsed.netloc or parsed.path
        if "/" in host:
            host = host.split("/")[0]
        secure = parsed.scheme == "https"
        ep = host
    ep = ep.rstrip("/")

    client = Minio(
        ep,
        access_key=os.getenv('MINIO_ACCESS_KEY', ''),
        secret_key=os.getenv('MINIO_SECRET_KEY', ''),
        secure=secure,
    )

    bucket = os.getenv('MINIO_BUCKET', 'pubmed')
    objects = list(client.list_objects(bucket, prefix="", recursive=True, max_keys=5))
    return f"bucket={bucket}, listed {len(objects)} objects"


# ── Qdrant ────────────────────────────────────────────────────────────────
def test_qdrant():
    from qdrant_client import QdrantClient

    url = os.getenv('QDRANT_URL', 'http://localhost')
    port = int(os.getenv('QDRANT_PORT', '6333'))
    api_key = os.getenv('QDRANT_API_KEY') or None
    collection = os.getenv('QDRANT_COLLECTION', 'pubmed_v2')

    kw = {"url": url, "port": port, "timeout": 30, "check_compatibility": False}
    if api_key:
        kw["api_key"] = api_key
    client = QdrantClient(**kw)

    info = client.get_collection(collection)
    points = info.points_count
    dims = info.config.params.vectors.size
    client.close()
    return f"collection={collection}, {points:,} points, {dims}-dim"


# ── Supabase / Postgres ──────────────────────────────────────────────────
def test_supabase():
    dsn = os.getenv('POSTGRES_DSN', '')
    if not dsn:
        raise ValueError("POSTGRES_DSN not set (optional — skip with --no-supabase)")

    import psycopg2
    conn = psycopg2.connect(dsn)
    try:
        schema = os.getenv('DB_SCHEMA', 'vyraid')
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.articles")
            count = cur.fetchone()[0]
        return f"{count:,} articles in {schema}.articles"
    finally:
        conn.close()


# ── Run ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\nConnectivity Tests")
    print("=" * 50)

    print("\nOllama:")
    test("Embedding endpoint", test_ollama)

    print("\nMinIO:")
    test("Object storage", test_minio)

    print("\nQdrant:")
    test("Vector database", test_qdrant)

    print("\nSupabase:")
    test("Postgres (optional)", test_supabase)

    # Summary
    print("\n" + "=" * 50)
    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    print(f"Results: {passed}/{total} passed")

    # Supabase is optional
    required = [(n, ok) for n, ok in results if "optional" not in n.lower()]
    required_pass = all(ok for _, ok in required)
    if required_pass:
        print(f"\n{PASS} All required services reachable. Ready to run revectorize.py")
    else:
        failed = [n for n, ok in required if not ok]
        print(f"\n{FAIL} Required services failed: {', '.join(failed)}")
        sys.exit(1)
