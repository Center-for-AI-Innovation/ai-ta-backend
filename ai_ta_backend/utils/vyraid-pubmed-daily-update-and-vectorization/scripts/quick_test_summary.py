#!/usr/bin/env python3
"""
Summarize a quick pipeline run (10-article or similar).
Reads pipeline_state, Postgres, MinIO, and Qdrant using the provided env file.
"""
import argparse
import json
import os
import sys
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from minio import Minio
from qdrant_client import QdrantClient


def read_pipeline_state(state_path: str) -> dict:
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _parse_time(ts: str):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def summarize_state(state: dict) -> str:
    processed = state.get("processed_global") or state.get("processed") or state.get("processed_count")
    status = state.get("status") or state.get("state")
    started_raw = state.get("started_at") or state.get("start_time")
    finished_raw = state.get("finished_at") or state.get("end_time")
    started_dt = _parse_time(started_raw)
    finished_dt = _parse_time(finished_raw)

    lines = []
    lines.append(f"State: {status or 'unknown'}")
    if processed is not None:
        lines.append(f"Processed: {processed}")
    if started_raw:
        lines.append(f"Started: {started_raw}")
    if finished_raw:
        lines.append(f"Finished: {finished_raw}")

    # Timing / throughput if possible
    if started_dt and finished_dt and processed is not None:
        elapsed = (finished_dt - started_dt).total_seconds()
        if elapsed > 0:
            rate = processed / elapsed
            lines.append(f"Elapsed: {elapsed:.1f}s")
            lines.append(f"Rate: {rate:.2f} items/s")
    return " | ".join(lines)


def summarize_postgres(dsn: str, schema: str) -> str:
    try:
        conn = psycopg2.connect(dsn, connect_timeout=10)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {schema}.articles;")
        articles = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {schema}.xml_processing_log;")
        xml_files = cur.fetchone()[0]
        cur.execute(f"""
            SELECT xml_filename, total_pmcid_processed, processed_all_pmcid
            FROM {schema}.xml_processing_log
            ORDER BY xml_filename DESC
            LIMIT 1;
        """)
        row = cur.fetchone()
        latest = None
        if row:
            fname, total, done = row
            latest = f"latest xml: {fname} ({total} processed, {'done' if done else 'in progress'})"
        cur.close()
        conn.close()
        msg = f"Articles: {articles} | XML files tracked: {xml_files}"
        if latest:
            msg += f" | {latest}"
        return msg
    except Exception as e:
        return f"Postgres error: {e}"


def summarize_minio(endpoint: str, access_key: str, secret_key: str, bucket: str, secure: bool) -> str:
    try:
        client = Minio(endpoint.replace("https://", "").replace("http://", ""), access_key=access_key, secret_key=secret_key, secure=secure)
        objects = list(client.list_objects(bucket, recursive=True))
        count = len(objects)
        total_size = sum(o.size for o in objects)
        return f"MinIO {bucket}: {count} objects, {total_size/1024/1024:.2f} MB"
    except Exception as e:
        return f"MinIO error: {e}"


def summarize_qdrant(url: str, api_key: str, collection: str) -> str:
    try:
        raw_url = (url or "").rstrip("/")
        client = QdrantClient(url=raw_url, api_key=api_key, timeout=60, check_compatibility=False)
        info = client.get_collection(collection)
        return f"Qdrant {collection}: {info.points_count} vectors, size={info.config.params.vector_size}, metric={info.config.params.distance}"
    except Exception as e:
        return f"Qdrant error: {e}"


def main():
    ap = argparse.ArgumentParser(description="Summarize quick pipeline run metrics")
    ap.add_argument("--env-file", default="/home/dadams/pub-med-daily/.env.testing", help="Path to env file (testing or production)")
    ap.add_argument("--state", default=None, help="Path to pipeline state file (defaults to PIPELINE_STATE_LOG from env)")
    args = ap.parse_args()

    load_dotenv(args.env_file)

    # Use state file from env if not explicitly provided
    state_path = args.state or os.getenv("PIPELINE_STATE_LOG", "pipeline_state/pipeline_state.json")
    state = read_pipeline_state(state_path)
    print("\n=== PIPELINE STATE ===")
    print(f"State file: {state_path}")
    print(summarize_state(state))

    # If metrics were recorded, print them
    metrics = state.get("metrics") if isinstance(state, dict) else None
    if metrics:
        totals = metrics.get("totals", {})
        avgs = metrics.get("averages", {})
        print("\n--- Metrics (per-item averages in seconds) ---")
        print(f"oa: {avgs.get('oa', 0):.2f} | download: {avgs.get('download', 0):.2f} | upload: {avgs.get('upload', 0):.2f} | qdrant_delete: {avgs.get('qdrant_delete', 0):.2f} | vectorize: {avgs.get('vectorize', 0):.2f}")
        print("Totals (s):",
              f"oa={totals.get('oa', 0):.1f}",
              f"download={totals.get('download', 0):.1f}",
              f"upload={totals.get('upload', 0):.1f}",
              f"qdrant_delete={totals.get('qdrant_delete', 0):.1f}",
              f"vectorize={totals.get('vectorize', 0):.1f}")

    dsn = os.getenv("POSTGRES_DSN")
    schema = os.getenv("DB_SCHEMA", "vyraid_test")
    print("\n=== POSTGRES ===")
    if dsn:
        print(summarize_postgres(dsn, schema))
    else:
        print("No POSTGRES_DSN configured")

    print("\n=== MINIO ===")
    endpoint = os.getenv("MINIO_ENDPOINT", "")
    access_key = os.getenv("MINIO_ACCESS_KEY", "")
    secret_key = os.getenv("MINIO_SECRET_KEY", "")
    bucket = os.getenv("MINIO_BUCKET", "")
    secure = os.getenv("MINIO_SECURE", "True").lower() == "true"
    if endpoint and bucket:
        print(summarize_minio(endpoint, access_key, secret_key, bucket, secure))
    else:
        print("MinIO env vars missing")

    print("\n=== QDRANT ===")
    q_url = os.getenv("QDRANT_URL", "")
    q_key = os.getenv("QDRANT_API_KEY", "")
    q_collection = os.getenv("QDRANT_COLLECTION", "")
    if q_url and q_collection:
        print(summarize_qdrant(q_url, q_key, q_collection))
    else:
        print("Qdrant env vars missing")

    print("\nCompleted at", datetime.now().astimezone().isoformat())


if __name__ == "__main__":
    sys.exit(main())
