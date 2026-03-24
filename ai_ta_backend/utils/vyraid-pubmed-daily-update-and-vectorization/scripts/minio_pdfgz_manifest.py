#!/usr/bin/env python3
"""
Create manifest for .pdf.gz objects in MinIO and optionally delete them.

Default behavior is read-only manifest generation.
"""

import argparse
import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from minio import Minio
from minio.deleteobjects import DeleteObject

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - fallback when python-dotenv missing
    load_dotenv = None


def normalize_endpoint(endpoint: str) -> str:
    endpoint = endpoint.strip().rstrip("/")
    if endpoint.startswith("http://"):
        endpoint = endpoint[len("http://") :]
    if endpoint.startswith("https://"):
        endpoint = endpoint[len("https://") :]
    return endpoint


def build_client() -> tuple[Minio, str]:
    if load_dotenv is not None:
        load_dotenv(".env.production")
    elif Path(".env.production").exists():
        for line in Path(".env.production").read_text().splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

    endpoint = normalize_endpoint(os.getenv("MINIO_ENDPOINT", ""))
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    bucket = os.getenv("MINIO_BUCKET", "pubmed")
    secure = os.getenv("MINIO_SECURE", "true").lower() in ("1", "true", "yes")

    if not endpoint or not access_key or not secret_key:
        raise RuntimeError("Missing MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY in environment")

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    return client, bucket


def batched(items: list[str], batch_size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), batch_size):
        yield items[index : index + batch_size]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate and optionally delete .pdf.gz objects from MinIO")
    parser.add_argument("--bucket", default=None, help="Override bucket name (default from env)")
    parser.add_argument("--prefix", default="", help="Optional MinIO prefix filter")
    parser.add_argument("--output", default="", help="Manifest CSV path (default auto-timestamp)")
    parser.add_argument("--delete", action="store_true", help="Delete matched objects listed in manifest")
    parser.add_argument("--yes", action="store_true", help="Confirm deletion (required with --delete)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Delete batch size (default: 1000)")
    args = parser.parse_args()

    client, env_bucket = build_client()
    bucket = args.bucket or env_bucket

    if args.delete and not args.yes:
        print("Refusing delete without --yes")
        return 2

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = args.output or f"pdfgz_manifest_{timestamp}.csv"
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    object_names: list[str] = []
    total_size = 0

    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["object_name", "size", "last_modified"])

        for obj in client.list_objects(bucket, prefix=args.prefix, recursive=True):
            if obj.object_name.endswith(".pdf.gz"):
                object_names.append(obj.object_name)
                total_size += obj.size or 0
                writer.writerow([
                    obj.object_name,
                    obj.size or 0,
                    obj.last_modified.isoformat() if obj.last_modified else "",
                ])

    print(f"bucket={bucket}")
    print(f"prefix={args.prefix or '<root>'}")
    print(f"matched_pdf_gz={len(object_names):,}")
    print(f"matched_bytes={total_size:,}")
    print(f"manifest={output_path}")

    if not args.delete:
        return 0

    deleted = 0
    errors = 0
    for chunk in batched(object_names, args.batch_size):
        delete_iter = (DeleteObject(name) for name in chunk)
        for err in client.remove_objects(bucket, delete_iter):
            errors += 1
            print(f"delete_error key={err.object_name} code={err.code} message={err.message}")
        deleted += len(chunk)
        print(f"delete_progress deleted_attempted={deleted:,} / total={len(object_names):,}")

    print(f"delete_complete attempted={deleted:,} errors={errors:,}")
    return 0 if errors == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
