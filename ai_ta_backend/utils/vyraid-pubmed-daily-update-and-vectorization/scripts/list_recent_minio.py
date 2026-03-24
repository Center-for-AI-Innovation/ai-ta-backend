#!/usr/bin/env python3
"""
List recent objects from MinIO using a bounded sample.
"""
import os
from itertools import islice

from dotenv import load_dotenv
from minio import Minio


def main() -> int:
    load_dotenv('.env.production')

    endpoint = os.getenv('MINIO_ENDPOINT', '').replace('https://', '').replace('http://', '')
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    bucket = os.getenv('MINIO_BUCKET', 'pubmed')

    if not endpoint or not access_key or not secret_key:
        print('Missing MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY in env')
        return 1

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=True)

    # Sample up to 500 objects and show the most recent 25
    objects = list(islice(client.list_objects(bucket, recursive=True), 500))
    objects.sort(key=lambda obj: obj.last_modified)

    print(f'Total sampled objects: {len(objects)}')
    print('Most recent 25 objects:')
    for obj in objects[-25:]:
        print(f'{obj.last_modified.isoformat()}\t{obj.size}\t{obj.object_name}')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
