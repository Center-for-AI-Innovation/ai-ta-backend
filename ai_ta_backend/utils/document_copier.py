#!/usr/bin/env python3
"""
USAGE
python document_copier_resumable.py --source-course "source_course" --target-course "destination_course" [--retry-file failed_documents.log] [--id-field readable_filename] [--batch-size 100] [--completed-batches-log completed_batches.log] [--failed-docs-log failed_documents.log]

This script copies documents from a source course to a destination course in Supabase, with robust resumption and retry capabilities:

1. Tracks completed batches (pages) in a log file. On resume, skips already completed batches and starts at the next uncompleted batch.
2. Logs failed documents after each batch to a log file. On retry, only those documents are processed.

Options:
  --source-course         Source course name
  --target-course         Target course name
  --dry-run               Show what would be copied without making changes
  --batch-size            Number of documents to process per batch (default: 100)
  --retry-file            Path to file with list of document identifiers to retry (one per line)
  --id-field              Field to use as document identifier (default: id)
  --source-url            Source Supabase URL (overrides env)
  --source-key            Source Supabase Key (overrides env)
  --destination-url       Destination Supabase URL (overrides env)
  --destination-key       Destination Supabase Key (overrides env)
  --completed-batches-log Path to log file for completed batches (default: completed_batches.log)
  --failed-docs-log       Path to log file for failed documents (default: failed_documents.log)

Example usage:
  python document_copier_resumable.py --source-course "math101" --target-course "math101_copy" --batch-size 200
  python document_copier_resumable.py --source-course "math101" --target-course "math101_copy" --retry-file failed_documents.log
"""

import argparse
import os
import sys
from typing import List, Dict, Any
from datetime import datetime
import time
import random
import functools

try:
    from supabase import create_client, Client
    from dotenv import load_dotenv
except ImportError:
    print("Required packages not found. Install with: pip install supabase-py python-dotenv")
    sys.exit(1)

try:
    load_dotenv()
except:
    pass

def get_supabase_client(url=None, key=None) -> 'Client':
    supabase_url = url or os.environ.get("SUPABASE_URL")
    supabase_key = key or os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        print("Error: SUPABASE_URL and SUPABASE_KEY environment variables must be set or provided as arguments.")
        sys.exit(1)
    return create_client(supabase_url, supabase_key)

def get_documents_by_course_batch(supabase: 'Client', course_name: str, start: int, end: int) -> List[Dict[Any, Any]]:
    response = supabase.table("documents") \
                      .select("*") \
                      .eq("course_name", course_name) \
                      .range(start, end) \
                      .execute()
    if hasattr(response, 'error') and response.error:
        print(f"Error fetching documents: {response.error}")
        return []
    return response.data

def get_documents_by_identifiers(supabase, course_name, identifiers, id_field="id"):
    """Fetch documents for a specific course by a list of identifiers, one at a time to avoid URL length issues."""
    docs = []
    for idx, identifier in enumerate(identifiers):
        try:
            response = supabase.table("documents") \
                              .select("*") \
                              .eq("course_name", course_name) \
                              .eq(id_field, identifier) \
                              .execute()
            if hasattr(response, 'error') and response.error:
                print(f"Error fetching document {identifier}: {response.error}")
                continue
            docs.extend(response.data)
            if idx % 100 == 0:
                print(f"Fetched {idx+1}/{len(identifiers)} documents...")
        except Exception as e:
            print(f"Exception fetching document {identifier}: {e}")
            continue
    return docs

def document_exists(supabase: 'Client', course_name: str, doc: dict, id_field: str = "source_unique_id") -> bool:
    response = supabase.table("documents") \
        .select("id") \
        .eq("course_name", course_name) \
        .eq(id_field, doc[id_field]) \
        .limit(1) \
        .execute()
    if hasattr(response, 'error') and response.error:
        print(f"Error checking existence for {doc.get(id_field)}: {response.error}")
        return False
    return bool(response.data)

def is_retryable_supabase_error(e):
    msg = str(e)
    return (
        "Web server is down" in msg or
        "Could not query the database" in msg or
        "PGRST002" in msg or
        "JSON could not be generated" in msg or
        "Error code 521" in msg or
        "timeout" in msg.lower() or
        "temporarily unavailable" in msg.lower()
    )

def copy_documents_batch(destination_supabase: 'Client', documents: List[Dict[Any, Any]], target_course: str, dry_run: bool = False, id_field: str = "source_unique_id", max_doc_retries: int = 3, backoff_base: float = 2.0, backoff_max: float = 30.0, already_copied_keys=None) -> (int, list):
    count = 0
    failed_docs = []
    if already_copied_keys is None:
        already_copied_keys = set()
    for doc in documents:
        if doc["source_unique_id"] in already_copied_keys:
            print(f"Skipping existing document by source_unique_id: {doc['source_unique_id']} in {target_course}")
            continue
        new_doc = {
            "id": doc["id"],  # Explicitly copy the id
            "s3_path": doc["s3_path"],
            "readable_filename": doc["readable_filename"],
            "course_name": target_course,
            "url": doc["url"],
            "contexts": doc["contexts"],
            "base_url": doc["base_url"],
            "source_unique_id": doc["source_unique_id"],
        }
        if dry_run:
            print(f"Would copy document: {doc['readable_filename']} to {target_course}")
            count += 1
            continue
        attempt = 0
        while attempt < max_doc_retries:
            try:
                response = destination_supabase.table("documents").insert(new_doc).execute()
                if hasattr(response, 'error') and response.error:
                    raise Exception(response.error)
                print(f"Copied document: {doc['readable_filename']} to {target_course}")
                count += 1
                already_copied_keys.add(doc["source_unique_id"])
                break
            except Exception as e:
                if is_retryable_supabase_error(e):
                    attempt += 1
                    wait_time = min(backoff_base ** attempt, backoff_max)
                    print(f"Retryable error copying document {doc['id']} (attempt {attempt}/{max_doc_retries}): {e}. Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    print(f"Non-retryable error copying document {doc['readable_filename']}: {str(e)}")
                    break
        else:
            print(f"Failed to copy document {doc['readable_filename']} after {max_doc_retries} attempts.")
            failed_docs.append(doc)
    return count, failed_docs

def verify_connection(client: 'Client', course_name: str) -> bool:
    try:
        response = client.table("documents") \
                      .select("id") \
                      .eq("course_name", course_name) \
                      .limit(1) \
                      .execute()
        return not (hasattr(response, 'error') and response.error)
    except Exception:
        return False

def get_all_destination_ids(destination_client, course_name, id_field = "source_unique_id"):
    ids = set()
    offset = 0
    batch_size = 1000
    while True:
        response = destination_client.table("documents") \
            .select(id_field) \
            .eq("course_name", course_name) \
            .range(offset, offset + batch_size - 1) \
            .execute()
        if hasattr(response, 'error') and response.error:
            print(f"Error fetching destination ids: {response.error}")
            break
        batch = [doc[id_field] for doc in response.data]
        if not batch:
            break
        ids.update(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size
    return ids

def document_exists(destination_supabase, course_name, doc):
    response = destination_supabase.table("documents") \
        .select("id") \
        .eq("course_name", course_name) \
        .eq("id", doc["id"]) \
        .eq("created_at", doc["created_at"]) \
        .eq("url", doc["url"]) \
        .limit(1) \
        .execute()
    if hasattr(response, 'error') and response.error:
        print(f"Error checking existence for composite key ({doc.get('id')}, {doc.get('created_at')}, {doc.get('url')}): {response.error}")
        return False
    return bool(response.data)

def append_completed_batch(log_file, batch_start):
    with open(log_file, "a") as f:
        f.write(f"{batch_start}\n")

def append_failed_docs(log_file, failed_docs, id_field):
    if not failed_docs:
        return
    with open(log_file, "a") as f:
        for doc in failed_docs:
            f.write(f"{doc[id_field]}\n")

def read_completed_batches(log_file):
    if not os.path.exists(log_file):
        return set()
    with open(log_file, "r") as f:
        return set(int(line.strip()) for line in f if line.strip().isdigit())

def main():
    parser = argparse.ArgumentParser(description="Copy documents from one course to another in Supabase with robust resumption and retry.")
    parser.add_argument("--source-course", required=True, help="Source course name")
    parser.add_argument("--target-course", required=True, help="Target course name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be copied without making changes")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of documents to process per batch")
    parser.add_argument("--retry-file", type=str, help="Path to file with list of document identifiers to retry")
    parser.add_argument("--id-field", type=str, default="source_unique_id", help="Field to use as document identifier (default: source_unique_id)")
    parser.add_argument("--source-url", type=str, help="Source Supabase URL (overrides env)")
    parser.add_argument("--source-key", type=str, help="Source Supabase Key (overrides env)")
    parser.add_argument("--destination-url", type=str, help="Destination Supabase URL (overrides env)")
    parser.add_argument("--destination-key", type=str, help="Destination Supabase Key (overrides env)")
    parser.add_argument("--completed-batches-log", type=str, default="completed_batches.log", help="Path to log file for completed batches")
    parser.add_argument("--failed-docs-log", type=str, default="failed_documents.log", help="Path to log file for failed documents")
    parser.add_argument("--max-retries", type=int, default=3, help="Max automatic retry attempts for failed documents at the end (default: 3)")
    parser.add_argument("--max-doc-retries", type=int, default=3, help="Max retry attempts for a single document on transient error (default: 3)")
    parser.add_argument("--backoff-base", type=float, default=2.0, help="Base for exponential backoff (default: 2.0)")
    parser.add_argument("--backoff-max", type=float, default=30.0, help="Max backoff time in seconds (default: 30.0)")
    args = parser.parse_args()

    if not args.source_url:
        args.source_url = os.environ.get("SUPABASE_URL")
    if not args.source_key:
        args.source_key = os.environ.get("SUPABASE_KEY")
    if not args.destination_url:
        args.destination_url = os.environ.get("CROPWIZARD_SUPABASE_URL")
    if not args.destination_key:
        args.destination_key = os.environ.get("CROPWIZARD_SUPABASE_KEY")

    source_client = get_supabase_client(args.source_url, args.source_key)
    destination_client = get_supabase_client(args.destination_url, args.destination_key)

    # Fetch all destination composite keys at the start
    print(f"Fetching all destination document composite keys for course '{args.target_course}'...")
    already_copied_keys = get_all_destination_ids(destination_client, args.target_course, args.id_field)
    print(f"Found {len(already_copied_keys)} already-copied documents in destination (by {args.id_field}). (This set is cached for this run.)")

    source_connected = verify_connection(source_client, args.source_course)
    destination_connected = verify_connection(destination_client, args.target_course)

    if not (source_connected and destination_connected):
        if not source_connected:
            print(f"Failed to connect to source database for course: {args.source_course}")
        if not destination_connected:
            print(f"Failed to connect to destination database for course: {args.target_course}")
        sys.exit(1)

    if args.retry_file:
        # Retry mode: only process docs in retry file
        with open(args.retry_file, "r") as f:
            identifiers = [line.strip() for line in f if line.strip()]
        print(f"Retrying {len(identifiers)} documents from {args.retry_file} using field '{args.id_field}'")
        docs = get_documents_by_identifiers(source_client, args.source_course, identifiers, args.id_field)
        # Filter using composite key
        docs_to_copy = [doc for doc in docs if doc[args.id_field] not in already_copied_keys]
        print(f"Found {len(docs_to_copy)} documents to retry (not already copied by {args.id_field}).")
        copied, failed_docs = copy_documents_batch(destination_client, docs_to_copy, args.target_course, args.dry_run, args.id_field, args.max_doc_retries, args.backoff_base, args.backoff_max, already_copied_keys)
        append_failed_docs(args.failed_docs_log, failed_docs, args.id_field)
        # Automatic retry for failed docs in retry mode
        retry_attempt = 1
        while failed_docs and retry_attempt <= args.max_retries:
            print(f"Automatic retry of {len(failed_docs)} failed documents (attempt {retry_attempt}/{args.max_retries})...")
            # Re-fetch composite keys to avoid recopying
            already_copied_keys = get_all_destination_ids(destination_client, args.target_course, args.id_field)
            docs_to_retry = [doc for doc in failed_docs if doc[args.id_field] not in already_copied_keys]
            copied_retry, failed_docs = copy_documents_batch(destination_client, docs_to_retry, args.target_course, args.dry_run, args.id_field, args.max_doc_retries, args.backoff_base, args.backoff_max, already_copied_keys)
            append_failed_docs(args.failed_docs_log, failed_docs, args.id_field)
            retry_attempt += 1
        if failed_docs:
            print(f"{len(failed_docs)} documents still failed after {args.max_retries} automatic retries. See '{args.failed_docs_log}' for details.")
        else:
            print("All failed documents copied successfully after automatic retries.")
        print(f"Retry operation completed. {copied} documents {'would be ' if args.dry_run else ''}copied.")
        if failed_docs:
            print(f"{len(failed_docs)} documents failed to copy. See '{args.failed_docs_log}' for details.")
        return

    # Normal mode: process in batches, track completed batches
    completed_batches = read_completed_batches(args.completed_batches_log)
    batch_size = args.batch_size
    offset = 0
    total_copied = 0
    failed_docs = []
    while True:
        if offset in completed_batches:
            print(f"Batch starting at offset {offset} already completed, skipping.")
            offset += batch_size
            continue
        print(f"Fetching documents {offset} to {offset + batch_size - 1}...")
        docs = get_documents_by_course_batch(source_client, args.source_course, offset, offset + batch_size - 1)
        if not docs:
            print("No more documents to process.")
            break
        # Filter using composite key
        docs_to_copy = [doc for doc in docs if doc[args.id_field] not in already_copied_keys]
        if not docs_to_copy:
            print("All documents in this batch already copied by composite key, skipping.")
            append_completed_batch(args.completed_batches_log, offset)
            offset += batch_size
            continue
        print(f"Processing batch of {len(docs_to_copy)} documents...")
        copied, failed = copy_documents_batch(destination_client, docs_to_copy, args.target_course, args.dry_run, args.id_field, args.max_doc_retries, args.backoff_base, args.backoff_max, already_copied_keys)
        total_copied += copied
        failed_docs.extend(failed)
        append_failed_docs(args.failed_docs_log, failed, args.id_field)
        append_completed_batch(args.completed_batches_log, offset)
        offset += batch_size
        # Add a small random delay between batches to avoid overloading the backend
        time.sleep(random.uniform(1, 3))
        if len(docs) < batch_size:
            break
    # Automatic retry for failed docs at the end of all batches
    retry_attempt = 1
    while failed_docs and retry_attempt <= args.max_retries:
        print(f"Automatic retry of {len(failed_docs)} failed documents (attempt {retry_attempt}/{args.max_retries})...")
        already_copied_keys = get_all_destination_ids(destination_client, args.target_course, args.id_field)
        docs_to_retry = [doc for doc in failed_docs if doc[args.id_field] not in already_copied_keys]
        copied_retry, failed_docs = copy_documents_batch(destination_client, docs_to_retry, args.target_course, args.dry_run, args.id_field, args.max_doc_retries, args.backoff_base, args.backoff_max, already_copied_keys)
        append_failed_docs(args.failed_docs_log, failed_docs, args.id_field)
        retry_attempt += 1
    if failed_docs:
        print(f"{len(failed_docs)} documents still failed after {args.max_retries} automatic retries. See '{args.failed_docs_log}' for details.")
    else:
        print("All failed documents copied successfully after automatic retries.")
    print(f"Operation completed. {total_copied} documents {'would be ' if args.dry_run else ''}copied.")
    print(f"Completed batches logged in '{args.completed_batches_log}'. Failed documents logged in '{args.failed_docs_log}'.")

if __name__ == "__main__":
    main() 