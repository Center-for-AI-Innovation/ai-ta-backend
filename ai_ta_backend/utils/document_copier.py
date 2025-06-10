#!/usr/bin/env python3
"""
USAGE
python document_copier.py --source "source_course" --destination "destination_course" [--identifiers file.txt] [--id-field readable_filename]

This utility copies documents from a source course to a destination course in the Supabase database.
You can copy all documents or specify a list of document identifiers to copy.

Options:
  --source          Source course name
  --destination     Destination course name
  --identifiers     Optional path to a file containing document identifiers (one per line)
  --id-field        Field to use for document identification (default: readable_filename)
"""

import argparse
import os
import sys
from typing import List, Dict, Any
from datetime import datetime
import subprocess
import time
import random

try:
    from supabase import create_client, Client
    from dotenv import load_dotenv
except ImportError:
    print("Required packages not found. Install with: pip install supabase-py python-dotenv")
    sys.exit(1)

# Try to load environment variables from .env file if it exists
try:
    load_dotenv()
except:
    pass

def get_supabase_client(url=None, key=None) -> Client:
    """Create and return a Supabase client using provided or environment variables."""
    supabase_url = url or os.environ.get("SUPABASE_URL")
    supabase_key = key or os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        print("Error: SUPABASE_URL and SUPABASE_KEY environment variables must be set or provided as arguments.")
        sys.exit(1)
    return create_client(supabase_url, supabase_key)

def get_documents_by_course_batch(supabase: Client, course_name: str, start: int, end: int) -> List[Dict[Any, Any]]:
    """Fetch a batch of documents for a specific course using pagination."""
    response = supabase.table("documents") \
                      .select("*") \
                      .eq("course_name", course_name) \
                      .range(start, end) \
                      .execute()
    if hasattr(response, 'error') and response.error:
        print(f"Error fetching documents: {response.error}")
        return []
    return response.data

def get_documents_by_identifiers(supabase: Client, course_name: str, identifiers: list, id_field: str = "readable_filename") -> List[Dict[Any, Any]]:
    """Fetch documents for a specific course by a list of identifiers."""
    # Supabase 'in_' filter supports up to 1000 items per call
    docs = []
    for i in range(0, len(identifiers), 1000):
        batch = identifiers[i:i+1000]
        response = supabase.table("documents") \
                          .select("*") \
                          .eq("course_name", course_name) \
                          .in_(id_field, batch) \
                          .execute()
        if hasattr(response, 'error') and response.error:
            print(f"Error fetching documents: {response.error}")
            continue
        docs.extend(response.data)
    return docs

def document_exists(supabase: Client, course_name: str, doc: dict, id_field: str = "readable_filename") -> bool:
    """Check if a document with the same id_field and course_name exists in the destination."""
    response = supabase.table("documents") \
        .select("id") \
        .eq("course_name", course_name) \
        .eq(id_field, doc[id_field]) \
        .limit(1) \
        .execute()
    if hasattr(response, 'error') and response.error:
        print(f"Error checking existence for {doc.get(id_field)}: {response.error}")
        return False  # Fail open: try to insert if unsure
    return bool(response.data)

def copy_documents_batch(destination_supabase: Client, documents: List[Dict[Any, Any]], target_course: str, dry_run: bool = False, failed_docs: list = None, id_field: str = "readable_filename") -> int:
    count = 0
    for doc in documents:
        new_doc = {
            "s3_path": doc["s3_path"],
            "readable_filename": doc["readable_filename"],
            "course_name": target_course,
            "url": doc["url"],
            "contexts": doc["contexts"],
            "base_url": doc["base_url"],
        }
        # Check if document already exists in destination
        if document_exists(destination_supabase, target_course, doc, id_field):
            print(f"Skipping existing document: {doc[id_field]} in {target_course}")
            continue
        if dry_run:
            print(f"Would copy document: {doc['readable_filename']} to {target_course}")
            count += 1
            continue
        try:
            response = destination_supabase.table("documents").insert(new_doc).execute()
            if hasattr(response, 'error') and response.error:
                print(f"Error copying document {doc['readable_filename']}: {response.error}")
                if failed_docs is not None:
                    failed_docs.append(doc)
            else:
                print(f"Copied document: {doc['readable_filename']} to {target_course}")
                count += 1
        except Exception as e:
            print(f"Exception copying document {doc['readable_filename']}: {str(e)}")
            if failed_docs is not None:
                failed_docs.append(doc)
    return count

def copy_documents_api(
    source_course,
    target_course,
    dry_run=False,
    batch_size=1000,
    retry_file=None,
    id_field="readable_filename",
    source_url=None,
    source_key=None,
    destination_url=None,
    destination_key=None
):
    """API-friendly wrapper for document copy. Returns a dict with status/results."""
    try:
        source_client = get_supabase_client(source_url, source_key)
        destination_client = get_supabase_client(destination_url, destination_key)
        failed_docs = []
        total_copied = 0
        if retry_file:
            with open(retry_file, "r") as f:
                identifiers = [line.strip() for line in f if line.strip()]
            docs = get_documents_by_identifiers(source_client, source_course, identifiers, id_field)
            total_copied = copy_documents_batch(destination_client, docs, target_course, dry_run, failed_docs, id_field)
        else:
            offset = 0
            while True:
                docs = get_documents_by_course_batch(source_client, source_course, offset, offset + batch_size - 1)
                if not docs:
                    break
                copied = copy_documents_batch(destination_client, docs, target_course, dry_run, failed_docs, id_field)
                total_copied += copied
                offset += batch_size
                if len(docs) < batch_size:
                    break
        return {"status": "success", "copied": total_copied, "failed": len(failed_docs), "failed_docs": [doc[id_field] for doc in failed_docs]}
    except Exception as e:
        return {"status": "error", "error": str(e)}

def verify_connection(client: Client, course_name: str) -> bool:
    """Verify connection to Supabase by attempting to fetch a single document."""
    try:
        response = client.table("documents") \
                      .select("id") \
                      .eq("course_name", course_name) \
                      .limit(1) \
                      .execute()
        return not (hasattr(response, 'error') and response.error)
    except Exception:
        return False

def is_retryable_supabase_error(e):
    msg = str(e)
    return (
        "Web server is down" in msg or
        "Could not query the database" in msg or
        "PGRST002" in msg or
        "JSON could not be generated" in msg or
        "Error code 521" in msg
    )

def retry_with_new_client(
    client_args,  # tuple: (url, key)
    client_factory,  # function to create client
    operation,  # function that takes a client and does the work
    max_retries=10,
    retry_wait=5
):
    attempt = 0
    while attempt < max_retries:
        try:
            client = client_factory(*client_args)
            return operation(client)
        except Exception as e:
            if is_retryable_supabase_error(e):
                print(f"Supabase connection error: {e}. Re-creating client and retrying (attempt {attempt+1}/{max_retries})...")
                attempt += 1
                time.sleep(retry_wait)
                continue
            else:
                raise  # Not a retryable error
    raise RuntimeError("Max retries exceeded for Supabase operation")

def get_all_destination_ids(destination_client, course_name, id_field):
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

def main():
    parser = argparse.ArgumentParser(description="Copy documents from one course to another in Supabase")
    parser.add_argument("--source-course", required=True, help="Source course name")
    parser.add_argument("--target-course", required=True, help="Target course name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be copied without making changes")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of documents to process per batch")
    parser.add_argument("--retry-file", type=str, help="Path to file with list of document identifiers to retry")
    parser.add_argument("--id-field", type=str, default="readable_filename", help="Field to use as document identifier (default: readable_filename)")
    parser.add_argument("--source-url", type=str, help="Source Supabase URL (overrides env)")
    parser.add_argument("--source-key", type=str, help="Source Supabase Key (overrides env)")
    parser.add_argument("--destination-url", type=str, help="Destination Supabase URL (overrides env)")
    parser.add_argument("--destination-key", type=str, help="Destination Supabase Key (overrides env)")
    parser.add_argument("--start-offset", type=int, default=0, help="Start offset for batch processing")
    parser.add_argument("--end-offset", type=int, help="End offset for batch processing (exclusive)")
    args = parser.parse_args()

    # Set source credentials: prefer CLI args, then SUPABASE_*
    if not args.source_url:
        args.source_url = os.environ.get("SUPABASE_URL")
    if not args.source_key:
        args.source_key = os.environ.get("SUPABASE_KEY")

    # Set destination credentials: prefer CLI args, then CROPWIZARD_*
    if not args.destination_url:
        args.destination_url = os.environ.get("CROPWIZARD_SUPABASE_URL")
    if not args.destination_key:
        args.destination_key = os.environ.get("CROPWIZARD_SUPABASE_KEY")

    # Warn if source and destination credentials are the same
    if (args.source_url and args.destination_url and args.source_url == args.destination_url \
        and args.source_key and args.destination_key and args.source_key == args.destination_key):
        print("Warning: Source and destination Supabase credentials are identical. You are copying within the same database/account.")

    # Determine progress log file name based on offset range
    progress_log = f"progress_{args.start_offset}_{args.end_offset if args.end_offset is not None else 'end'}.log"

    def read_progress(log_file, default_offset):
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                try:
                    return int(f.read().strip())
                except Exception:
                    return default_offset
        return default_offset

    def write_progress(log_file, offset):
        with open(log_file, "w") as f:
            f.write(str(offset))

    try:
        source_client_args = (args.source_url, args.source_key)
        destination_client_args = (args.destination_url, args.destination_key)
        source_client = get_supabase_client(*source_client_args)
        destination_client = get_supabase_client(*destination_client_args)

        # Fetch all destination IDs at the start
        print(f"Fetching all destination document IDs for course '{args.target_course}'...")
        def fetch_dest_ids(client):
            return get_all_destination_ids(client, args.target_course, args.id_field)
        already_copied_ids = retry_with_new_client(destination_client_args, get_supabase_client, fetch_dest_ids)
        print(f"Found {len(already_copied_ids)} already-copied documents in destination.")

        # Verify connections
        source_connected = verify_connection(source_client, args.source_course)
        destination_connected = verify_connection(destination_client, args.target_course)

        if source_connected and destination_connected:
            print(f"Successfully connected to both source and destination databases for course: {args.source_course}")

            # Print first 10 documents from source course
            try:
                def fetch_source_docs(client):
                    return client.table("documents") \
                        .select("*") \
                        .eq("course_name", args.source_course) \
                        .limit(10) \
                        .execute()
                response = retry_with_new_client(source_client_args, get_supabase_client, fetch_source_docs)
                if hasattr(response, 'error') and response.error:
                    print("Error fetching documents:", response.error)
                else:
                    print(f"\nFirst 10 documents in source course '{args.source_course}':")
                    for doc in response.data:
                        print(f"- {doc.get('readable_filename', 'No filename')} (ID: {doc.get('id', 'No ID')})")
            except Exception as e:
                print(f"Error fetching documents from source: {str(e)}")

            # Print first 10 documents from destination course
            try:
                def fetch_dest_docs(client):
                    return client.table("documents") \
                        .select("*") \
                        .eq("course_name", args.target_course) \
                        .limit(10) \
                        .execute()
                dest_response = retry_with_new_client(destination_client_args, get_supabase_client, fetch_dest_docs)
                if hasattr(dest_response, 'error') and dest_response.error:
                    print("Error fetching destination documents:", dest_response.error)
                else:
                    print(f"\nFirst 10 documents in destination course '{args.target_course}':")
                    for doc in dest_response.data:
                        print(f"- {doc.get('readable_filename', 'No filename')} (ID: {doc.get('id', 'No ID')})")
            except Exception as e:
                print(f"Error fetching documents from destination: {str(e)}")

            failed_docs = []
            total_copied = 0

            # Determine offset range
            start_offset = args.start_offset
            end_offset = args.end_offset if args.end_offset is not None else float('inf')
            offset = read_progress(progress_log, start_offset)
            batch_size = args.batch_size
            print(f"Resuming from offset {offset} (range: {start_offset} to {end_offset})")

            if args.retry_file:
                # Load identifiers from file
                with open(args.retry_file, "r") as f:
                    identifiers = [line.strip() for line in f if line.strip()]
                print(f"Retrying {len(identifiers)} documents from {args.retry_file} using field '{args.id_field}'")
                def fetch_docs(client):
                    return get_documents_by_identifiers(client, args.source_course, identifiers, args.id_field)
                docs = retry_with_new_client(source_client_args, get_supabase_client, fetch_docs)
                print(f"Found {len(docs)} documents to retry.")
                # Filter out already-copied docs
                docs_to_copy = [doc for doc in docs if doc[args.id_field] not in already_copied_ids]
                if not docs_to_copy:
                    print("All documents in retry file already copied, skipping.")
                else:
                    def copy_docs(client):
                        return copy_documents_batch(client, docs_to_copy, args.target_course, args.dry_run, failed_docs, args.id_field)
                    total_copied = retry_with_new_client(destination_client_args, get_supabase_client, copy_docs)
            else:
                while offset < end_offset:
                    print(f"Fetching documents {offset} to {min(offset + batch_size - 1, end_offset - 1)}...")
                    def fetch_docs(client):
                        return get_documents_by_course_batch(client, args.source_course, offset, min(offset + batch_size - 1, end_offset - 1))
                    docs = retry_with_new_client(source_client_args, get_supabase_client, fetch_docs)
                    if not docs:
                        print("No more documents to process.")
                        break
                    # Filter out already-copied docs
                    docs_to_copy = [doc for doc in docs if doc[args.id_field] not in already_copied_ids]
                    if not docs_to_copy:
                        print("All documents in this batch already copied, skipping.")
                        offset += batch_size
                        write_progress(progress_log, offset)
                        continue
                    print(f"Processing batch of {len(docs_to_copy)} documents...")
                    def copy_docs(client):
                        return copy_documents_batch(client, docs_to_copy, args.target_course, args.dry_run, failed_docs, args.id_field)
                    copied = retry_with_new_client(destination_client_args, get_supabase_client, copy_docs)
                    total_copied += copied
                    offset += batch_size
                    write_progress(progress_log, offset)
                    # Add a small random delay between batches to avoid overloading the backend
                    time.sleep(random.uniform(1, 3))
                    if len(docs) < batch_size or offset >= end_offset:
                        break  # Last batch or reached end_offset

            print(f"Operation completed. {total_copied} documents {'would be ' if args.dry_run else ''}copied.")
            if failed_docs:
                print(f"{len(failed_docs)} documents failed to copy. See 'failed_documents.log' for details.")
                with open("failed_documents.log", "w") as f:
                    for doc in failed_docs:
                        f.write(f"{doc[args.id_field]}\n")
        else:
            if not source_connected:
                print(f"Failed to connect to source database for course: {args.source_course}")
            if not destination_connected:
                print(f"Failed to connect to destination database for course: {args.target_course}")
            sys.exit(1)

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()