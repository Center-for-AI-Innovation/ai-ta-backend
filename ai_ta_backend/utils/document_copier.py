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

def main():
    parser = argparse.ArgumentParser(description="Copy documents from one course to another in Supabase")
    parser.add_argument("--source-course", required=True, help="Source course name")
    parser.add_argument("--target-course", required=True, help="Target course name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be copied without making changes")
    parser.add_argument("--batch-size", type=int, default=1000, help="Number of documents to process per batch")
    parser.add_argument("--retry-file", type=str, help="Path to file with list of document identifiers to retry")
    parser.add_argument("--id-field", type=str, default="readable_filename", help="Field to use as document identifier (default: readable_filename)")
    parser.add_argument("--source-url", type=str, help="Source Supabase URL (overrides env)")
    parser.add_argument("--source-key", type=str, help="Source Supabase Key (overrides env)")
    parser.add_argument("--destination-url", type=str, help="Destination Supabase URL (overrides env)")
    parser.add_argument("--destination-key", type=str, help="Destination Supabase Key (overrides env)")
    args = parser.parse_args()

    source_client = get_supabase_client(args.source_url, args.source_key)
    destination_client = get_supabase_client(args.destination_url, args.destination_key)
    failed_docs = []
    total_copied = 0

    if args.retry_file:
        # Load identifiers from file
        with open(args.retry_file, "r") as f:
            identifiers = [line.strip() for line in f if line.strip()]
        print(f"Retrying {len(identifiers)} documents from {args.retry_file} using field '{args.id_field}'")
        docs = get_documents_by_identifiers(source_client, args.source_course, identifiers, args.id_field)
        print(f"Found {len(docs)} documents to retry.")
        total_copied = copy_documents_batch(destination_client, docs, args.target_course, args.dry_run, failed_docs, args.id_field)
    else:
        # ... existing batch loop code ...
        offset = 0
        batch_size = args.batch_size
        print(f"Starting batch copy from {args.source_course} to {args.target_course} (batch size: {batch_size})")
        while True:
            print(f"Fetching documents {offset} to {offset + batch_size - 1}...")
            docs = get_documents_by_course_batch(source_client, args.source_course, offset, offset + batch_size - 1)
            if not docs:
                print("No more documents to process.")
                break
            print(f"Processing batch of {len(docs)} documents...")
            copied = copy_documents_batch(destination_client, docs, args.target_course, args.dry_run, failed_docs, args.id_field)
            total_copied += copied
            offset += batch_size
            if len(docs) < batch_size:
                break  # Last batch

    print(f"Operation completed. {total_copied} documents {'would be ' if args.dry_run else ''}copied.")
    if failed_docs:
        print(f"{len(failed_docs)} documents failed to copy. See 'failed_documents.log' for details.")
        with open("failed_documents.log", "w") as f:
            for doc in failed_docs:
                f.write(f"{doc[args.id_field]}\n")

if __name__ == "__main__":
    main()