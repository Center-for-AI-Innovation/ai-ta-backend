#!/usr/bin/env python3
"""
USAGE:
python delete_documents.py [--course COURSE_NAME] [--destination-url URL] [--destination-key KEY] [--id-field FIELD]

Deletes all documents from the 'documents' table in the destination Supabase database.
Since all documents are for the same course, no course filtering is needed.

Options:
  --course            Course name (optional, kept for compatibility)
  --destination-url   Destination Supabase URL (overrides env)
  --destination-key   Destination Supabase Key (overrides env)
  --id-field          Field to use as document identifier (default: id)
  --dry-run           Show what would be deleted without making changes
  --batch-size        Number of documents to delete per batch (default: 10)
  --max-retries       Maximum number of retries for failed operations (default: 3)
"""

import argparse
import os
import sys
import time
from typing import Any, Optional

try:
    from supabase import create_client, Client
    from dotenv import load_dotenv
    from postgrest.exceptions import APIError
except ImportError:
    print("Required packages not found. Install with: pip install supabase-py python-dotenv")
    sys.exit(1)

try:
    load_dotenv()
except:
    pass

def get_supabase_client(url=None, key=None) -> Any:
    supabase_url = url or os.environ.get("CROPWIZARD_SUPABASE_URL")
    supabase_key = key or os.environ.get("CROPWIZARD_SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        print("Error: CROPWIZARD_SUPABASE_URL and CROPWIZARD_SUPABASE_KEY must be set or provided as arguments.")
        sys.exit(1)
    return create_client(supabase_url, supabase_key)

def delete_documents_for_course(
    client: Any, 
    course_name: str = None, 
    id_field: str = "id", 
    dry_run: bool = False, 
    batch_size: int = 50,
    max_retries: int = 5
) -> int:
    """
    Delete all documents from the documents table (since all are for the same course).
    Conservative approach optimized for large datasets without hammering the database.
    
    Args:
        client: Supabase client
        course_name: Not used anymore, kept for compatibility
        id_field: Field to use as document identifier
        dry_run: If True, only show what would be deleted
        batch_size: Number of documents to delete per batch (default: 50 for safety)
        max_retries: Maximum number of retries for failed operations
    
    Returns:
        Total number of documents deleted
    """
    total_deleted = 0
    consecutive_failures = 0
    current_batch_size = batch_size
    
    print(f"Starting conservative deletion of all documents (batch size: {batch_size})...")
    
    if dry_run:
        # For dry run, count documents first
        try:
            count_response = client.table("documents") \
                .select("count", count="exact") \
                .execute()
            
            if hasattr(count_response, 'error') and count_response.error:
                print(f"Error counting documents: {count_response.error}")
                return 0
            
            total_count = count_response.count if hasattr(count_response, 'count') else 0
            print(f"[DRY RUN] Would delete all {total_count} documents")
            return total_count
        except Exception as e:
            print(f"Error during dry run: {e}")
            return 0
    
    # For actual deletion, use a very conservative approach
    while True:
        try:
            # Get a small batch of document IDs to delete
            response = client.table("documents") \
                .select(id_field) \
                .limit(current_batch_size) \
                .execute()
            
            if hasattr(response, 'error') and response.error:
                print(f"Error fetching documents: {response.error}")
                consecutive_failures += 1
                if consecutive_failures >= max_retries:
                    print(f"Too many consecutive failures ({consecutive_failures}). Stopping.")
                    break
                print(f"Waiting 5 seconds before retry...")
                time.sleep(5)
                continue
            
            docs = response.data
            if not docs:
                print("No more documents to delete.")
                break
            
            ids = [doc[id_field] for doc in docs]
            batch_count = len(ids)
            
            print(f"Deleting batch of {batch_count} documents...")
            
            # Delete this batch by IDs
            try:
                del_response = client.table("documents") \
                    .delete() \
                    .in_(id_field, ids) \
                    .execute()
                
                if hasattr(del_response, 'error') and del_response.error:
                    print(f"Error deleting batch: {del_response.error}")
                    consecutive_failures += 1
                    if consecutive_failures >= max_retries:
                        print(f"Too many consecutive failures ({consecutive_failures}). Stopping.")
                        break
                    print(f"Waiting 5 seconds before retry...")
                    time.sleep(5)
                    continue
                
                deleted_count = len(del_response.data) if del_response.data else batch_count
                total_deleted += deleted_count
                consecutive_failures = 0  # Reset failure count on success
                
                print(f"✓ Deleted {deleted_count} documents (Total: {total_deleted})")
                
                # If we got fewer documents than the batch size, we're done
                if batch_count < current_batch_size:
                    print("Reached end of documents.")
                    break
                
                # Conservative pause between batches to avoid overwhelming the database
                print("Waiting 2 seconds before next batch...")
                time.sleep(2)
                
            except APIError as e:
                error_msg = str(e).lower()
                if "timeout" in error_msg or "57014" in str(e):
                    consecutive_failures += 1
                    print(f"⚠ Timeout occurred (failure #{consecutive_failures}/{max_retries})")
                    
                    if consecutive_failures >= max_retries:
                        print(f"Too many consecutive timeouts. Stopping.")
                        break
                    
                    # Reduce batch size on timeout
                    old_batch_size = current_batch_size
                    current_batch_size = max(10, current_batch_size // 2)
                    print(f"Reducing batch size from {old_batch_size} to {current_batch_size}")
                    
                    print(f"Waiting 10 seconds before retry...")
                    time.sleep(10)  # Much longer wait for timeouts
                    continue
                else:
                    print(f"API Error: {e}")
                    consecutive_failures += 1
                    if consecutive_failures >= max_retries:
                        print(f"Too many consecutive failures ({consecutive_failures}). Stopping.")
                        break
                    print(f"Waiting 5 seconds before retry...")
                    time.sleep(5)
                    continue
                    
        except Exception as e:
            print(f"Unexpected error: {e}")
            consecutive_failures += 1
            if consecutive_failures >= max_retries:
                print(f"Too many consecutive failures ({consecutive_failures}). Stopping due to errors.")
                break
            print(f"Waiting 10 seconds before retry...")
            time.sleep(10)
            continue
    
    print(f"Deletion completed. Total documents deleted: {total_deleted}")
    return total_deleted

def main():
    parser = argparse.ArgumentParser(description="Delete all documents from the destination Supabase database.")
    parser.add_argument("--course", type=str, help="Course name (optional, kept for compatibility)")
    parser.add_argument("--destination-url", type=str, help="Destination Supabase URL (overrides env)")
    parser.add_argument("--destination-key", type=str, help="Destination Supabase Key (overrides env)")
    parser.add_argument("--id-field", type=str, default="id", help="Field to use as document identifier (default: id)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without making changes")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of documents to delete per batch (default: 50)")
    parser.add_argument("--max-retries", type=int, default=5, help="Maximum number of retries for failed operations (default: 5)")
    args = parser.parse_args()

    client = get_supabase_client(args.destination_url, args.destination_key)
    print(f"Deleting all documents from destination database...")
    print(f"Configuration: batch_size={args.batch_size}, max_retries={args.max_retries}")
    
    start_time = time.time()
    total = delete_documents_for_course(
        client, 
        args.course, 
        args.id_field, 
        args.dry_run, 
        args.batch_size,
        args.max_retries
    )
    end_time = time.time()
    
    if args.dry_run:
        print(f"[DRY RUN] Would delete {total} documents.")
    else:
        print(f"Deleted {total} documents in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main() 