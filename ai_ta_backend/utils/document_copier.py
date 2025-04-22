#!/usr/bin/env python3
"""
document_copier.py - Copy UIUC.chat documents from one course to another in Supabase

Usage:
    python document_copier.py --source-course "COURSE1" --target-course "COURSE2" [--dry-run]

Requirements:
    - supabase-py
    - python-dotenv (optional, for loading environment variables)
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

def get_supabase_client() -> Client:
    """Create and return a Supabase client using environment variables."""
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        print("Error: SUPABASE_URL and SUPABASE_KEY environment variables must be set.")
        sys.exit(1)
    
    return create_client(supabase_url, supabase_key)

def get_documents_by_course(supabase: Client, course_name: str) -> List[Dict[Any, Any]]:
    """Fetch all documents for a specific course."""
    response = supabase.table("documents") \
                      .select("*") \
                      .eq("course_name", course_name) \
                      .execute()
    
    if hasattr(response, 'error') and response.error:
        print(f"Error fetching documents: {response.error}")
        return []
    
    return response.data

def copy_documents(supabase: Client, documents: List[Dict[Any, Any]], 
                   target_course: str, dry_run: bool = False) -> int:
    """
    Copy documents to the target course.
    Returns the number of documents copied.
    """
    count = 0
    
    for doc in documents:
        # Create a new document record with the target course
        new_doc = {
            "s3_path": doc["s3_path"],
            "readable_filename": doc["readable_filename"],
            "course_name": target_course,
            "url": doc["url"],
            "contexts": doc["contexts"],
            "base_url": doc["base_url"],
            # created_at will be set automatically by default value
        }
        
        if dry_run:
            print(f"Would copy document: {doc['readable_filename']} to {target_course}")
            count += 1
            continue
        
        try:
            # Insert the new document
            response = supabase.table("documents").insert(new_doc).execute()
            
            if hasattr(response, 'error') and response.error:
                print(f"Error copying document {doc['readable_filename']}: {response.error}")
            else:
                print(f"Copied document: {doc['readable_filename']} to {target_course}")
                count += 1
        except Exception as e:
            print(f"Exception copying document {doc['readable_filename']}: {str(e)}")
    
    return count

def main():
    parser = argparse.ArgumentParser(description="Copy documents from one course to another in Supabase")
    parser.add_argument("--source-course", required=True, help="Source course name")
    parser.add_argument("--target-course", required=True, help="Target course name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be copied without making changes")
    
    args = parser.parse_args()
    
    # Initialize Supabase client
    supabase = get_supabase_client()
    
    # Get documents from source course
    print(f"Fetching documents from course: {args.source_course}")
    source_docs = get_documents_by_course(supabase, args.source_course)
    
    if not source_docs:
        print(f"No documents found for course: {args.source_course}")
        return
    
    print(f"Found {len(source_docs)} documents in {args.source_course}")
    
    # Check if the target course exists by fetching a document
    target_docs = get_documents_by_course(supabase, args.target_course)
    
    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
    
    # Copy documents to target course
    print(f"Copying documents to {args.target_course}...")
    copied_count = copy_documents(supabase, source_docs, args.target_course, args.dry_run)
    
    print(f"Operation completed. {copied_count} documents {'would be ' if args.dry_run else ''}copied.")

if __name__ == "__main__":
    main()