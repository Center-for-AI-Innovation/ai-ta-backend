import os
import sys
from collections import Counter
from supabase import create_client
from dotenv import load_dotenv

ID_FIELD = "source_unique_id"  # or "readable_filename" if you want
SOURCE_COURSE = "cropwizard-1.5"  # <-- EDIT THIS
DEST_COURSE = "cropwizard-1.5"  # <-- EDIT THIS

# Option to save missing IDs as CSV for Supabase import
SAVE_MISSING_AS_CSV = True  # Set to True to save as CSV, False for TXT

# Load environment variables
load_dotenv()

def get_supabase_client(url, key):
    return create_client(url, key)

def get_all_ids(client, course_name, id_field="source_unique_id"):
    ids = []
    offset = 0
    batch_size = 1000
    batch_num = 0
    while True:
        response = client.table("documents") \
            .select(id_field) \
            .eq("course_name", course_name) \
            .order(id_field, desc=False) \
            .range(offset, offset + batch_size - 1) \
            .execute()
        if hasattr(response, 'error') and response.error:
            print(f"Error fetching ids: {response.error}")
            break
        batch = [doc[id_field] for doc in response.data]
        print(f"Batch {batch_num}: offset {offset}, fetched {len(batch)} records")
        if not batch:
            break
        # Check for overlap with previous batch
        if ids and batch and ids[-1] == batch[0]:
            print(f"Overlap detected at offset {offset}: {batch[0]}")
        ids.extend(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size
        batch_num += 1
    return ids

def main():
    # Set these or use environment variables
    SOURCE_URL = os.environ.get("SUPABASE_URL")
    SOURCE_KEY = os.environ.get("SUPABASE_KEY")
    DEST_URL = os.environ.get("CROPWIZARD_SUPABASE_URL")
    DEST_KEY = os.environ.get("CROPWIZARD_SUPABASE_KEY")

    if not all([SOURCE_URL, SOURCE_KEY, DEST_URL, DEST_KEY]):
        print("Missing Supabase credentials in environment variables.")
        sys.exit(1)

    source_client = get_supabase_client(SOURCE_URL, SOURCE_KEY)
    dest_client = get_supabase_client(DEST_URL, DEST_KEY)

    print(f"Fetching all IDs from source course '{SOURCE_COURSE}'...")
    source_ids_list = get_all_ids(source_client, SOURCE_COURSE, ID_FIELD)
    print(f"Found {len(source_ids_list)} IDs in source (including duplicates if any).")
    print(f"Unique IDs in source: {len(set(source_ids_list))}")

    # Check for duplicates in source
    source_id_counts = Counter(source_ids_list)
    duplicate_ids = [doc_id for doc_id, count in source_id_counts.items() if count > 1]
    print(f"Duplicate IDs in source: {len(duplicate_ids)}")
    if duplicate_ids:
        print(f"Sample duplicate IDs: {duplicate_ids[:10]}")

    # Convert to set for set operations
    source_ids = set(source_ids_list)

    print(f"Fetching all IDs from destination course '{DEST_COURSE}'...")
    dest_ids_list = get_all_ids(dest_client, DEST_COURSE, ID_FIELD)
    print(f"Found {len(dest_ids_list)} IDs in destination.")
    print(f"Unique IDs in destination: {len(set(dest_ids_list))}")
    dest_ids = set(dest_ids_list)

    # Overlap and difference analysis
    overlap = source_ids & dest_ids
    only_in_source = source_ids - dest_ids
    only_in_dest = dest_ids - source_ids

    print(f"IDs present in both: {len(overlap)}")
    print(f"IDs only in source: {len(only_in_source)}")
    print(f"IDs only in destination: {len(only_in_dest)}")

    # Write missing IDs to file
    if SAVE_MISSING_AS_CSV:
        import csv
        # Define the columns to match the destination schema (in the correct order)
        DEST_COLUMNS = [
            "id",
            "created_at",
            "s3_path",
            "readable_filename",
            "course_name",
            "url",
            "base_url",
            "source_unique_id",
        ]
        # Fetch full documents for missing IDs from source
        def fetch_docs_by_ids(client, course_name, ids, id_field):
            docs = []
            for doc_id in ids:
                try:
                    response = client.table("documents") \
                        .select(",".join(DEST_COLUMNS)) \
                        .eq("course_name", course_name) \
                        .eq(id_field, doc_id) \
                        .execute()
                    if hasattr(response, 'error') and response.error:
                        print(f"Error fetching document {doc_id}: {response.error}")
                        continue
                    docs.extend(response.data)
                except Exception as e:
                    print(f"Exception fetching document {doc_id}: {e}")
            return docs
        missing_docs = fetch_docs_by_ids(source_client, SOURCE_COURSE, only_in_source, ID_FIELD)
        with open("missing_ids.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=DEST_COLUMNS)
            writer.writeheader()
            for doc in missing_docs:
                # Ensure all columns are present, fill missing with empty string
                row = {col: doc.get(col, "") for col in DEST_COLUMNS}
                writer.writerow(row)
        print(f"Missing documents written to missing_ids.csv ({len(missing_docs)} docs)")
    else:
        with open("missing_ids.txt", "w") as f:
            for doc_id in only_in_source:
                f.write(f"{doc_id}\n")
        print(f"Missing IDs written to missing_ids.txt ({len(only_in_source)} IDs)")

if __name__ == "__main__":
    main() 