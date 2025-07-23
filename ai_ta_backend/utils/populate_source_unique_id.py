import os
import uuid
import time
from dotenv import load_dotenv
from supabase import create_client
import httpx

load_dotenv()

BATCH_SIZE = 10000
COURSE_NAME = "cropwizard-1.5"
RETRY_SLEEP = 5  # seconds to sleep on connection error
BATCH_SLEEP = 1  # seconds to sleep between batches
MAX_UPDATE_RETRIES = 3


def get_supabase_client():
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables are required.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def add_uuids_in_batches():
    total_updated = 0
    while True:
        try:
            supabase = get_supabase_client()
            # Fetch a batch of documents where source_unique_id is NULL and course_name matches
            response = supabase.table("documents") \
                .select("id") \
                .eq("course_name", COURSE_NAME) \
                .is_("source_unique_id", None) \
                .limit(BATCH_SIZE) \
                .execute()
            if hasattr(response, 'error') and response.error:
                print(f"Error fetching documents: {response.error}")
                time.sleep(RETRY_SLEEP)
                continue
            rows = response.data
            if not rows:
                print(f"All documents for course_name '{COURSE_NAME}' have a UUID. Total updated: {total_updated}")
                break
            print(f"Processing batch of {len(rows)} documents...")
            for doc in rows:
                new_uuid = str(uuid.uuid4())
                for attempt in range(1, MAX_UPDATE_RETRIES + 1):
                    try:
                        update_resp = supabase.table("documents") \
                            .update({"source_unique_id": new_uuid}) \
                            .eq("id", doc["id"]) \
                            .execute()
                        if hasattr(update_resp, 'error') and update_resp.error:
                            print(f"Error updating document id {doc['id']}: {update_resp.error}")
                        break
                    except httpx.RemoteProtocolError as e:
                        print(f"RemoteProtocolError updating doc {doc['id']} (attempt {attempt}/{MAX_UPDATE_RETRIES}), retrying in {RETRY_SLEEP}s...")
                        time.sleep(RETRY_SLEEP)
                    except Exception as e:
                        print(f"Unexpected error updating doc {doc['id']} (attempt {attempt}/{MAX_UPDATE_RETRIES}): {e}")
                        time.sleep(RETRY_SLEEP)
                # If all retries fail, skip this doc and continue
            total_updated += len(rows)
            print(f"Batch updated: {len(rows)} records. Total updated so far: {total_updated}")
            time.sleep(BATCH_SLEEP)
        except httpx.RemoteProtocolError as e:
            print(f"RemoteProtocolError in batch, reconnecting and retrying batch in {RETRY_SLEEP} seconds...")
            time.sleep(RETRY_SLEEP)
            continue
        except Exception as e:
            print(f"Unexpected error in batch: {e}. Retrying in {RETRY_SLEEP} seconds...")
            time.sleep(RETRY_SLEEP)
            continue
    print("Done.")

if __name__ == "__main__":
    add_uuids_in_batches() 