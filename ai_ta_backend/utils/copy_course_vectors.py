"""
USAGE:
    python copy_course_vectors.py "source_course" "destination_course"
    python copy_course_vectors.py "source_course" "destination_course" --retry-failed
"""

import os
import sys
import uuid
import json
from qdrant_client import QdrantClient, models
from dotenv import load_dotenv

FAILED_BATCHES_LOG = "failed_batches.log"
FAILED_POINTS_LOG = "failed_points.log"

def save_failed_batches(failed_batches):
    with open(FAILED_BATCHES_LOG, "w") as f:
        json.dump(failed_batches, f)

def save_failed_points(failed_points):
    with open(FAILED_POINTS_LOG, "w") as f:
        json.dump(failed_points, f)

def load_failed_batches():
    if not os.path.exists(FAILED_BATCHES_LOG):
        return []
    with open(FAILED_BATCHES_LOG, "r") as f:
        return json.load(f)

def load_failed_points():
    if not os.path.exists(FAILED_POINTS_LOG):
        return []
    with open(FAILED_POINTS_LOG, "r") as f:
        return json.load(f)

def get_qdrant_client(url=None, api_key=None):
    load_dotenv()
    qdrant_url = url or os.environ.get('QDRANT_URL')
    qdrant_api_key = api_key or os.environ.get('QDRANT_API_KEY')
    if not qdrant_url or not qdrant_api_key:
        print("Error: QDRANT_URL and QDRANT_API_KEY must be set in env or provided as arguments.")
        sys.exit(1)
    return QdrantClient(
        url=qdrant_url,
        port=6333,
        https=False,
        api_key=qdrant_api_key
    )

def vector_exists(qdrant_client, collection_name, course_name, readable_filename, chunk_index):
    """Check if a vector with the same readable_filename, chunk_index, and course_name exists in the destination collection."""
    res = qdrant_client.scroll(
        collection_name=collection_name,
        scroll_filter=models.Filter(must=[
            models.FieldCondition(key="course_name", match=models.MatchValue(value=course_name)),
            models.FieldCondition(key="readable_filename", match=models.MatchValue(value=readable_filename)),
            models.FieldCondition(key="chunk_index", match=models.MatchValue(value=chunk_index)),
        ]),
        limit=1,
        with_payload=False,
        with_vectors=False,
    )
    return bool(res[0])

def copy_course_vectors(source_course, destination_course, retry_failed=False, source_url=None, source_key=None, dest_url=None, dest_key=None):
    source_client = get_qdrant_client(source_url, source_key)
    destination_client = get_qdrant_client(dest_url, dest_key)
    collection_name = os.environ['QDRANT_COLLECTION_NAME']
    batch_size = 1000
    total_copied = 0
    failed_batches = []
    failed_points = []

    if retry_failed:
        print("Retrying failed batches and points from last run...")
        failed_batches = load_failed_batches()
        failed_points = load_failed_points()
        # Retry failed batches
        for batch_info in failed_batches:
            offset = batch_info["offset"]
            try:
                res = source_client.scroll(
                    collection_name=collection_name,
                    scroll_filter=models.Filter(must=[
                        models.FieldCondition(
                            key="course_name",
                            match=models.MatchValue(value=source_course)
                        ),
                    ]),
                    limit=batch_size,
                    with_payload=True,
                    with_vectors=True,
                    offset=offset
                )
                points = res[0]
                new_points = []
                for point in points:
                    readable_filename = point.payload.get("readable_filename")
                    chunk_index = point.payload.get("chunk_index")
                    if vector_exists(destination_client, collection_name, destination_course, readable_filename, chunk_index):
                        print(f"Skipping existing vector: {readable_filename} chunk {chunk_index} in {destination_course}")
                        continue
                    new_payload = dict(point.payload)
                    new_payload["course_name"] = destination_course
                    new_id = str(uuid.uuid4())
                    new_points.append(
                        models.PointStruct(
                            id=new_id,
                            vector=point.vector,
                            payload=new_payload
                        )
                    )
                if new_points:
                    destination_client.upsert(
                        collection_name=collection_name,
                        points=new_points,
                        wait=True
                    )
                    total_copied += len(new_points)
                print(f"Retried and copied {len(new_points)} vectors for batch offset {offset}")
            except Exception as e:
                print(f"[ERROR] Failed to retry batch at offset {offset}: {e}")
        # Retry failed points
        for point_info in failed_points:
            try:
                point = point_info["point"]
                readable_filename = point["payload"].get("readable_filename")
                chunk_index = point["payload"].get("chunk_index")
                if vector_exists(destination_client, collection_name, destination_course, readable_filename, chunk_index):
                    print(f"Skipping existing vector: {readable_filename} chunk {chunk_index} in {destination_course}")
                    continue
                new_payload = dict(point["payload"])
                new_payload["course_name"] = destination_course
                new_id = str(uuid.uuid4())
                np = models.PointStruct(
                    id=new_id,
                    vector=point["vector"],
                    payload=new_payload
                )
                destination_client.upsert(
                    collection_name=collection_name,
                    points=[np],
                    wait=True
                )
                total_copied += 1
                print(f"Retried and copied failed point {point['id']}")
            except Exception as e:
                print(f"[ERROR] Failed to retry point {point_info['point']['id']}: {e}")
        print(f"Done retrying! Total vectors copied: {total_copied}")
        return

    offset = None
    batch_idx = 0
    batch_offsets = []
    while True:
        try:
            res = source_client.scroll(
                collection_name=collection_name,
                scroll_filter=models.Filter(must=[
                    models.FieldCondition(
                        key="course_name",
                        match=models.MatchValue(value=source_course)
                    ),
                ]),
                limit=batch_size,
                with_payload=True,
                with_vectors=True,
                offset=offset
            )
        except Exception as e:
            print(f"[ERROR] Failed to scroll batch {batch_idx} (offset {offset}): {e}")
            failed_batches.append({"batch_idx": batch_idx, "offset": offset})
            break

        points = res[0]
        if not points:
            break

        new_points = []
        for point in points:
            try:
                readable_filename = point.payload.get("readable_filename")
                chunk_index = point.payload.get("chunk_index")
                if vector_exists(destination_client, collection_name, destination_course, readable_filename, chunk_index):
                    print(f"Skipping existing vector: {readable_filename} chunk {chunk_index} in {destination_course}")
                    continue
                new_payload = dict(point.payload)
                new_payload["course_name"] = destination_course
                new_id = str(uuid.uuid4())
                new_points.append(
                    models.PointStruct(
                        id=new_id,
                        vector=point.vector,
                        payload=new_payload
                    )
                )
            except Exception as e:
                print(f"[ERROR] Failed to prepare point {point.id}: {e}")
                failed_points.append({"point": {
                    "id": point.id,
                    "vector": point.vector,
                    "payload": point.payload
                }})

        try:
            if new_points:
                destination_client.upsert(
                    collection_name=collection_name,
                    points=new_points,
                    wait=True
                )
                total_copied += len(new_points)
            print(f"Copied {total_copied} vectors so far...")
        except Exception as e:
            print(f"[ERROR] Failed to upsert batch {batch_idx} (offset {offset}): {e}")
            failed_batches.append({"batch_idx": batch_idx, "offset": offset})
            for np, orig_point in zip(new_points, points):
                try:
                    destination_client.upsert(
                        collection_name=collection_name,
                        points=[np],
                        wait=True
                    )
                    total_copied += 1
                except Exception as e2:
                    print(f"[ERROR] Failed to upsert point {orig_point.id}: {e2}")
                    failed_points.append({"point": {
                        "id": orig_point.id,
                        "vector": orig_point.vector,
                        "payload": orig_point.payload
                    }})

        offset = res[1]
        if offset is None:
            break
        batch_idx += 1

    print(f"Done! Total vectors copied from '{source_course}' to '{destination_course}': {total_copied}")
    if failed_batches:
        print(f"[WARNING] Failed batches: {failed_batches}")
        save_failed_batches(failed_batches)
    if failed_points:
        print(f"[WARNING] Failed points: {failed_points}")
        save_failed_points(failed_points)
    if failed_batches or failed_points:
        sys.exit(3)

def copy_course_vectors_api(
    source_course,
    destination_course,
    retry_failed=False,
    source_url=None,
    source_key=None,
    dest_url=None,
    dest_key=None
):
    """API-friendly wrapper for copy_course_vectors. Returns a dict with status/results."""
    try:
        copy_course_vectors(
            source_course,
            destination_course,
            retry_failed=retry_failed,
            source_url=source_url,
            source_key=source_key,
            dest_url=dest_url,
            dest_key=dest_key
        )
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "error": str(e)}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Copy course vectors from one course to another in Qdrant.")
    parser.add_argument("source_course", type=str, help="Source course name")
    parser.add_argument("destination_course", type=str, help="Destination course name")
    parser.add_argument("--retry-failed", action="store_true", help="Retry failed batches/points from previous run")
    parser.add_argument("--source-url", type=str, help="Source Qdrant URL (overrides env)")
    parser.add_argument("--source-key", type=str, help="Source Qdrant API Key (overrides env)")
    parser.add_argument("--destination-url", type=str, help="Destination Qdrant URL (overrides env)")
    parser.add_argument("--destination-key", type=str, help="Destination Qdrant API Key (overrides env)")
    args = parser.parse_args()
    copy_course_vectors(
        args.source_course,
        args.destination_course,
        retry_failed=args.retry_failed,
        source_url=args.source_url,
        source_key=args.source_key,
        dest_url=args.destination_url,
        dest_key=args.destination_key
    )