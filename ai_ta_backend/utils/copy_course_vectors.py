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

def copy_course_vectors(source_course, destination_course, retry_failed=False):
    load_dotenv()
    try:
        client = QdrantClient(
            url=os.environ['QDRANT_URL'],
            port=6333,
            https=False,
            api_key=os.environ['QDRANT_API_KEY']
        )
        collection_name = os.environ['QDRANT_COLLECTION_NAME']
    except Exception as e:
        print(f"[ERROR] Failed to initialize Qdrant client: {e}")
        sys.exit(2)

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
                res = client.scroll(
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
                client.upsert(
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
                new_payload = dict(point["payload"])
                new_payload["course_name"] = destination_course
                new_id = str(uuid.uuid4())
                np = models.PointStruct(
                    id=new_id,
                    vector=point["vector"],
                    payload=new_payload
                )
                client.upsert(
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
            res = client.scroll(
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
            client.upsert(
                collection_name=collection_name,
                points=new_points,
                wait=True
            )
            total_copied += len(new_points)
            print(f"Copied {total_copied} vectors so far...")
        except Exception as e:
            print(f"[ERROR] Failed to upsert batch {batch_idx} (offset {offset}): {e}")
            failed_batches.append({"batch_idx": batch_idx, "offset": offset})
            # Optionally, try to upsert points one by one to isolate failures
            for np, orig_point in zip(new_points, points):
                try:
                    client.upsert(
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

if __name__ == "__main__":
    if len(sys.argv) not in [3, 4]:
        print("Usage: python copy_course_vectors.py <source_course> <destination_course> [--retry-failed]")
        sys.exit(1)
    source_course = sys.argv[1]
    destination_course = sys.argv[2]
    retry_failed = len(sys.argv) == 4 and sys.argv[3] == "--retry-failed"
    copy_course_vectors(source_course, destination_course, retry_failed=retry_failed)