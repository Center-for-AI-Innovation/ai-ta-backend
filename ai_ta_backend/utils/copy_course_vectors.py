"""
USAGE
python copy_course_vectors.py "source_course" "destination_course"
"""
import os
import sys
import uuid
from qdrant_client import QdrantClient, models
from dotenv import load_dotenv

def copy_course_vectors(source_course, destination_course):
    load_dotenv()
    # Connect to Qdrant
    client = QdrantClient(
        url=os.environ['QDRANT_URL'],
        port=6333,
        https=False,
        api_key=os.environ['QDRANT_API_KEY']
    )
    collection_name = os.environ['QDRANT_COLLECTION_NAME']

    offset = None
    batch_size = 1000
    total_copied = 0

    while True:
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
        if not points:
            break

        # Prepare new points with updated course_name and new IDs
        new_points = []
        for point in points:
            new_payload = dict(point.payload)
            new_payload["course_name"] = destination_course
            # Use a new UUID to avoid ID collision
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
        print(f"Copied {total_copied} vectors so far...")

        offset = res[1]
        if offset is None:
            break

    print(f"Done! Total vectors copied from '{source_course}' to '{destination_course}': {total_copied}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python copy_course_vectors.py <source_course> <destination_course>")
        sys.exit(1)
    source_course = sys.argv[1]
    destination_course = sys.argv[2]
    copy_course_vectors(source_course, destination_course)