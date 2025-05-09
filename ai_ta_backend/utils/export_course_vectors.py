"""
USAGE
Export vectors for a course to a JSON file:
python export_course_vectors.py "course_name" output.json

Example:
python export_course_vectors.py "cropwizard-1.5" vectors.json

This utility exports vector embeddings and their associated metadata from Qdrant 
for a specific course. The data is exported as a JSON file containing an array of 
objects with id, vector, and payload fields.
"""
import os
import sys
from qdrant_client import QdrantClient, models
from dotenv import load_dotenv

def export_vectors_for_course(course_name, output_path=None):
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
    all_vectors = []

    while True:
        res = client.scroll(
            collection_name=collection_name,
            scroll_filter=models.Filter(must=[
                models.FieldCondition(
                    key="course_name",
                    match=models.MatchValue(value=course_name)
                ),
            ]),
            limit=batch_size,
            with_payload=True,
            with_vectors=True,
            offset=offset
        )
        points = res[0]
        for point in points:
            all_vectors.append({
                "id": point.id,
                "vector": point.vector,
                "payload": point.payload
            })
        offset = res[1]
        if offset is None:
            break

    # Output results
    import json
    if output_path:
        with open(output_path, "w") as f:
            json.dump(all_vectors, f, indent=2)
        print(f"Exported {len(all_vectors)} vectors to {output_path}")
    else:
        print(json.dumps(all_vectors, indent=2))

def export_vectors_for_course_api(course_name):
    """API-friendly wrapper for export_vectors_for_course. Returns the vectors as a list of dicts."""
    load_dotenv()
    client = QdrantClient(
        url=os.environ['QDRANT_URL'],
        port=6333,
        https=False,
        api_key=os.environ['QDRANT_API_KEY']
    )
    collection_name = os.environ['QDRANT_COLLECTION_NAME']
    offset = None
    batch_size = 1000
    all_vectors = []
    while True:
        res = client.scroll(
            collection_name=collection_name,
            scroll_filter=models.Filter(must=[
                models.FieldCondition(
                    key="course_name",
                    match=models.MatchValue(value=course_name)
                ),
            ]),
            limit=batch_size,
            with_payload=True,
            with_vectors=True,
            offset=offset
        )
        points = res[0]
        for point in points:
            all_vectors.append({
                "id": point.id,
                "vector": point.vector,
                "payload": point.payload
            })
        offset = res[1]
        if offset is None:
            break
    return all_vectors

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python export_course_vectors.py <course_name> [output_path]")
        sys.exit(1)
    course_name = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    export_vectors_for_course(course_name, output_path)