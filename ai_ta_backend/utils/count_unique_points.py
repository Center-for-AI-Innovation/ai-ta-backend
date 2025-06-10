from qdrant_client import QdrantClient, models
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

client = QdrantClient(
    url=os.environ["QDRANT_URL"],
    api_key=os.environ["QDRANT_API_KEY"],
    port=6333,
    https=False
)

collection_name = os.environ["QDRANT_COLLECTION_NAME"]
course_name = "cropwizard-1.5"

def count_unique_points():
    unique_keys = set()
    offset = None
    
    while True:
        res = client.scroll(
            collection_name=collection_name,
            scroll_filter=models.Filter(must=[
                models.FieldCondition(key="course_name", match=models.MatchValue(value=course_name)),
            ]),
            limit=1000,
            with_payload=True,
            with_vectors=False,
            offset=offset
        )
        points, offset = res
        
        for point in points:
            rf = point.payload.get("readable_filename")
            ci = point.payload.get("chunk_index")
            if rf and ci is not None:  # Only add if both values exist
                unique_keys.add((rf, ci))
                
        if offset is None:
            break
            
    return len(unique_keys)

if __name__ == "__main__":
    count = count_unique_points()
    print(f"Unique (readable_filename, chunk_index) pairs: {count}")