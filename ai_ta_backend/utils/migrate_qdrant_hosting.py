import os

from qdrant_client import QdrantClient, models

source_vector_db = QdrantClient(url=os.environ['QDRANT_URL'],
                                port=6333,
                                https=False,
                                api_key=os.environ['QDRANT_API_KEY'])

destination_vector_db = QdrantClient("http://localhost", port=6333, https=False, api_key=os.environ['QDRANT_API_KEY'])

source_collection_name = os.environ['QDRANT_COLLECTION_NAME']
destination_collection_name = os.environ['QDRANT_COLLECTION_NAME']
vector_size = 1536

destination_vector_db.recreate_collection(
    collection_name=destination_collection_name,
    on_disk_payload=True, # ON DISK Payload (metadata)
    optimizers_config=models.OptimizersConfigDiff(indexing_threshold=100_000_000),
    vectors_config=models.VectorParams(
        size=vector_size,
        distance=models.Distance.COSINE,
        on_disk=True, # ON DISK Vectors
        hnsw_config=models.HnswConfigDiff(on_disk=False),  # In memory HNSW.
    ),
)

offset = None
counter = 0
batch_size = 1000

while True:
  res = source_vector_db.scroll(
      collection_name=source_collection_name,
      #   scroll_filter=models.Filter(must=[
      #       models.FieldCondition(key="course_name", match=models.MatchValue(value="cropwizard-1.5")),
      #   ]),
      limit=batch_size,
      with_payload=True,
      with_vectors=True,
      offset=offset)
  counter += batch_size
  print(f"Processing records: {counter}")
  # print(res[0])  # Print the records

  points = [models.PointStruct(
      id=point.id,
      payload=point.payload,
      vector=point.vector,
  ) for point in res[0]]

  destination_vector_db.upsert(wait=False, collection_name=destination_collection_name, points=points)

  offset = res[1]  # Get next_page_offset
  if offset is None:  # If next_page_offset is None, we've reached the last page
    break

print("Done copying over vectors. Now creating vector index (Will happen in background)")
destination_vector_db.update_collection(
    collection_name=destination_collection_name,
    optimizer_config=models.OptimizersConfigDiff(indexing_threshold=1_000),
)
print("Done. (Still indexing in background)")
