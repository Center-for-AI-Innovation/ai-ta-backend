# OLD DB: http://ec2-3-81-233-108.compute-1.amazonaws.com:6333/
# New DB: cropwizard:6333
import os

from qdrant_client import QdrantClient, models

prod_client = QdrantClient(url=os.environ['QDRANT_URL'], port=6333, https=False, api_key=os.environ['QDRANT_API_KEY'])

cropwizard_client = QdrantClient(url=os.environ['CROPWIZARD_QDRANT_URL'],
                                 port=6333,
                                 https=False,
                                 api_key=os.environ['QDRANT_API_KEY'])

cropwizard_collection_name = "cropwizard"
vector_size = 1536

cropwizard_client.recreate_collection(
    collection_name=cropwizard_collection_name,
    on_disk_payload=True,
    optimizers_config=models.OptimizersConfigDiff(indexing_threshold=1_000_000),
    vectors_config=models.VectorParams(
        size=vector_size,
        distance=models.Distance.COSINE,
        on_disk=True,
        hnsw_config=models.HnswConfigDiff(on_disk=True),
    ),
)

offset = None
counter = 0
while True:
  res = prod_client.scroll(
      collection_name=os.environ['QDRANT_COLLECTION_NAME'],
      scroll_filter=models.Filter(must=[
          models.FieldCondition(key="course_name", match=models.MatchValue(value="cropwizard-1.5")),
      ]),
      limit=100,
      with_payload=True,
      with_vectors=True,
      offset=offset)
  counter += 100
  print(f"Processing records: {counter}")
  # print(res[0])  # Print the records

  points = [models.PointStruct(
      id=point.id,
      payload=point.payload,
      vector=point.vector,
  ) for point in res[0]]

  cropwizard_client.upsert(collection_name=cropwizard_collection_name, points=points)

  offset = res[1]  # Get next_page_offset
  if offset is None:  # If next_page_offset is None, we've reached the last page
    break
