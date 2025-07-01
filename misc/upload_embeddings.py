# Iterate through, make embeddings, upload to Qdrant.

import concurrent.futures
import os
import time
import uuid

import pandas as pd
from dotenv import load_dotenv
from ollama import Client
from qdrant_client import QdrantClient, models
from qdrant_client.models import PointStruct
from tqdm import tqdm

load_dotenv()
full_start_time = time.monotonic()

ollama_client = Client(host=os.environ['OLLAMA_URL'])

qdrant_client = QdrantClient(
    url=os.environ['QDRANT_URL'],
    #   port=int(os.environ['QDRANT_PORT']),
    https=False,
    api_key=os.environ['QDRANT_API_KEY'])

try:
  collection_info = qdrant_client.get_collection(collection_name=os.environ['QDRANT_COLLECTION_NAME'])
  print(f"Collection {os.environ['QDRANT_COLLECTION_NAME']} already exists.")
except Exception as e:
  print("Collection does not exist, creating a new one.")
  # Low memory usage w/ high precision (on disk).
  qdrant_client.create_collection(
      collection_name=os.environ['QDRANT_COLLECTION_NAME'],
      vectors_config=models.VectorParams(size=768, distance=models.Distance.COSINE, on_disk=True),
      hnsw_config=models.HnswConfigDiff(on_disk=True),
  )


def process_row(row_tuple):
  index, row = row_tuple
  try:
    # Create a unique identifier based on the triplet content
    triplet = f"{row['x_name']} -- {row['relation']} -- {row['y_name']}"

    # Check if this triplet already exists in Qdrant
    # search_result = qdrant_client.scroll(
    #     collection_name=os.environ['QDRANT_COLLECTION_NAME'],
    #     scroll_filter=models.Filter(
    #         must=[
    #             models.FieldCondition(
    #                 key="triplet",
    #                 match=models.MatchValue(value=triplet)
    #             )
    #         ]
    #     ),
    #     limit=1
    # )

    # if search_result[0]:  # If we found any matches
    #     return True  # Skip this row as it's already processed

    # If not found, create new embedding
    result = ollama_client.embeddings(model='nomic-embed-text:v1.5', prompt=row['final_triplet_string'])

    payload = {'triplet': triplet, 'triplet_string': row['final_triplet_string']}

    point = PointStruct(id=str(uuid.uuid4()), vector=result["embedding"], payload=payload)

    qdrant_client.upsert(collection_name=os.environ['QDRANT_COLLECTION_NAME'], points=[point])
    return True
  except Exception as e:
    print(f"Error processing row {index}: {str(e)}")
    return False


# Read CSV in chunks to reduce memory usage
chunk_size = 100_000
total_rows = sum(1 for _ in pd.read_csv('kg_enriched_strings_minified_v3.csv', chunksize=chunk_size))
print("Loaded in the CSV")
successful_rows = 0
total_processed = 0

for chunk in tqdm(pd.read_csv('kg_enriched_strings_minified_v3.csv', chunksize=chunk_size),
                  total=total_rows // chunk_size,
                  desc="Processing chunks"):

  rows = list(chunk.iterrows())

  with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
    results = list(executor.map(process_row, rows))

  successful_rows += sum(results)
  total_processed += len(results)

  # Clear memory
  del chunk
  del rows
  del results

print(f"Successfully processed {successful_rows} out of {total_processed} rows")
print(f"⏰ Overall Runtime: {(time.monotonic() - full_start_time):.2f} seconds")
