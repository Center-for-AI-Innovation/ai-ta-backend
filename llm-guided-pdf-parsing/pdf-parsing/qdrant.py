import json
import os
import sqlite3
from uuid import uuid4

from qdrant_client import QdrantClient, models

db_path = '/home/guest/ai-ta-backend/UIUC_Chat/pdf-parsing/v2-articles.db'
client = QdrantClient(url=os.environ['QDRANT_URL'],
                      port=int(os.environ['QDRANT_PORT']),
                      https=True,
                      api_key=os.environ['QDRANT_API_KEY'])


def create_qdrant(client):
  # try:
  #     client.delete_collection(collection_name="embedding")
  #     print("Collection 'embedding' deleted successfully.")
  # except Exception as e:
  #     print(f"Error deleting collection: {e}")

  try:
    collection_info = client.get_collection(collection_name="embedding")
    print("Collection 'embedding' already exists.")
  except Exception as e:
    print("Collection does not exist, creating a new one.")
    client.create_collection(
        collection_name="embedding",
        vectors_config=models.VectorParams(size=768, distance=models.Distance.COSINE),
    )
