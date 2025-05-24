from qdrant_client import QdrantClient
from dotenv import load_dotenv
import stamina
from minio import Minio
from pdf_to_images import pdf_to_images
from qdrant_client.http import models
import os
import torch
from PIL import Image
from colpali_engine.models import ColQwen2, ColQwen2Processor
from tqdm import tqdm

import concurrent.futures
import threading
from queue import Queue

load_dotenv()

qdrant_client = QdrantClient(
    url=os.environ['QDRANT_URL'],
    port=os.environ['QDRANT_PORT'],
    https=True,
    api_key=os.environ['QDRANT_API_KEY']
)

collection_name = "colpali"

colpali_model = ColQwen2.from_pretrained(
    "vidore/colqwen2-v1.0",
    device_map="cpu",
).eval()
colpali_processor = ColQwen2Processor.from_pretrained("vidore/colqwen2-v1.0")

def search_images_by_text(query_text, top_k=5):
    # Process and encode the text query
    with torch.no_grad():
        batch_query = colpali_processor.process_queries([query_text]).to(
            colpali_model.device
        )
        query_embedding = colpali_model(**batch_query)

    # Convert the query embedding to a list of vectors
    multivector_query = query_embedding[0].cpu().float().numpy().tolist()
    # Search in Qdrant
    search_result = qdrant_client.query_points(
        collection_name=collection_name, query=multivector_query, limit=top_k
    )

    return search_result


# Example usage
query_text = "Early-life"
results = search_images_by_text(query_text)

for result in results.points:
    print(result)


# query_text = "asthma"
# with torch.no_grad():
#     batch_query = colpali_processor.process_queries([query_text]).to(
#         colpali_model.device
#     )
#     query_embedding = colpali_model(**batch_query)

# multivector_query = query_embedding[0].cpu().float().numpy().tolist()

# search_result = qdrant_client.query_points(
#     collection_name=collection_name, query=multivector_query, limit=10, timeout=60
# )

# search_result.points