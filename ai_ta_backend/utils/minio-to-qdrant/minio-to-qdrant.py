import os
import json
import uuid
import requests
from dotenv import load_dotenv
from minio import Minio
from qdrant_client import QdrantClient, models

load_dotenv()

bucket_name = "clinical-trials"
collection_name = "clinical-trials"
embedding_url = os.environ["EMBEDDING_BASE_URL"]
log_file = "uploaded_qdrant.log"

if os.path.exists(log_file):
    with open(log_file, "r") as f:
        uploaded_files = set(line.strip() for line in f)
else:
    uploaded_files = set()

minio_client = Minio(
    endpoint=os.environ["MINIO_ENDPOINT"],
    access_key=os.environ["MINIO_ACCESS_KEY"],
    secret_key=os.environ["MINIO_SECRET_KEY"],
    secure=False
)

qdrant_client = QdrantClient(
    url=os.environ['QDRANT_URL'],
    port=int(os.environ['QDRANT_PORT']),
    https=False,
    api_key=os.environ.get('QDRANT_API_KEY')
)

#delete bucket content
# qdrant_client.delete_collection(collection_name="clinical-trials")
# print("✅ Collection deleted.")

def make_valid_point_id(object_name):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, object_name))

def get_ollama_embedding(text, embedding_url):
    response = requests.post(
        embedding_url,
        json={"model": "nomic-embed-text:v1.5", "prompt": text}
    )
    response.raise_for_status()
    return response.json()["embedding"]

print(f"Creating collection '{collection_name}' in Qdrant...")
vector_size = len(get_ollama_embedding("test", embedding_url))

qdrant_client.recreate_collection(
    collection_name=collection_name,
    on_disk_payload=True,
    optimizers_config=models.OptimizersConfigDiff(indexing_threshold=10_000),
    vectors_config=models.VectorParams(
        size=vector_size,
        distance=models.Distance.COSINE,
        on_disk=True,
        hnsw_config=models.HnswConfigDiff(on_disk=False),
    ),
)

print("Starting upload process...")

objects = minio_client.list_objects(bucket_name, recursive=True)

for obj in objects:
    if obj.object_name in uploaded_files:
        print(f"⏩ Skipping already uploaded: {obj.object_name}")
        continue

    try:
        response = minio_client.get_object(bucket_name, obj.object_name)
        content = response.read().decode("utf-8")

        try:
            data = json.loads(content)
            text = data.get("text", content)
        except json.JSONDecodeError:
            text = content

        embedding = get_ollama_embedding(text, embedding_url)
        point_id = make_valid_point_id(obj.object_name)

        identifier = obj.object_name.split("/")[-1].split(".")[0]

        url = f"https://clinicaltrials.gov/study/{identifier}"

        qdrant_client.upload_points(
            collection_name=collection_name,
            points=[
                models.PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={
                        "text": text,
                        "filename": obj.object_name,
                        "identifier": identifier,
                        "url": url
                    }
                )
            ]
        )


        with open(log_file, "a") as f:
            f.write(obj.object_name + "\n")

        print(f"✅ Uploaded: {obj.object_name}")

    except Exception as e:
        print(f"❌ Failed to process {obj.object_name}: {e}")
