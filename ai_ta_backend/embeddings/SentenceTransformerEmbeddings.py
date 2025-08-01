from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance, CollectionStatus
import uuid

# ---- Config ----
MODEL_NAME = "BAAI/bge-small-en-v1.5"
MAX_TOKENS = 128
COLLECTION_NAME = "docs"
DIM = 384  # Embedding size for bge-small-en-v1.5
QDRANT_URL = "http://localhost:6333"

# ---- Load Model and Tokenizer ----
model = SentenceTransformer(MODEL_NAME)
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

# ---- Helper: Truncate Text ----
def truncate_text(text, tokenizer, max_tokens):
    tokens = tokenizer(text, truncation=True, max_length=max_tokens)
    return tokenizer.decode(tokens["input_ids"], skip_special_tokens=True)

# ---- Sample Texts ----
documents = [
    "This is a long piece of text about artificial intelligence and machine learning.",
    "Short text about biology.",
    "Another text that discusses the importance of token limits and embeddings in LLMs.",
]

# ---- Truncate + Embed ----
truncated_docs = [truncate_text(doc, tokenizer, MAX_TOKENS) for doc in documents]
vectors = model.encode(truncated_docs)

# ---- Connect to Qdrant ----
client = QdrantClient(url=QDRANT_URL)

# ---- Create Collection if Not Exists ----
if COLLECTION_NAME not in [col.name for col in client.get_collections().collections]:
    client.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=DIM, distance=Distance.COSINE)
    )

# ---- Upload Embeddings ----
points = [
    PointStruct(
        id=str(uuid.uuid4()),
        vector=vec,
        payload={"text": doc}
    )
    for vec, doc in zip(vectors, truncated_docs)
]

client.upsert(collection_name=COLLECTION_NAME, points=points)

print(f"Uploaded {len(points)} documents to Qdrant collection '{COLLECTION_NAME}'")
