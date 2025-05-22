import os
import json
import glob
import time
import requests
import traceback
from pathlib import Path
from typing import List, Dict, Any, Set
from dotenv import load_dotenv
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from dataclasses import dataclass
from tqdm import tqdm
import uuid
import re
import tempfile
import boto3
from minio import Minio
import logging
import pymupdf 

# Langchain imports
from langchain_text_splitters.character import RecursiveCharacterTextSplitter

# Qdrant imports
from qdrant_client import QdrantClient
from qdrant_client import models

# Load environment variables
load_dotenv(override=True)

# Configure only specific loggers to silence HTTP requests
logging.getLogger('qdrant_client').setLevel(logging.ERROR)
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.ERROR)

# Test mode settings
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
MAX_TEST_FILES = int(os.getenv("MAX_TEST_FILES", 2))

# Constants
SUCCESS_FILE    = "successful_qdrant_uploads.txt"
FAILED_FILE     = "failed_qdrant_uploads.txt"
COLLECTION_NAME = "ncbi_pdfs"                     # — PUBMED MOD
VECTOR_SIZE     = 768
CHUNK_SIZE      = 7_000
CHUNK_OVERLAP   = 200
BUCKET_NAME     = os.environ.get('BUCKET_NAME', 'pubmed')  # — PUBMED MOD
MAX_WORKERS     = 18

# Lock objects for thread-safe file operations
success_lock = multiprocessing.Lock()
failed_lock  = multiprocessing.Lock()

def extract_text_from_pdf(file_path, s3_path):
    """Extract text from a PDF file using pymupdf"""
    try:
        doc = pymupdf.open(file_path)
        pdf_text = ""
        for page in doc:
            text = page.get_text().encode("utf8").decode("utf8", errors='ignore')
            pdf_text += text + "\n"
        return {"s3_path": s3_path, "text": pdf_text, "status": "success"}
    except pymupdf.EmptyFileError:
        print(f"Empty PDF file: {s3_path}")
        return {"s3_path": s3_path, "text": "", "status": "empty_file"}
    except Exception as e:
        print(f"Error processing {s3_path}: {e}")
        return {"s3_path": s3_path, "text": "", "status": "error", "error": str(e)}

def process_pdf(key, bucket, temp_dir):
    """Process a single PDF file - for parallel execution"""
    minio_client = Minio(
        endpoint=os.environ['MINIO_ENDPOINT'],
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=os.environ.get('MINIO_SECURE', 'false').lower() == 'true'
    )

    temp_file_path = os.path.join(temp_dir, os.path.basename(key))
    try:
        minio_client.fget_object(bucket, key, temp_file_path)
        result = extract_text_from_pdf(temp_file_path, key)
    except Exception as e:
        print(f"Error downloading {key}: {e}")
        result = {"s3_path": key, "text": "", "status": "download_error", "error": str(e)}
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
    return result

@dataclass
class ProcessingResult:
    filename: str
    success: bool
    num_chunks: int = 0
    error_msg: str = ""

class MinioClient:
    """MinIO client wrapper for patent file operations."""
    
    def __init__(self):
        """Initialize MinIO client."""
        self.client = Minio(
            endpoint=os.environ['MINIO_ENDPOINT'],
            access_key=os.environ['MINIO_ACCESS_KEY'],
            secret_key=os.environ['MINIO_SECRET_KEY'],
            secure=os.environ.get('MINIO_SECURE', 'false').lower() == 'true'
        )
        
    def list_pubmed_pdfs(self) -> List[str]:
        """Recursively list every .pdf under bucket/<folder>/*.pdf."""
        return [
            obj.object_name
            for obj in self.client.list_objects(BUCKET_NAME, recursive=True)
            if obj.object_name.lower().endswith('.pdf')
        ]

    def download(self, object_name: str, target_path: str):
        """Download object to local path."""
        self.client.fget_object(BUCKET_NAME, object_name, target_path)

def setup_qdrant_collection():
    """Set up Qdrant collection if it doesn't exist"""
    try:
        qdrant_client = QdrantClient(
            url=os.environ['QDRANT_URL'],
            port=int(os.environ['QDRANT_PORT']),
            https=False,
            api_key=os.environ['QDRANT_API_KEY']
        )
        existing = [c.name for c in qdrant_client.get_collections().collections]
        if COLLECTION_NAME not in existing:
            qdrant_client.recreate_collection(
                collection_name=COLLECTION_NAME,
                on_disk_payload=True,
                optimizers_config=models.OptimizersConfigDiff(indexing_threshold=10_000_000),
                vectors_config=models.VectorParams(
                    size=VECTOR_SIZE,
                    distance=models.Distance.COSINE,
                    on_disk=True,
                    hnsw_config=models.HnswConfigDiff(on_disk=False),
                ),
            )
        return qdrant_client
    except Exception as e:
        print(f"Error setting up Qdrant collection: {e}")
        traceback.print_exc()
        return None

def get_embedding(text: str) -> List[float]:
    """Get embedding for text using Ollama API"""
    url = os.environ['EMBEDDING_BASE_URL']
    max_retries = 20
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json={"model":"nomic-embed-text:v1.5","prompt":text})
            resp.raise_for_status()
            return resp.json()["embedding"]
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("🚨 Embedding failed after retries")

def chunk_text(text: str) -> List[str]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", " ", ""]
    )
    return splitter.split_text(text)

def load_processed_files() -> tuple[Set[str], Set[str]]:
    successful, failed = set(), set()
    if os.path.exists(SUCCESS_FILE):
        successful = set(line.strip() for line in open(SUCCESS_FILE))
    if os.path.exists(FAILED_FILE):
        failed = set(line.strip() for line in open(FAILED_FILE))
    return successful, failed

def update_tracking_files(result: ProcessingResult):
    lock = success_lock if result.success else failed_lock
    fname = SUCCESS_FILE if result.success else FAILED_FILE
    with lock, open(fname, 'a') as f:
        f.write(result.filename + "\n")

def process_pudmed_file(object_name: str, processed_files: Set[str]) -> ProcessingResult:
    """Process a single PubMed PDF (minimal edits)."""
    # Initialize Qdrant client for upsert operations
    qdrant_client = QdrantClient(
        url=os.environ['QDRANT_URL'],
        port=int(os.environ['QDRANT_PORT']),
        https=False,
        api_key=os.environ['QDRANT_API_KEY']
    )
    if object_name in processed_files:
        return ProcessingResult(filename=object_name, success=True, num_chunks=0)

    try:
        #Download PDF locally
        with tempfile.TemporaryDirectory() as tmp:
            local_pdf = os.path.join(tmp, Path(object_name).name)
            MinioClient().download(object_name, local_pdf)

            #Open with pymupdf and chunk per page
            doc = pymupdf.open(local_pdf)
            page_chunks = []
            for page_num, page in enumerate(doc, start=1):
                raw = page.get_text().encode("utf8", errors="ignore").decode("utf8")
                for chunk in chunk_text(raw):
                    page_chunks.append((page_num, chunk))
            total_chunks = len(page_chunks)

        #Build Points with new payload fields
        points = []
        for i, (page_num, chunk) in enumerate(page_chunks):
            emb = get_embedding(chunk)
            points.append(models.PointStruct(
                id=str(uuid.uuid4()),
                vector=emb,
                payload={
                    'page_content':        chunk,
                    's3_path':             object_name,
                    'readable_filename':   Path(object_name).name,
                    'pagenumber':          page_num,
                    'chunk_index':         i,
                    'total_chunks':        total_chunks
                }
            ))
            if len(points) >= 1000:
                qdrant_client.upsert(collection_name=COLLECTION_NAME, points=points)
                points = []
        if points:
            qdrant_client.upsert(collection_name=COLLECTION_NAME, points=points)

        return ProcessingResult(filename=object_name, success=True, num_chunks=total_chunks)

    except Exception as e:
        return ProcessingResult(filename=object_name, success=False, error_msg=str(e))

def main():
    start = time.time()
    qdrant_client = setup_qdrant_collection()
    if not qdrant_client:
        return

    minio_client = MinioClient()
    all_pdfs = minio_client.list_pubmed_pdfs()

    if TEST_MODE:
        all_pdfs = all_pdfs[:MAX_TEST_FILES]
        print(f"[TEST_MODE] limiting to {len(all_pdfs)} files")
    else:
        print(f"Found {len(all_pdfs)} PDFs in bucket {BUCKET_NAME}")

    successful, failed = load_processed_files()
    processed = successful.union(failed)
    remaining = [f for f in all_pdfs if f not in successful]
    print(f"Processing {len(remaining)} new PDFs...")

    results = []
    workers = 1 if TEST_MODE else MAX_WORKERS
    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_file = {
            executor.submit(process_pudmed_file, fn, processed): fn
            for fn in remaining
        }
        for future in tqdm(as_completed(future_to_file), total=len(remaining), desc="Vectorizing"):
            res = future.result()
            update_tracking_files(res)
            results.append(res)

    suc = sum(1 for r in results if r.success)
    fai = len(results) - suc
    chunks = sum(r.num_chunks for r in results if r.success)
    elapsed = time.time() - start
    print(f"Done in {elapsed:.2f}s: {suc} succeeded, {fai} failed, {chunks} chunks.")
    for r in results:
        if not r.success:
            print(f"❌ {r.filename} → {r.error_msg}")

if __name__ == "__main__":
    main()