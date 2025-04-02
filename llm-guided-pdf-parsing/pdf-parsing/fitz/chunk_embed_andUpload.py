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
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from tqdm import tqdm
import uuid

# Langchain imports
from langchain.text_splitter import RecursiveCharacterTextSplitter

# Qdrant imports
from qdrant_client import QdrantClient
from qdrant_client import models

# Load environment variables
load_dotenv()

# Constants
EXTRACTION_DIR = "./extraction_results"
SUCCESS_FILE = "successful_qdrant_uploads.txt"
FAILED_FILE = "failed_qdrant_uploads.txt"
COLLECTION_NAME = "pubmed_documents"
VECTOR_SIZE = 768  # Default size for Ollama embeddings
CHUNK_SIZE = 7_000
CHUNK_OVERLAP = 200
# MAX_WORKERS = max(1, multiprocessing.cpu_count())
MAX_WORKERS = 4

# Lock objects for thread-safe file operations
success_lock = multiprocessing.Lock()
failed_lock = multiprocessing.Lock()

@dataclass
class ProcessingResult:
    """Class to store processing results for tracking"""
    filename: str
    success: bool
    num_chunks: int = 0
    error_msg: str = ""

def setup_qdrant_collection():
    """Set up Qdrant collection if it doesn't exist"""
    try:
        qdrant_client = QdrantClient(
            url=os.environ['QDRANT_URL'], 
            port=int(os.environ['QDRANT_PORT']), 
            https=True, 
            api_key=os.environ['QDRANT_API_KEY']
        )
        
        # Check if collection exists
        collections = qdrant_client.get_collections().collections
        collection_exists = any(collection.name == COLLECTION_NAME for collection in collections)
        
        if not collection_exists:
            print(f"Creating collection {COLLECTION_NAME}...")
            qdrant_client.recreate_collection(
                collection_name=COLLECTION_NAME,
                on_disk_payload=True,
                optimizers_config=models.OptimizersConfigDiff(indexing_threshold=10_000_000),
                vectors_config=models.VectorParams(
                    size=VECTOR_SIZE,
                    distance=models.Distance.COSINE,
                    on_disk=True,
                    hnsw_config=models.HnswConfigDiff(on_disk=False), # Keep HNSW in memory
                ),
            )
            print(f"Collection {COLLECTION_NAME} created successfully")
        else:
            print(f"Collection {COLLECTION_NAME} already exists")
            
        return qdrant_client
    except Exception as e:
        print(f"Error setting up Qdrant collection: {e}")
        traceback.print_exc()
        return None

def get_embedding(text: str) -> List[float]:
    """Get embedding for text using Ollama API"""
    url = os.environ['EMBEDDING_BASE_URL']
    
    max_retries = 20
    retry_count = 0
    retry_delay = 0.25  # 250ms between retries
    
    while retry_count < max_retries:
        try:
            response = requests.post(
                url,
                json={
                    "model": "nomic-embed-text:v1.5",
                    "prompt": text
                }
            )
            response.raise_for_status()
            return response.json()["embedding"]
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"🚨 Error getting embedding after ALL {max_retries} retries: {e}")
                raise
            # print(f"Embedding request failed (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(retry_delay)

def chunk_text(text: str) -> List[str]:
    """Chunk text using Langchain RecursiveCharacterTextSplitter"""
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", " ", ""]
    )
    return text_splitter.split_text(text)

def load_processed_files() -> tuple[Set[str], Set[str]]:
    """Load sets of successfully processed and failed files"""
    successful_files = set()
    if os.path.exists(SUCCESS_FILE):
        with open(SUCCESS_FILE, 'r') as f:
            successful_files = set(line.strip() for line in f if line.strip())
        print(f"Loaded {len(successful_files)} previously successful files")
    
    failed_files = set()
    if os.path.exists(FAILED_FILE):
        with open(FAILED_FILE, 'r') as f:
            failed_files = set(line.strip() for line in f if line.strip())
        print(f"Loaded {len(failed_files)} previously failed files")
    
    return successful_files, failed_files

def update_tracking_files(result: ProcessingResult):
    """Update tracking files with processing result"""
    if result.success:
        with success_lock:
            with open(SUCCESS_FILE, 'a') as f:
                f.write(f"{result.filename}\n")
    else:
        with failed_lock:
            with open(FAILED_FILE, 'a') as f:
                f.write(f"{result.filename}\n")

def process_jsonl_file(jsonl_file: str, processed_files: Set[str]) -> ProcessingResult:
    """Process a single JSONL file, chunking and uploading to Qdrant"""
    # Skip if already processed
    if jsonl_file in processed_files:
        return ProcessingResult(filename=jsonl_file, success=True, num_chunks=0)
    
    try:
        # Create per-process Qdrant client
        qdrant_client = QdrantClient(
            url=os.environ['QDRANT_URL'], 
            port=int(os.environ['QDRANT_PORT']), 
            https=True, 
            api_key=os.environ['QDRANT_API_KEY']
        )
        
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        total_chunks = 0
        batch_size = 100  # Upload vectors in batches
        points = []
        
        for line in lines:
            try:
                data = json.loads(line)
                
                # Skip if status is not success
                if data.get('status') != 'success':
                    continue
                
                text = data.get('text', '')
                s3_path = data.get('s3_path', '')
                
                if not text or not s3_path:
                    continue
                
                # Chunk the text
                chunks = chunk_text(text)
                total_chunks += len(chunks)
                
                # Process each chunk
                for i, chunk in enumerate(chunks):
                    # Generate a unique ID for each chunk
                    point_id = str(uuid.uuid4())
                    
                    # Get embedding for chunk
                    embedding = get_embedding(chunk)
                    
                    # Create point for Qdrant
                    point = models.PointStruct(
                        id=point_id,
                        vector=embedding,
                        payload={
                            'text': chunk,
                            's3_path': s3_path,
                            'chunk_index': i,
                            'total_chunks': len(chunks)
                        }
                    )
                    points.append(point)
                    
                    # Upload in batches to improve performance
                    if len(points) >= batch_size:
                        qdrant_client.upsert(
                            collection_name=COLLECTION_NAME,
                            points=points
                        )
                        points = []
            
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"Error processing line in {jsonl_file}: {e}")
                continue
        
        # Upload any remaining points
        if points:
            qdrant_client.upsert(
                collection_name=COLLECTION_NAME,
                points=points
            )
        
        return ProcessingResult(
            filename=jsonl_file,
            success=True,
            num_chunks=total_chunks
        )
        
    except Exception as e:
        error_msg = f"Error processing {jsonl_file}: {e}"
        print(error_msg)
        traceback.print_exc()
        return ProcessingResult(
            filename=jsonl_file,
            success=False,
            error_msg=error_msg
        )

def main():
    """Main function to orchestrate parallel processing"""
    start_time = time.time()
    
    # Setup Qdrant collection
    qdrant_client = setup_qdrant_collection()
    if qdrant_client is None:
        print("Failed to set up Qdrant client. Exiting.")
        return
    
    # Get list of JSONL files to process
    jsonl_files = glob.glob(os.path.join(EXTRACTION_DIR, "*.jsonl"))
    print(f"Found {len(jsonl_files)} JSONL files to process")
    
    # Load previously processed files
    successful_files, failed_files = load_processed_files()
    processed_files = successful_files.union(failed_files)
    
    # Filter out already processed files
    remaining_files = [f for f in jsonl_files if f not in processed_files]
    print(f"Processing {len(remaining_files)} new files...")
    
    if not remaining_files:
        print("No new files to process. Exiting.")
        return
    
    # Process files in parallel
    results = []
    total_chunks = 0
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit tasks
        future_to_file = {
            executor.submit(process_jsonl_file, file, processed_files): file 
            for file in remaining_files
        }
        
        # Process results as they complete
        for future in tqdm(future_to_file, total=len(remaining_files), desc="Processing files"):
            result = future.result()
            results.append(result)
            
            # Update tracking files immediately
            update_tracking_files(result)
            
            if result.success:
                total_chunks += result.num_chunks
    
    # Calculate statistics
    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful
    
    elapsed_time = time.time() - start_time
    print(f"\nProcessed {len(results)} files in {elapsed_time:.2f} seconds")
    print(f"Successfully processed: {successful} files with {total_chunks} total chunks")
    print(f"Failed: {failed} files")
    
    if total_chunks > 0:
        print(f"Average processing time per chunk: {elapsed_time/total_chunks:.4f} seconds")

if __name__ == "__main__":
    main()
