import boto3
import os
import tempfile
import pymupdf
import json
import time
import traceback
from tqdm import tqdm
from typing import List, Dict, Set
from tempfile import NamedTemporaryFile
from dotenv import load_dotenv
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

load_dotenv()

# Set up Minio client using boto3
s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv('MINIO_API_ENDPOINT'),
    aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('MINIO_SECRET_KEY')
)

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
    # Create a new S3 client for each process to avoid sharing connections
    s3_process_client = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_API_ENDPOINT'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY')
    )
    
    temp_file_path = os.path.join(temp_dir, os.path.basename(key))
    
    try:
        # Download the PDF
        s3_process_client.download_file(bucket, key, temp_file_path)
        
        # Extract text
        result = extract_text_from_pdf(temp_file_path, key)
        
    except Exception as e:
        print(f"Error downloading {key}: {e}")
        result = {"s3_path": key, "text": "", "status": "download_error", "error": str(e)}
    
    # Clean up
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
        
    return result

def get_cache_filename(bucket, prefix):
    """Generate a consistent cache filename based on bucket and prefix"""
    # Create a clean filename by replacing invalid chars with underscores
    safe_prefix = prefix.replace('/', '_').replace('\\', '_').rstrip('_')
    return f"s3_keys_cache_{bucket}_{safe_prefix}.json"

def save_keys_to_cache(bucket, prefix, pdf_keys):
    """Save the list of PDF keys to a local cache file"""
    cache_file = get_cache_filename(bucket, prefix)
    cache_data = {
        "bucket": bucket,
        "prefix": prefix,
        "timestamp": time.time(),
        "pdf_keys": pdf_keys
    }
    
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f)
        print(f"Saved {len(pdf_keys)} keys to cache file: {cache_file}")
        return True
    except Exception as e:
        print(f"Error saving keys to cache: {e}")
        return False

def load_keys_from_cache(bucket, prefix):
    """Load the list of PDF keys from a local cache file if it exists"""
    cache_file = get_cache_filename(bucket, prefix)
    
    if not os.path.exists(cache_file):
        return None
    
    try:
        with open(cache_file, 'r') as f:
            cache_data = json.load(f)
            
        # Verify cache matches current request
        if cache_data.get("bucket") != bucket or cache_data.get("prefix") != prefix:
            print("Cache mismatch: bucket or prefix has changed")
            return None
            
        pdf_keys = cache_data.get("pdf_keys", [])
        timestamp = cache_data.get("timestamp", 0)
        
        # Calculate age of cache in hours
        cache_age_hours = (time.time() - timestamp) / 3600
        
        print(f"Loaded {len(pdf_keys)} keys from cache (age: {cache_age_hours:.2f} hours)")
        return pdf_keys
    except Exception as e:
        print(f"Error loading keys from cache: {e}")
        return None

def get_tracking_filenames(bucket, prefix):
    """Generate consistent tracking filenames based on bucket and prefix"""
    safe_prefix = prefix.replace('/', '_').replace('\\', '_').rstrip('_')
    success_file = f"processed_success_{bucket}_{safe_prefix}.txt"
    failed_file = f"processed_failed_{bucket}_{safe_prefix}.txt"
    return success_file, failed_file

def load_processed_files(bucket, prefix) -> tuple[Set[str], Set[str]]:
    """Load the sets of successfully processed and failed files"""
    success_file, failed_file = get_tracking_filenames(bucket, prefix)
    
    # Load successful files
    successful_files = set()
    if os.path.exists(success_file):
        try:
            with open(success_file, 'r') as f:
                successful_files = set(line.strip() for line in f if line.strip())
            print(f"Loaded {len(successful_files)} previously successful files")
        except Exception as e:
            print(f"Error loading successful files: {e}")
    
    # Load failed files
    failed_files = set()
    if os.path.exists(failed_file):
        try:
            with open(failed_file, 'r') as f:
                failed_files = set(line.strip() for line in f if line.strip())
            print(f"Loaded {len(failed_files)} previously failed files")
        except Exception as e:
            print(f"Error loading failed files: {e}")
    
    return successful_files, failed_files

def update_tracking_files(successful_batch: List[str], failed_batch: List[str], bucket: str, prefix: str):
    """Append newly processed files to tracking files"""
    success_file, failed_file = get_tracking_filenames(bucket, prefix)
    
    # Append successful files
    if successful_batch:
        try:
            with open(success_file, 'a') as f:
                for s3_path in successful_batch:
                    f.write(f"{s3_path}\n")
        except Exception as e:
            print(f"Error updating success tracking file: {e}")
    
    # Append failed files
    if failed_batch:
        try:
            with open(failed_file, 'a') as f:
                for s3_path in failed_batch:
                    f.write(f"{s3_path}\n")
        except Exception as e:
            print(f"Error updating failed tracking file: {e}")

def speed_test_fitz_extraction(bucket, prefix, batch_size=10_000, max_concurrent_workers=None, resume=True):
    """Speed test pymupdf text extraction with batched streaming processing and resume capability"""
    start_time = time.time()
    total_processed = 0
    
    if max_concurrent_workers is None:
        # overprovision to help saturate the network, more parallel downloads
        max_concurrent_workers = max(1, multiprocessing.cpu_count() + 4)
    
    # Create output directory for batch results
    output_dir = "extraction_results"
    os.makedirs(output_dir, exist_ok=True)
    
    # Load previously processed files if resuming
    processed_successful = set()
    processed_failed = set()
    if resume:
        processed_successful, processed_failed = load_processed_files(bucket, prefix)
        all_processed = processed_successful.union(processed_failed)
        print(f"Resuming: {len(all_processed)} files already processed ({len(processed_successful)} successful, {len(processed_failed)} failed)")
    
    # Process PDF keys in batches using pagination to avoid loading all keys at once
    continuation_token = None
    batch_num = 0
    
    print(f"Processing PDFs from bucket {bucket} with prefix {prefix}...")
    
    try:
        while True:
            batch_num += 1
            batch_start_time = time.time()
            
            # Get batch of PDF keys directly from S3 with pagination
            pdf_keys_batch = []
            
            # Accumulate keys until we reach the desired batch size
            # S3 has a limit of 1000 keys per request, so we need to make multiple requests for larger batches
            while len(pdf_keys_batch) < batch_size:
                # Use 1000 as MaxKeys (S3 limit) or remaining keys needed to reach batch_size, whichever is smaller
                max_keys = min(1000, batch_size - len(pdf_keys_batch))
                list_kwargs = {'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': max_keys}
                
                if continuation_token:
                    list_kwargs['ContinuationToken'] = continuation_token
                    
                response = s3_client.list_objects_v2(**list_kwargs)
                
                # Process the contents of the response
                if 'Contents' in response:
                    for obj in response['Contents']:
                        key = obj['Key']
                        if key.endswith('.pdf'):
                            # Skip files that have already been processed if resuming
                            if resume and (key in processed_successful or key in processed_failed):
                                continue
                            pdf_keys_batch.append(key)
                            # Break early if we've reached the batch size
                            if len(pdf_keys_batch) >= batch_size:
                                break
                
                # Update continuation token for next batch
                if response.get('IsTruncated', False):
                    continuation_token = response.get('NextContinuationToken')
                else:
                    continuation_token = None
                    break  # No more objects available
                
                # If we've gathered enough keys or there are no more objects, break the loop
                if len(pdf_keys_batch) >= batch_size or continuation_token is None:
                    break
            
            if not pdf_keys_batch:
                break  # No more PDFs to process
            
            print(f"Batch {batch_num}: Processing {len(pdf_keys_batch)} PDF files...")
            
            # Process this batch
            batch_results = []
            
            # Create a temporary directory for downloading PDFs
            with tempfile.TemporaryDirectory() as temp_dir:
                # Create a partial function with fixed arguments
                process_pdf_with_args = partial(process_pdf, bucket=bucket, temp_dir=temp_dir)
                
                # Process PDFs in parallel within this batch
                with ProcessPoolExecutor(max_workers=max_concurrent_workers) as executor:
                    # Submit batch tasks and get futures
                    futures = [executor.submit(process_pdf_with_args, key) for key in pdf_keys_batch]
                    
                    # Process results as they complete
                    for future in tqdm(futures, desc=f"Batch {batch_num}", total=len(pdf_keys_batch)):
                        result = future.result()
                        batch_results.append(result)
            
            # Write batch results to a separate JSONL file
            batch_file = os.path.join(output_dir, f"extraction_batch_{batch_num}.jsonl")
            with open(batch_file, "w", encoding="utf-8") as f:
                for result in batch_results:
                    f.write(json.dumps(result) + "\n")
            
            # Track successful and failed files for this batch
            successful_batch = []
            failed_batch = []
            
            for result in batch_results:
                s3_path = result["s3_path"]
                if result["status"] == "success":
                    successful_batch.append(s3_path)
                else:
                    failed_batch.append(s3_path)
            
            # Update tracking files
            update_tracking_files(successful_batch, failed_batch, bucket, prefix)
            
            # Update processed sets
            processed_successful.update(successful_batch)
            processed_failed.update(failed_batch)
            
            total_processed += len(batch_results)
            batch_time = time.time() - batch_start_time
            
            print(f"Batch {batch_num} completed: {len(batch_results)} PDFs in {batch_time:.2f} seconds")
            print(f"Success: {len(successful_batch)}, Failed: {len(failed_batch)}")
            print(f"Total processed so far: {total_processed} PDFs")
            
            # Exit loop if no more objects
            if not continuation_token:
                break
    
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Progress has been saved and can be resumed.")
        print(f"Successfully processed: {len(processed_successful)} PDFs")
        print(f"Failed: {len(processed_failed)} PDFs")
        print(f"Total processed: {total_processed} PDFs")
    
    except Exception as e:
        print(f"Error during batch processing: {e}")
        traceback.print_exc()
        print(f"Processed {total_processed} PDFs before error")
    
    elapsed_time = time.time() - start_time
    print(f"\nProcessed {total_processed} PDFs in {elapsed_time:.2f} seconds")
    if total_processed > 0:
        print(f"Average time per PDF: {elapsed_time/total_processed:.2f} seconds")
    else:
        print("No PDFs processed")
    print(f"Results saved to {output_dir}/ directory")
    print(f"Successfully processed: {len(processed_successful)} PDFs")
    print(f"Failed: {len(processed_failed)} PDFs")

# Example usage
dest_bucket = os.getenv('MINIO_BUCKET_NAME', 'pubmed')
dest_prefix = '' 

# Run the speed test
if __name__ == "__main__":
    speed_test_fitz_extraction(dest_bucket, dest_prefix)