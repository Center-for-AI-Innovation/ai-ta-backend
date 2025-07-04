import os
import json
import io
import time
from pathlib import Path
from dotenv import load_dotenv
from minio import Minio
from json_to_text import extract_relevant_text
from multiprocessing import Pool, Lock, Manager, current_process, Value
from datetime import timedelta

load_dotenv(override=True)

client = None

def get_minio_client():
    return Minio(
        os.environ['MINIO_ENDPOINT'],
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False,
    )

def init_worker():
    global client
    client = get_minio_client()

bucket_name = "clinical-trials"
input_dir = Path("/projects/uiucchat/ctg-data/clinical_trials_data")
state_dir = Path("/projects/uiucchat/minio-to-qdrant")
uploaded_txt_file = state_dir / "uploaded.txt"
MAX_FILES_PER_FOLDER = 10_000

state_dir.mkdir(parents=True, exist_ok=True)

if uploaded_txt_file.exists():
    with uploaded_txt_file.open("r") as f:
        uploaded_files = set(line.strip() for line in f if line.strip())
else:
    uploaded_files = set()

client_main = get_minio_client()
if not client_main.bucket_exists(bucket_name):
    client_main.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' created.")
else:
    print(f"Bucket '{bucket_name}' already exists.")

existing_files = list(client_main.list_objects(bucket_name, recursive=True))
batch_index = len(existing_files) // MAX_FILES_PER_FOLDER + 1

all_json_files = [p for p in input_dir.glob("*.json") if p.name not in uploaded_files]

def process_file(path, lock, processed_counter, start_time, total_files, batch_index):
    global client
    filename = path.name
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        text_data = extract_relevant_text(data)
        if not text_data.strip():
            print(f"[{current_process().name}] Skipped empty text: {filename}")
            return

        text_bytes = io.BytesIO(text_data.encode("utf-8"))

        batch_folder = f"batch_{batch_index:04d}"
        object_name = f"{batch_folder}/{path.stem}.txt"

        upload_start = time.time()
        client.put_object(
            bucket_name,
            object_name,
            data=text_bytes,
            length=len(text_data.encode("utf-8")),
            content_type="text/plain"
        )
        duration = time.time() - upload_start

        with lock:
            with uploaded_txt_file.open("a") as f:
                f.write(filename + "\n")
            processed_counter.value += 1
            elapsed = time.time() - start_time.value
            processed = processed_counter.value
            rate = elapsed / processed
            remaining = (total_files - processed) * rate
            print(
                f"[{current_process().name}] Uploaded: {object_name} ({duration:.2f}s) "
                f"| {processed}/{total_files} done | ETA: {str(timedelta(seconds=int(remaining)))}"
            )

    except Exception as e:
        print(f"[{current_process().name}] Error processing {filename}: {e}")

if __name__ == "__main__":
    manager = Manager()
    lock = manager.Lock()
    processed_counter = manager.Value('i', 0)
    start_time = manager.Value('d', time.time())
    total_files = len(all_json_files)

    with Pool(processes=64, initializer=init_worker) as pool:
        pool.starmap(
            process_file,
            [(path, lock, processed_counter, start_time, total_files, batch_index) for path in all_json_files]
        )
