import os
import json
from pathlib import Path
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv(override=True)

client = Minio(
    os.environ['MINIO_ENDPOINT'],
    access_key=os.environ['MINIO_ACCESS_KEY'],
    secret_key=os.environ['MINIO_SECRET_KEY'],
    secure=False,
)

bucket_name = "clinical-trials"
local_dir = "/projects/uiucchat/ctg-data/clinical_trials_data"
state_file = "upload_state.json"

if os.path.exists(state_file):
    with open(state_file, "r") as f:
        uploaded_files = set(json.load(f))
else:
    uploaded_files = set()

if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' created.")
else:
    print(f"Bucket '{bucket_name}' already exists.")

newly_uploaded = []

for root, dirs, files in os.walk(local_dir):
    for file in files:
        local_path = os.path.join(root, file)
        relative_path = os.path.relpath(local_path, local_dir)
        minio_path = relative_path.replace("\\", "/")

        if minio_path in uploaded_files:
            print(f"Skipping already uploaded: {minio_path}")
            continue

        try:
            client.fput_object(bucket_name, minio_path, local_path)
            print(f"Uploaded: {minio_path}")
            uploaded_files.add(minio_path)
            newly_uploaded.append(minio_path)

        except S3Error as err:
            print(f"Failed to upload {minio_path}: {err}")

if newly_uploaded:
    with open(state_file, "w") as f:
        json.dump(list(uploaded_files), f, indent=2)
