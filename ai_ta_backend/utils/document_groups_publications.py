import os
import pandas as pd
import json
import requests
import supabase
from ai_ta_backend.database import aws, sql

s3_client = aws.AWSStorage()
aws_bucket = os.getenv('S3_BUCKET_NAME')
supabase_client = supabase.create_client(  # type: ignore
      supabase_url=os.environ['SUPABASE_URL'], supabase_key=os.environ['SUPABASE_API_KEY'])


def ingest_and_group():
    """
    Ingest and groups publication documents.
    Requires: article folder with pdf files and associated metadata file.
    Iterate over the metadata -
        a. Upload file to s3
        b. Create payload for ingest
        c. Call modified beam ingest w document group API
    """
    filename = "wiley_metadata.xlsx"
    df = pd.read_excel(filename)
    metadata = df.to_dict(orient="records") # list of dictionaries
    course_name = "cropwizard-1.5"
    for record in metadata:
        if record['ingested'] == 'yes':
            continue

        groups = [record['license'], record['publisher'], "Research Papers"]
        record['groups'] = groups
        print("Record: ", record)

        file_path = "wiley_papers/" + record['filename']
        local_file_path = os.path.join(os.getcwd(), file_path)  # type: ignore
        print("Local file path: ", local_file_path)
        print(os.getcwd())
        
        # check if file exists
        if not os.path.exists(local_file_path):
            print("File does not exist: ", local_file_path)
            continue

        s3_path = "courses/cropwizard-1.5/" + record['filename']
        print("S3 path: ", s3_path)

        # Upload file to s3
        s3_client.upload_file(local_file_path, aws_bucket, s3_path)  # type: ignore

        # Create payload for ingest
        ingest_payload = {
            "course_name": course_name,
            "groups": groups,
            "readable_filename": record['filename'],
            "s3_paths": s3_path,
            "base_url": "",
            "url": record['url'],
        }
        
        print("Ingest payload: ", ingest_payload)

        # call ingest API
        beam_url = "https://3xn8l.apps.beam.cloud"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Basic " + os.getenv('BEAM_AUTH_TOKEN')    # type: ignore
        }
        
        payload_json = json.dumps(ingest_payload)
        response = requests.post(beam_url, headers=headers, data=payload_json)
        if response.status_code == 200:
            print("Ingest successful")
            record['ingested'] = 'yes'
        else:
            print("Ingest failed: ", response.text)
        
        print("-------------------")
    
    # save metadata as updated excel
    df = pd.DataFrame(metadata)
    df.to_excel("wiley_metadata_updated.xlsx", index=False)

    return "success"
