import os

import boto3
from injector import inject


class AWSStorage:

  @inject
  def __init__(self):
    s3_config = {}

    # If running against local MinIO
    if os.environ.get("LOCAL_MINIO") == "true" and os.environ.get("MINIO_ENDPOINT"):
        s3_config["endpoint_url"] = os.environ["MINIO_ENDPOINT"]
    
    # AWS credentials
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        s3_config["aws_access_key_id"] = os.environ["AWS_ACCESS_KEY_ID"]
        s3_config["aws_secret_access_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]

    self.s3_client = boto3.client("s3", **s3_config)

  def upload_file(self, file_path: str, bucket_name: str, object_name: str):
    self.s3_client.upload_file(file_path, bucket_name, object_name)

  def download_file(self, object_name: str, bucket_name: str, file_path: str):
    self.s3_client.download_file(bucket_name, object_name, file_path)

  def delete_file(self, bucket_name: str, s3_path: str):
    return self.s3_client.delete_object(Bucket=bucket_name, Key=s3_path)

  def generatePresignedUrl(self, object: str, bucket_name: str, s3_path: str, expiration: int = 3600):
    # generate presigned URL
    return self.s3_client.generate_presigned_url('get_object',
                                                 Params={
                                                     'Bucket': bucket_name,
                                                     'Key': s3_path
                                                 },
                                                 ExpiresIn=expiration)
