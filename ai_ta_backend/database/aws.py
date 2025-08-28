import os

import boto3
from injector import inject


class AWSStorage:

  @inject
  def __init__(self):
    # Default client (MinIO/self-hosted if provided)
    self.s3_client = boto3.client(
        's3',
        endpoint_url=os.environ.get('MINIO_API_URL'),  # for Self hosted MinIO bucket
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    )

    # AWS S3 client (no endpoint override)
    self.aws_s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    )

  def _select_client_for_path(self, object_or_path: str):
    """
    Select S3 client based on object path.
    Any course starting with 'cropwizard' should use AWS S3. Our convention
    for exports is 'courses/{course_name}/...'.
    """
    try:
      normalized = (object_or_path or '').lstrip('/')
      # Expecting paths like: courses/cropwizard.../...
      if normalized.startswith('courses/cropwizard'):
        return self.aws_s3_client
    except Exception:
      pass
    return self.s3_client

  def upload_file(self, file_path: str, bucket_name: str, object_name: str):
    client = self._select_client_for_path(object_name)
    client.upload_file(file_path, bucket_name, object_name)

  def download_file(self, object_name: str, bucket_name: str, file_path: str):
    client = self._select_client_for_path(object_name)
    client.download_file(bucket_name, object_name, file_path)

  def delete_file(self, bucket_name: str, s3_path: str):
    client = self._select_client_for_path(s3_path)
    return client.delete_object(Bucket=bucket_name, Key=s3_path)

  def generatePresignedUrl(self, object: str, bucket_name: str, s3_path: str, expiration: int = 3600):
    # generate presigned URL
    client = self._select_client_for_path(s3_path)
    return client.generate_presigned_url('get_object',
                                         Params={
                                             'Bucket': bucket_name,
                                             'Key': s3_path
                                         },
                                         ExpiresIn=expiration)
