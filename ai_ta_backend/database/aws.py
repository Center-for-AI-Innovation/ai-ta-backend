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

    # CropWizard-specific AWS S3 client
    self.cropwizard_s3_client = boto3.client(
        's3',
        region_name=os.environ.get('CROPWIZARD_AWS_REGION'),
        aws_access_key_id=os.environ.get('CROPWIZARD_AWS_KEY'),
        aws_secret_access_key=os.environ.get('CROPWIZARD_AWS_SECRET'),
    )

  def _select_client_for_path(self, object_or_path: str):
    """
    Select S3 client based on object path.
    Any course starting with 'cropwizard' should use CropWizard-specific AWS S3 client.
    Our convention for exports is 'courses/{course_name}/...'.
    """
    try:
      normalized = (object_or_path or '').lstrip('/')
      # Expecting paths like: courses/cropwizard.../...
      if normalized.startswith('courses/cropwizard'):
        return self.cropwizard_s3_client
    except Exception:
      pass
    return self.s3_client

  def _select_client_for_course(self, course_name: str):
    """
    Select S3 client based on course name.
    Any course name containing 'cropwizard' should use CropWizard-specific AWS S3 client.
    """
    if course_name and 'cropwizard' in course_name.lower():
      return self.cropwizard_s3_client
    return self.s3_client

  def upload_file(self, file_path: str, bucket_name: str, object_name: str, course_name: str = None):
    if course_name:
      client = self._select_client_for_course(course_name)
    else:
      client = self._select_client_for_path(object_name)
    client.upload_file(file_path, bucket_name, object_name)

  def download_file(self, object_name: str, bucket_name: str, file_path: str, course_name: str = None):
    if course_name:
      client = self._select_client_for_course(course_name)
    else:
      client = self._select_client_for_path(object_name)
    client.download_file(bucket_name, object_name, file_path)

  def delete_file(self, bucket_name: str, s3_path: str, course_name: str = None):
    if course_name:
      client = self._select_client_for_course(course_name)
    else:
      client = self._select_client_for_path(s3_path)
    return client.delete_object(Bucket=bucket_name, Key=s3_path)

  def upload_fileobj(self, fileobj, bucket_name: str, object_name: str, course_name: str = None):
    """Upload file object using course-specific client."""
    if course_name:
      client = self._select_client_for_course(course_name)
    else:
      client = self._select_client_for_path(object_name)
    client.upload_fileobj(fileobj, bucket_name, object_name)

  def download_fileobj(self, bucket_name: str, object_name: str, fileobj, course_name: str = None):
    """Download file object using course-specific client."""
    if course_name:
      client = self._select_client_for_course(course_name)
    else:
      client = self._select_client_for_path(object_name)
    client.download_fileobj(bucket_name, object_name, fileobj)

  def get_object(self, bucket_name: str, object_name: str, course_name: str = None):
    """Get object using course-specific client."""
    if course_name:
      client = self._select_client_for_course(course_name)
    else:
      client = self._select_client_for_path(object_name)
    return client.get_object(Bucket=bucket_name, Key=object_name)

  def delete_object(self, bucket_name: str, object_name: str, course_name: str = None):
    """Delete object using course-specific client."""
    if course_name:
      client = self._select_client_for_course(course_name)
    else:
      client = self._select_client_for_path(object_name)
    return client.delete_object(Bucket=bucket_name, Key=object_name)

  def generatePresignedUrl(self, object: str, bucket_name: str, s3_path: str, expiration: int = 3600, course_name: str = None):
    # generate presigned URL
    if course_name:
      client = self._select_client_for_course(course_name)
    else:
      client = self._select_client_for_path(s3_path)
    return client.generate_presigned_url('get_object',
                                         Params={
                                             'Bucket': bucket_name,
                                             'Key': s3_path
                                         },
                                         ExpiresIn=expiration)
