"""
To deploy: beam deploy ingest.py:ingest
Use CAII gmail to auth.
"""

from typing import Any, Callable, Dict, List, Optional, Union, cast

import beam
from beam import BotContext  # To obtain task_id
from beam import QueueDepthAutoscaler  # RequestLatencyAutoscaler,

if beam.env.is_remote():
  # Only import these in the Cloud container, not when building the container.
  import asyncio
  import inspect
  import json
  import logging
  import mimetypes
  import os
  import re
  import shutil
  import subprocess
  import time
  import traceback
  import uuid
  from pathlib import Path
  from tempfile import NamedTemporaryFile

  # from typing import Any, Callable, Dict, List, Optional, Union
  import beam
  import boto3
  import fitz
  import openai
  import pdfplumber
  import pytesseract
  import sentry_sdk
  import supabase
  from bs4 import BeautifulSoup
  from git.repo import Repo
  from langchain.document_loaders import (
      Docx2txtLoader,
      GitLoader,
      PythonLoader,
      TextLoader,
      UnstructuredExcelLoader,
      UnstructuredPowerPointLoader,
  )
  from langchain.document_loaders.csv_loader import CSVLoader
  from langchain.embeddings.openai import OpenAIEmbeddings
  from langchain.schema import Document
  from langchain.text_splitter import RecursiveCharacterTextSplitter
  from langchain.vectorstores import Qdrant
  from OpenaiEmbeddings import OpenAIAPIProcessor
  from PIL import Image
  from posthog import Posthog
  from pydub import AudioSegment
  from qdrant_client import QdrantClient, models
  from qdrant_client.models import PointStruct
  from requests.exceptions import Timeout
  from supabase.client import ClientOptions

  sentry_sdk.init(
      dsn=os.getenv("SENTRY_DSN"),
      # Set traces_sample_rate to 1.0 to capture 100% of transactions for performance monitoring.
      traces_sample_rate=1.0,
      # Set profiles_sample_rate to 1.0 to profile 100% of sampled transactions.
      # We recommend adjusting this value in production.
      profiles_sample_rate=1.0,
      enable_tracing=True)

requirements = [
    "openai<1.0",
    "pandas",
    "supabase==2.5.3",
    "tiktoken==0.5.1",
    "boto3==1.28.79",
    "qdrant-client==1.7.3",
    "langchain==0.0.331",
    "posthog==3.1.0",
    "pysrt==1.1.2",
    "docx2txt==0.8",
    "pydub==0.25.1",
    "ffmpeg-python==0.2.0",
    "ffprobe==0.5",
    "ffmpeg==1.4",
    "PyMuPDF==1.23.6",
    "pytesseract==0.3.10",  # image OCR
    "openpyxl==3.1.2",  # excel
    "networkx==3.2.1",  # unused part of excel partitioning :(
    "python-pptx==0.6.23",
    "unstructured==0.15.12",
    "GitPython==3.1.40",
    "beautifulsoup4==4.12.2",
    "sentry-sdk==1.39.1",
    "pdfplumber==0.11.0",  # PDF OCR, better performance than Fitz/PyMuPDF in my Gies PDF testing.
]

image = (beam.Image(
    python_version="python3.10",
    commands=(["apt-get update && apt-get install -y ffmpeg tesseract-ocr"]),
    python_packages=requirements,
))

# autoscaler = RequestLatencyAutoscaler(desired_latency=30, max_replicas=2)
autoscaler = QueueDepthAutoscaler(tasks_per_container=300, max_containers=3)

ourSecrets = [
    "SUPABASE_URL",
    "SUPABASE_API_KEY",
    "VLADS_OPENAI_KEY",
    "REFACTORED_MATERIALS_SUPABASE_TABLE",
    "S3_BUCKET_NAME",
    "QDRANT_URL",
    "QDRANT_API_KEY",
    "QDRANT_COLLECTION_NAME",
    "OPENAI_API_TYPE",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "POSTHOG_API_KEY",
    "CROPWIZARD_QDRANT_URL",
    "CROPWIZARD_QDRANT_API_KEY",
    "CROPWIZARD_OPENAI_KEY",
    # "AZURE_OPENAI_KEY",
    # "AZURE_OPENAI_ENGINE",
    # "AZURE_OPENAI_KEY",
    # "AZURE_OPENAI_ENDPOINT",
]


def loader():
  """
  The loader function will run once for each worker that starts up. https://docs.beam.cloud/deployment/loaders
  """
  openai.api_key = os.getenv("VLADS_OPENAI_KEY")

  # vector DB
  qdrant_client = QdrantClient(
      url=os.getenv('QDRANT_URL'),
      api_key=os.getenv('QDRANT_API_KEY'),
  )

  cropwizard_qdrant_client = QdrantClient(
      url=os.getenv('CROPWIZARD_QDRANT_URL'),
      port=443,
      https=True,
      api_key=os.getenv('QDRANT_API_KEY'),
  )

  vectorstore = Qdrant(
      client=qdrant_client,
      collection_name=os.environ['QDRANT_COLLECTION_NAME'],
      embeddings=OpenAIEmbeddings(
          openai_api_type=os.environ['OPENAI_API_TYPE'],  # "openai" or "azure"
          openai_api_key=os.getenv('VLADS_OPENAI_KEY')))

  # S3
  s3_client = boto3.client(
      's3',
      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
  )

  # Create a Supabase client
  supabase_client = supabase.create_client(  # type: ignore
      supabase_url=os.environ['SUPABASE_URL'],
      supabase_key=os.environ['SUPABASE_API_KEY'],
      options=ClientOptions(postgrest_client_timeout=60,))

  # llm = AzureChatOpenAI(
  #     temperature=0,
  #     deployment_name=os.getenv('AZURE_OPENAI_ENGINE'),  #type:ignore
  #     openai_api_base=os.getenv('AZURE_OPENAI_ENDPOINT'),  #type:ignore
  #     openai_api_key=os.getenv('AZURE_OPENAI_KEY'),  #type:ignore
  #     openai_api_version=os.getenv('OPENAI_API_VERSION'),  #type:ignore
  #     openai_api_type=OPENAI_API_TYPE)

  posthog = Posthog(sync_mode=True, project_api_key=os.environ['POSTHOG_API_KEY'], host='https://app.posthog.com')

  return qdrant_client, cropwizard_qdrant_client, vectorstore, s3_client, supabase_client, posthog


# Triggers determine how your app is deployed
# @app.rest_api(
@beam.task_queue(
    name='ingest_task_queue',
    workers=4,
    cpu=1,
    memory=3_072,
    max_pending_tasks=15_000,
    # DEPRICATED, not needed -- callback_url='https://uiuc.chat/api/UIUC-api/ingestTaskCallback',
    timeout=60 * 25,
    retries=1,
    secrets=ourSecrets,
    on_start=loader,
    image=image,
    autoscaler=autoscaler)
def ingest(context, **inputs: Dict[str | List[str], Any]):
  qdrant_client, cropwizard_qdrant_client, vectorstore, s3_client, supabase_client, posthog = context.on_start_value
  course_name: List[str] | str = inputs.get('course_name', '')
  s3_paths: List[str] | str = inputs.get('s3_paths', '')
  url: List[str] | str | None = inputs.get('url', None)
  base_url: List[str] | str | None = inputs.get('base_url', None)
  readable_filename: List[str] | str = inputs.get('readable_filename', '')
  content: str | List[str] | None = inputs.get('content', None)
  doc_groups: List[str] | str = inputs.get('groups', [])

  print(
      f"In top of /ingest route. course: {course_name}, s3paths: {s3_paths}, readable_filename: {readable_filename}, base_url: {base_url}, url: {url}, content: {content}, doc_groups: {doc_groups}"
  )

  ingester = Ingest(qdrant_client, cropwizard_qdrant_client, vectorstore, s3_client, supabase_client, posthog)

  def run_ingest(course_name, s3_paths, base_url, url, readable_filename, content, groups):
    if content:
      return ingester.ingest_single_web_text(course_name, base_url, url, content, readable_filename, groups=groups)
    elif readable_filename == '':
      return ingester.bulk_ingest(course_name, s3_paths, base_url=base_url, url=url, groups=groups)
    else:
      return ingester.bulk_ingest(course_name,
                                  s3_paths,
                                  readable_filename=readable_filename,
                                  base_url=base_url,
                                  url=url,
                                  groups=groups)

  # First try
  # Full Exception is unexpected, don't bother retrying
  # If success_fail_dict has failures, then enter retry loop
  success_fail_dict = {}
  try:
    success_fail_dict = run_ingest(course_name, s3_paths, base_url, url, readable_filename, content, doc_groups)
  except Exception as e:
    # Don't bother retrying
    print("Exception in main ingest", e)
    success_fail_dict = {'failure_ingest': str(e)}
    handle_ingest_failure(supabase_client, posthog, course_name, s3_paths, readable_filename, url, base_url, e)

  # Catch failed ingest that is not unexpected exception
  if isinstance(success_fail_dict, str):
    success_fail_dict = {'failure_ingest': success_fail_dict}

  # Retry failed ingests (but not unexpected exceptions)
  if success_fail_dict.get('failure_ingest'):
    success_fail_dict = retry_ingest(supabase_client, posthog, run_ingest, course_name, s3_paths, base_url, url,
                                     readable_filename, content, doc_groups)
    if isinstance(success_fail_dict, str) or success_fail_dict.get('failure_ingest'):
      error = str(success_fail_dict if isinstance(success_fail_dict, str) else success_fail_dict['failure_ingest'])
      handle_ingest_failure(supabase_client, posthog, course_name, s3_paths, readable_filename, url, base_url, error)

  # Cleanup: Remove from docs_in_progress table
  try:
    if base_url:
      print('Removing URL-based document from in_progress')
      supabase_client.table('documents_in_progress').delete()\
          .eq('url', url)\
          .eq('base_url', base_url)\
          .execute()
    else:
      supabase_client.table('documents_in_progress').delete()\
          .eq('course_name', course_name)\
          .eq('s3_path', s3_paths)\
          .eq('readable_filename', readable_filename)\
          .execute()
  except Exception as e:
    print(f"Error cleaning up documents_in_progress: {e}")
    sentry_sdk.capture_exception(e)

  if success_fail_dict.get('success_ingest'):
    posthog.capture('distinct_id_of_the_user',
                    event='ingest_success',
                    properties={
                        'course_name': course_name,
                        's3_path': s3_paths,
                        's3_paths': s3_paths,
                        'url': url,
                        'base_url': base_url,
                        'readable_filename': readable_filename,
                        'content': content,
                        'doc_groups': doc_groups,
                    })

  print(f"Final success_fail_dict: {success_fail_dict}")
  sentry_sdk.flush(timeout=20)
  return json.dumps(success_fail_dict)


def handle_ingest_failure(supabase_client, posthog, course_name, s3_paths, readable_filename, url, base_url, error):
  document = {
      "course_name": course_name,
      "s3_path": s3_paths,
      "readable_filename": readable_filename,
      "url": url,
      "base_url": base_url,
      "error": str(error)
  }
  # Check if document already exists
  existing = supabase_client.table('documents_failed')\
      .select('*')\
      .eq('course_name', course_name)\
      .eq('s3_path', s3_paths)\
      .eq('readable_filename', readable_filename)\
      .eq('url', url)\
      .eq('base_url', base_url)\
      .execute()

  # Only insert if no matching document exists
  if not existing.data:
    supabase_client.table('documents_failed').insert(document).execute()

  posthog.capture('distinct_id_of_the_user',
                  event='ingest_failure',
                  properties={
                      'course_name': course_name,
                      's3_path': s3_paths,
                      'url': url,
                      'base_url': base_url,
                      'readable_filename': readable_filename,
                      'error': str(error)
                  })


def retry_ingest(supabase_client, posthog, run_ingest, course_name, s3_paths, base_url, url, readable_filename, content,
                 doc_groups):
  num_retries = 3
  last_error = None

  for retry_num in range(1, num_retries):
    try:
      success_fail_dict = run_ingest(course_name, s3_paths, base_url, url, readable_filename, content, doc_groups)
      if isinstance(success_fail_dict, str) or success_fail_dict.get('failure_ingest'):
        print(f"Retry attempt {retry_num}. File: {success_fail_dict}")
        last_error = success_fail_dict
        time.sleep(13 * retry_num)
      else:
        return success_fail_dict
    except Exception as e:
      print(f"Exception in ingest retry loop {retry_num}", e)
      last_error = {'failure_ingest': str(e)}
      handle_ingest_failure(supabase_client, posthog, course_name, s3_paths, readable_filename, url, base_url, e)
      time.sleep(13 * retry_num)

  return last_error or {'failure_ingest': 'All retries failed'}


class Ingest():

  def __init__(self, qdrant_client, cropwizard_qdrant_client, vectorstore, s3_client, supabase_client, posthog):
    self.qdrant_client = qdrant_client
    self.cropwizard_qdrant_client = cropwizard_qdrant_client
    self.vectorstore = vectorstore
    self.s3_client = s3_client
    self.supabase_client = supabase_client
    self.posthog = posthog

  def bulk_ingest(self, course_name: str, s3_paths: Union[str, List[str]],
                  **kwargs) -> Dict[str, None | str | Dict[str, str]]:
    """ 
    Bulk ingest a list of s3 paths into the vectorstore, and also into the supabase database.
    -> Dict[str, str | Dict[str, str]]
    """
    print('s3 path from bulk ingest', s3_paths)

    def _ingest_single(ingest_method: Callable, s3_path, *args, **kwargs):
      """Handle running an arbitrary ingest function for an individual file."""
      # RUN INGEST METHOD
      ret = ingest_method(s3_path, *args, **kwargs)
      if ret == "Success":
        success_status['success_ingest'] = str(s3_path)
      else:
        success_status['failure_ingest'] = {'s3_path': str(s3_path), 'error': str(ret)}

    # 👇👇👇👇 ADD NEW INGEST METHODS HERE 👇👇👇👇🎉
    file_ingest_methods = {
        '.html': self._ingest_html,
        '.py': self._ingest_single_py,
        '.pdf': self._ingest_single_pdf,
        '.txt': self._ingest_single_txt,
        '.md': self._ingest_single_txt,
        '.srt': self._ingest_single_srt,
        '.vtt': self._ingest_single_vtt,
        '.docx': self._ingest_single_docx,
        '.ppt': self._ingest_single_ppt,
        '.pptx': self._ingest_single_ppt,
        '.xlsx': self._ingest_single_excel,
        '.xls': self._ingest_single_excel,
        '.xlsm': self._ingest_single_excel,
        '.xlsb': self._ingest_single_excel,
        '.xltx': self._ingest_single_excel,
        '.xltm': self._ingest_single_excel,
        '.xlt': self._ingest_single_excel,
        '.xml': self._ingest_single_excel,
        '.xlam': self._ingest_single_excel,
        '.xla': self._ingest_single_excel,
        '.xlw': self._ingest_single_excel,
        '.xlr': self._ingest_single_excel,
        '.csv': self._ingest_single_csv,
        '.png': self._ingest_single_image,
        '.jpg': self._ingest_single_image,
    }

    # Ingest methods via MIME type (more general than filetype)
    mimetype_ingest_methods = {
        'video': self._ingest_single_video,
        'audio': self._ingest_single_video,
        'text': self._ingest_single_txt,
        'image': self._ingest_single_image,
    }
    # 👆👆👆👆 ADD NEW INGEST METHODhe 👆👆👆👆🎉

    print(f"Top of ingest, Course_name {course_name}. S3 paths {s3_paths}")
    success_status: Dict[str, None | str | Dict[str, str]] = {"success_ingest": None, "failure_ingest": None}
    try:
      if isinstance(s3_paths, str):
        s3_paths = [s3_paths]

      for s3_path in s3_paths:
        file_extension = Path(s3_path).suffix
        with NamedTemporaryFile(suffix=file_extension) as tmpfile:
          self.s3_client.download_fileobj(Bucket=os.environ['S3_BUCKET_NAME'], Key=s3_path, Fileobj=tmpfile)
          mime_type = str(mimetypes.guess_type(tmpfile.name, strict=False)[0])
          mime_category = mime_type.split('/')[0] if '/' in mime_type else mime_type

        if file_extension in file_ingest_methods:
          # Use specialized functions when possible, fallback to mimetype. Else raise error.
          ingest_method = file_ingest_methods[file_extension]
          _ingest_single(ingest_method, s3_path, course_name, **kwargs)
        elif mime_category in mimetype_ingest_methods:
          # fallback to MimeType
          print("mime category", mime_category)
          ingest_method = mimetype_ingest_methods[mime_category]
          _ingest_single(ingest_method, s3_path, course_name, **kwargs)
        else:
          # No supported ingest... Fallback to attempting utf-8 decoding, otherwise fail.
          try:
            self._ingest_single_txt(s3_path, course_name, **kwargs)
            success_status['success_ingest'] = s3_path
            print(f"No ingest methods -- Falling back to UTF-8 INGEST... s3_path = {s3_path}")
          except Exception as e:
            sentry_sdk.capture_exception(e)
            print(
                f"We don't have a ingest method for this filetype: {file_extension}. As a last-ditch effort, we tried to ingest the file as utf-8 text, but that failed too. File is unsupported: {s3_path}. UTF-8 ingest error: {e}"
            )
            success_status['failure_ingest'] = {
                's3_path':
                    s3_path,
                'error':
                    f"We don't have a ingest method for this filetype: {file_extension} (with generic type {mime_type}), for file: {s3_path}"
            }
            self.posthog.capture(
                'distinct_id_of_the_user',
                event='ingest_failure',
                properties={
                    'course_name':
                        course_name,
                    's3_path':
                        s3_paths,
                    'kwargs':
                        kwargs,
                    'error':
                        f"We don't have a ingest method for this filetype: {file_extension} (with generic type {mime_type}), for file: {s3_path}"
                })

      return success_status
    except Exception as e:
      err = f"❌❌ Error in /ingest: `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )  # type: ignore
      sentry_sdk.capture_exception(e)

      success_status['failure_ingest'] = {'s3_path': s3_path, 'error': f"MAJOR ERROR DURING INGEST: {err}"}
      self.posthog.capture('distinct_id_of_the_user',
                           event='ingest_failure',
                           properties={
                               'course_name': course_name,
                               's3_path': s3_paths,
                               'kwargs': kwargs,
                               'error': err
                           })

      sentry_sdk.capture_exception(e)
      print(f"MAJOR ERROR IN /bulk_ingest: {str(e)}")
      return success_status

  def ingest_single_web_text(self, course_name: str, base_url: str, url: str, content: str, readable_filename: str,
                             **kwargs):
    """Crawlee integration
    """
    self.posthog.capture('distinct_id_of_the_user',
                         event='ingest_single_web_text_invoked',
                         properties={
                             'course_name': course_name,
                             'base_url': base_url,
                             'url': url,
                             'content': content,
                             'title': readable_filename
                         })
    success_or_failure: Dict[str, None | str | Dict[str, str]] = {"success_ingest": None, "failure_ingest": None}
    try:
      # if not, ingest the text
      text = [content]
      metadatas: List[Dict[str, Any]] = [{
          'course_name': course_name,
          's3_path': '',
          'readable_filename': readable_filename,
          'pagenumber': '',
          'timestamp': '',
          'url': url,
          'base_url': base_url,
      }]
      self.split_and_upload(texts=text, metadatas=metadatas, **kwargs)
      self.posthog.capture('distinct_id_of_the_user',
                           event='ingest_single_web_text_succeeded',
                           properties={
                               'course_name': course_name,
                               'base_url': base_url,
                               'url': url,
                               'title': readable_filename
                           })
      success_or_failure['success_ingest'] = url
      return success_or_failure
    except Exception as e:
      err = f"❌❌ Error in (web text ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )  # type: ignore
      print(err)
      sentry_sdk.capture_exception(e)
      success_or_failure['failure_ingest'] = {'url': url, 'error': str(err)}
      return success_or_failure

  def _ingest_single_py(self, s3_path: str, course_name: str, **kwargs):
    try:
      file_name = s3_path.split("/")[-1]
      os.makedirs("../media", exist_ok=True)
      file_path = "../media/" + file_name  # download from s3 to local folder for ingest

      self.s3_client.download_file(os.getenv('S3_BUCKET_NAME'), s3_path, file_path)

      loader = PythonLoader(file_path)
      documents = loader.load()

      texts = [doc.page_content for doc in documents]

      metadatas: List[Dict[str, Any]] = [{
          'course_name': course_name,
          's3_path': s3_path,
          'readable_filename': kwargs.get('readable_filename',
                                          Path(s3_path).name[37:]),
          'pagenumber': '',
          'timestamp': '',
          'url': kwargs.get('url', ''),
          'base_url': kwargs.get('base_url', ''),
      } for doc in documents]
      #print(texts)
      os.remove(file_path)

      success_or_failure = self.split_and_upload(texts=texts, metadatas=metadatas, **kwargs)
      print("Python ingest: ", success_or_failure)
      return success_or_failure

    except Exception as e:
      err = f"❌❌ Error in (Python ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return err

  def _ingest_single_vtt(self, s3_path: str, course_name: str, **kwargs):
    """
    Ingest a single .vtt file from S3.
    """
    try:
      with NamedTemporaryFile() as tmpfile:
        # download from S3 into vtt_tmpfile
        self.s3_client.download_fileobj(Bucket=os.environ['S3_BUCKET_NAME'], Key=s3_path, Fileobj=tmpfile)
        loader = TextLoader(tmpfile.name)
        documents = loader.load()
        texts = [doc.page_content for doc in documents]

        metadatas: List[Dict[str, Any]] = [{
            'course_name': course_name,
            's3_path': s3_path,
            'readable_filename': kwargs.get('readable_filename',
                                            Path(s3_path).name[37:]),
            'pagenumber': '',
            'timestamp': '',
            'url': kwargs.get('url', ''),
            'base_url': kwargs.get('base_url', ''),
        } for doc in documents]

        success_or_failure = self.split_and_upload(texts=texts, metadatas=metadatas, **kwargs)
        return success_or_failure
    except Exception as e:
      err = f"❌❌ Error in (VTT ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return err

  def _ingest_html(self, s3_path: str, course_name: str, **kwargs) -> str:
    print(f"IN _ingest_html s3_path `{s3_path}` kwargs: {kwargs}")
    try:
      response = self.s3_client.get_object(Bucket=os.environ['S3_BUCKET_NAME'], Key=s3_path)
      raw_html = response['Body'].read().decode('utf-8', errors='ignore')

      soup = BeautifulSoup(raw_html, 'html.parser')
      title = s3_path.replace("courses/" + course_name, "")
      title = title.replace(".html", "")
      title = title.replace("_", " ")
      title = title.replace("/", " ")
      title = title.strip()
      title = title[37:]  # removing the uuid prefix
      text = [soup.get_text()]

      metadata: List[Dict[str, Any]] = [{
          'course_name': course_name,
          's3_path': s3_path,
          'readable_filename': str(title),  # adding str to avoid error: unhashable type 'slice'
          'url': kwargs.get('url', ''),
          'base_url': kwargs.get('base_url', ''),
          'pagenumber': '',
          'timestamp': '',
      }]

      success_or_failure = self.split_and_upload(text, metadata, **kwargs)
      print(f"_ingest_html: {success_or_failure}")
      return success_or_failure
    except Exception as e:
      err: str = f"ERROR IN _ingest_html: {e}\nTraceback: {traceback.extract_tb(e.__traceback__)}❌❌ Error in {inspect.currentframe().f_code.co_name}:{e}"  # type: ignore
      print(err)
      sentry_sdk.capture_exception(e)
      return err

  def _ingest_single_video(self, s3_path: str, course_name: str, **kwargs) -> str:
    """
    Ingest a single video file from S3.
    """
    print("Starting ingest video or audio")
    try:
      # Ensure the media directory exists
      media_dir = "media"
      if not os.path.exists(media_dir):
        os.makedirs(media_dir)

      # check for file extension
      file_ext = Path(s3_path).suffix
      openai.api_key = os.getenv('VLADS_OPENAI_KEY')
      transcript_list = []
      with NamedTemporaryFile(suffix=file_ext) as video_tmpfile:
        # download from S3 into an video tmpfile
        self.s3_client.download_fileobj(Bucket=os.environ['S3_BUCKET_NAME'], Key=s3_path, Fileobj=video_tmpfile)

        # try with original file first
        try:
          mp4_version = AudioSegment.from_file(video_tmpfile.name, file_ext[1:])
        except Exception as e:
          print("Applying moov atom fix and retrying...")
          # Fix the moov atom issue using FFmpeg
          fixed_video_tmpfile = NamedTemporaryFile(suffix=file_ext, delete=False)
          try:
            result = subprocess.run([
                'ffmpeg', '-y', '-i', video_tmpfile.name, '-c', 'copy', '-movflags', 'faststart',
                fixed_video_tmpfile.name
            ],
                                    check=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            #print(result.stdout.decode())
            #print(result.stderr.decode())
          except subprocess.CalledProcessError as e:
            #print(e.stdout.decode())
            #print(e.stderr.decode())
            print("Error in FFmpeg command: ", e)
            raise e

          # extract audio from video tmpfile
          mp4_version = AudioSegment.from_file(fixed_video_tmpfile.name, file_ext[1:])

      # save the extracted audio as a temporary webm file
      with NamedTemporaryFile(suffix=".webm", dir=media_dir, delete=False) as webm_tmpfile:
        mp4_version.export(webm_tmpfile, format="webm")

      # check file size
      file_size = os.path.getsize(webm_tmpfile.name)
      # split the audio into 25MB chunks
      if file_size > 26214400:
        # load the webm file into audio object
        full_audio = AudioSegment.from_file(webm_tmpfile.name, "webm")
        file_count = file_size // 26214400 + 1
        split_segment = 35 * 60 * 1000
        start = 0
        count = 0

        while count < file_count:
          with NamedTemporaryFile(suffix=".webm", dir=media_dir, delete=False) as split_tmp:
            if count == file_count - 1:
              # last segment
              audio_chunk = full_audio[start:]
            else:
              audio_chunk = full_audio[start:split_segment]

            audio_chunk.export(split_tmp.name, format="webm")

            # transcribe the split file and store the text in dictionary
            with open(split_tmp.name, "rb") as f:
              transcript = openai.Audio.transcribe("whisper-1", f)
            transcript_list.append(transcript['text'])  # type: ignore
          start += split_segment
          split_segment += split_segment
          count += 1
          os.remove(split_tmp.name)
      else:
        # transcribe the full audio
        with open(webm_tmpfile.name, "rb") as f:
          transcript = openai.Audio.transcribe("whisper-1", f)
        transcript_list.append(transcript['text'])  # type: ignore

      os.remove(webm_tmpfile.name)

      text = [txt for txt in transcript_list]
      metadatas: List[Dict[str, Any]] = [{
          'course_name': course_name,
          's3_path': s3_path,
          'readable_filename': kwargs.get('readable_filename',
                                          Path(s3_path).name[37:]),
          'pagenumber': '',
          'timestamp': text.index(txt),
          'url': kwargs.get('url', ''),
          'base_url': kwargs.get('base_url', ''),
      } for txt in text]

      self.split_and_upload(texts=text, metadatas=metadatas, **kwargs)
      return "Success"
    except Exception as e:
      err = f"❌❌ Error in (VIDEO ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return str(err)

  def _ingest_single_docx(self, s3_path: str, course_name: str, **kwargs) -> str:
    try:
      with NamedTemporaryFile() as tmpfile:
        self.s3_client.download_fileobj(Bucket=os.getenv('S3_BUCKET_NAME'), Key=s3_path, Fileobj=tmpfile)

        loader = Docx2txtLoader(tmpfile.name)
        documents = loader.load()

        texts = [doc.page_content for doc in documents]
        metadatas: List[Dict[str, Any]] = [{
            'course_name': course_name,
            's3_path': s3_path,
            'readable_filename': kwargs.get('readable_filename',
                                            Path(s3_path).name[37:]),
            'pagenumber': '',
            'timestamp': '',
            'url': kwargs.get('url', ''),
            'base_url': kwargs.get('base_url', ''),
        } for doc in documents]

        self.split_and_upload(texts=texts, metadatas=metadatas, **kwargs)
        return "Success"
    except Exception as e:
      err = f"❌❌ Error in (DOCX ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return str(err)

  def _ingest_single_srt(self, s3_path: str, course_name: str, **kwargs) -> str:
    try:
      import pysrt

      # NOTE: slightly different method for .txt files, no need for download. It's part of the 'body'
      response = self.s3_client.get_object(Bucket=os.environ['S3_BUCKET_NAME'], Key=s3_path)
      raw_text = response['Body'].read().decode('utf-8', errors='ignore')

      print("UTF-8 text to ingest as SRT:", raw_text)
      parsed_info = pysrt.from_string(raw_text)
      text = " ".join([t.text for t in parsed_info])  # type: ignore
      print(f"Final SRT ingest: {text}")

      texts = [text]
      metadatas: List[Dict[str, Any]] = [{
          'course_name': course_name,
          's3_path': s3_path,
          'readable_filename': kwargs.get('readable_filename',
                                          Path(s3_path).name[37:]),
          'pagenumber': '',
          'timestamp': '',
          'url': kwargs.get('url', ''),
          'base_url': kwargs.get('base_url', ''),
      }]
      if len(text) == 0:
        return "Error: SRT file appears empty. Skipping."

      self.split_and_upload(texts=texts, metadatas=metadatas, **kwargs)
      return "Success"
    except Exception as e:
      err = f"❌❌ Error in (SRT ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return str(err)

  def _ingest_single_excel(self, s3_path: str, course_name: str, **kwargs) -> str:
    try:
      with NamedTemporaryFile() as tmpfile:
        # download from S3 into pdf_tmpfile
        self.s3_client.download_fileobj(Bucket=os.getenv('S3_BUCKET_NAME'), Key=s3_path, Fileobj=tmpfile)

        loader = UnstructuredExcelLoader(tmpfile.name, mode="elements")
        # loader = SRTLoader(tmpfile.name)
        documents = loader.load()

        texts = [doc.page_content for doc in documents]
        metadatas: List[Dict[str, Any]] = [{
            'course_name': course_name,
            's3_path': s3_path,
            'readable_filename': kwargs.get('readable_filename',
                                            Path(s3_path).name[37:]),
            'pagenumber': '',
            'timestamp': '',
            'url': kwargs.get('url', ''),
            'base_url': kwargs.get('base_url', ''),
        } for doc in documents]

        self.split_and_upload(texts=texts, metadatas=metadatas, **kwargs)
        return "Success"
    except Exception as e:
      err = f"❌❌ Error in (Excel/xlsx ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return str(err)

  def _ingest_single_image(self, s3_path: str, course_name: str, **kwargs) -> str:
    try:
      with NamedTemporaryFile() as tmpfile:
        # download from S3 into pdf_tmpfile
        self.s3_client.download_fileobj(Bucket=os.getenv('S3_BUCKET_NAME'), Key=s3_path, Fileobj=tmpfile)
        """
        # Unstructured image loader makes the install too large (700MB --> 6GB. 3min -> 12 min build times). AND nobody uses it.
        # The "hi_res" strategy will identify the layout of the document using detectron2. "ocr_only" uses pdfminer.six. https://unstructured-io.github.io/unstructured/core/partition.html#partition-image
        loader = UnstructuredImageLoader(tmpfile.name, unstructured_kwargs={'strategy': "ocr_only"})
        documents = loader.load()
        """

        res_str = pytesseract.image_to_string(Image.open(tmpfile.name))
        print("IMAGE PARSING RESULT:", res_str)
        documents = [Document(page_content=res_str)]

        texts = [doc.page_content for doc in documents]
        metadatas: List[Dict[str, Any]] = [{
            'course_name': course_name,
            's3_path': s3_path,
            'readable_filename': kwargs.get('readable_filename',
                                            Path(s3_path).name[37:]),
            'pagenumber': '',
            'timestamp': '',
            'url': kwargs.get('url', ''),
            'base_url': kwargs.get('base_url', ''),
        } for doc in documents]

        self.split_and_upload(texts=texts, metadatas=metadatas, **kwargs)
        return "Success"
    except Exception as e:
      err = f"❌❌ Error in (png/jpg ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return str(err)

  def _ingest_single_csv(self, s3_path: str, course_name: str, **kwargs) -> str:
    try:
      with NamedTemporaryFile() as tmpfile:
        # download from S3 into pdf_tmpfile
        self.s3_client.download_fileobj(Bucket=os.getenv('S3_BUCKET_NAME'), Key=s3_path, Fileobj=tmpfile)

        loader = CSVLoader(file_path=tmpfile.name)
        documents = loader.load()

        texts = [doc.page_content for doc in documents]
        metadatas: List[Dict[str, Any]] = [{
            'course_name': course_name,
            's3_path': s3_path,
            'readable_filename': kwargs.get('readable_filename',
                                            Path(s3_path).name[37:]),
            'pagenumber': '',
            'timestamp': '',
            'url': kwargs.get('url', ''),
            'base_url': kwargs.get('base_url', ''),
        } for doc in documents]

        self.split_and_upload(texts=texts, metadatas=metadatas, **kwargs)
        return "Success"
    except Exception as e:
      err = f"❌❌ Error in (CSV ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return str(err)

  def _ingest_single_pdf(self, s3_path: str, course_name: str, **kwargs):
    """
    Both OCR the PDF. And grab the first image as a PNG.
      LangChain `Documents` have .metadata and .page_content attributes.
    Be sure to use TemporaryFile() to avoid memory leaks!
    """
    print("IN PDF ingest: s3_path: ", s3_path, "and kwargs:", kwargs)

    try:
      with NamedTemporaryFile() as pdf_tmpfile:
        # download from S3 into pdf_tmpfile
        self.s3_client.download_fileobj(Bucket=os.getenv('S3_BUCKET_NAME'), Key=s3_path, Fileobj=pdf_tmpfile)
        ### READ OCR of PDF
        try:
          doc = fitz.open(pdf_tmpfile.name)  # type: ignore
        except fitz.fitz.EmptyFileError as e:
          print(f"Empty PDF file: {s3_path}")
          return "Failed ingest: Could not detect ANY text in the PDF. OCR did not help. PDF appears empty of text."

        # improve quality of the image
        zoom_x = 2.0  # horizontal zoom
        zoom_y = 2.0  # vertical zoom
        mat = fitz.Matrix(zoom_x, zoom_y)  # zoom factor 2 in each dimension

        pdf_pages_no_OCR: List[Dict] = []
        for i, page in enumerate(doc):  # type: ignore

          # UPLOAD FIRST PAGE IMAGE to S3
          if i == 0:
            with NamedTemporaryFile(suffix=".png") as first_page_png:
              pix = page.get_pixmap(matrix=mat)
              pix.save(first_page_png)  # store image as a PNG

              s3_upload_path = str(Path(s3_path)).rsplit('.pdf')[0] + "-pg1-thumb.png"
              first_page_png.seek(0)  # Seek the file pointer back to the beginning
              with open(first_page_png.name, 'rb') as f:
                print("Uploading image png to S3")
                self.s3_client.upload_fileobj(f, os.getenv('S3_BUCKET_NAME'), s3_upload_path)

          # Extract text
          text = page.get_text().encode("utf8").decode("utf8", errors='ignore')  # get plain text (is in UTF-8)
          pdf_pages_no_OCR.append(dict(text=text, page_number=i, readable_filename=Path(s3_path).name[37:]))

        metadatas: List[Dict[str, Any]] = [
            {
                'course_name': course_name,
                's3_path': s3_path,
                'pagenumber': page['page_number'] + 1,  # +1 for human indexing
                'timestamp': '',
                'readable_filename': kwargs.get('readable_filename', page['readable_filename']),
                'url': kwargs.get('url', ''),
                'base_url': kwargs.get('base_url', ''),
            } for page in pdf_pages_no_OCR
        ]
        pdf_texts = [page['text'] for page in pdf_pages_no_OCR]

        # count the total number of words in the pdf_texts. If it's less than 100, we'll OCR the PDF
        has_words = any(text.strip() for text in pdf_texts)
        if has_words:
          success_or_failure = self.split_and_upload(texts=pdf_texts, metadatas=metadatas, **kwargs)
        else:
          print("⚠️ PDF IS EMPTY -- OCR-ing the PDF.")
          success_or_failure = self._ocr_pdf(s3_path=s3_path, course_name=course_name, **kwargs)

        return success_or_failure
    except Exception as e:
      err = f"❌❌ Error in PDF ingest (no OCR): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )  # type: ignore
      print(err)
      sentry_sdk.capture_exception(e)
      return err
    return "Success"

  def _ocr_pdf(self, s3_path: str, course_name: str, **kwargs):
    self.posthog.capture('distinct_id_of_the_user',
                         event='ocr_pdf_invoked',
                         properties={
                             'course_name': course_name,
                             's3_path': s3_path,
                         })

    pdf_pages_OCRed: List[Dict] = []
    try:
      with NamedTemporaryFile() as pdf_tmpfile:
        # download from S3 into pdf_tmpfile
        self.s3_client.download_fileobj(Bucket=os.getenv('S3_BUCKET_NAME'), Key=s3_path, Fileobj=pdf_tmpfile)

        with pdfplumber.open(pdf_tmpfile.name) as pdf:
          # for page in :
          for i, page in enumerate(pdf.pages):
            im = page.to_image()
            text = pytesseract.image_to_string(im.original)
            print("Page number: ", i, "Text: ", text[:100])
            pdf_pages_OCRed.append(dict(text=text, page_number=i, readable_filename=Path(s3_path).name[37:]))

      metadatas: List[Dict[str, Any]] = [
          {
              'course_name': course_name,
              's3_path': s3_path,
              'pagenumber': page['page_number'] + 1,  # +1 for human indexing
              'timestamp': '',
              'readable_filename': kwargs.get('readable_filename', page['readable_filename']),
              'url': kwargs.get('url', ''),
              'base_url': kwargs.get('base_url', ''),
          } for page in pdf_pages_OCRed
      ]
      pdf_texts = [page['text'] for page in pdf_pages_OCRed]
      self.posthog.capture('distinct_id_of_the_user',
                           event='ocr_pdf_succeeded',
                           properties={
                               'course_name': course_name,
                               's3_path': s3_path,
                           })

      has_words = any(text.strip() for text in pdf_texts)
      if not has_words:
        raise ValueError(
            "Failed ingest: Could not detect ANY text in the PDF. OCR did not help. PDF appears empty of text.")

      success_or_failure = self.split_and_upload(texts=pdf_texts, metadatas=metadatas, **kwargs)
      return success_or_failure
    except Exception as e:
      err = f"❌❌ Error in PDF ingest (with OCR): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return err

  def _ingest_single_txt(self, s3_path: str, course_name: str, **kwargs) -> str:
    """Ingest a single .txt or .md file from S3.
    Args:
        s3_path (str): A path to a .txt file in S3
        course_name (str): The name of the course
    Returns:
        str: "Success" or an error message
    """
    print("In text ingest, UTF-8")
    print("kwargs", kwargs)
    try:
      # NOTE: slightly different method for .txt files, no need for download. It's part of the 'body'
      response = self.s3_client.get_object(Bucket=os.environ['S3_BUCKET_NAME'], Key=s3_path)
      text = response['Body'].read().decode('utf-8', errors='ignore')
      print("UTF-8 text to ignest (from s3)", text)
      text = [text]

      metadatas: List[Dict[str, Any]] = [{
          'course_name': course_name,
          's3_path': s3_path,
          'readable_filename': kwargs.get('readable_filename',
                                          Path(s3_path).name[37:]),
          'pagenumber': '',
          'timestamp': '',
          'url': kwargs.get('url', ''),
          'base_url': kwargs.get('base_url', '')
      }]
      print("Prior to ingest", metadatas)

      success_or_failure = self.split_and_upload(texts=text, metadatas=metadatas, **kwargs)
      return success_or_failure
    except Exception as e:
      err = f"❌❌ Error in (TXT ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return str(err)

  def _ingest_single_ppt(self, s3_path: str, course_name: str, **kwargs) -> str:
    """
    Ingest a single .ppt or .pptx file from S3.
    """
    try:
      with NamedTemporaryFile() as tmpfile:
        # download from S3 into pdf_tmpfile
        #print("in ingest PPTX")
        self.s3_client.download_fileobj(Bucket=os.environ['S3_BUCKET_NAME'], Key=s3_path, Fileobj=tmpfile)

        loader = UnstructuredPowerPointLoader(tmpfile.name)
        documents = loader.load()

        texts = [doc.page_content for doc in documents]
        metadatas: List[Dict[str, Any]] = [{
            'course_name': course_name,
            's3_path': s3_path,
            'readable_filename': kwargs.get('readable_filename',
                                            Path(s3_path).name[37:]),
            'pagenumber': '',
            'timestamp': '',
            'url': kwargs.get('url', ''),
            'base_url': kwargs.get('base_url', '')
        } for doc in documents]

        self.split_and_upload(texts=texts, metadatas=metadatas, **kwargs)
        return "Success"
    except Exception as e:
      err = f"❌❌ Error in (PPTX ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc(
      )
      print(err)
      sentry_sdk.capture_exception(e)
      return str(err)

  def ingest_github(self, github_url: str, course_name: str) -> str:
    """
    Clones the given GitHub URL and uses Langchain to load data.
    1. Clone the repo
    2. Use Langchain to load the data
    3. Pass to split_and_upload()
    Args:
        github_url (str): The Github Repo URL to be ingested.
        course_name (str): The name of the course in our system.

    Returns:
        _type_: Success or error message.
    """
    try:
      repo_path = "media/cloned_repo"
      repo = Repo.clone_from(github_url, to_path=repo_path, depth=1, clone_submodules=False)
      branch = repo.head.reference

      loader = GitLoader(repo_path="media/cloned_repo", branch=str(branch))
      data = loader.load()
      shutil.rmtree("media/cloned_repo")
      # create metadata for each file in data

      for doc in data:
        texts = doc.page_content
        metadatas: Dict[str, Any] = {
            'course_name': course_name,
            's3_path': '',
            'readable_filename': doc.metadata['file_name'],
            'url': f"{github_url}/blob/main/{doc.metadata['file_path']}",
            'pagenumber': '',
            'timestamp': '',
        }
        self.split_and_upload(texts=[texts], metadatas=[metadatas])
      return "Success"
    except Exception as e:
      err = f"❌❌ Error in (GITHUB ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n{traceback.format_exc()}"
      print(err)
      sentry_sdk.capture_exception(e)
      return err

  def split_and_upload(self, texts: List[str], metadatas: List[Dict[str, Any]], **kwargs):
    """ This is usually the last step of document ingest. Chunk & upload to Qdrant (and Supabase.. todo).
    Takes in Text and Metadata (from Langchain doc loaders) and splits / uploads to Qdrant.

    good examples here: https://langchain.readthedocs.io/en/latest/modules/utils/combine_docs_examples/textsplitter.html

    Args:
        texts (List[str]): _description_
        metadatas (List[Dict[str, Any]]): _description_
    """
    # return "Success"
    print(f"In split and upload. Metadatas: {metadatas}")
    self.posthog.capture('distinct_id_of_the_user',
                         event='split_and_upload_invoked',
                         properties={
                             'course_name': metadatas[0].get('course_name', None),
                             's3_path': metadatas[0].get('s3_path', None),
                             'readable_filename': metadatas[0].get('readable_filename', None),
                             'url': metadatas[0].get('url', None),
                             'base_url': metadatas[0].get('base_url', None),
                         })

    #print(f"In split and upload. Metadatas: {metadatas}")
    #print(f"Texts: {texts}")
    assert len(texts) == len(
        metadatas
    ), f'must have equal number of text strings and metadata dicts. len(texts) is {len(texts)}. len(metadatas) is {len(metadatas)}'

    try:
      chunk_size = 2_000
      if metadatas[0].get('course_name') == 'GROWMARK-Crop-Protection-Guide':
        # Special case for this project, try to embed entire PDF page as 1 chunk (better at tables)
        chunk_size = 6_000

      text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
          chunk_size=chunk_size,
          chunk_overlap=150,
          separators=[
              "\n\n", "\n", ". ", " ", ""
          ]  # try to split on paragraphs... fallback to sentences, then chars, ensure we always fit in context window
      )
      contexts: List[Document] = text_splitter.create_documents(texts=texts, metadatas=metadatas)
      input_texts = [{'input': context.page_content, 'model': 'text-embedding-ada-002'} for context in contexts]

      # check for duplicates
      is_duplicate = self.check_for_duplicates(input_texts, metadatas)
      if is_duplicate:
        self.posthog.capture('distinct_id_of_the_user',
                             event='split_and_upload_succeeded',
                             properties={
                                 'course_name': metadatas[0].get('course_name', None),
                                 's3_path': metadatas[0].get('s3_path', None),
                                 'readable_filename': metadatas[0].get('readable_filename', None),
                                 'url': metadatas[0].get('url', None),
                                 'base_url': metadatas[0].get('base_url', None),
                                 'is_duplicate': True,
                             })
        return "Success"

      # adding chunk index to metadata for parent doc retrieval
      print("GROUPS: ", kwargs.get('groups', ''))
      for i, context in enumerate(contexts):
        context.metadata['chunk_index'] = i
        context.metadata['doc_groups'] = kwargs.get('groups', [])

      openai_embeddings_key = os.getenv('VLADS_OPENAI_KEY')
      if metadatas[0].get('course_name') == 'cropwizard-1.5':
        print("Using Cropwizard OpenAI key")
        openai_embeddings_key = os.getenv('CROPWIZARD_OPENAI_KEY')

      print("Starting to call embeddings API")
      embeddings_start_time = time.monotonic()
      oai = OpenAIAPIProcessor(
          input_prompts_list=input_texts,
          request_url='https://api.openai.com/v1/embeddings',
          api_key=openai_embeddings_key,
          # request_url='https://uiuc-chat-canada-east.openai.azure.com/openai/deployments/text-embedding-ada-002/embeddings?api-version=2023-05-15',
          # api_key=os.getenv('AZURE_OPENAI_KEY'),
          max_requests_per_minute=10_000,
          max_tokens_per_minute=10_000_000,
          max_attempts=1_000,
          logging_level=logging.INFO,
          token_encoding_name='cl100k_base')
      asyncio.run(oai.process_api_requests_from_file())
      print(f"⏰ embeddings runtime: {(time.monotonic() - embeddings_start_time):.2f} seconds")
      # parse results into dict of shape page_content -> embedding
      embeddings_dict: dict[str, List[float]] = {
          item[0]['input']: item[1]['data'][0]['embedding'] for item in oai.results
      }

      ### BULK upload to Qdrant ###
      vectors: list[PointStruct] = []
      for context in contexts:
        # !DONE: Updated the payload so each key is top level (no more payload.metadata.course_name. Instead, use payload.course_name), great for creating indexes.
        upload_metadata = {**context.metadata, "page_content": context.page_content}
        vectors.append(
            PointStruct(id=str(uuid.uuid4()), vector=embeddings_dict[context.page_content], payload=upload_metadata))

      try:
        # ----------------------------
        # SPECIAL CASE FOR CROPWIZARD INGEST
        # ----------------------------
        if metadatas[0].get('course_name') == 'cropwizard-1.5':
          print("Uploading to cropwizard collection...")
          self.cropwizard_qdrant_client.upsert(
              collection_name='cropwizard',
              points=vectors,
          )
        else:
          self.qdrant_client.upsert(
              collection_name=os.environ['QDRANT_COLLECTION_NAME'],
              points=vectors,
          )
      except Exception as e:
        logging.error("Error in QDRANT upload: ", exc_info=True)
        err = f"Error in QDRANT upload: {e}"
        if "timed out" in str(e):
          # timed out error is fine, task will continue in background
          pass
        else:
          print(err)
          sentry_sdk.capture_exception(e)
          raise Exception(err)

      ### Supabase SQL ###
      contexts_for_supa = [{
          "text": context.page_content,
          "pagenumber": context.metadata.get('pagenumber'),
          "timestamp": context.metadata.get('timestamp'),
          "chunk_index": context.metadata.get('chunk_index'),
          "embedding": embeddings_dict[context.page_content]
      } for context in contexts]

      document = {
          "course_name": contexts[0].metadata.get('course_name'),
          "s3_path": contexts[0].metadata.get('s3_path'),
          "readable_filename": contexts[0].metadata.get('readable_filename'),
          "url": contexts[0].metadata.get('url'),
          "base_url": contexts[0].metadata.get('base_url'),
          "contexts": contexts_for_supa,
      }

      # Calculate the size of the document object in MB
      document_size_mb = len(json.dumps(document).encode('utf-8')) / (1024 * 1024)
      print(f"Document size: {document_size_mb:.2f} MB")

      response = self.supabase_client.table(
          os.getenv('REFACTORED_MATERIALS_SUPABASE_TABLE')).insert(document).execute()  # type: ignore

      # need to update Supabase tables with doc group info
      if len(response.data) > 0:
        # get groups from kwargs
        groups = kwargs.get('groups', '')
        if groups:
          # call the supabase function to add the document to the group
          if contexts[0].metadata.get('url'):
            data, count = self.supabase_client.rpc(
                'add_document_to_group_url', {
                    "p_course_name": contexts[0].metadata.get('course_name'),
                    "p_s3_path": contexts[0].metadata.get('s3_path'),
                    "p_url": contexts[0].metadata.get('url'),
                    "p_readable_filename": contexts[0].metadata.get('readable_filename'),
                    "p_doc_groups": groups,
                }).execute()
          else:
            data, count = self.supabase_client.rpc(
                'add_document_to_group', {
                    "p_course_name": contexts[0].metadata.get('course_name'),
                    "p_s3_path": contexts[0].metadata.get('s3_path'),
                    "p_url": contexts[0].metadata.get('url'),
                    "p_readable_filename": contexts[0].metadata.get('readable_filename'),
                    "p_doc_groups": groups,
                }).execute()

          if len(data) == 0:
            print("Error in adding to doc groups")
            raise ValueError("Error in adding to doc groups")

      self.posthog.capture('distinct_id_of_the_user',
                           event='split_and_upload_succeeded',
                           properties={
                               'course_name': metadatas[0].get('course_name', None),
                               's3_path': metadatas[0].get('s3_path', None),
                               'readable_filename': metadatas[0].get('readable_filename', None),
                               'url': metadatas[0].get('url', None),
                               'base_url': metadatas[0].get('base_url', None),
                               'is_duplicate': False,
                           })
      print("successful END OF split_and_upload")
      return "Success"
    except Exception as e:
      err: str = f"ERROR IN split_and_upload(): Traceback: {traceback.extract_tb(e.__traceback__)}❌❌ Error in {inspect.currentframe().f_code.co_name}:{e}"  # type: ignore
      print(err)
      sentry_sdk.capture_exception(e)
      sentry_sdk.flush(timeout=20)
      raise Exception(err)

  def check_for_duplicates(self, texts: List[Dict], metadatas: List[Dict[str, Any]]) -> bool:
    """
    For given metadata, fetch docs from Supabase based on S3 path or URL.
    If docs exists, concatenate the texts and compare with current texts, if same, return True.
    """
    doc_table = os.getenv('REFACTORED_MATERIALS_SUPABASE_TABLE')
    course_name = metadatas[0]['course_name']
    incoming_s3_path = metadatas[0]['s3_path']
    url = metadatas[0]['url']

    if incoming_s3_path:
      # check if uuid exists in s3_path -- not all s3_paths have uuids!
      incoming_filename = incoming_s3_path.split('/')[-1]
      # print("Full filename: ", incoming_filename)
      pattern = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}',
                           re.I)  # uuid V4 pattern, and v4 only.
      if bool(pattern.search(incoming_filename)):
        # uuid pattern exists -- remove the uuid and proceed with duplicate checking
        original_filename = incoming_filename[37:]
      else:
        # do not remove anything and proceed with duplicate checking
        original_filename = incoming_filename
      print(f"Filename after removing uuid: {original_filename}")

      supabase_contents = self.supabase_client.table(doc_table).select('id', 'contexts', 's3_path').eq(
          'course_name', course_name).like('s3_path', '%' + original_filename + '%').order('id', desc=True).execute()
      supabase_contents = supabase_contents.data
      print(f"No. of S3 path based records retrieved: {len(supabase_contents)}"
           )  # multiple records can be retrieved: 3.pdf and 453.pdf

    elif url:
      original_filename = url
      supabase_contents = self.supabase_client.table(doc_table).select('id', 'contexts', 'url').eq(
          'course_name', course_name).eq('url', url).order('id', desc=True).execute()
      supabase_contents = supabase_contents.data
      print(f"No. of URL-based records retrieved: {len(supabase_contents)}")
    else:
      original_filename = None
      supabase_contents = []

    supabase_whole_text = ""
    exact_doc_exists = False
    if len(supabase_contents) > 0:  # a doc with same filename exists in Supabase
      for record in supabase_contents:
        if incoming_s3_path:
          curr_filename = record['s3_path'].split('/')[-1]
          older_s3_path = record['s3_path']
          if bool(pattern.search(curr_filename)):
            # uuid pattern exists -- remove the uuid and proceed with duplicate checking
            sql_filename = curr_filename[37:]
          else:
            # do not remove anything and proceed with duplicate checking
            sql_filename = curr_filename
        elif url:
          print("URL retrieved from SQL: ", record.keys())
          sql_filename = record['url']
        else:
          continue
        print("Original filename: ", original_filename, "Current SQL filename: ", sql_filename)

        if original_filename == sql_filename:  # compare og s3_path/url with incoming s3_path/url
          supabase_contexts = record

          exact_doc_exists = True
          print("Exact doc exists in Supabase:", sql_filename)
          break

      if exact_doc_exists:
        # concatenate og texts
        for text in supabase_contexts['contexts']:
          supabase_whole_text += text['text']

        current_whole_text = ""
        for text in texts:
          current_whole_text += text['input']

        if supabase_whole_text == current_whole_text:  # matches the previous file
          print(f"Duplicate ingested! 📄 s3_path/url: {original_filename}.")
          return True

        else:  # the file is updated
          print(f"Updated file detected! Same filename, new contents. 📄s3_path/url: {original_filename}")

          # call the delete function on older doc
          print("older s3_path/url to be deleted: ", sql_filename)
          if incoming_s3_path:
            delete_status = self.delete_data(course_name, older_s3_path, '')
          else:
            delete_status = self.delete_data(course_name, '', url)
          print("delete_status: ", delete_status)
          return False
      else:
        print(f"NOT a duplicate! 📄s3_path: {original_filename}")
        return False

    else:  # filename does not already exist in Supabase, so its a brand new file
      print(f"NOT a duplicate! 📄s3_path: {original_filename}")
      return False

  def delete_data(self, course_name: str, s3_path: str, source_url: str):
    """Delete file from S3, Qdrant, and Supabase."""
    print(f"Deleting {s3_path} from S3, Qdrant, and Supabase for course {course_name}")
    # add delete from doc map logic here
    try:
      # Delete file from S3
      bucket_name = os.getenv('S3_BUCKET_NAME')

      # Delete files by S3 path
      if s3_path:
        try:
          self.s3_client.delete_object(Bucket=bucket_name, Key=s3_path)
        except Exception as e:
          print("Error in deleting file from s3:", e)
          sentry_sdk.capture_exception(e)
        # Delete from Qdrant
        # docs for nested keys: https://qdrant.tech/documentation/concepts/filtering/#nested-key
        # Qdrant "points" look like this: Record(id='000295ca-bd28-ac4a-6f8d-c245f7377f90', payload={'metadata': {'course_name': 'zotero-extreme', 'pagenumber_or_timestamp': 15, 'readable_filename': 'Dunlosky et al. - 2013 - Improving Students’ Learning With Effective Learni.pdf', 's3_path': 'courses/zotero-extreme/Dunlosky et al. - 2013 - Improving Students’ Learning With Effective Learni.pdf'}, 'page_content': '18  \nDunlosky et al.\n3.3 Effects in representative educational contexts. Sev-\neral of the large summarization-training studies have been \nconducted in regular classrooms, indicating the feasibility of \ndoing so. For example, the study by A. King (1992) took place \nin the context of a remedial study-skills course for undergrad-\nuates, and the study by Rinehart et al. (1986) took place in \nsixth-grade classrooms, with the instruction led by students \nregular teachers. In these and other cases, students benefited \nfrom the classroom training. We suspect it may actually be \nmore feasible to conduct these kinds of training  ...
        try:
          if course_name == 'cropwizard-1.5':
            print("Deleting from cropwizard collection...")
            self.cropwizard_qdrant_client.delete(
                collection_name='cropwizard',
                points_selector=models.Filter(must=[
                    models.FieldCondition(
                        key="s3_path",
                        match=models.MatchValue(value=s3_path),
                    ),
                ]),
            )
          else:
            self.qdrant_client.delete(
                collection_name=os.environ['QDRANT_COLLECTION_NAME'],
                points_selector=models.Filter(must=[
                    models.FieldCondition(
                        key="s3_path",
                        match=models.MatchValue(value=s3_path),
                    ),
                ]),
            )
        except Exception as e:
          if "timed out" in str(e):
            # Timed out is fine. Still deletes.
            # https://github.com/qdrant/qdrant/issues/3654#issuecomment-1955074525
            pass
          else:
            print("Error in deleting file from Qdrant:", e)
            sentry_sdk.capture_exception(e)

        try:
          self.supabase_client.from_(os.environ['REFACTORED_MATERIALS_SUPABASE_TABLE']).delete().eq(
              's3_path', s3_path).eq('course_name', course_name).execute()
        except Exception as e:
          print("Error in deleting file from supabase:", e)
          sentry_sdk.capture_exception(e)

      # Delete files by their URL identifier
      elif source_url:
        try:
          # Delete from Qdrant
          self.qdrant_client.delete(
              collection_name=os.environ['QDRANT_COLLECTION_NAME'],
              points_selector=models.Filter(must=[
                  models.FieldCondition(
                      key="url",
                      match=models.MatchValue(value=source_url),
                  ),
              ]),
          )
        except Exception as e:
          if "timed out" in str(e):
            # Timed out is fine. Still deletes.
            # https://github.com/qdrant/qdrant/issues/3654#issuecomment-1955074525
            pass
          else:
            print("Error in deleting file from Qdrant:", e)
            sentry_sdk.capture_exception(e)

        try:
          # delete from Supabase
          self.supabase_client.from_(os.environ['REFACTORED_MATERIALS_SUPABASE_TABLE']).delete().eq(
              'url', source_url).eq('course_name', course_name).execute()
        except Exception as e:
          print("Error in deleting file from supabase:", e)
          sentry_sdk.capture_exception(e)

      # Delete from Supabase
      return "Success"
    except Exception as e:
      err: str = f"ERROR IN delete_data: Traceback: {traceback.extract_tb(e.__traceback__)}❌❌ Error in {inspect.currentframe().f_code.co_name}:{e}"  # type: ignore
      print(err)
      sentry_sdk.capture_exception(e)
      return err

  # def ingest_coursera(self, coursera_course_name: str, course_name: str) -> str:
  #   """ Download all the files from a coursera course and ingest them.

  #   1. Download the coursera content.
  #   2. Upload to S3 (so users can view it)
  #   3. Run everything through the ingest_bulk method.

  #   Args:
  #       coursera_course_name (str): The name of the coursera course.
  #       course_name (str): The name of the course in our system.

  #   Returns:
  #       _type_: Success or error message.
  #   """
  #   certificate = "-ca 'FVhVoDp5cb-ZaoRr5nNJLYbyjCLz8cGvaXzizqNlQEBsG5wSq7AHScZGAGfC1nI0ehXFvWy1NG8dyuIBF7DLMA.X3cXsDvHcOmSdo3Fyvg27Q.qyGfoo0GOHosTVoSMFy-gc24B-_BIxJtqblTzN5xQWT3hSntTR1DMPgPQKQmfZh_40UaV8oZKKiF15HtZBaLHWLbpEpAgTg3KiTiU1WSdUWueo92tnhz-lcLeLmCQE2y3XpijaN6G4mmgznLGVsVLXb-P3Cibzz0aVeT_lWIJNrCsXrTFh2HzFEhC4FxfTVqS6cRsKVskPpSu8D9EuCQUwJoOJHP_GvcME9-RISBhi46p-Z1IQZAC4qHPDhthIJG4bJqpq8-ZClRL3DFGqOfaiu5y415LJcH--PRRKTBnP7fNWPKhcEK2xoYQLr9RxBVL3pzVPEFyTYtGg6hFIdJcjKOU11AXAnQ-Kw-Gb_wXiHmu63veM6T8N2dEkdqygMre_xMDT5NVaP3xrPbA4eAQjl9yov4tyX4AQWMaCS5OCbGTpMTq2Y4L0Mbz93MHrblM2JL_cBYa59bq7DFK1IgzmOjFhNG266mQlC9juNcEhc'"
  #   always_use_flags = "-u kastanvday@gmail.com -p hSBsLaF5YM469# --ignore-formats mp4 --subtitle-language en --path ./coursera-dl"

  #   try:
  #     subprocess.run(
  #         f"coursera-dl {always_use_flags} {certificate} {coursera_course_name}",
  #         check=True,
  #         shell=True,  # nosec -- reasonable bandit error suppression
  #         stdout=subprocess.PIPE,
  #         stderr=subprocess.PIPE)  # capture_output=True,
  #     dl_results_path = os.path.join('coursera-dl', coursera_course_name)
  #     s3_paths: Union[List, None] = upload_data_files_to_s3(course_name, dl_results_path)

  #     if s3_paths is None:
  #       return "Error: No files found in the coursera-dl directory"

  #     print("starting bulk ingest")
  #     start_time = time.monotonic()
  #     self.bulk_ingest(s3_paths, course_name)
  #     print("completed bulk ingest")
  #     print(f"⏰ Runtime: {(time.monotonic() - start_time):.2f} seconds")

  #     # Cleanup the coursera downloads
  #     shutil.rmtree(dl_results_path)

  #     return "Success"
  #   except Exception as e:
  #     err: str = f"Traceback: {traceback.extract_tb(e.__traceback__)}❌❌ Error in {inspect.currentframe().f_code.co_name}:{e}"  # type: ignore
  #     print(err)
  #     return err

  # def list_files_recursively(self, bucket, prefix):
  #   all_files = []
  #   continuation_token = None

  #   while True:
  #     list_objects_kwargs = {
  #         'Bucket': bucket,
  #         'Prefix': prefix,
  #     }
  #     if continuation_token:
  #       list_objects_kwargs['ContinuationToken'] = continuation_token

  #     response = self.s3_client.list_objects_v2(**list_objects_kwargs)

  #     if 'Contents' in response:
  #       for obj in response['Contents']:
  #         all_files.append(obj['Key'])

  #     if response['IsTruncated']:
  #       continuation_token = response['NextContinuationToken']
  #     else:
  #       break

  #   return all_files


if __name__ == "__main__":
  raise NotImplementedError("This file is not meant to be run directly")
  text = "Testing 123"
  # ingest(text=text)
