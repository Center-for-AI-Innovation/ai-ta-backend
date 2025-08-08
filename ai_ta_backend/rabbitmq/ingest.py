import os
import re
import json
import time
import uuid
import shutil
import logging
import asyncio
import inspect
import traceback
import mimetypes
import subprocess
from typing import Any, Callable, Dict, List, Optional, Union, cast
from pathlib import Path
from tempfile import NamedTemporaryFile
from dotenv import load_dotenv

import boto3
from botocore.config import Config
import openai
import sentry_sdk
from posthog import Posthog
from qdrant_client import QdrantClient, models
from qdrant_client.models import PointStruct
from langchain.document_loaders import (
      Docx2txtLoader,
      GitLoader,
      PythonLoader,
      TextLoader,
)
from langchain_community.document_loaders import (
      UnstructuredExcelLoader,
      UnstructuredPowerPointLoader,
)
from langchain.document_loaders.csv_loader import CSVLoader
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import Qdrant

import fitz
import pdfplumber
import pytesseract
from PIL import Image
from git.repo import Repo
from bs4 import BeautifulSoup
from pydub import AudioSegment

try:
    from ai_ta_backend.rabbitmq.rmsql import SQLAlchemyIngestDB
    from ai_ta_backend.rabbitmq.embeddings import OpenAIAPIProcessor
except ModuleNotFoundError:
    # When running as worker outside Flask app, import from local path
    from rmsql import SQLAlchemyIngestDB
    from embeddings import OpenAIAPIProcessor

load_dotenv()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class Ingest:
    """
    Class for ingesting documents into the vector database.
    """

    def __init__(self):
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.openai_api_base = os.getenv('EMBEDDING_API_BASE') + "/embeddings" if os.getenv('EMBEDDING_API_BASE') else None
        self.embedding_model = os.environ['EMBEDDING_MODEL']
        self.qdrant_url = os.getenv('QDRANT_URL')
        self.qdrant_api_key = os.getenv('QDRANT_API_KEY')
        self.qdrant_collection_name = os.getenv('QDRANT_COLLECTION_NAME')
        self.minio_url = os.getenv('MINIO_URL')
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.s3_bucket_name = os.getenv('S3_BUCKET_NAME')
        self.posthog_api_key = os.getenv('POSTHOG_API_KEY')

        self.posthog = None
        self.qdrant_client = None
        self.vectorstore = None
        self.s3_client = None
        self.sql_session = None

    def initialize_resources(self):
        # Initialize Qdrant client and create collection if necessary
        if self.qdrant_api_key and self.qdrant_url:
            self.qdrant_client = QdrantClient(url=self.qdrant_url, api_key=self.qdrant_api_key)
            if not self.qdrant_client.get_collection(self.qdrant_collection_name):
                logging.info(f"Creating collection {self.qdrant_collection_name}")
                self.qdrant_client.create_collection(
                    collection_name=self.qdrant_collection_name,
                    vectors_config={"size": 4096, "distance": "Cosine"}
                )
            self.vectorstore = Qdrant(
                client=self.qdrant_client,
                collection_name=self.qdrant_collection_name,
                embeddings=OpenAIEmbeddings(openai_api_type='openai', openai_api_key=self.openai_api_key, 
                                            openai_api_base=self.openai_api_base, model=self.embedding_model)
            )
        else:
            logging.error("QDRANT API KEY OR URL NOT FOUND!")

        # Connect to AWS S3 file store
        if self.aws_access_key_id and self.aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.minio_url,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                config=Config(s3={'addressing_style': 'path'}),
            )
        else:
            logging.error("AWS ACCESS KEY ID OR SECRET ACCESS KEY NOT FOUND!")

        if self.posthog_api_key:
            self.posthog = Posthog(sync_mode=False, project_api_key=self.posthog_api_key,
                                   host='https://app.posthog.com')
        else:
            self.posthog = None
            print("POSTHOG API KEY NOT FOUND!")

        self.sql_session = SQLAlchemyIngestDB()

    def main_ingest(self, job_id: str, **inputs: Dict[str | List[str], Any]):
        """
        Main ingest function.
        """
        try:
            self.initialize_resources()

            course_name: List[str] | str = inputs.get('course_name', '')
            s3_paths: List[str] | str = inputs.get('s3_paths', '')
            url: List[str] | str | None = inputs.get('url', None)
            base_url: List[str] | str | None = inputs.get('base_url', None)
            readable_filename: List[str] | str = inputs.get('readable_filename', '')

            content: str | List[str] | None = inputs.get('content', None)  # defined if ingest type is webtext
            doc_groups: List[str] | str = inputs.get('groups', '')

            print(
                f"In top of /ingest route. course: {course_name}, s3paths: {s3_paths}, readable_filename: {readable_filename}, base_url: {base_url}, url: {url}, content: {content}, doc_groups: {doc_groups}"
            )
            success_fail_dict = self.run_ingest(course_name, s3_paths, base_url, url, readable_filename, content,
                                                doc_groups)
            for retry_num in range(1, 3):
                if isinstance(success_fail_dict, str):  # TODO: What does this indicate?
                    success_fail_dict = self.run_ingest(course_name, s3_paths, base_url, url, readable_filename, content,
                                                        doc_groups)
                    time.sleep(13 * retry_num)  # max is 65
                elif success_fail_dict['failure_ingest']:
                    logging.error(f"Ingest failure -- Retry attempt {retry_num}. File: {success_fail_dict}")
                    success_fail_dict = self.run_ingest(course_name, s3_paths, base_url, url, readable_filename, content,
                                                        doc_groups)
                    time.sleep(13 * retry_num)  # max is 65
                else:
                    break
            if success_fail_dict['failure_ingest']:
                logging.error(f"INGEST FAILURE -- About to send to database. success_fail_dict: {success_fail_dict}")

            # Remove from documents in progress
            self.sql_session.delete_document_in_progress(job_id)

            sentry_sdk.flush(timeout=20)
            return json.dumps(success_fail_dict)

        except Exception as e:
            logging.error("Error in main_ingest: ", e)
            sentry_sdk.capture_exception(e)
            success_fail_dict = {"failure_ingest": {'error': str(e)}}
            return json.dumps(success_fail_dict)

    def run_ingest(self, course_name, s3_paths, base_url, url, readable_filename, content, document_groups):
        """Routes ingest jobs based on the input data -> webscrape, url, readable_filename"""
        if content:
            return self.ingest_single_web_text(course_name, base_url, url, content, readable_filename,
                                               groups=document_groups)
        elif readable_filename == '':
            return self.bulk_ingest(course_name, s3_paths, base_url=base_url, url=url,
                                    groups=document_groups)
        else:
            return self.bulk_ingest(course_name, s3_paths, base_url=base_url, url=url,
                                    groups=document_groups, readable_filename=readable_filename)

    def bulk_ingest(self, course_name: str, s3_paths: Union[str, List[str]],
                  **kwargs) -> Dict[str, None | str | Dict[str, str]]:
        """Bulk ingest a list of s3 paths into the vectorstore, and also into the database."""
        print(f"Top of bulk_ingest: ", kwargs)

        def _ingest_single(ingest_method: Callable, s3_path, *args, **kwargs):
            """Handle running an arbitrary ingest function for an individual file."""
            ret = ingest_method(s3_path, *args, **kwargs)
            if ret == "Success":
                success_status['success_ingest'] = str(s3_path)
            else:
                success_status['failure_ingest'] = {'s3_path': str(s3_path), 'error': str(ret)}

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

        mimetype_ingest_methods = {
            'video': self._ingest_single_video,
            'audio': self._ingest_single_video,
            'text': self._ingest_single_txt,
            'image': self._ingest_single_image,
        }

        success_status: Dict[str, None | str | Dict[str, str]] = {"success_ingest": None, "failure_ingest": None}
        try:
            if isinstance(s3_paths, str):
                s3_paths = [s3_paths]

            for s3_path in s3_paths:
                file_extension = Path(s3_path).suffix
                with NamedTemporaryFile(suffix=file_extension) as tmpfile:
                    self.s3_client.download_fileobj(Bucket=self.s3_bucket_name, Key=s3_path, Fileobj=tmpfile)
                    mime_type = str(mimetypes.guess_type(tmpfile.name, strict=False)[0])
                    mime_category = mime_type.split('/')[0] if '/' in mime_type else mime_type

                # Ingest with specialized functions when possible, fallback to mimetype.
                if file_extension in file_ingest_methods:
                    ingest_method = file_ingest_methods[file_extension]
                    _ingest_single(ingest_method, s3_path, course_name, **kwargs)
                elif mime_category in mimetype_ingest_methods:
                    ingest_method = mimetype_ingest_methods[mime_category]
                    _ingest_single(ingest_method, s3_path, course_name, **kwargs)
                else:
                    # No supported ingest... Fallback to attempting utf-8 decoding, otherwise fail.
                    try:
                        self._ingest_single_txt(s3_path, course_name, **kwargs)
                        success_status['success_ingest'] = s3_path
                    except Exception as e:
                        err_msg = f"No ingest method for filetype: {file_extension} (with generic type {mime_type}), for file: {s3_path}"
                        success_status['failure_ingest'] = {
                            's3_path': s3_path,
                            'error': err_msg
                        }
                        if self.posthog:
                            self.posthog.capture('distinct_id_of_the_user', event='ingest_failure',
                                properties={
                                    'course_name': course_name,
                                    's3_path': s3_paths,
                                    'kwargs': kwargs,
                                    'error': err_msg
                                })
            return success_status
        except Exception as e:
            err = f"❌❌ Error in /ingest: `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc()  # type: ignore
            success_status['failure_ingest'] = {
                's3_path': s3_path,
                'error': f"MAJOR ERROR DURING INGEST: {err}"
            }
            if self.posthog:
                self.posthog.capture('distinct_id_of_the_user', event='ingest_failure',
                                    properties={
                                        'course_name': course_name,
                                        's3_path': s3_paths,
                                        'kwargs': kwargs,
                                        'error': err
                                    })
            sentry_sdk.capture_exception(e)
            return success_status

    def split_and_upload(self, texts: List[str], metadatas: List[Dict[str, Any]], **kwargs):
        """
        This is usually the last step of document ingest. Chunk & upload to Qdrant (and Supabase.. todo).
        Takes in Text and Metadata (from Langchain doc loaders) and splits / uploads to Qdrant.
        """
        logging.info(f"Split and upload invoked with {len(texts)} texts and {len(metadatas)} metadatas")
        if self.posthog:
            self.posthog.capture('distinct_id_of_the_user', event='split_and_upload_invoked',
                                 properties={
                                     'course_name': metadatas[0].get('course_name', None),
                                     's3_path': metadatas[0].get('s3_path', None),
                                     'readable_filename': metadatas[0].get('readable_filename', None),
                                     'url': metadatas[0].get('url', None),
                                     'base_url': metadatas[0].get('base_url', None),
                                 })
        assert len(texts) == len(metadatas), f'Text ({len(texts)}) and metadata ({len(metadatas)}) must be equal.'

        try:
            # try to split on paragraphs... fallback to sentences, then chars, ensure we always fit in context window
            text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
                chunk_size=1000,
                chunk_overlap=150,
                separators=["\n\n", "\n", ". ", " ", ""]
            )
            contexts: List[Document] = text_splitter.create_documents(texts=texts, metadatas=metadatas)
            input_texts = [{'input': context.page_content, 'model': self.embedding_model} for context in contexts]

            # Check for duplicates
            is_duplicate = self.check_for_duplicates(input_texts, metadatas)
            if is_duplicate:
                if self.posthog:
                    self.posthog.capture('distinct_id_of_the_user', event='split_and_upload_succeeded',
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
            for i, context in enumerate(contexts):
                context.metadata['chunk_index'] = i
                context.metadata['doc_groups'] = kwargs.get('groups', [])

            # Generate embeddings from OpenAI
            logging.info(f"Generating embeddings for {len(input_texts)} texts")
            embeddings_start_time = time.monotonic()
            oai = OpenAIAPIProcessor(
                input_prompts_list=input_texts,
                request_url=self.openai_api_base,
                api_key=self.openai_api_key,
                max_requests_per_minute=10_000,
                max_tokens_per_minute=10_000_000,
                token_encoding_name='cl100k_base',
                max_attempts=1_000,
                logging_level=logging.INFO,
                model=self.embedding_model)
            asyncio.run(oai.process_api_requests_from_file())
            print(f"⏰ embeddings runtime: {(time.monotonic() - embeddings_start_time):.2f} seconds")
            embeddings_dict: dict[str, List[float]] = {
                item[0]['input']: item[1]['data'][0]['embedding'] for item in oai.results
            }

            # Bulk upload to Qdrant
            vectors: list[PointStruct] = []
            for context in contexts:
                upload_metadata = {**context.metadata, "page_content": context.page_content}
                vectors.append(
                    PointStruct(id=str(uuid.uuid4()),
                                vector=embeddings_dict[context.page_content],
                                payload=upload_metadata)
                )
            try:
                self.qdrant_client.upsert(
                    collection_name=os.environ['QDRANT_COLLECTION_NAME'],  # type: ignore
                    points=vectors,  # type: ignore
                )
            except Exception as e:
                # it's fine if this gets timeout error. it will still post: https://github.com/qdrant/qdrant/issues/3654
                logging.warning("Update/upsert timeouts are fine, but errors might not be: ", e)
                pass

            # Supabase SQL insertion
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
            document_size_mb = len(json.dumps(document).encode('utf-8')) / (1024 * 1024)
            logging.info("Inserting document (size: %.2f MB)", document_size_mb)
            insert_status = self.sql_session.insert_document(document)
            if insert_status:
                groups = kwargs.get('groups', '')
                if groups:
                    if contexts[0].metadata.get('url'):
                        count = self.sql_session.add_document_to_group_url(contexts, groups)
                    else:
                        count = self.sql_session.add_document_to_group(contexts, groups)
                    if count == 0:
                        raise ValueError("Error in adding to doc groups")

            if self.posthog:
                self.posthog.capture('distinct_id_of_the_user', event='split_and_upload_succeeded',
                                     properties={
                                         'course_name': metadatas[0].get('course_name', None),
                                         's3_path': metadatas[0].get('s3_path', None),
                                         'readable_filename': metadatas[0].get('readable_filename', None),
                                         'url': metadatas[0].get('url', None),
                                         'base_url': metadatas[0].get('base_url', None),
                                         'is_duplicate': False,
                                     })
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
        course_name = metadatas[0]['course_name']
        incoming_s3_path = metadatas[0]['s3_path']
        url = metadatas[0]['url']

        if incoming_s3_path:
            # Check if uuid (v4) exists in s3_path (not all s3_paths have uuids) and remove if necessary
            incoming_filename = incoming_s3_path.split('/')[-1]
            logging.debug(f"Full filename: {incoming_filename}")
            pattern = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', re.I)
            if bool(pattern.search(incoming_filename)):
                original_filename = incoming_filename[37:]
            else:
                original_filename = incoming_filename
            logging.info(f"Filename after removing uuid: {original_filename}")

            contents = self.sql_session.get_like_docs_by_s3_path(course_name, original_filename)
            contents = contents['data']
            logging.info(
                f"No. of S3 path based records retrieved: {len(contents)}")  # multiple records can be retrieved: 3.pdf and 453.pdf
        elif url:
            original_filename = url
            contents = self.sql_session.get_like_docs_by_url(course_name, url)
            contents = contents['data']
            logging.info(f"No. of URL-based records retrieved: {len(contents)}")
        else:
            original_filename = None
            contents = []

        db_whole_text = ""
        exact_doc_exists = False
        if len(contents) > 0:  # a doc with same filename exists in Supabase
            logging.info(f"Checking for Supabase contents: {contents}")
            for record in contents:
                logging.info(f"Record: {record}")
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
                    contexts = record

                    exact_doc_exists = True
                    print("Exact doc exists in Supabase:", sql_filename)
                    break

            if exact_doc_exists:
                for text in contexts['contexts']:
                    db_whole_text += text['text']
                current_whole_text = ""
                for text in texts:
                    current_whole_text += text['input']

                if db_whole_text == current_whole_text:
                    logging.info(f"Duplicate detected: {original_filename}.")
                    return True
                else:
                    print(f"Updated file detected: {original_filename}")
                    print("older s3_path/url to be deleted: ", sql_filename)
                    if incoming_s3_path:
                        delete_status = self.delete_data(course_name, older_s3_path, '')
                    else:
                        delete_status = self.delete_data(course_name, '', url)
                return False
            else:
                print(f"NOT a duplicate: {original_filename}")
                return False

        else:
            print(f"NOT a duplicate: {original_filename}")
            return False

    def delete_data(self, course_name: str, s3_path: str, source_url: str):
        """Delete file from S3, Qdrant, and Supabase."""
        print(f"Deleting {s3_path} from S3, Qdrant, and Supabase for course {course_name}")
        try:
            if s3_path:
                try:
                    self.s3_client.delete_object(Bucket=self.s3_bucket_name, Key=s3_path)
                except Exception as e:
                    print("Error in deleting file from s3:", e)
                    sentry_sdk.capture_exception(e)
                # Delete from Qdrant
                # docs for nested keys: https://qdrant.tech/documentation/concepts/filtering/#nested-key
                try:
                    self.qdrant_client.delete(
                        collection_name=self.qdrant_collection_name,
                        points_selector=models.Filter(must=[
                            models.FieldCondition(
                                key="s3_path",
                                match=models.MatchValue(value=s3_path),
                            ),
                        ]),
                    )
                except Exception as e:
                    if "timed out" in str(e):
                        # Timed out still deletes: https://github.com/qdrant/qdrant/issues/3654#issuecomment-1955074525
                        pass
                    else:
                        print("Error in deleting file from Qdrant:", e)
                        sentry_sdk.capture_exception(e)
                        raise e

                try:
                    self.sql_session.delete_document_by_s3_path(course_name=course_name, s3_path=s3_path)
                except Exception as e:
                    print("Error in deleting file from database:", e)
                    sentry_sdk.capture_exception(e)

            # Delete files by their URL identifier
            elif source_url:
                try:
                    self.qdrant_client.delete(
                        collection_name=self.qdrant_collection_name,
                        points_selector=models.Filter(must=[
                            models.FieldCondition(
                                key="url",
                                match=models.MatchValue(value=source_url),
                            ),
                        ]),
                    )
                except Exception as e:
                    if "timed out" in str(e):
                        pass
                    else:
                        print("Error in deleting file from Qdrant:", e)
                        sentry_sdk.capture_exception(e)
                        raise e

                try:
                    self.sql_session.delete_document_by_url(course_name=course_name, url=source_url)
                except Exception as e:
                    print("Error in deleting file from database:", e)
                    sentry_sdk.capture_exception(e)

            return "Success"
        except Exception as e:
            err: str = f"ERROR IN delete_data: Traceback: {traceback.extract_tb(e.__traceback__)}❌❌ Error in {inspect.currentframe().f_code.co_name}:{e}"  # type: ignore
            sentry_sdk.capture_exception(e)
            return err

    def ingest_single_web_text(self, course_name: str, base_url: str, url: str, content: str, readable_filename: str,
                               **kwargs) -> Dict[str, None | str | Dict[str, str]]:
        """Crawlee integration"""
        if self.posthog:
            self.posthog.capture('distinct_id_of_the_user', event='ingest_single_web_text_invoked',
                                 properties={
                                     'course_name': course_name,
                                     'base_url': base_url,
                                     'url': url,
                                     'content': content,
                                     'title': readable_filename
                                 })
        success_or_failure: Dict[str, None | str | Dict[str, str]] = {"success_ingest": None, "failure_ingest": None}
        try:
            metadatas: List[Dict[str, Any]] = [{
                'course_name': course_name,
                's3_path': '',
                'readable_filename': readable_filename,
                'pagenumber': '',
                'timestamp': '',
                'url': url,
                'base_url': base_url,
            }]
            self.split_and_upload(texts=[content], metadatas=metadatas, **kwargs)
            if self.posthog:
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
            file_path = "media/" + file_name  # download from s3 to local folder for ingest

            self.s3_client.download_file(self.s3_bucket_name, s3_path, file_path)

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
            # print(texts)
            os.remove(file_path)

            success_or_failure = self.split_and_upload(texts=texts, metadatas=metadatas)
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
                self.s3_client.download_fileobj(Bucket=self.s3_bucket_name, Key=s3_path, Fileobj=tmpfile)
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
            err = f"❌❌ Error in (VTT ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc()
            print(err)
            sentry_sdk.capture_exception(e)
            return err

    def _ingest_html(self, s3_path: str, course_name: str, **kwargs) -> str:
        print(f"IN _ingest_html s3_path `{s3_path}` kwargs: {kwargs}")
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket_name, Key=s3_path)
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
            openai.api_key = os.getenv('OPENAI_API_KEY')
            transcript_list = []
            with NamedTemporaryFile(suffix=file_ext) as video_tmpfile:
                # download from S3 into an video tmpfile
                self.s3_client.download_fileobj(Bucket=self.s3_bucket_name, Key=s3_path, Fileobj=video_tmpfile)

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
                        # print(result.stdout.decode())
                        # print(result.stderr.decode())
                    except subprocess.CalledProcessError as e:
                        # print(e.stdout.decode())
                        # print(e.stderr.decode())
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
                self.s3_client.download_fileobj(Bucket=self.s3_bucket_name, Key=s3_path, Fileobj=tmpfile)

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
            response = self.s3_client.get_object(Bucket=self.s3_bucket_name, Key=s3_path)
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
                self.s3_client.download_fileobj(Bucket=self.s3_bucket_name, Key=s3_path, Fileobj=tmpfile)

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
            readable_filename = kwargs.get('readable_filename', Path(s3_path).name[37:])
            with NamedTemporaryFile(suffix="."+readable_filename.split(".")[-1]) as tmpfile:
                # download from S3 into pdf_tmpfile
                self.s3_client.download_fileobj(Bucket=self.s3_bucket_name, Key=s3_path, Fileobj=tmpfile)
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
                    'readable_filename': readable_filename,
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
                self.s3_client.download_fileobj(Bucket=self.s3_bucket_name, Key=s3_path, Fileobj=tmpfile)

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
                self.s3_client.download_fileobj(Bucket=self.s3_bucket_name, Key=s3_path, Fileobj=pdf_tmpfile)
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
                                self.s3_client.upload_fileobj(f, self.s3_bucket_name, s3_upload_path)

                    # Extract text
                    text = page.get_text().encode("utf8").decode("utf8",
                                                                 errors='ignore')  # get plain text (is in UTF-8)
                    pdf_pages_no_OCR.append(dict(text=text, page_number=i, readable_filename=Path(s3_path).name[37:]))

                metadatas: List[Dict[str, Any]] = [{
                    'course_name': course_name,
                    's3_path': s3_path,
                    'pagenumber': page['page_number'] + 1,  # +1 for human indexing
                    'timestamp': '',
                    'readable_filename': kwargs.get('readable_filename', page['readable_filename']),
                    'url': kwargs.get('url', ''),
                    'base_url': kwargs.get('base_url', ''),
                } for page in pdf_pages_no_OCR]

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

    def _ocr_pdf(self, s3_path: str, course_name: str, **kwargs):
        if self.posthog:
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
                self.s3_client.download_fileobj(Bucket=self.s3_bucket_name, Key=s3_path, Fileobj=pdf_tmpfile)
                pdf_tmpfile.flush()  # Ensure all data is written to disk
                pdf_tmpfile.seek(0)  # Reset file pointer to beginning

                with pdfplumber.open(pdf_tmpfile.name) as pdf:
                    # for page in :
                    for i, page in enumerate(pdf.pages):
                        im = page.to_image()
                        text = pytesseract.image_to_string(im.original)
                        print("Page number: ", i, "Text: ", text[:100])
                        pdf_pages_OCRed.append(
                            dict(text=text, page_number=i, readable_filename=Path(s3_path).name[37:]))

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
            if self.posthog:
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
            err = f"❌❌ Error in PDF ingest (with OCR): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc()
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
            response = self.s3_client.get_object(Bucket=self.s3_bucket_name, Key=s3_path)
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
            err = f"❌❌ Error in (TXT ingest): `{inspect.currentframe().f_code.co_name}`: {e}\nTraceback:\n", traceback.format_exc()
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
                # print("in ingest PPTX")
                self.s3_client.download_fileobj(Bucket=self.s3_bucket_name, Key=s3_path, Fileobj=tmpfile)

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
