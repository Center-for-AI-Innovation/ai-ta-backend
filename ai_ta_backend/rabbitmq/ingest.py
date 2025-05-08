from __future__ import annotations
import os
from typing import Any, Callable, Dict, List, Optional, Union, cast


class Ingest:
    """
    Class for ingesting documents into the vector database.
    """

    def __init__(self):
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.qdrant_url = os.getenv('QDRANT_URL')
        self.qdrant_api_key = os.getenv('QDRANT_API_KEY')
        self.qdrant_collection_name = os.getenv('QDRANT_COLLECTION_NAME')
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.posthog_api_key = os.getenv('POSTHOG_API_KEY')
        self.posthog = None

    def main_ingest(self, **inputs: Dict[str | List[str], Any]):
        """
        Main ingest function.
        """
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
