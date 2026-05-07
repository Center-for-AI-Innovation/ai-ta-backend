import os
import logging
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

from injector import inject
from qdrant_client import QdrantClient, models
from qdrant_client.http.models import FieldCondition, MatchAny, MatchValue

logger = logging.getLogger(__name__)


def _process_pubmed_results(results, course_name):
    """Normalize pubmed search results into the standard payload format."""
    processed = []
    for result in results:
        try:
            result.payload["page_content"] = result.payload.get("page_content", "")
            result.payload["readable_filename"] = (
                "Pubmed: " + result.payload.get("readable_filename", "Unknown Pubmed Document")
            )
            result.payload["s3_path"] = "pubmed/" + result.payload.get("s3_path", "")
            result.payload["pagenumber"] = result.payload.get("pagenumber", 0)
            result.payload["course_name"] = course_name
            processed.append(result)
        except Exception as e:
            logger.warning("Error processing pubmed result: %s", e)
    return processed


def _process_patents_results(results, course_name):
    """Normalize patents search results into the standard payload format."""
    processed = []
    for result in results:
        try:
            result.payload["page_content"] = result.payload.get("text", "")
            s3_path = "patents/" + result.payload.get("s3_path", "unknown.txt")
            result.payload["readable_filename"] = "Patent: " + s3_path.split("/")[-1].replace(".txt", "")
            result.payload["course_name"] = course_name
            result.payload["url"] = result.payload.get("uspto_url", "")
            result.payload["s3_path"] = s3_path
            processed.append(result)
        except Exception as e:
            logger.warning("Error processing patents result: %s", e)
    return processed


def _process_ncbi_books_results(results, course_name):
    """Normalize NCBI books search results into the standard payload format."""
    processed = []
    for result in results:
        try:
            result.payload["page_content"] = result.payload.get("page_content", "")
            result.payload["readable_filename"] = (
                "NCBI Book: " + result.payload.get("readable_filename", "Unknown NCBI Document")
            )
            result.payload["s3_path"] = "ncbi-output/" + result.payload.get("s3_path", "")
            result.payload["course_name"] = course_name
            result.payload["pagenumber"] = result.payload.get("page_number", 0)
            result.payload["url"] = result.payload.get("url", None)
            processed.append(result)
        except Exception as e:
            logger.warning("Error processing NCBI books result: %s", e)
    return processed


def _process_clinical_trials_results(results, course_name):
    """Normalize clinical-trials search results into the standard payload format."""
    processed = []
    for result in results:
        try:
            result.payload["page_content"] = result.payload.get("text", "")
            s3_path = "clinical-trials/" + result.payload.get("s3_path", "unknown.txt")
            filename = os.path.basename(s3_path)
            readable_name = os.path.splitext(filename)[0] if filename else "Unknown Clinical Trial"
            result.payload["readable_filename"] = f"Clinical Trial: {readable_name}"
            result.payload["url"] = result.payload.get("url") or ""
            result.payload["s3_path"] = s3_path
            result.payload["course_name"] = course_name
            processed.append(result)
        except Exception as e:
            logger.warning("Error processing clinical trials result: %s", e)
    return processed


RESULT_PROCESSORS = {
    "pubmed": _process_pubmed_results,
    "patents": _process_patents_results,
    "ncbi_books": _process_ncbi_books_results,
    "clinical_trials": _process_clinical_trials_results,
}


class VectorDatabase:
    """
    Contains all methods for building and using vector databases.
    """

    @inject
    def __init__(self, qdrant_client: QdrantClient = None, qdrant_config: dict = None):
        """Initialize with an explicit client/config or create defaults from env vars.

        If no client is provided and the env doesn't have QDRANT_URL/QDRANT_API_KEY,
        leaves ``qdrant_client = None``. This is the pgvector-default deployment shape;
        callers must check ``ConnectionManager.get_vector_engine_kind()`` before using
        Qdrant-only methods, or be ready for ``self.qdrant_client`` to be ``None``.
        """
        if qdrant_client is not None:
            self.qdrant_client = qdrant_client
            self.qdrant_config = qdrant_config or {}
            return

        url = os.environ.get("QDRANT_URL")
        api_key = os.environ.get("QDRANT_API_KEY")
        if not url or not api_key:
            self.qdrant_client = None
            self.qdrant_config = {
                "default_collection": os.environ.get("QDRANT_COLLECTION_NAME", ""),
                "skip_quantization_rescore": True,
            }
            return

        self.qdrant_client = QdrantClient(
            url=url,
            api_key=api_key,
            port=int(port_str) if (port_str := os.getenv("QDRANT_PORT")) else None,
            timeout=20,
        )
        self.qdrant_config = {
            "default_collection": os.environ.get("QDRANT_COLLECTION_NAME", ""),
            "skip_quantization_rescore": True,
        }

    def execute_search(self, query_filter, query_vector, course_name: str, top_n: int = 100):
        """Config-driven search entry point.

        Always searches `default_collection`. If `collections` is also set,
        searches every entry too — `default_collection` is auto-prepended if
        its name is not already listed there. With one effective collection
        the single-collection path runs; with more, the multi-collection path
        runs.
        """
        default_name = self.qdrant_config.get("default_collection", "")
        extras = self.qdrant_config.get("collections") or []

        if not extras:
            return self._single_collection_search(query_filter, query_vector, top_n)

        listed_names = {c.get("name") for c in extras}
        effective = list(extras)
        if default_name and default_name not in listed_names:
            effective.insert(0, {"name": default_name})

        if len(effective) == 1:
            return self._single_collection_search(query_filter, query_vector, top_n)
        return self._multi_collection_search(
            effective, query_filter, query_vector, course_name, top_n
        )

    def _single_collection_search(self, query_filter, query_vector, top_n: int):
        """Standard search against the project's default collection."""
        search_kwargs = dict(
            collection_name=self.qdrant_config["default_collection"],
            query_filter=query_filter,
            with_vectors=False,
            query_vector=query_vector,
            limit=top_n,
        )
        if self.qdrant_config.get("skip_quantization_rescore"):
            search_kwargs["search_params"] = models.SearchParams(
                quantization=models.QuantizationSearchParams(rescore=False)
            )
        return self.qdrant_client.search(**search_kwargs)

    def _multi_collection_search(self, collections: list[dict],
                                  query_filter, query_vector, course_name: str,
                                  top_n: int):
        """Search multiple collections in parallel and combine results.

        Each entry in *collections* is a dict with keys:
            name (str): collection name
            top_n (int, optional): override for limit
            use_filter (bool, optional): whether to apply the course filter (default True)
            processor (str, optional): key into RESULT_PROCESSORS for post-processing
        """
        all_results = []

        def _search_one(col_cfg: dict):
            col_name = col_cfg["name"]
            col_top_n = col_cfg.get("top_n", top_n)
            use_filter = col_cfg.get("use_filter", True)
            effective_filter = query_filter if use_filter else None
            try:
                results = self.qdrant_client.search(
                    collection_name=col_name,
                    query_filter=effective_filter,
                    with_vectors=False,
                    query_vector=query_vector,
                    limit=col_top_n,
                )
                processor_key = col_cfg.get("processor")
                if processor_key and processor_key in RESULT_PROCESSORS:
                    results = RESULT_PROCESSORS[processor_key](results, course_name)
                return results
            except Exception as e:
                logger.warning("Error searching collection %s: %s", col_name, e)
                return []

        if self.qdrant_config.get("parallel", True) and len(collections) > 1:
            with ThreadPoolExecutor(max_workers=len(collections)) as executor:
                futures = {executor.submit(_search_one, cfg): cfg["name"] for cfg in collections}
                for future in as_completed(futures):
                    col_name = futures[future]
                    try:
                        all_results.extend(future.result())
                    except Exception as e:
                        logger.warning("Unexpected error searching %s: %s", col_name, e)
        else:
            for cfg in collections:
                all_results.extend(_search_one(cfg))

        if self.qdrant_config.get("sort_combined", True):
            all_results.sort(key=lambda x: x.score, reverse=True)

        return all_results

    def _create_search_filter(
        self,
        course_name: str,
        doc_groups: List[str],
        admin_disabled_doc_groups: List[str],
        public_doc_groups: List[dict],
    ) -> models.Filter:
        """
        Create search conditions for regular searches (no conversation filtering).
        Excludes chunks with any conversation_id.

        Args:
            course_name: The course/project name to filter by
            doc_groups: List of document groups to include
            admin_disabled_doc_groups: List of document groups to exclude
            public_doc_groups: List of public document groups that can be accessed
        """

        must_conditions = []
        should_conditions = []

        # Exclude admin-disabled doc_groups
        must_not_conditions = []
        if admin_disabled_doc_groups:
            must_not_conditions.append(
                FieldCondition(
                    key="doc_groups", match=MatchAny(any=admin_disabled_doc_groups)
                )
            )

        # For regular searches, only include chunks that have NO conversation_id field
        # This ensures we only get regular course chunks and prevents cross-conversation leaks
        must_conditions.append(
            models.IsEmptyCondition(
                is_empty={
                    "key": "conversation_id"
                }  # Only include chunks where conversation_id field is empty/missing
            )
        )

        # Handle public_doc_groups
        if public_doc_groups:
            for public_doc_group in public_doc_groups:
                if public_doc_group["enabled"]:
                    # Create a combined condition for each public_doc_group
                    combined_condition = models.Filter(
                        must=[
                            FieldCondition(
                                key="course_name",
                                match=MatchValue(value=public_doc_group["course_name"]),
                            ),
                            FieldCondition(
                                key="doc_groups",
                                match=MatchAny(any=[public_doc_group["name"]]),
                            ),
                        ]
                    )
                    should_conditions.append(combined_condition)

        # Handle user's own course documents
        own_course_condition = models.Filter(
            must=[
                FieldCondition(key="course_name", match=MatchValue(value=course_name))
            ]
        )

        # If specific doc_groups are specified
        if doc_groups and "All Documents" not in doc_groups:
            if own_course_condition.must:
                own_course_condition.must.append(
                    FieldCondition(key="doc_groups", match=MatchAny(any=doc_groups))
                )

        # Add the own_course_condition to should_conditions
        should_conditions.append(own_course_condition)

        # Construct the final filter (apply must to enforce no conversation_id)
        vector_search_filter = models.Filter(
            must=must_conditions, should=should_conditions, must_not=must_not_conditions
        )

        print(f"Vector search filter: {vector_search_filter}")
        return vector_search_filter

    def _create_conversation_search_filter(self, conversation_id: str) -> models.Filter:
        """
        Create search conditions for conversation-specific chunks.
        Only includes chunks with the specified conversation_id.

        Args:
            conversation_id: The specific conversation ID to filter by
        """

        must_conditions = []

        # Conversation ID filter - this is sufficient since conversation_id is unique
        must_conditions.append(
            FieldCondition(
                key="conversation_id", match=MatchValue(value=conversation_id)
            )
        )

        return models.Filter(must=must_conditions)

    def _create_conversation_filter(self, conversation_id: str) -> models.Filter:
        """
        Create a filter for conversation-specific documents.
        """
        return models.Filter(
            must=[
                FieldCondition(
                    key="conversation_id", match=MatchValue(value=conversation_id)
                )
            ]
        )

    def _combine_filters(
        self, search_filter: models.Filter, conversation_filter: models.Filter = None
    ) -> models.Filter:
        """
        Combine search filter with conversation filter using AND logic.

        Args:
            search_filter: The main search filter (course_name, doc_groups, etc.)
            conversation_filter: The conversation-specific filter (optional)

        Returns:
            Combined filter using AND logic for security
        """
        combined_conditions = []

        # Add conditions from search filter
        if search_filter.must:
            combined_conditions.extend(search_filter.must)

        # Add conditions from conversation filter if provided
        if conversation_filter and conversation_filter.must:
            combined_conditions.extend(conversation_filter.must)

        # Combine must_not conditions
        combined_must_not = []
        if search_filter.must_not:
            combined_must_not.extend(search_filter.must_not)
        if conversation_filter and conversation_filter.must_not:
            combined_must_not.extend(conversation_filter.must_not)

        return models.Filter(must=combined_conditions, must_not=combined_must_not)

    def delete(self, collection_name: str, key: str, value: str):
        """Delete points matching a field condition from the given collection."""
        return self.qdrant_client.delete(
            collection_name=collection_name,
            wait=True,
            points_selector=models.Filter(
                must=[
                    models.FieldCondition(
                        key=key,
                        match=models.MatchValue(value=value),
                    ),
                ]
            ),
        )

    def upsert(self, points: list):
        """Upsert points into the default collection."""
        return self.qdrant_client.upsert(
            collection_name=self.qdrant_config["default_collection"],
            points=points,
            wait=True,
        )
