"""
VectorDatabase: per-project vector store wrapper supporting Qdrant and pgvector.

Routing rule (resolved by ``ConnectionManager.get_vector_db``):
    1. Project has an active non-null ``qdrant_config``  -> external Qdrant
    2. Else ``VECTOR_ENGINE == 'qdrant'`` env             -> shared Qdrant
    3. Else                                                -> host pgvector

When this instance is constructed with an explicit ``qdrant_client`` (case 1
above) it behaves as a project-specific Qdrant wrapper. When constructed with
no explicit client (the default-injected instance held by ``ConnectionManager``)
it tries to bring up a shared Qdrant client from env vars; if those aren't
present, ``qdrant_client`` stays ``None`` and the pgvector store is used for
upsert/delete via ``pgvector_store``.

The legacy direct-client special cases from main (cropwizard, vyriad, pubmed,
patents) are no longer hardcoded against ad-hoc clients -- per-project Qdrant
configuration drives them via the resolver. Convenience helpers are kept for
backward compatibility but now operate on ``self.qdrant_client`` so the right
client is used automatically.
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from injector import inject
from qdrant_client import QdrantClient, models
from qdrant_client.http.models import FieldCondition, MatchAny, MatchValue

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result post-processors for multi-collection / specialty searches.
# ---------------------------------------------------------------------------


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

    May be constructed two ways:
      * Default injection (no args): brings up the shared Qdrant from env if
        QDRANT_URL/QDRANT_API_KEY are set; otherwise leaves ``qdrant_client = None``
        and prepares a pgvector store for upsert/delete.
      * Explicit per-project: ``VectorDatabase(qdrant_client=..., qdrant_config=...)``
        produced by ``ConnectionManager.get_vector_db`` for projects with an
        external Qdrant override.
    """

    @inject
    def __init__(self, qdrant_client: QdrantClient = None, qdrant_config: dict = None):
        """Initialize with an explicit client/config or create defaults from env vars.

        Engine selection precedence handled here:
          * Explicit ``qdrant_client`` (per-project override)  -> use it.
          * env ``QDRANT_URL`` + ``QDRANT_API_KEY`` present     -> build shared Qdrant.
          * Otherwise                                           -> ``qdrant_client``
            stays ``None``; pgvector is the default. The pgvector store is
            lazily resolved on first use to avoid forcing the psycopg2 import
            on pure-Qdrant deployments.
        """
        # Lazily-resolved pgvector handle (only built for engine=pgvector path).
        self._pgvector_store = None

        if qdrant_client is not None:
            self.qdrant_client = qdrant_client
            self.qdrant_config = qdrant_config or {}
            return

        url = os.environ.get("QDRANT_URL")
        api_key = os.environ.get("QDRANT_API_KEY")
        if not url or not api_key:
            # No Qdrant credentials -> pgvector-default deployment.
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

    # ------------------------------------------------------------------
    # pgvector helpers
    # ------------------------------------------------------------------

    @property
    def pgvector_store(self):
        """Lazy accessor for the host pgvector store.

        Imported on first access so deployments running pure-Qdrant don't pay
        the psycopg2 import cost at startup.
        """
        if self._pgvector_store is None:
            from ai_ta_backend.database.vector_store import get_vector_store
            self._pgvector_store = get_vector_store()
        return self._pgvector_store

    def _using_pgvector(self) -> bool:
        """True iff this instance has no Qdrant client and should fall back to pgvector."""
        return self.qdrant_client is None

    # ------------------------------------------------------------------
    # Search (Qdrant-backed; the config drives single- vs multi-collection)
    # ------------------------------------------------------------------

    def execute_search(self, query_filter, query_vector, course_name: str, top_n: int = 100):
        """Config-driven search entry point.

        Always searches ``default_collection``. If ``collections`` is also set,
        searches every entry too -- ``default_collection`` is auto-prepended if
        its name is not already listed there. With one effective collection
        the single-collection path runs; with more, the multi-collection path
        runs.

        Requires a Qdrant client; on a pgvector-default instance this raises so
        callers branch on ``ConnectionManager.get_vector_engine_kind`` first.
        """
        if self._using_pgvector():
            raise RuntimeError(
                "execute_search called on a pgvector-default VectorDatabase. "
                "Resolve a Qdrant-backed VectorDatabase via ConnectionManager, "
                "or branch on ConnectionManager.get_vector_engine_kind first."
            )

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

    # ------------------------------------------------------------------
    # Filter builders (Qdrant filter shape; reused by retrieval_service)
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Backward-compatible thin wrappers around qdrant_client.
    # Previously dispatched to hardcoded shared clients
    # (vyriad_qdrant_client, cropwizard_qdrant_client). With per-project
    # routing they now always operate on ``self.qdrant_client`` -- the
    # resolver hands back the right instance.
    # ------------------------------------------------------------------

    def vector_search(self, search_query, course_name, doc_groups: List[str],
                      user_query_embedding, top_n, disabled_doc_groups: List[str],
                      public_doc_groups: List[dict]):
        """Standard course search against the default collection."""
        return self.qdrant_client.search(
            collection_name=self.qdrant_config.get(
                "default_collection", os.environ.get("QDRANT_COLLECTION_NAME", "")
            ),
            query_filter=self._create_search_filter(
                course_name, doc_groups, disabled_doc_groups, public_doc_groups
            ),
            with_vectors=False,
            query_vector=user_query_embedding,
            limit=top_n,
            search_params=models.SearchParams(
                quantization=models.QuantizationSearchParams(rescore=False)
            ),
        )

    def vector_search_with_filter(self, search_query, course_name, doc_groups: List[str],
                                  user_query_embedding, top_n, disabled_doc_groups: List[str],
                                  public_doc_groups: List[dict], custom_filter: models.Filter):
        """Search the default collection with a caller-supplied filter."""
        return self.qdrant_client.search(
            collection_name=self.qdrant_config.get(
                "default_collection", os.environ.get("QDRANT_COLLECTION_NAME", "")
            ),
            query_filter=custom_filter,
            with_vectors=False,
            query_vector=user_query_embedding,
            limit=top_n,
            search_params=models.SearchParams(
                quantization=models.QuantizationSearchParams(rescore=False)
            ),
        )

    # ------------------------------------------------------------------
    # Mutations (engine-aware)
    # ------------------------------------------------------------------

    def delete(self, collection_name: str, key: str, value: str):
        """Delete points matching a field condition.

        On a Qdrant-backed instance this runs against the provided
        ``collection_name``. On a pgvector-default instance this falls back to
        ``PgVectorStore.delete_by_filter``; ``collection_name`` is ignored
        because pgvector has a single embeddings table.
        """
        if self._using_pgvector():
            return self.pgvector_store.delete_by_filter(key, value)
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

    def delete_data(self, collection_name: str, key: str, value: str):
        """Engine-aware delete by field condition.

        Backward-compatible name for ``delete``. On pgvector, ``collection_name``
        is ignored; the host main embeddings table is the only target.
        """
        return self.delete(collection_name, key, value)

    def upsert(self, points: list):
        """Upsert points into the default collection (Qdrant only).

        For pgvector use ``upsert_main_collection``; this method requires a
        Qdrant client.
        """
        if self._using_pgvector():
            raise RuntimeError(
                "upsert() requires a Qdrant client. For pgvector ingest use "
                "upsert_main_collection(ids, vectors, payloads)."
            )
        return self.qdrant_client.upsert(
            collection_name=self.qdrant_config["default_collection"],
            points=points,
            wait=True,
        )

    def upsert_main_collection(self, ids: List[str], vectors: List[List[float]],
                                payloads: List[dict], wait: bool = True):
        """Upsert points into the main collection.

        Engine-aware: routes to pgvector when the host is configured for it,
        otherwise to Qdrant.
        """
        if self._using_pgvector():
            self.pgvector_store.upsert_batch(
                ids=ids, vectors=vectors, payloads=payloads, wait=wait
            )
            return
        points = [
            models.PointStruct(id=pid, vector=vec, payload=pl)
            for pid, vec, pl in zip(ids, vectors, payloads)
        ]
        self.qdrant_client.upsert(
            collection_name=self.qdrant_config.get(
                "default_collection", os.environ.get("QDRANT_COLLECTION_NAME", "")
            ),
            points=points,
            wait=wait,
        )
