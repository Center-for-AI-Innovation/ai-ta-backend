import os
from typing import List

import requests
from injector import inject
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.vectorstores import Qdrant
from qdrant_client import QdrantClient, models
from qdrant_client.http.models import FieldCondition, MatchAny, MatchValue


class VectorDatabase():
  """
  Contains all methods for building and using vector databases.
  """

  @inject
  def __init__(self):
    """
    Initialize AWS S3, Qdrant, and Supabase.
    """
    # vector DB
    self.qdrant_client = QdrantClient(
        url=os.environ['QDRANT_URL'],
        api_key=os.environ['QDRANT_API_KEY'],
        port=os.getenv('QDRANT_PORT') if os.getenv('QDRANT_PORT') else None,
        timeout=20,  # default is 5 seconds. Getting timeout errors w/ document groups.
    )

    self.vyriad_qdrant_client = QdrantClient(url=os.environ['VYRIAD_QDRANT_URL'],
                                             port=int(os.environ['VYRIAD_QDRANT_PORT']),
                                             https=True,
                                             api_key=os.environ['VYRIAD_QDRANT_API_KEY'])

    try:
      # No major uptime guarantees
      self.cropwizard_qdrant_client = QdrantClient(url="https://cropwizard-qdrant.ncsa.ai",
                                                   port=443,
                                                   https=True,
                                                   api_key=os.environ['QDRANT_API_KEY'])
    except Exception as e:
      print(f"Error in cropwizard_qdrant_client: {e}")
      self.cropwizard_qdrant_client = None

    self.vectorstore = Qdrant(client=self.qdrant_client,
                              collection_name=os.environ['QDRANT_COLLECTION_NAME'],
                              embeddings=OpenAIEmbeddings(openai_api_key=os.environ['VLADS_OPENAI_KEY']))

  def vector_search(self, search_query, course_name, doc_groups: List[str], user_query_embedding, top_n,
                    disabled_doc_groups: List[str], public_doc_groups: List[dict]):
    """
    Search the vector database for a given query.
    """
    # Search the vector database
    search_results = self.qdrant_client.search(
        collection_name=os.environ['QDRANT_COLLECTION_NAME'],
        query_filter=self._create_search_filter(course_name, doc_groups, disabled_doc_groups, public_doc_groups),
        with_vectors=False,
        query_vector=user_query_embedding,
        limit=top_n,  # Return n closest points
        # In a system with high disk latency, the re-scoring step may become a bottleneck: https://qdrant.tech/documentation/guides/quantization/
        search_params=models.SearchParams(quantization=models.QuantizationSearchParams(rescore=False)))
    # print(f"Search results: {search_results}")
    return search_results

  def cropwizard_vector_search(self, search_query, course_name, doc_groups: List[str], user_query_embedding, top_n,
                               disabled_doc_groups: List[str], public_doc_groups: List[dict]):
    """
    Search the vector database for a given query.
    """
    top_n = 120

    search_results = self.cropwizard_qdrant_client.search(
        collection_name='cropwizard',
        query_filter=self._create_search_filter(course_name, doc_groups, disabled_doc_groups, public_doc_groups),
        with_vectors=False,
        query_vector=user_query_embedding,
        limit=top_n,  # Return n closest points
    )

    return search_results
    
  def pubmed_vector_search(self, search_query, course_name, doc_groups: List[str], user_query_embedding, top_n,
                           disabled_doc_groups: List[str], public_doc_groups: List[dict]):
    """
    Search the vector database for a given query.
    """
    # top_n = 10
    # Search the vector database
    search_results = self.vyriad_qdrant_client.search(
        collection_name='embedding',  # Pubmed embeddings
        with_vectors=False,
        query_vector=user_query_embedding,
        limit=120,  # Return n closest points
    )

    # Post-process the Qdrant results (hydrate the vectors with the full text from SQL)
    try:
      # Get context IDs from search results
      context_ids = [result.payload['context_id'] for result in search_results]

      # Call API to get text for all context IDs in bulk
      api_url = "https://pubmed-db-query.kastan.ai/getTextFromContextIDBulk"
      response = requests.post(api_url, json={"ids": context_ids}, timeout=30)

      if not response.ok:
        print(f"Error in bulk API request: {response.status_code}")
        return []

      # Create mapping of context_id to text from response
      context_texts = response.json()

      # Update search results with texts from bulk response
      updated_results = []
      for result in search_results:
        context_id = result.payload['context_id']
        if context_id in context_texts:
          result.payload['page_content'] = context_texts[context_id]['page_content']
          result.payload['readable_filename'] = context_texts[context_id]['readable_filename']
          result.payload['s3_path'] = str(result.payload['minio_path']).replace('pubmed/', '')  # remove bucket name
          result.payload['course_name'] = course_name
          updated_results.append(result)

      return updated_results

    except Exception as e:
      print(f"Error in pubmed_vector_search: {e}")
      return []

  def vyriad_vector_search(self, search_query, course_name, doc_groups: List[str], user_query_embedding, top_n,
                           disabled_doc_groups: List[str], public_doc_groups: List[dict]):
    """
    Search the vector database for a given query.
    """
    # top_n = 10
    # Search the vector database
    search_results = self.vyriad_qdrant_client.search(
        collection_name='embedding',  # Pubmed embeddings
        with_vectors=False,
        query_vector=user_query_embedding,
        limit=100,  # Return n closest points
    )

    # Post-process the Qdrant results (hydrate the vectors with the full text from SQL)
    try:
      # Get context IDs from search results
      context_ids = [result.payload['context_id'] for result in search_results]

      # Call API to get text for all context IDs in bulk
      api_url = "https://pubmed-db-query.kastan.ai/getTextFromContextIDBulk"
      response = requests.post(api_url, json={"ids": context_ids}, timeout=30)

      if not response.ok:
        print(f"Error in bulk API request: {response.status_code}")
        return []

      # Create mapping of context_id to text from response
      context_texts = response.json()

      # Update search results with texts from bulk response
      updated_results = []
      for result in search_results:
        context_id = result.payload['context_id']
        if context_id in context_texts:
          result.payload['page_content'] = context_texts[context_id]['page_content']
          result.payload['readable_filename'] = context_texts[context_id]['readable_filename']
          result.payload['s3_path'] = str(result.payload['minio_path']).replace('pubmed/', '')  # remove bucket name
          result.payload['course_name'] = course_name
          updated_results.append(result)

      # return updated_results

      # ----- Do Prime KG retrieval -----

      prime_kg_triplets = self.vyriad_qdrant_client.search(
          collection_name='prime_kg_nomic',  # Pubmed embeddings
          with_vectors=False,
          query_vector=user_query_embedding,
          limit=20,  # not so many KG triplets
      )

      for result in prime_kg_triplets:
        result.payload['page_content'] = result.payload["triplet_string"]
        result.payload['readable_filename'] = result.payload["triplet"]
        result.payload['course_name'] = course_name

      return updated_results + prime_kg_triplets

    except Exception as e:
      print(f"Error in _vyriad_special_case: {e}")
      return []

  def _create_search_filter(self, course_name: str, doc_groups: List[str], admin_disabled_doc_groups: List[str],
                            public_doc_groups: List[dict]) -> models.Filter:
    """
    Create search conditions for the vector search.
    """

    must_conditions = []
    should_conditions = []

    # Exclude admin-disabled doc_groups
    must_not_conditions = []
    if admin_disabled_doc_groups:
      must_not_conditions.append(FieldCondition(key='doc_groups', match=MatchAny(any=admin_disabled_doc_groups)))

    # Handle public_doc_groups
    if public_doc_groups:
      for public_doc_group in public_doc_groups:
        if public_doc_group['enabled']:
          # Create a combined condition for each public_doc_group
          combined_condition = models.Filter(must=[
              FieldCondition(key='course_name', match=MatchValue(value=public_doc_group['course_name'])),
              FieldCondition(key='doc_groups', match=MatchAny(any=[public_doc_group['name']]))
          ])
          should_conditions.append(combined_condition)

    # Handle user's own course documents
    own_course_condition = models.Filter(must=[FieldCondition(key='course_name', match=MatchValue(value=course_name))])

    # If specific doc_groups are specified
    if doc_groups and 'All Documents' not in doc_groups:
      own_course_condition.must.append(FieldCondition(key='doc_groups', match=MatchAny(any=doc_groups)))

    # Add the own_course_condition to should_conditions
    should_conditions.append(own_course_condition)

    # Construct the final filter
    vector_search_filter = models.Filter(should=should_conditions, must_not=must_not_conditions)

    print(f"Vector search filter: {vector_search_filter}")
    return vector_search_filter

  def delete_data(self, collection_name: str, key: str, value: str):
    """
    Delete data from the vector database.
    """
    return self.qdrant_client.delete(
        collection_name=collection_name,
        wait=True,
        points_selector=models.Filter(must=[
            models.FieldCondition(
                key=key,
                match=models.MatchValue(value=value),
            ),
        ]),
    )

  def delete_data_cropwizard(self, key: str, value: str):
    """
    Delete data from the vector database.
    """
    return self.cropwizard_qdrant_client.delete(
        collection_name='cropwizard',
        wait=True,
        points_selector=models.Filter(must=[
            models.FieldCondition(
                key=key,
                match=models.MatchValue(value=value),
            ),
        ]),
    )
