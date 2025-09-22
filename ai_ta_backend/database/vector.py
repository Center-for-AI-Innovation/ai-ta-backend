import os
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

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
                              embeddings=OpenAIEmbeddings(openai_api_key=os.environ['OPENAI_API_KEY']))

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

  def patents_vector_search(self, search_query, course_name, doc_groups: List[str], user_query_embedding, top_n,
                            disabled_doc_groups: List[str], public_doc_groups: List[dict]):
    """
    Search the vector database for a given query.
    """
    top_n = 120

    search_results = self.vyriad_qdrant_client.search(
        collection_name='patents',  # Patents embeddings
        with_vectors=False,
        query_vector=user_query_embedding,
        limit=top_n,  # Return n closest points
    )

    # Post-process the Qdrant results, format the results
    try:
      updated_results = []
      for result in search_results:
        result.payload['page_content'] = result.payload['text']
        result.payload['readable_filename'] = "Patent: " + result.payload['s3_path'].split("/")[-1].replace('.txt', '')
        result.payload['course_name'] = course_name
        result.payload['url'] = result.payload['uspto_url']
        result.payload['s3_path'] = result.payload['s3_path']
        updated_results.append(result)
      return updated_results

    except Exception as e:
      print(f"Error in patents_vector_search: {e}")
      return []

  def pubmed_vector_search(self, search_query, course_name, doc_groups: List[str], user_query_embedding, top_n,
                           disabled_doc_groups: List[str], public_doc_groups: List[dict]):
    """
    Search the vector database for a given query.
    """
    # Search the vector database
    search_results = self.vyriad_qdrant_client.search(
        collection_name='pubmed',  # Pubmed embeddings
        with_vectors=False,
        query_vector=user_query_embedding,
        limit=120,  # Return n closest points
    )

    # Post process the Qdrant results, format the results
    try:
      updated_results = []
      for result in search_results:
        result.payload['page_content'] = result.payload['page_content']
        result.payload['readable_filename'] = result.payload['readable_filename']
        result.payload['s3_path'] = result.payload['s3_path']
        result.payload['pagenumber'] = result.payload['pagenumber']
        result.payload['course_name'] = course_name
        updated_results.append(result)
      return updated_results

    except Exception as e:
      print(f"Error in pubmed_vector_search: {e}")
      return []

  def vyriad_vector_search(self, search_query, course_name, doc_groups: List[str], user_query_embedding, top_n,
                           disabled_doc_groups: List[str], public_doc_groups: List[dict]):
    """
    Search the vector database for a given query, combining results from both pubmed and patents collections.
    """
    top_n = 50

    def search_pubmed():
      """Search pubmed collection with error handling"""
      try:
        results = self.vyriad_qdrant_client.search(
            collection_name='pubmed',
            with_vectors=False,
            query_vector=user_query_embedding,
            limit=top_n,
        )
        print(f"Pubmed search completed successfully with {len(results)} results")
        return ('pubmed', results, None)
      except Exception as e:
        error_msg = f"Error in pubmed vector search: {str(e)}"
        print(error_msg)
        return ('pubmed', [], error_msg)

    def search_patents():
      """Search patents collection with error handling"""
      try:
        results = self.vyriad_qdrant_client.search(
            collection_name='patents',
            with_vectors=False,
            query_vector=user_query_embedding,
            limit=top_n,
        )
        print(f"Patents search completed successfully with {len(results)} results")
        return ('patents', results, None)
      except Exception as e:
        error_msg = f"Error in patents vector search: {str(e)}"
        print(error_msg)
        return ('patents', [], error_msg)

    def search_ncbi_books():
      """Search ncbi books collection with error handling"""
      try:
        results = self.vyriad_qdrant_client.search(
            collection_name='ncbi_pdfs',
            with_vectors=False,
            query_vector=user_query_embedding,
            limit=100,
        )
        print(f"NCBI books search completed successfully with {len(results)} results")
        return ('ncbi_books', results, None)
      except Exception as e:
        error_msg = f"Error in NCBI books vector search: {str(e)}"
        print(error_msg)
        return ('ncbi_books', [], error_msg)

    def search_clinicaltrials():
      """Search clinicaltrials collection with error handling"""
      try:
        results = self.vyriad_qdrant_client.search(
            collection_name='clinical-file',
            with_vectors=False,
            query_vector=user_query_embedding,
            limit=top_n,
        )
        print(f"Clinical trials search completed successfully with {len(results)} results")
        return ('clinicaltrials', results, None)
      except Exception as e:
        error_msg = f"Error in clinical trials vector search: {str(e)}"
        print(error_msg)
        return ('clinicaltrials', [], error_msg)

    # Execute all searches in parallel
    search_functions = [search_pubmed, search_patents, search_ncbi_books, search_clinicaltrials]
    search_results = {}
    search_errors = []

    with ThreadPoolExecutor(max_workers=4) as executor:
      # Submit all search tasks
      future_to_search = {executor.submit(func): func.__name__ for func in search_functions}

      # Collect results as they complete
      for future in as_completed(future_to_search):
        search_name = future_to_search[future]
        try:
          search_type, results, error = future.result()
          search_results[search_type] = results
          if error:
            search_errors.append(error)
        except Exception as e:
          error_msg = f"Unexpected error in {search_name}: {str(e)}"
          print(error_msg)
          search_errors.append(error_msg)
          search_results[search_name.replace('search_', '')] = []

    # Extract results from the dictionary
    pubmed_results = search_results.get('pubmed', [])
    patents_results = search_results.get('patents', [])
    ncbi_books_results = search_results.get('ncbi_books', [])
    clinicaltrials_results = search_results.get('clinicaltrials', [])

    # Log any errors that occurred
    if search_errors:
      print(f"Search errors encountered: {search_errors}")

    print(
        f"Total results - Pubmed: {len(pubmed_results)}, Patents: {len(patents_results)}, NCBI Books: {len(ncbi_books_results)}, Clinical Trials: {len(clinicaltrials_results)}"
    )

    def process_pubmed_results(results):
      """Process pubmed results with error handling"""
      try:
        updated_results = []
        for result in results:
          result.payload['page_content'] = result.payload.get('page_content', '')
          result.payload['readable_filename'] = "Pubmed: " + result.payload.get('readable_filename',
                                                                                'Unknown Pubmed Document')
          result.payload['s3_path'] = 'pubmed/' + result.payload.get('s3_path', '')
          result.payload['pagenumber'] = result.payload.get('pagenumber', 0)
          result.payload['course_name'] = course_name
          updated_results.append(result)
        print(f"Processed {len(updated_results)} pubmed results successfully")
        return ('pubmed', updated_results, None)
      except Exception as e:
        error_msg = f"Error processing pubmed results: {str(e)}"
        print(error_msg)
        return ('pubmed', [], error_msg)

    def process_patents_results(results):
      """Process patents results with error handling"""
      try:
        updated_results = []
        for result in results:
          result.payload['page_content'] = result.payload.get('text', '')
          s3_path = 'patents/' + result.payload.get('s3_path', 'unknown.txt')
          result.payload['readable_filename'] = "Patent: " + s3_path.split("/")[-1].replace('.txt', '')
          result.payload['course_name'] = course_name
          result.payload['url'] = result.payload.get('uspto_url', '')
          result.payload['s3_path'] = s3_path
          updated_results.append(result)
        print(f"Processed {len(updated_results)} patents results successfully")
        return ('patents', updated_results, None)
      except Exception as e:
        error_msg = f"Error processing patents results: {str(e)}"
        print(error_msg)
        return ('patents', [], error_msg)

    def process_ncbi_books_results(results):
      """Process ncbi books results with error handling"""
      try:
        updated_results = []
        for result in results:
          result.payload['page_content'] = result.payload.get('page_content', '')
          result.payload['readable_filename'] = "NCBI Book: " + result.payload.get('readable_filename',
                                                                                   'Unknown NCBI Document')
          result.payload['s3_path'] = 'ncbi-output/' + result.payload.get('s3_path', '')
          result.payload['course_name'] = course_name
          result.payload['pagenumber'] = result.payload.get('page_number', 0)
          # Use .get() to safely access 'url' field with None as default
          result.payload['url'] = result.payload.get('url', None)
          updated_results.append(result)
        print(f"Processed {len(updated_results)} NCBI books results successfully")
        return ('ncbi_books', updated_results, None)
      except Exception as e:
        error_msg = f"Error processing NCBI books results: {str(e)}"
        print(error_msg)
        return ('ncbi_books', [], error_msg)

    def process_clinicaltrials_results(results):
      """Process clinicaltrials results with error handling"""
      try:
        updated_results = []
        for result in results:
          result.payload['page_content'] = result.payload.get('text', '')
          s3_path = 'clinical-trials/' + result.payload.get('s3_path', 'unknown.txt')
          result.payload['readable_filename'] = "Clinical Trial: " + s3_path.split("/")[-1].replace('.txt', '')
          result.payload['url'] = result.payload.get('uspto_url', '')
          result.payload['s3_path'] = s3_path
          result.payload['course_name'] = course_name
          updated_results.append(result)
        print(f"Processed {len(updated_results)} clinical trials results successfully")
        return ('clinicaltrials', updated_results, None)
      except Exception as e:
        error_msg = f"Error processing clinical trials results: {str(e)}"
        print(error_msg)
        return ('clinicaltrials', [], error_msg)

    # Process all results in parallel
    processing_functions_and_data = [(process_pubmed_results, pubmed_results),
                                     (process_patents_results, patents_results),
                                     (process_ncbi_books_results, ncbi_books_results),
                                     (process_clinicaltrials_results, clinicaltrials_results)]

    processed_results = {}
    processing_errors = []

    with ThreadPoolExecutor(max_workers=4) as executor:
      # Submit all processing tasks
      future_to_processor = {executor.submit(func, data): func.__name__ for func, data in processing_functions_and_data}

      # Collect processed results as they complete
      for future in as_completed(future_to_processor):
        processor_name = future_to_processor[future]
        try:
          result_type, processed_data, error = future.result()
          processed_results[result_type] = processed_data
          if error:
            processing_errors.append(error)
        except Exception as e:
          error_msg = f"Unexpected error in {processor_name}: {str(e)}"
          print(error_msg)
          processing_errors.append(error_msg)
          # Extract result type from processor name
          result_type = processor_name.replace('process_', '').replace('_results', '')
          processed_results[result_type] = []

    # Extract processed results
    updated_pubmed_results = processed_results.get('pubmed', [])
    updated_patents_results = processed_results.get('patents', [])
    updated_ncbi_books_results = processed_results.get('ncbi_books', [])
    updated_clinicaltrials_results = processed_results.get('clinicaltrials', [])

    # Log any processing errors
    if processing_errors:
      print(f"Processing errors encountered: {processing_errors}")

    # Combine results
    combined_results = updated_pubmed_results + updated_patents_results + updated_ncbi_books_results + updated_clinicaltrials_results

    # Sort combined results by score (higher score = better match)
    combined_results.sort(key=lambda x: x.score, reverse=True)

    print(f"Final combined results: {len(combined_results)} total documents")

    # Return combined results (remove the top_n limit to return all results)
    return combined_results

  def _create_search_filter(self, course_name: str, doc_groups: List[str], admin_disabled_doc_groups: List[str],
                            public_doc_groups: List[dict]) -> models.Filter:
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
      must_not_conditions.append(FieldCondition(key='doc_groups', match=MatchAny(any=admin_disabled_doc_groups)))

    # For regular searches, only include chunks that have NO conversation_id field
    # This ensures we only get regular course chunks and prevents cross-conversation leaks
    must_conditions.append(models.IsEmptyCondition(
        is_empty={"key": "conversation_id"}  # Only include chunks where conversation_id field is empty/missing
    ))
    
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

    # Construct the final filter (apply must to enforce no conversation_id)
    vector_search_filter = models.Filter(must=must_conditions, should=should_conditions, must_not=must_not_conditions)

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
    must_conditions.append(FieldCondition(
        key='conversation_id', 
        match=MatchValue(value=conversation_id)
    ))
    
    return models.Filter(must=must_conditions)

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

  def _create_conversation_filter(self, conversation_id: str) -> models.Filter:
    """
    Create a filter for conversation-specific documents.
    """
    return models.Filter(
        must=[
            FieldCondition(
                key='conversation_id',
                match=MatchValue(value=conversation_id)
            )
        ]
    )

  def _combine_filters(self, search_filter: models.Filter, conversation_filter: models.Filter = None) -> models.Filter:
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

  def vector_search_with_filter(self, search_query, course_name, doc_groups: List[str], 
                                 user_query_embedding, top_n, disabled_doc_groups: List[str], 
                                 public_doc_groups: List[dict], custom_filter: models.Filter):
    """
    Search the vector database with a custom filter.
    Used for conversation-specific document filtering.
    """
    search_results = self.qdrant_client.search(
        collection_name=os.environ['QDRANT_COLLECTION_NAME'],
        query_filter=custom_filter,
        with_vectors=False,
        query_vector=user_query_embedding,
        limit=top_n,
        search_params=models.SearchParams(
            quantization=models.QuantizationSearchParams(rescore=False)
        )
    )
    return search_results
