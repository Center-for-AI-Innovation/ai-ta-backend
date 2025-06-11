import asyncio
import inspect
import os
import time
import traceback
from collections import defaultdict
from typing import Dict, List, Union

import openai
import pytz
from dateutil import parser
from injector import inject
from langchain.embeddings.ollama import OllamaEmbeddings

from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from langchain_openai import ChatOpenAI

# from langchain.chat_models import AzureChatOpenAI
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.schema import Document

from ai_ta_backend.database.aws import AWSStorage
from ai_ta_backend.database.sql import (
    ModelUsage,
    ProjectStats,
    SQLDatabase,
    WeeklyMetric,
)
from ai_ta_backend.database.vector import VectorDatabase
from ai_ta_backend.database.graph import GraphDatabase
from ai_ta_backend.executors.thread_pool_executor import ThreadPoolExecutorAdapter

# from ai_ta_backend.service.nomic_service import NomicService
from ai_ta_backend.service.posthog_service import PosthogService
from ai_ta_backend.service.sentry_service import SentryService


class RetrievalService:
  """
    Contains all methods for business logic of the retrieval service.
  """

  @inject
  def __init__(self, vdb: VectorDatabase, sqlDb: SQLDatabase, aws: AWSStorage, graphDb: GraphDatabase, posthog: PosthogService,
               sentry: SentryService, thread_pool_executor: ThreadPoolExecutorAdapter):
    self.vdb = vdb
    self.sqlDb = sqlDb
    self.graphDb = graphDb
    self.aws = aws
    self.sentry = sentry
    self.posthog = posthog
    self.thread_pool_executor = thread_pool_executor
    openai.api_key = os.environ["VLADS_OPENAI_KEY"]

    self.embeddings = OpenAIEmbeddings(
        model='text-embedding-ada-002',
        openai_api_key=os.environ["VLADS_OPENAI_KEY"],
        # openai_api_key=os.environ["AZURE_OPENAI_KEY"],
        # openai_api_base=os.environ["AZURE_OPENAI_ENDPOINT"],
        # openai_api_type=os.environ['OPENAI_API_TYPE'],
        # openai_api_version=os.environ["OPENAI_API_VERSION"],
    )

    self.nomic_embeddings = OllamaEmbeddings(base_url=os.environ['OLLAMA_SERVER_URL'], model='nomic-embed-text:v1.5')   
    
    
    # self.llm = AzureChatOpenAI(
    #     temperature=0,
    #     deployment_name=os.environ["AZURE_OPENAI_ENGINE"],
    #     openai_api_base=os.environ["AZURE_OPENAI_ENDPOINT"],
    #     openai_api_key=os.environ["AZURE_OPENAI_KEY"],
    #     openai_api_version=os.environ["OPENAI_API_VERSION"],
    #     openai_api_type=os.environ['OPENAI_API_TYPE'],
    # )


  async def getTopContexts(self,
                           search_query: str,
                           course_name: str,
                           doc_groups: List[str] | None = None,
                           top_n: int = 100) -> Union[List[Dict], str]:
    """
    Retrieve top contexts for a query, including vector search and parallel KG retrieval (PrimeKG and Clinical KG).
    """
    if doc_groups is None:
      doc_groups = []
    try:
      start_time_overall = time.monotonic()
      # Parallelize independent operations:
      if course_name == "vyriad":
        embedding_client = self.nomic_embeddings
      elif course_name == "pubmed" or course_name == "patents":
        embedding_client = self.nomic_embeddings
      else:
        embedding_client = self.embeddings

      with self.thread_pool_executor as executor:
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(executor, self.sqlDb.getDisabledDocGroups, course_name),
            loop.run_in_executor(executor, self.sqlDb.getPublicDocGroups, course_name),
            loop.run_in_executor(executor, self._embed_query_and_measure_latency, search_query, embedding_client)
        ]

      disabled_doc_groups_response, public_doc_groups_response, user_query_embedding = await asyncio.gather(*tasks)

      disabled_doc_groups = [doc_group['name'] for doc_group in disabled_doc_groups_response.data]
      public_doc_groups = [doc_group['doc_groups'] for doc_group in public_doc_groups_response.data]

      time_for_parallel_operations = time.monotonic() - start_time_overall
      start_time_vector_search = time.monotonic()

      # Perform vector search
      found_docs: list[Document] = self.vector_search(search_query=search_query,
                                                      course_name=course_name,
                                                      doc_groups=doc_groups,
                                                      user_query_embedding=user_query_embedding,
                                                      disabled_doc_groups=disabled_doc_groups,
                                                      public_doc_groups=public_doc_groups,
                                                      top_n=top_n)

      time_to_retrieve_docs = time.monotonic() - start_time_vector_search

      valid_docs = []
      for doc in found_docs:
        valid_docs.append(doc)

      print(f"Course: {course_name} ||| search_query: {search_query}\n"
            f"⏰ Runtime of getTopContexts: {(time.monotonic() - start_time_overall):.2f} seconds\n"
            f"Runtime for parallel operations: {time_for_parallel_operations:.2f} seconds, "
            f"Runtime to complete vector_search: {time_to_retrieve_docs:.2f} seconds")
      if len(valid_docs) == 0:
        return []

      self.posthog.capture(
          event_name="getTopContexts_success_DI",
          properties={
              "user_query": search_query,
              "course_name": course_name,
              # "total_tokens_used": token_counter,
              "total_contexts_used": len(valid_docs),
              "total_unique_docs_retrieved": len(found_docs),
              "getTopContext_total_latency_sec": time.monotonic() - start_time_overall,
          },
      )

      formatted = self.format_for_json(valid_docs)

      # Helper to run KG context retrieval in parallel
      async def get_kg_context(should_query_fn, kg_fn, *args):
          if should_query_fn(search_query):
              result = await asyncio.to_thread(kg_fn, *args)
              if result and isinstance(result, dict) and result.get("text"):
                  return result
          return None

      # Run both KG retrievals in parallel
      primekg_task = get_kg_context(self.should_query_primekg, self.getPrimeKGContexts, search_query)
      clinicalkg_task = get_kg_context(self.should_query_clinical_kg, self.getKnowledgeGraphContexts, search_query, course_name)
      primekg_context, clinicalkg_context = await asyncio.gather(primekg_task, clinicalkg_task)

      if primekg_context:
          formatted.append(primekg_context)
      if clinicalkg_context:
          formatted.append(clinicalkg_context)
      return formatted
    except Exception as e:
      # return full traceback to front end
      # err: str = f"ERROR: In /getTopContexts. Course: {course_name} ||| search_query: {search_query}\nTraceback: {traceback.extract_tb(e.__traceback__)}❌❌ Error in {inspect.currentframe().f_code.co_name}:\n{e}"  # type: ignore
      err: str = f"ERROR: In /getTopContexts. Course: {course_name} ||| search_query: {search_query}\nTraceback: {traceback.print_exc} \n{e}"  # type: ignore
      traceback.print_exc()
      print(err)
      self.sentry.capture_exception(e)
      return err

  def getAll(
      self,
      course_name: str,
  ):
    """Get all course materials based on course name.
    Args:
        course_name (as uploaded on supabase)
    Returns:
        list of dictionaries with distinct s3 path, readable_filename and course_name, url, base_url.
    """

    response = self.sqlDb.getAllMaterialsForCourse(course_name)

    data = response.data
    unique_combinations = set()
    distinct_dicts = []

    for item in data:
      combination = (item['s3_path'], item['readable_filename'], item['course_name'], item['url'], item['base_url'])
      if combination not in unique_combinations:
        unique_combinations.add(combination)
        distinct_dicts.append(item)

    return distinct_dicts

  def llm_monitor_message(self, course_name: str, conversation_id: str, user_email: str, model_name: str) -> str:
    """
    Will store categories in DB, send email if an alert is triggered.
    """
    import json

    from ollama import Client as OllamaClient

    from ai_ta_backend.utils.email.send_transactional_email import send_email

    client = OllamaClient(os.environ['OLLAMA_SERVER_URL'])

    messages = self.sqlDb.getMessagesFromConvoID(conversation_id).data

    llm_monitor_model = 'llama-guard3:8b'

    # Map the preset categories
    safety_categories = {
        'S1': 'Violent Crimes',
        'S2': 'Non-Violent Crimes',
        'S3': 'Sex-Related Crimes',
        'S4': 'Child Sexual Exploitation',
        'S5': 'Defamation',
        'S6': 'Specialized Advice',
        'S7': 'Privacy',
        'S8': 'Intellectual Property',
        'S9': 'Indiscriminate Weapons',
        'S10': 'Hate',
        'S11': 'Suicide & Self-Harm',
        'S12': 'Sexual Content',
        'S13': 'Elections'
    }

    # Analyze each message using LLM
    for message in messages:
      if message.get('llm-monitor-tags'):
        continue

      message_content = message['content_text']

      if message.get('role'):
        message_content = "Message from " + message.get('role') + ":\n" + message_content

      analysis_result = client.chat(model=llm_monitor_model, messages=[{'role': 'user', 'content': message_content}])

      response_content = analysis_result['message']['content']

      # Prepare default LLM monitor tags
      llm_monitor_tags = {"llm_monitor_model": llm_monitor_model}

      # Assign tags to unsafe messages and send email when necessary
      if 'unsafe' in response_content.lower():
        llm_monitor_tags["status"] = "unsafe"
        # Identify and store triggered categories
        llm_monitor_tags["triggered_categories"] = [
            category_name for category_code, category_name in safety_categories.items()
            if category_code in response_content
        ]

        # Prepare alert email if unsafe
        alert_details = llm_monitor_tags.get("triggered_categories", [])
        if alert_details:
          alert_body = "\n".join([
              "LLM Monitor Alert",
              "------------------------",
              f"Course Name: {course_name}",
              f"User Email: {user_email}",
              f"Conversation Model Name: {model_name}",
              f"LLM Monitor Model Name: {llm_monitor_model}",
              f"Convo ID: {conversation_id}",
              "------------------------",
              f"Responsible Role: {message.get('role')}",
              f"Categories: {', '.join(alert_details)}",
              "------------------------",
              f"Message Content:\n{json.dumps(message_content, indent=2)}",
              "",
          ])

          message_id = message.get('id')
          # print(f"LLM Monitor Alert Triggered! Message ID: {message_id}")

          send_email(subject=f"LLM Monitor Alert - {', '.join(alert_details)}",
                     body_text=alert_body,
                     sender="hi@uiuc.chat",
                     recipients=["kvday2@illinois.edu", "hbroome@illinois.edu", "rohan13@illinois.edu"],
                     bcc_recipients=[])
      else:
        llm_monitor_tags["status"] = "safe"

      # Update llm_monitor_tags in messages database
      message_id = message.get('id')
      self.sqlDb.updateMessageFromLlmMonitor(message_id, llm_monitor_tags)

    return "Success"

  def delete_data(self, course_name: str, s3_path: str, source_url: str):
    """Delete file from S3, Qdrant, and Supabase."""
    print(f"Deleting data for course {course_name}")
    # add delete from doc map logic here
    try:
      # Delete file from S3
      bucket_name = os.environ['S3_BUCKET_NAME']
      if bucket_name is None:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")

      identifier_key, identifier_value = ("s3_path", s3_path) if s3_path else ("url", source_url)
      print(f"Deleting {identifier_value} from S3, Qdrant, and Supabase using {identifier_key}")

      # Delete from S3
      if identifier_key == "s3_path":
        self.delete_from_s3(bucket_name, s3_path)

      # Delete from Qdrant
      self.delete_from_qdrant(identifier_key, identifier_value, course_name)

      # Delete from Nomic and Supabase
      self.delete_from_nomic_and_supabase(course_name, identifier_key, identifier_value)

      return "Success"

    except Exception as e:
      err: str = f"ERROR IN delete_data: Traceback: {traceback.extract_tb(e.__traceback__)}❌❌ Error in {inspect.currentframe().f_code.co_name}:{e}"  # type: ignore
      print(err)
      self.sentry.capture_exception(e)
      return err

  def delete_from_s3(self, bucket_name: str, s3_path: str):
    try:
      print("Deleting from S3")
      response = self.aws.delete_file(bucket_name, s3_path)
      print(f"AWS response: {response}")
    except Exception as e:
      print("Error in deleting file from s3:", e)
      self.sentry.capture_exception(e)

  def delete_from_qdrant(self, identifier_key: str, identifier_value: str, course_name: str):
    try:
      print("Deleting from Qdrant")
      if course_name == 'cropwizard-1.5':
        # delete from cw db
        response = self.vdb.delete_data_cropwizard(identifier_key, identifier_value)
      else:
        response = self.vdb.delete_data(os.environ['QDRANT_COLLECTION_NAME'], identifier_key, identifier_value)
      print(f"Qdrant response: {response}")
    except Exception as e:
      if "timed out" in str(e):
        # Timed out is fine. Still deletes.
        pass
      else:
        print("Error in deleting file from Qdrant:", e)
        self.sentry.capture_exception(e)

  def getTopContextsWithMQR(self,
                            search_query: str,
                            course_name: str,
                            token_limit: int = 4_000) -> Union[List[Dict], str]:
    """
    New info-retrieval pipeline that uses multi-query retrieval + filtering + reciprocal rank fusion + context padding.
    1. Generate multiple queries based on the input search query.
    2. Retrieve relevant docs for each query.
    3. Filter the relevant docs based on the user query and pass them to the rank fusion step.
    4. [CANCELED BEC POINTLESS] Rank the docs based on the relevance score.
    5. Parent-doc-retrieval: Pad just the top 5 docs with expanded context from the original document.
    """
    raise NotImplementedError("Method deprecated for performance reasons. Hope to bring back soon.")

    # try:
    #   top_n_per_query = 40  # HARD CODE TO ENSURE WE HIT THE MAX TOKENS
    #   start_time_overall = time.monotonic()
    #   mq_start_time = time.monotonic()

    #   # 1. GENERATE MULTIPLE QUERIES
    #   generate_queries = (
    #       MULTI_QUERY_PROMPT | self.llm | StrOutputParser() | (lambda x: x.split("\n")) |
    #       (lambda x: list(filter(None, x)))  # filter out non-empty strings
    #   )

    #   generated_queries = generate_queries.invoke({"original_query": search_query})
    #   print("generated_queries", generated_queries)

    #   # 2. VECTOR SEARCH FOR EACH QUERY
    #   batch_found_docs_nested: list[list[Document]] = self.batch_vector_search(search_queries=generated_queries,
    #                                                                            course_name=course_name,
    #                                                                            top_n=top_n_per_query)

    #   # 3. RANK REMAINING DOCUMENTS -- good for parent doc padding of top 5 at the end.
    #   found_docs = self.reciprocal_rank_fusion(batch_found_docs_nested)
    #   found_docs = [doc for doc, score in found_docs]
    #   print(f"Num docs after re-ranking: {len(found_docs)}")
    #   if len(found_docs) == 0:
    #     return []
    #   print(f"⏰ Total multi-query processing runtime: {(time.monotonic() - mq_start_time):.2f} seconds")

    #   # 4. FILTER DOCS
    #   filtered_docs = filter_top_contexts(contexts=found_docs, user_query=search_query, timeout=30, max_concurrency=180)
    #   if len(filtered_docs) == 0:
    #     return []

    #   # 5. TOP DOC CONTEXT PADDING // parent document retriever
    #   final_docs = context_parent_doc_padding(filtered_docs, search_query, course_name)
    #   print(f"Number of final docs after context padding: {len(final_docs)}")

    #   pre_prompt = "Please answer the following question. Use the context below, called your documents, only if it's helpful and don't use parts that are very irrelevant. It's good to quote from your documents directly, when you do always use Markdown footnotes for citations. Use react-markdown superscript to number the sources at the end of sentences (1, 2, 3...) and use react-markdown Footnotes to list the full document names for each number. Use ReactMarkdown aka 'react-markdown' formatting for super script citations, use semi-formal style. Feel free to say you don't know. \nHere's a few passages of the high quality documents:\n"
    #   token_counter, _ = count_tokens_and_cost(pre_prompt + '\n\nNow please respond to my query: ' +
    #                                            search_query)  # type: ignore

    #   valid_docs = []
    #   num_tokens = 0
    #   for doc in final_docs:
    #     doc_string = f"Document: {doc['readable_filename']}{', page: ' + str(doc['pagenumber']) if doc['pagenumber'] else ''}\n{str(doc['text'])}\n"
    #     num_tokens, prompt_cost = count_tokens_and_cost(doc_string)  # type: ignore

    #     print(f"token_counter: {token_counter}, num_tokens: {num_tokens}, max_tokens: {token_limit}")
    #     if token_counter + num_tokens <= token_limit:
    #       token_counter += num_tokens
    #       valid_docs.append(doc)
    #     else:
    #       # filled our token size, time to return
    #       break

    #   print(f"Total tokens used: {token_counter} Used {len(valid_docs)} of total unique docs {len(found_docs)}.")
    #   print(f"Course: {course_name} ||| search_query: {search_query}")
    #   print(f"⏰ ^^ Runtime of getTopContextsWithMQR: {(time.monotonic() - start_time_overall):.2f} seconds")

    #   if len(valid_docs) == 0:
    #     return []

    #   self.posthog.capture('distinct_id_of_the_user',
    #                        event='filter_top_contexts_succeeded',
    #                        properties={
    #                            'user_query': search_query,
    #                            'course_name': course_name,
    #                            'token_limit': token_limit,
    #                            'total_tokens_used': token_counter,
    #                            'total_contexts_used': len(valid_docs),
    #                            'total_unique_docs_retrieved': len(found_docs),
    #                        })

    #   return self.format_for_json_mqr(valid_docs)
    # except Exception as e:
    #   # return full traceback to front end
    #   err: str = f"ERROR: In /getTopContextsWithMQR. Course: {course_name} ||| search_query: {search_query}\nTraceback: {traceback.format_exc()}❌❌ Error in {inspect.currentframe().f_code.co_name}:\n{e}"  # type: ignore
    #   print(err)
    #   sentry_sdk.capture_exception(e)
    #   return err

  def delete_from_nomic_and_supabase(self, course_name: str, identifier_key: str, identifier_value: str):
    # try:
    #   print(f"Nomic delete. Course: {course_name} using {identifier_key}: {identifier_value}")
    #   response = self.sqlDb.getMaterialsForCourseAndKeyAndValue(course_name, identifier_key, identifier_value)
    #   if not response.data:
    #     raise Exception(f"No materials found for {course_name} using {identifier_key}: {identifier_value}")
    #   data = response.data[0]  # single record fetched
    #   nomic_ids_to_delete = [str(data['id']) + "_" + str(i) for i in range(1, len(data['contexts']) + 1)]

    # delete from Nomic
    # response = self.sqlDb.getProjectsMapForCourse(course_name)
    # if not response.data:
    #   raise Exception(f"No document map found for this course: {course_name}")
    # project_id = response.data[0]['doc_map_id']
    # self.nomicService.delete_from_document_map(project_id, nomic_ids_to_delete)
    # except Exception as e:
    #   print(f"Nomic Error in deleting. {identifier_key}: {identifier_value}", e)
    #   self.sentry.capture_exception(e)

    try:
      print(f"Supabase Delete. course: {course_name} using {identifier_key}: {identifier_value}")
      response = self.sqlDb.deleteMaterialsForCourseAndKeyAndValue(course_name, identifier_key, identifier_value)
    except Exception as e:
      print(f"Supabase Error in delete. {identifier_key}: {identifier_value}", e)
      self.sentry.capture_exception(e)

  def vector_search(self,
                    search_query,
                    course_name,
                    doc_groups: List[str],
                    user_query_embedding,
                    disabled_doc_groups,
                    public_doc_groups,
                    top_n: int = 100):
    """
    Search the vector database for a given query, course name, and document groups.
    """
    if doc_groups is None:
      doc_groups = []

    if disabled_doc_groups is None:
      disabled_doc_groups = []

    if public_doc_groups is None:
      public_doc_groups = []

    # Capture the search invoked event to PostHog
    self._capture_search_invoked_event(search_query, course_name, doc_groups)

    # Perform the vector search
    start_time_vector_search = time.monotonic()

    # ----------------------------
    # SPECIAL CASE FOR VYRIAD, CROPWIZARD
    # ----------------------------
    if course_name == "vyriad":
      search_results = self.vdb.vyriad_vector_search(search_query, course_name, doc_groups, user_query_embedding, top_n,
                                                     disabled_doc_groups, public_doc_groups)
    elif course_name == "cropwizard":
      search_results = self.vdb.cropwizard_vector_search(search_query, course_name, doc_groups, user_query_embedding,
                                                         top_n, disabled_doc_groups, public_doc_groups)
    elif course_name == "pubmed":
      search_results = self.vdb.pubmed_vector_search(search_query, course_name, doc_groups, user_query_embedding, top_n,
                                                     disabled_doc_groups, public_doc_groups)
    elif course_name == "patents":
      search_results = self.vdb.patents_vector_search(search_query, course_name, doc_groups, user_query_embedding,
                                                      top_n, disabled_doc_groups, public_doc_groups)
    else:
      search_results = self.vdb.vector_search(search_query, course_name, doc_groups, user_query_embedding, top_n,
                                              disabled_doc_groups, public_doc_groups)
    self.qdrant_latency_sec = time.monotonic() - start_time_vector_search

    # Process the search results by extracting the page content and metadata
    start_time_process_search_results = time.monotonic()
    found_docs = self._process_search_results(search_results, course_name)
    time_for_process_search_results = time.monotonic() - start_time_process_search_results

    # Capture the search succeeded event to PostHog with the vector scores
    start_time_capture_search_succeeded_event = time.monotonic()
    self._capture_search_succeeded_event(search_query, course_name, search_results)
    time_for_capture_search_succeeded_event = time.monotonic() - start_time_capture_search_succeeded_event

    print(f"Runtime for embedding query: {self.openai_embedding_latency:.2f} seconds\n"
          f"Runtime for vector search: {self.qdrant_latency_sec:.2f} seconds\n"
          f"Runtime for process search results: {time_for_process_search_results:.2f} seconds\n"
          f"Runtime for capture search succeeded event: {time_for_capture_search_succeeded_event:.2f} seconds")
    return found_docs

  def _embed_query_and_measure_latency(self, search_query, embedding_client):
    openai_start_time = time.monotonic()
    user_query_embedding = embedding_client.embed_query(search_query)
    self.openai_embedding_latency = time.monotonic() - openai_start_time
    return user_query_embedding

  def _capture_search_invoked_event(self, search_query, course_name, doc_groups):
    self.posthog.capture(
        event_name="vector_search_invoked",
        properties={
            "user_query": search_query,
            "course_name": course_name,
            "doc_groups": doc_groups,
        },
    )

  def _process_search_results(self, search_results, course_name):
    found_docs: list[Document] = []
    for d in search_results:
      try:
        metadata = d.payload
        # print(f"Metadata: {metadata}")
        page_content = metadata["page_content"]
        del metadata["page_content"]
        if "pagenumber" not in metadata.keys() and "pagenumber_or_timestamp" in metadata.keys():
          metadata["pagenumber"] = metadata["pagenumber_or_timestamp"]

        found_docs.append(Document(page_content=page_content, metadata=metadata))
      except Exception as e:
        print(f"Error in vector_search(), for course: `{course_name}`. Error: {e}")
        self.sentry.capture_exception(e)
    return found_docs

  def _capture_search_succeeded_event(self, search_query, course_name, search_results):
    vector_score_calc_latency_sec = time.monotonic()
    # Removed because it takes 0.15 seconds to _calculate_vector_scores... not worth it rn.
    # max_vector_score, min_vector_score, avg_vector_score = self._calculate_vector_scores(search_results)
    self.posthog.capture(
        event_name="vector_search_succeeded",
        properties={
            "user_query": search_query,
            "course_name": course_name,
            "qdrant_latency_sec": self.qdrant_latency_sec,
            "openai_embedding_latency_sec": self.openai_embedding_latency,
            # "max_vector_score": max_vector_score,
            # "min_vector_score": min_vector_score,
            # "avg_vector_score": avg_vector_score,
            "vector_score_calculation_latency_sec": time.monotonic() - vector_score_calc_latency_sec,
        },
    )

  def _calculate_vector_scores(self, search_results):
    max_vector_score = 0
    min_vector_score = 0
    total_vector_score = 0
    for result in search_results:
      max_vector_score = max(max_vector_score, result.score)
      min_vector_score = min(min_vector_score, result.score)
      total_vector_score += result.score
    avg_vector_score = total_vector_score / len(search_results) if search_results else 0
    return max_vector_score, min_vector_score, avg_vector_score

  def format_for_json(self, found_docs: List[Document]) -> List[Dict]:
    """Format documents into JSON-serializable dictionaries.
      
      Args:
          found_docs: List of Document objects containing page content and metadata
          
      Returns:
          List of dictionaries with text content and metadata fields
      """
    return [
        {
            "text": doc.page_content,
            "readable_filename": doc.metadata["readable_filename"],
            "course_name ": doc.metadata["course_name"],
            # OPTIONAL
            "s3_path": doc.metadata.get("s3_path"),
            "pagenumber": doc.metadata.get("pagenumber"),  # Handles both old and new schema
            "url": doc.metadata.get("url"),
            "base_url": doc.metadata.get("base_url"),
            "doc_groups": doc.metadata.get("doc_groups"),
        } for doc in found_docs
    ]

  def getConversationStats(self, course_name: str, from_date: str = '', to_date: str = ''):
    """
    Fetches conversation data from the database and groups them by day, hour, and weekday.
    
    Args:
        course_name (str): Name of the course
        from_date (str, optional): Start date in ISO format
        to_date (str, optional): End date in ISO format
    """
    try:
      conversations, total_count = self.sqlDb.getConversationsCreatedAtByCourse(course_name, from_date, to_date)

      response_data = {
          'per_day': {},
          'per_hour': {
              str(hour): 0 for hour in range(24)
          },
          'per_weekday': {
              day: 0 for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
          },
          'heatmap': {
              day: {
                  str(hour): 0 for hour in range(24)
              } for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
          },
          'total_count': 0
      }

      if not conversations:
        return response_data

      central_tz = pytz.timezone('America/Chicago')
      grouped_data = {
          'per_day': defaultdict(int),
          'per_hour': defaultdict(int),
          'per_weekday': defaultdict(int),
          'heatmap': defaultdict(lambda: defaultdict(int)),
      }

      for record in conversations:
        try:
          created_at = record['created_at']
          parsed_date = parser.parse(created_at).astimezone(central_tz)

          day = parsed_date.date()
          hour = parsed_date.hour
          day_of_week = parsed_date.strftime('%A')

          grouped_data['per_day'][str(day)] += 1
          grouped_data['per_hour'][str(hour)] += 1
          grouped_data['per_weekday'][day_of_week] += 1
          grouped_data['heatmap'][day_of_week][str(hour)] += 1
        except Exception as e:
          print(f"Error processing record: {str(e)}")
          continue

      return {
          'per_day': dict(grouped_data['per_day']),
          'per_hour': {
              str(k): v for k, v in grouped_data['per_hour'].items()
          },
          'per_weekday': dict(grouped_data['per_weekday']),
          'heatmap': {
              day: {
                  str(h): count for h, count in hours.items()
              } for day, hours in grouped_data['heatmap'].items()
          },
          'total_count': total_count
      }

    except Exception as e:
      print(f"Error in getConversationStats for course {course_name}: {str(e)}")
      self.sentry.capture_exception(e)
      return {
          'per_day': {},
          'per_hour': {
              str(hour): 0 for hour in range(24)
          },
          'per_weekday': {
              day: 0 for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
          },
          'heatmap': {
              day: {
                  str(hour): 0 for hour in range(24)
              } for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
          },
          'total_count': 0
      }

  def getProjectStats(self, project_name: str) -> ProjectStats:
    """
    Get statistics for a project.
    
    Args:
        project_name (str)

    Returns:
        ProjectStats: TypedDict containing:
            - total_messages (int): Total number of messages
            - total_conversations (int): Total number of conversations
            - unique_users (int): Number of unique users
            - avg_conversations_per_user (float): Average conversations per user
            - avg_messages_per_user (float): Average messages per user
            - avg_messages_per_conversation (float): Average messages per conversation
    """
    return self.sqlDb.getProjectStats(project_name)

  def getWeeklyTrends(self, project_name: str) -> List[WeeklyMetric]:
    """
    Get weekly trends for a project, showing percentage changes in metrics.
    
    Args:
        project_name (str): Name of the project
        
    Returns:
        List[WeeklyMetric]: List of metrics with their current week value, 
        previous week value, and percentage change.
    """
    return self.sqlDb.getWeeklyTrends(project_name)

  def getModelUsageCounts(self, project_name: str) -> List[ModelUsage]:
    """
    Get counts of different models used in conversations for a project.
    
    Args:
        project_name (str): Name of the project
        
    Returns:
        List[ModelUsage]: List of model usage statistics containing model name,
        count and percentage of total usage
    """
    try:
      return self.sqlDb.getModelUsageCounts(project_name)

    except Exception as e:
      print(f"Error fetching model usage counts for {project_name}: {str(e)}")
      self.sentry.capture_exception(e)
      return []
    
  def getKnowledgeGraphContexts(self, user_query: str, course_name: str) -> Dict:
    """
    Get knowledge graph contexts from Clinical KG, including an OpenAI summary of the results.
    """
    try:
        start_time = time.monotonic()
        response = self.graphDb.run_clinicalkg_query_with_retries(user_query)
        execution_time = time.monotonic() - start_time
        print(f"Knowledge graph query completed in {execution_time:.2f} seconds for query: {user_query}")

        # Post-process: If the result is "I don't know the answer", return empty
        # Try to extract the nested 'result' field from the 'results' object
        if isinstance(response, dict):
            results_obj = response.get("results")
            if isinstance(results_obj, dict) and "result" in results_obj:
                result_text = results_obj["result"]
                if isinstance(result_text, str) and "i don't know the answer" in result_text.lower():
                    return {}
                # Generate summary
                summary = self.generate_openai_summary(user_query, results_obj["result"])
                return {
                    "text": summary,
                    "kg_result": results_obj["result"],
                    "readable_filename": "ClinicalKG",
                    "course_name": course_name,
                    "s3_path": None,
                    "pagenumber": None,
                    "url": None,
                    "base_url": None,
                    "doc_groups": None,
                    "clinicalkg_intermediate_steps": results_obj.get("intermediate_steps"),
                    "clinicalkg_query": results_obj.get("query"),
                }
        return response
    except Exception as e:
        error_msg = f"Error in knowledge graph query for '{user_query}': {str(e)}"
        print(error_msg)
        self.sentry.capture_exception(e)
        return {"result": "An error occurred while processing your knowledge graph query."}

  
  def getPrimeKGContexts(self, user_query: str) -> Dict:
    """
    Get knowledge graph contexts from Prime KG, including an OpenAI summary of the results.
    """
    try:
        start_time = time.monotonic()
        response = self.graphDb.run_primekg_query_with_retries(user_query)
        print("RESPONSE pre post processing: ", response)
        execution_time = time.monotonic() - start_time
        print(f"Knowledge graph query completed in {execution_time:.2f} seconds for query: {user_query}")

        # Post-process: If the result is "I don't know the answer", return empty
        # Try to extract the nested 'result' field from the 'results' object
        if isinstance(response, dict):
            results_obj = response.get("results")
            if isinstance(results_obj, dict) and "result" in results_obj:
                result_text = results_obj["result"]
                if isinstance(result_text, str) and "i don't know the answer" in result_text.lower():
                    return {}
                # Generate summary
                summary = self.generate_openai_summary(user_query, results_obj["result"])
                return {
                    "kg_result": results_obj["result"],
                    "readable_filename": "PrimeKG",
                    "course_name": None,
                    "s3_path": None,
                    "pagenumber": None,
                    "url": None,
                    "base_url": None,
                    "doc_groups": None,
                    "text": summary,
                }
        return response
    except Exception as e:  
        print(f"Error in getPrimeKGContexts: {str(e)}")
        self.sentry.capture_exception(e)
        return {"result": "An error occurred while processing your knowledge graph query."}

  def should_query_primekg(self, search_query: str) -> bool:
    """
    Simple agent to decide if PrimeKG should be queried based on keywords in the search query.
    """
    keywords = ["disease", "drug", "gene", "symptom", "side effect", "treatment", "primekg", "cell"]
    return any(kw in search_query.lower() for kw in keywords)

  def should_query_clinical_kg(self, search_query: str) -> bool:
    """
    Simple agent to decide if Clinical KG should be queried based on keywords in the search query.
    """
    keywords = ["patient", "symptom", "diagnosis", "treatment", "clinical", "disease", "drug", "gene", "side effect"]
    return any(kw in search_query.lower() for kw in keywords)

  def generate_openai_summary(self, user_query: str, kg_result: list) -> str:
    """
    Generate a summary using the OpenAI API, given the user query and the first 5 results from the KG.
    """
    from openai import OpenAI
    import os

    client = OpenAI(api_key=os.environ["VLADS_OPENAI_KEY"])
    # Truncate or format kg_result for prompt if it's very large
    context_str = str(kg_result)[:4000]  # adjust as needed for token limits

    prompt = (
        f"User question: {user_query}\n"
        f"Knowledge graph results: {context_str}\n"
        "Please provide a concise, readable summary of the key findings that answer the user's question."
    )

    response = client.chat.completions.create(
        model="gpt-4o",  # or "gpt-3.5-turbo" if you want to save cost
        messages=[{"role": "user", "content": prompt}],
        max_tokens=256,
        temperature=0.3,
    )
    return response.choices[0].message.content.strip()
