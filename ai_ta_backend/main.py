import asyncio
import json
import os
import time
from typing import List

from dotenv import load_dotenv
from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    make_response,
    request,
    send_from_directory,
)
from flask_cors import CORS
from flask_executor import Executor
from flask_injector import FlaskInjector, RequestScope
from injector import Binder, SingletonScope

from ai_ta_backend.database.aws import AWSStorage
from ai_ta_backend.database.sql import SQLDatabase
from ai_ta_backend.database.vector import VectorDatabase
from ai_ta_backend.executors.flask_executor import (
    ExecutorInterface,
    FlaskExecutorAdapter,
)
from ai_ta_backend.executors.process_pool_executor import (
    ProcessPoolExecutorAdapter,
    ProcessPoolExecutorInterface,
)
from ai_ta_backend.executors.thread_pool_executor import (
    ThreadPoolExecutorAdapter,
    ThreadPoolExecutorInterface,
)
from ai_ta_backend.service.export_service import ExportService
from ai_ta_backend.service.nomic_service import NomicService
from ai_ta_backend.service.posthog_service import PosthogService
from ai_ta_backend.service.project_service import ProjectService
from ai_ta_backend.service.retrieval_service import RetrievalService
from ai_ta_backend.service.sentry_service import SentryService
from ai_ta_backend.service.workflow_service import WorkflowService
from ai_ta_backend.utils.email.send_transactional_email import send_email
from ai_ta_backend.utils.pubmed_extraction import extractPubmedData
from ai_ta_backend.utils.rerun_webcrawl_for_project import webscrape_documents

app = Flask(__name__)
CORS(app)
executor = Executor(app)
# app.config['EXECUTOR_MAX_WORKERS'] = 5 nothing == picks defaults for me
#app.config['SERVER_TIMEOUT'] = 1000  # seconds

# load API keys from globally-availabe .env file
load_dotenv()


@app.route('/')
def index() -> Response:
  """_summary_

  Args:
      test (int, optional): _description_. Defaults to 1.

  Returns:
      JSON: _description_
  """
  response = jsonify(
      {"hi there, this is a 404": "Welcome to UIUC.chat backend 🚅 Read the docs here: https://docs.uiuc.chat/ "})
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/getTopContexts', methods=['POST'])
def getTopContexts(service: RetrievalService) -> Response:
  """Get most relevant contexts for a given search query.
  
  Return value

  ## POST body
  course name (optional) str
      A json response with TBD fields.
  search_query
  token_limit
  doc_groups
  
  Returns
  -------
  JSON
      A json response with TBD fields.
  Metadata fields
  * pagenumber_or_timestamp
  * readable_filename
  * s3_pdf_path
  
  Example: 
  [
    {
      'readable_filename': 'Lumetta_notes', 
      'pagenumber_or_timestamp': 'pg. 19', 
      's3_pdf_path': '/courses/<course>/Lumetta_notes.pdf', 
      'text': 'In FSM, we do this...'
    }, 
  ]

  Raises
  ------
  Exception
      Testing how exceptions are handled.
  """
  start_time = time.monotonic()
  data = request.get_json()
  search_query: str = data.get('search_query', '')
  course_name: str = data.get('course_name', '')
  doc_groups: List[str] = data.get('doc_groups', [])
  top_n: int = data.get('top_n', 100)

  if search_query == '' or course_name == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing one or more required parameters: 'search_query' and 'course_name' must be provided. Search query: `{search_query}`, Course name: `{course_name}`"
    )

  found_documents = asyncio.run(service.getTopContexts(search_query, course_name, doc_groups, top_n))
  response = jsonify(found_documents)
  response.headers.add('Access-Control-Allow-Origin', '*')
  print(f"⏰ Runtime of getTopContexts in main.py: {(time.monotonic() - start_time):.2f} seconds")
  return response


@app.route('/llm-monitor-message', methods=['POST'])
def llm_monitor_message_main(service: RetrievalService, flaskExecutor: ExecutorInterface) -> Response:
  """
  Analyze a message from a conversation and store the results in the database.
  """
  start_time = time.monotonic()
  data = request.get_json()
  # messages: List[str] = data.get('messages', [])
  course_name: str = data.get('course_name', None)
  conversation_id: str = data.get('conversation_id', None)
  user_email: str = data.get('user_email', None)
  model_name: str = data.get('model_name', None)

  if course_name == '':
    # proper web error "400 Bad request"
    abort(400,
          description=
          f"Missing one or more required parameters: 'course_name' must be provided. Course name: `{course_name}`")

  flaskExecutor.submit(service.llm_monitor_message, course_name, conversation_id, user_email, model_name)
  response = jsonify({"outcome": "Task started"})
  response.headers.add('Access-Control-Allow-Origin', '*')
  print(f"⏰ Runtime of /llm-monitor-message in main.py: {(time.monotonic() - start_time):.2f} seconds")

  return response


@app.route('/getAll', methods=['GET'])
def getAll(service: RetrievalService) -> Response:
  """Get all course materials based on the course_name
  """
  course_name: List[str] | str = request.args.get('course_name', default='', type=str)

  if course_name == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=f"Missing the one required parameter: 'course_name' must be provided. Course name: `{course_name}`")

  distinct_dicts = service.getAll(course_name)

  response = jsonify({"distinct_files": distinct_dicts})
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/delete', methods=['DELETE'])
def delete(service: RetrievalService, flaskExecutor: ExecutorInterface):
  """
  Delete a single file from all our database: S3, Qdrant, and Supabase (for now).
  Note, of course, we still have parts of that file in our logs.
  """
  course_name: str = request.args.get('course_name', default='', type=str)
  s3_path: str = request.args.get('s3_path', default='', type=str)
  source_url: str = request.args.get('url', default='', type=str)

  if course_name == '' or (s3_path == '' and source_url == ''):
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing one or more required parameters: 'course_name' and ('s3_path' or 'source_url') must be provided. Course name: `{course_name}`, S3 path: `{s3_path}`, source_url: `{source_url}`"
    )

  start_time = time.monotonic()
  # background execution of tasks!!
  flaskExecutor.submit(service.delete_data, course_name, s3_path, source_url)
  print(f"From {course_name}, deleted file: {s3_path}")
  print(f"⏰ Runtime of FULL delete func: {(time.monotonic() - start_time):.2f} seconds")
  # we need instant return. Delets are "best effort" assume always successful... sigh :(
  response = jsonify({"outcome": 'success'})
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/getNomicMap', methods=['GET'])
def nomic_map(service: NomicService):
  course_name: str = request.args.get('course_name', default='', type=str)
  map_type: str = request.args.get('map_type', default='conversation', type=str)

  if course_name == '':
    # proper web error "400 Bad request"
    abort(400, description=f"Missing required parameter: 'course_name' must be provided. Course name: `{course_name}`")

  map_id = service.get_nomic_map(course_name, map_type)
  print("nomic map\n", map_id)

  response = jsonify(map_id)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/updateConversationMaps', methods=['GET'])
def updateConversationMaps(service: NomicService, flaskExecutor: ExecutorInterface):
  print("Starting conversation map update...")

  response = flaskExecutor.submit(service.update_conversation_maps)

  response = jsonify({"outcome": "Task started"})
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/updateDocumentMaps', methods=['GET'])
def updateDocumentMaps(service: NomicService, flaskExecutor: ExecutorInterface):
  print("Starting conversation map update...")

  response = flaskExecutor.submit(service.update_document_maps)

  response = jsonify({"outcome": "Task started"})
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/cleanUpConversationMaps', methods=['GET'])
def cleanUpConversationMaps(service: NomicService, flaskExecutor: ExecutorInterface):
  print("Starting conversation map cleanup...")

  #response = flaskExecutor.submit(service.clean_up_conversation_maps)

  response = jsonify({"outcome": "Task started"})
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/cleanUpDocumentMaps', methods=['GET'])
def cleanUpDocumentMaps(service: NomicService, flaskExecutor: ExecutorInterface):
  print("Starting document map cleanup...")

  #response = flaskExecutor.submit(service.clean_up_document_maps)

  response = jsonify({"outcome": "Document Map cleanup temporarily disabled"})
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/createDocumentMap', methods=['GET'])
def createDocumentMap(service: NomicService):
  course_name: str = request.args.get('course_name', default='', type=str)

  if course_name == '':
    # proper web error "400 Bad request"
    abort(400, description=f"Missing required parameter: 'course_name' must be provided. Course name: `{course_name}`")

  map_id = service.create_document_map(course_name)

  response = jsonify(map_id)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/createConversationMap', methods=['GET'])
def createConversationMap(service: NomicService):
  course_name: str = request.args.get('course_name', default='', type=str)

  if course_name == '':
    # proper web error "400 Bad request"
    abort(400, description=f"Missing required parameter: 'course_name' must be provided. Course name: `{course_name}`")

  map_id = service.create_conversation_map(course_name)

  response = jsonify(map_id)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/export-convo-history-csv', methods=['GET'])
def export_convo_history(service: ExportService):
  course_name: str = request.args.get('course_name', default='', type=str)
  from_date: str = request.args.get('from_date', default='', type=str)
  to_date: str = request.args.get('to_date', default='', type=str)

  if course_name == '':
    # proper web error "400 Bad request"
    abort(400, description=f"Missing required parameter: 'course_name' must be provided. Course name: `{course_name}`")

  export_status = service.export_convo_history_json(course_name, from_date, to_date)
  print("EXPORT FILE LINKS: ", export_status)

  if export_status['response'] == "No data found between the given dates.":
    response = Response(status=204)
    response.headers.add('Access-Control-Allow-Origin', '*')

  elif export_status['response'] == "Download from S3":
    response = jsonify({"response": "Download from S3", "s3_path": export_status['s3_path']})
    response.headers.add('Access-Control-Allow-Origin', '*')

  else:
    response = make_response(
        send_from_directory(export_status['response'][2], export_status['response'][1], as_attachment=True))
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers["Content-Disposition"] = f"attachment; filename={export_status['response'][1]}"
    os.remove(export_status['response'][0])

  return response


@app.route('/test-process', methods=['GET'])
def test_process(service: ExportService):
  service.test_process()
  return jsonify({"response": "success"})


@app.route('/export-convo-history', methods=['GET'])
def export_convo_history_v2(service: ExportService):
  course_name: str = request.args.get('course_name', default='', type=str)
  from_date: str = request.args.get('from_date', default='', type=str)
  to_date: str = request.args.get('to_date', default='', type=str)

  if course_name == '':
    abort(400, description=f"Missing required parameter: 'course_name' must be provided. Course name: `{course_name}`")

  export_status = service.export_convo_history(course_name, from_date, to_date)
  print("Export processing response: ", export_status)

  if export_status['response'] == "No data found between the given dates.":
    response = Response(status=204)
    response.headers.add('Access-Control-Allow-Origin', '*')

  elif export_status['response'] == "Download from S3":
    response = jsonify({"response": "Download from S3", "s3_path": export_status['s3_path']})
    response.headers.add('Access-Control-Allow-Origin', '*')

  else:
    response = make_response(
        send_from_directory(export_status['response'][2], export_status['response'][1], as_attachment=True))
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers["Content-Disposition"] = f"attachment; filename={export_status['response'][1]}"
    os.remove(export_status['response'][0])

  return response


@app.route('/export-convo-history-user', methods=['GET'])
def export_convo_history_user(service: ExportService):
  user_email: str = request.args.get('user_email', default='', type=str)
  project_name: str = request.args.get('project_name', default='', type=str)

  if user_email == '' or project_name == '':
    abort(400, description=f"Missing required parameters: 'user_email' and 'project_name' must be provided.")

  print("user_email: ", user_email)
  print("project_name: ", project_name)
  export_status = service.export_convo_history_user(user_email, project_name)
  print("Export processing response: ", export_status)

  if export_status['response'] == "No data found for the given user and project.":
    response = Response(status=204)
    response.headers.add('Access-Control-Allow-Origin', '*')

  elif export_status['response'] == "Download from S3":
    response = jsonify({"response": "Download from S3", "s3_path": export_status['s3_path']})
    response.headers.add('Access-Control-Allow-Origin', '*')
  elif export_status['response'] == "Error fetching conversations!":
    response = jsonify({'response': 'Error fetching conversations'})
    response.status_code = 500
    response.headers.add('Access-Control-Allow-Origin', '*')

  else:
    print("export_status['response'][2]: ", export_status['response'][2])
    print("export_status['response'][1]: ", export_status['response'][1])
    response = make_response(
        send_from_directory(export_status['response'][2], export_status['response'][1], as_attachment=True))
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers["Content-Disposition"] = f"attachment; filename={export_status['response'][1]}"
    os.remove(export_status['response'][0])

  return response


@app.route('/export-conversations-custom', methods=['GET'])
def export_conversations_custom(service: ExportService):
  course_name: str = request.args.get('course_name', default='', type=str)
  from_date: str = request.args.get('from_date', default='', type=str)
  to_date: str = request.args.get('to_date', default='', type=str)
  emails: str = request.args.getlist('destination_emails_list')

  if course_name == '' and emails == []:
    # proper web error "400 Bad request"
    abort(400, description=f"Missing required parameter: 'course_name' and 'destination_email_ids' must be provided.")

  export_status = service.export_conversations(course_name, from_date, to_date, emails)
  print("EXPORT FILE LINKS: ", export_status)

  if export_status['response'] == "No data found between the given dates.":
    response = Response(status=204)
    response.headers.add('Access-Control-Allow-Origin', '*')

  elif export_status['response'] == "Download from S3":
    response = jsonify({"response": "Download from S3", "s3_path": export_status['s3_path']})
    response.headers.add('Access-Control-Allow-Origin', '*')

  else:
    response = make_response(
        send_from_directory(export_status['response'][2], export_status['response'][1], as_attachment=True))
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers["Content-Disposition"] = f"attachment; filename={export_status['response'][1]}"
    os.remove(export_status['response'][0])

  return response


@app.route('/exportDocuments', methods=['GET'])
def exportDocuments(service: ExportService):
  course_name: str = request.args.get('course_name', default='', type=str)
  from_date: str = request.args.get('from_date', default='', type=str)
  to_date: str = request.args.get('to_date', default='', type=str)

  if course_name == '':
    # proper web error "400 Bad request"
    abort(400, description=f"Missing required parameter: 'course_name' must be provided. Course name: `{course_name}`")

  export_status = service.export_documents_json(course_name, from_date, to_date)
  print("EXPORT FILE LINKS: ", export_status)

  if export_status['response'] == "No data found between the given dates.":
    response = Response(status=204)
    response.headers.add('Access-Control-Allow-Origin', '*')

  elif export_status['response'] == "Download from S3":
    response = jsonify({"response": "Download from S3", "s3_path": export_status['s3_path']})
    response.headers.add('Access-Control-Allow-Origin', '*')

  else:
    response = make_response(
        send_from_directory(export_status['response'][2], export_status['response'][1], as_attachment=True))
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers["Content-Disposition"] = f"attachment; filename={export_status['response'][1]}"
    os.remove(export_status['response'][0])

  return response


@app.route('/getTopContextsWithMQR', methods=['GET'])
def getTopContextsWithMQR(service: RetrievalService, posthog_service: PosthogService) -> Response:
  """
  Get relevant contexts for a given search query, using Multi-query retrieval + filtering method.
  """
  search_query: str = request.args.get('search_query', default='', type=str)
  course_name: str = request.args.get('course_name', default='', type=str)
  token_limit: int = request.args.get('token_limit', default=3000, type=int)
  if search_query == '' or course_name == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing one or more required parameters: 'search_query' and 'course_name' must be provided. Search query: `{search_query}`, Course name: `{course_name}`"
    )

  posthog_service.capture(event_name='filter_top_contexts_invoked',
                          properties={
                              'user_query': search_query,
                              'course_name': course_name,
                              'token_limit': token_limit,
                          })

  found_documents = service.getTopContextsWithMQR(search_query, course_name, token_limit)

  response = jsonify(found_documents)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/getworkflows', methods=['GET'])
def get_all_workflows(service: WorkflowService) -> Response:
  """
  Get all workflows from user.
  """

  api_key = request.args.get('api_key', default='', type=str)
  limit = request.args.get('limit', default=100, type=int)
  pagination = request.args.get('pagination', default=True, type=bool)
  active = request.args.get('active', default=False, type=bool)
  name = request.args.get('workflow_name', default='', type=str)
  print(request.args)

  print("In get_all_workflows.. api_key: ", api_key)

  # if no API Key, return empty set.
  # if api_key == '':
  #   # proper web error "400 Bad request"
  #   abort(400, description=f"Missing N8N API_KEY: 'api_key' must be provided. Search query: `{api_key}`")

  try:
    response = service.get_workflows(limit, pagination, api_key, active, name)
    response = jsonify(response)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
  except Exception as e:
    if "unauthorized" in str(e).lower():
      print("Unauthorized error in get_all_workflows: ", e)
      abort(401, description=f"Unauthorized: 'api_key' is invalid. Search query: `{api_key}`")
    else:
      print("Error in get_all_workflows: ", e)
      abort(500, description=f"Failed to fetch n8n workflows: {e}")


@app.route('/switch_workflow', methods=['GET'])
def switch_workflow(service: WorkflowService) -> Response:
  """
  Activate or deactivate flow for user.
  """

  api_key = request.args.get('api_key', default='', type=str)
  activate = request.args.get('activate', default='', type=str)
  id = request.args.get('id', default='', type=str)

  print(request.args)

  if api_key == '':
    # proper web error "400 Bad request"
    abort(400, description=f"Missing N8N API_KEY: 'api_key' must be provided. Search query: `{api_key}`")

  try:
    print("activation!!!!!!!!!!!", activate)
    response = service.switch_workflow(id, api_key, activate)
    response = jsonify(response)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
  except Exception as e:
    if e == "Unauthorized":
      abort(401, description=f"Unauthorized: 'api_key' is invalid. Search query: `{api_key}`")
    else:
      abort(400, description=f"Bad request: {e}")


@app.route('/getConversationStats', methods=['GET'])
def get_conversation_stats(service: RetrievalService) -> Response:
    """
    Retrieves statistical metrics about conversations for a specific course.
    """
    course_name = request.args.get('course_name', default='', type=str)
    from_date = request.args.get('from_date', default='', type=str)
    to_date = request.args.get('to_date', default='', type=str)

    if course_name == '':
        abort(400, description="Missing required parameter: 'course_name' must be provided.")

    conversation_stats = service.getConversationStats(course_name, from_date, to_date)

    response = jsonify(conversation_stats)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


@app.route('/run_flow', methods=['POST'])
def run_flow(service: WorkflowService) -> Response:
  """
  Run flow for a user and return results.
  """

  api_key = request.json.get('api_key', '')
  name = request.json.get('name', '')
  data = request.json.get('data', '')

  print("Got /run_flow request:", request.json)

  if api_key == '':
    # proper web error "400 Bad request"
    abort(400, description=f"Missing N8N API_KEY: 'api_key' must be provided. Search query: `{api_key}`")

  try:
    response = service.main_flow(name, api_key, data)
    response = jsonify(response)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
  except Exception as e:
    if e == "Unauthorized":
      response = jsonify(error=str(e), message=f"Unauthorized: 'api_key' is invalid. Search query: `{api_key}`")
      response.status_code = 401
      response.headers.add('Access-Control-Allow-Origin', '*')
      return response
    else:
      response = jsonify(error=str(e), message=f"Internal Server Error {e}")
      response.status_code = 500
      response.headers.add('Access-Control-Allow-Origin', '*')
      return response


@app.route('/createProject', methods=['POST'])
def createProject(service: ProjectService, flaskExecutor: ExecutorInterface) -> Response:
  """
  Create a new project in UIUC.Chat
  """
  data = request.get_json()
  project_name = data.get('project_name', '')
  project_description = data.get('project_description', '')
  project_owner_email = data.get('project_owner_email', '')

  if project_name == '':
    # proper web error "400 Bad request"
    abort(400, description=f"Missing one or more required parameters: 'project_name' must be provided.")
  print(f"In /createProject for: {project_name}")
  result = service.create_project(project_name, project_description, project_owner_email)

  # Do long-running LLM task in the background.
  flaskExecutor.submit(service.generate_json_schema, project_name, project_description)

  response = jsonify(result)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/pubmedExtraction', methods=['GET'])
def pubmedExtraction():
  """
  Extracts metadata and download papers from PubMed.
  """
  result = extractPubmedData()

  response = jsonify(result)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/getProjectStats', methods=['GET'])
def get_project_stats(service: RetrievalService) -> Response:
  project_name = request.args.get('project_name', default='', type=str)

  if project_name == '':
    abort(400, description="Missing required parameter: 'project_name' must be provided.")

  project_stats = service.getProjectStats(project_name)

  response = jsonify(project_stats)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/getWeeklyTrends', methods=['GET'])
def get_weekly_trends(service: RetrievalService) -> Response:
  """
    Provides week-over-week percentage changes in key project metrics.
    """
  project_name = request.args.get('project_name', default='', type=str)

  if project_name == '':
    abort(400, description="Missing required parameter: 'project_name' must be provided.")

  weekly_trends = service.getWeeklyTrends(project_name)

  response = jsonify(weekly_trends)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/getModelUsageCounts', methods=['GET'])
def get_model_usage_counts(service: RetrievalService) -> Response:
  """
    Get counts of different models used in conversations.
    """
  project_name = request.args.get('project_name', default='', type=str)

  if project_name == '':
    abort(400, description="Missing required parameter: 'project_name' must be provided.")

  model_counts = service.getModelUsageCounts(project_name)

  response = jsonify(model_counts)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/send-transactional-email', methods=['POST'])
def send_transactional_email(service: ExportService):
  to_recipients: str = request.json.get('to_recipients_list', [])
  bcc_recipients: str = request.json.get('bcc_recipients_list', [])
  sender: str = request.json.get('sender', '')
  subject: str = request.json.get('subject', '')
  body_text: str = request.json.get('body_text', '')

  if sender == '' or to_recipients == [] or body_text == '':
    # proper web error "400 Bad request"
    abort(400,
          description=f"Missing required parameter: 'sender' and 'to_recipients' and 'body_text' must be provided.")

  try:
    send_email(subject=subject,
               body_text=body_text,
               sender=sender,
               recipients=to_recipients,
               bcc_recipients=bcc_recipients)
    response = Response(status=200)
  except Exception as e:
    response = Response(status=500)
    response.data = f"An unexpected error occurred: {e}".encode()

  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/updateProjectDocuments', methods=['GET'])
def updateProjectDocuments(flaskExecutor: ExecutorInterface) -> Response:
  project_name = request.args.get('project_name', default='', type=str)

  if project_name == '':
    abort(400, description="Missing required parameter: 'project_name' must be provided.")

  result = flaskExecutor.submit(webscrape_documents, project_name)

  response = jsonify({"message": "success"})
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


def configure(binder: Binder) -> None:
  binder.bind(ThreadPoolExecutorInterface, to=ThreadPoolExecutorAdapter(max_workers=10), scope=SingletonScope)
  binder.bind(ProcessPoolExecutorInterface, to=ProcessPoolExecutorAdapter(max_workers=10), scope=SingletonScope)
  binder.bind(RetrievalService, to=RetrievalService, scope=RequestScope)
  binder.bind(PosthogService, to=PosthogService, scope=SingletonScope)
  binder.bind(SentryService, to=SentryService, scope=SingletonScope)
  binder.bind(NomicService, to=NomicService, scope=SingletonScope)
  binder.bind(ExportService, to=ExportService, scope=SingletonScope)
  binder.bind(WorkflowService, to=WorkflowService, scope=SingletonScope)
  binder.bind(VectorDatabase, to=VectorDatabase, scope=SingletonScope)
  binder.bind(SQLDatabase, to=SQLDatabase, scope=SingletonScope)
  binder.bind(AWSStorage, to=AWSStorage, scope=SingletonScope)
  binder.bind(ExecutorInterface, to=FlaskExecutorAdapter(executor), scope=SingletonScope)


FlaskInjector(app=app, modules=[configure])

if __name__ == '__main__':
  app.run(debug=True, port=int(os.getenv("PORT", default=8000)))  # nosec -- reasonable bandit error suppression
