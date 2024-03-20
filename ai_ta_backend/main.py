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
from ai_ta_backend.service.retrieval_service import RetrievalService
from ai_ta_backend.service.sentry_service import SentryService
from posthog import Posthog
import ray
import sentry_sdk

# from ai_ta_backend.canvas import CanvasAPI

# from ai_ta_backend.export_data import export_convo_history_json, export_documents_json, check_s3_path_and_download
# from ai_ta_backend.nomic_logging import get_nomic_map, log_convo_to_nomic, create_document_map
# from ai_ta_backend.vector_database import Ingest
# from ai_ta_backend.web_scrape import WebScrape, mit_course_download
from ai_ta_backend.journal_ingest import (get_arxiv_fulltext, downloadSpringerFulltext, 
                                          downloadElsevierFulltextFromId, getFromDoi, 
                                          downloadPubmedArticles, searchPubmedArticlesWithEutils,
                                          searchScopusArticles, searchScienceDirectArticles)

# Sentry.io error logging
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    # Set traces_sample_rate to 1.0 to capture 100% of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100% of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
    enable_tracing=True)

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


@app.route('/getTopContexts', methods=['GET'])
def getTopContexts(service: RetrievalService) -> Response:
  """Get most relevant contexts for a given search query.
  
  Return value

  ## GET arguments
  course name (optional) str
      A json response with TBD fields.
  search_query
  top_n
  
  Returns
  -------
  JSON
      A json response with TBD fields.
  Metadata fileds
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

  found_documents = service.getTopContexts(search_query, course_name, token_limit)

  response = jsonify(found_documents)
  response.headers.add('Access-Control-Allow-Origin', '*')
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


@app.route('/onResponseCompletion', methods=['POST'])
def logToNomic(service: NomicService, flaskExecutor: ExecutorInterface):
  data = request.get_json()
  course_name = data['course_name']
  conversation = data['conversation']

  if course_name == '' or conversation == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing one or more required parameters: 'course_name' and 'conversation' must be provided. Course name: `{course_name}`, Conversation: `{conversation}`"
    )
  print(f"In /onResponseCompletion for course: {course_name}")

  # background execution of tasks!!
  response = flaskExecutor.submit(service.log_convo_to_nomic, course_name, data)
  response = jsonify({'outcome': 'success'})
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

@app.route('/get-arxiv-fulltext', methods=['GET'])
def get_arxiv_data():
  search_query: str = request.args.get('search_query', default='', type=str)
  arxiv_id = request.args.get('arxiv_id', default='', type=str)
  print("In /get-arxiv-fulltext: ", search_query)

  if search_query == '' and arxiv_id == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing required parameters: 'arxiv_id' or 'search_query' must be provided."
    )

  fulltext = get_arxiv_fulltext(search_query, arxiv_id)

  response = jsonify(fulltext)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response

@app.route('/get-springer-fulltext', methods=['GET'])
def get_springer_data():
  course_name: str = request.args.get('course_name', default='', type=str)
  issn = request.args.get('issn', default='', type=str)
  subject = request.args.get('subject', default='', type=str)
  journal = request.args.get('journal', default='', type=str)
  title = request.args.get('title', default='', type=str)
  doi = request.args.get('doi', default='', type=str)

  print("In /get-springer-fulltext")

  if (issn == '' and subject == '' and journal == '' and title == '' and doi == '') or course_name == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing required parameters: 'issn' or 'subject' or 'title' or 'journal' or 'doi' and 'course_name' must be provided."
    )

  fulltext = downloadSpringerFulltext(issn, subject, journal, title, doi, course_name)

  response = jsonify(fulltext)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response

@app.route('/get-elsevier-fulltext', methods=['GET'])
def get_elsevier_data():
  id = request.args.get('id', default='', type=str)
  id_type = request.args.get('id_type', default='doi', type=str)
  course_name = request.args.get('course_name', default='', type=str)

  print("In /get-elsevier-fulltext")

  if id == '' or id_type == '' or course_name == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing required parameters: 'id', 'id_type' [doi, eid, pii, pubmed_id] and 'course_name' must be provided."
    )

  fulltext = downloadElsevierFulltextFromId(id, id_type, course_name)

  response = jsonify(fulltext)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


@app.route('/getArticleFromDoi', methods=['GET'])
def getArticleFromDoi():
  doi = request.args.get('doi', default='', type=str)
  course_name = request.args.get('course_name', default='', type=str)

  print("In /getArticleFromDoi")

  if doi == '' or course_name == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing required parameters: 'doi' and 'course_name' must be provided."
    )

  fulltext = getFromDoi(doi, course_name)

  response = jsonify(fulltext)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response

@app.route('/getArticleFromPubmed', methods=['GET'])
def getArticleFromPubmed():
  id = request.args.get('id', default='', type=str)
  from_date = request.args.get('from_date', default='', type=str)
  until_date = request.args.get('until_date', default='', type=str)
  format = request.args.get('format', default='', type=str)
  course_name = request.args.get('course_name', default='', type=str)

  print("In /getArticleFromPubmed")

  if (id == '' and from_date  == '' and until_date == '') or course_name == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing required parameters: 'id', 'from_date', or 'until_date' and 'course_name' must be provided."
    )

  fulltext = downloadPubmedArticles(id, course_name, from_date=from_date, until_date=until_date, format=format)

  response = jsonify(fulltext)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response

@app.route('/getPubmedArticleWithEutils', methods=['GET'])
def getPubmedArticleWithEutils():
  course_name = request.args.get('course_name', default='', type=str)
  title = request.args.get('title', default='', type=str)
  journal = request.args.get('journal', default='', type=str)
  search_query = request.args.get('search_query', default='', type=str)

  print("In /getPubmedArticleWithEutils")

  if (title == '' and journal  == '' and search_query == '') or course_name == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing required parameters: 'title', 'journal', or 'search_query' and 'course_name' must be provided."
    )

  fulltext = searchPubmedArticlesWithEutils(course_name, search_query, title, journal)

  response = jsonify(fulltext)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response

@app.route('/getScopusArticles', methods=['GET'])
def getScopusArticles() -> Response:
  """
  Download full-text article from Scopus
  """
  course_name = request.args.get('course_name', default='', type=str)
  article_title = request.args.get('article_title', default='', type=str)
  journal_title = request.args.get('journal_title', default='', type=str)
  search_str = request.args.get('search_str', default='', type=str)
  subject = request.args.get('subject', default='', type=str)
  issn = request.args.get('issn', default='', type=str)

  print("In /getScopusArticles")

  if (article_title == '' and journal_title  == '' and search_str == '' and issn == '' and subject == '') or course_name == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing required parameters: 'article_title', 'journal_title', 'issn', 'subject' or 'search_str' and 'course_name' must be provided."
    )

  fulltext = searchScopusArticles(course_name, search_str, article_title, journal_title, subject, issn)

  response = jsonify(fulltext)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response

@app.route('/getScienceDirectArticles', methods=['GET'])
def getScienceDirectArticles() -> Response:
  """
  Download full-text article from Scopus
  """
  course_name = request.args.get('course_name', default='', type=str)
  article_title = request.args.get('article_title', default='', type=str)
  journal_title = request.args.get('journal_title', default='', type=str)
  search_str = request.args.get('search_str', default='', type=str)
  

  print("In /getScienceDirectArticles")

  if (article_title == '' and journal_title  == '' and search_str == '') or course_name == '':
    # proper web error "400 Bad request"
    abort(
        400,
        description=
        f"Missing required parameters: 'article_title', 'journal_title' or 'search_str' and 'course_name' must be provided."
    )

  fulltext = searchScienceDirectArticles(course_name, search_str, article_title, journal_title)

  response = jsonify(fulltext)
  response.headers.add('Access-Control-Allow-Origin', '*')
  return response


def configure(binder: Binder) -> None:
  binder.bind(RetrievalService, to=RetrievalService, scope=RequestScope)
  binder.bind(PosthogService, to=PosthogService, scope=SingletonScope)
  binder.bind(SentryService, to=SentryService, scope=SingletonScope)
  binder.bind(NomicService, to=NomicService, scope=SingletonScope)
  binder.bind(ExportService, to=ExportService, scope=SingletonScope)
  binder.bind(VectorDatabase, to=VectorDatabase, scope=SingletonScope)
  binder.bind(SQLDatabase, to=SQLDatabase, scope=SingletonScope)
  binder.bind(AWSStorage, to=AWSStorage, scope=SingletonScope)
  binder.bind(ExecutorInterface, to=FlaskExecutorAdapter(executor), scope=SingletonScope)
  binder.bind(ThreadPoolExecutorInterface, to=ThreadPoolExecutorAdapter, scope=SingletonScope)
  binder.bind(ProcessPoolExecutorInterface, to=ProcessPoolExecutorAdapter, scope=SingletonScope)


FlaskInjector(app=app, modules=[configure])

if __name__ == '__main__':
  app.run(debug=True, port=int(os.getenv("PORT", default=8000)))  # nosec -- reasonable bandit error suppression
