import base64
import json
import os
import tempfile
import uuid
import zipfile
from urllib.parse import urlparse

import pandas as pd
import requests
import xlsxwriter
from injector import inject

from ai_ta_backend.database.aws import AWSStorage
from ai_ta_backend.database.sql import SQLDatabase
from ai_ta_backend.executors.process_pool_executor import ProcessPoolExecutorAdapter
from ai_ta_backend.service.sentry_service import SentryService
from ai_ta_backend.utils.email.send_transactional_email import send_email
from ai_ta_backend.utils.export_utils import (
    _cleanup,
    _create_zip,
    _create_zip_for_user_convo_export,
    _initialize_base_name,
    _initialize_excel,
    _initialize_file_paths,
    _process_conversation,
    _process_conversation_for_user_convo_export,
)


def _task_method(index):
  print(f"Task {index} is running in process {os.getpid()}", flush=True)
  return index


class ExportService:

  @inject
  def __init__(self, sql: SQLDatabase, s3: AWSStorage, sentry: SentryService, executor: ProcessPoolExecutorAdapter):
    self.sql = sql
    self.s3 = s3
    self.sentry = sentry
    self.executor = executor

  def test_process(self):
    """
        This function is used to test the process.
        """
    futures = [self.executor.submit(_task_method, i) for i in range(5)]
    results = [future.result() for future in futures]
    print(results)
    return {"response": "Test process successful.", "results": results}

  def export_documents_json(self, course_name: str, from_date='', to_date=''):
    """
		This function exports the documents to a json file.
		1. If the number of documents is greater than 1000, it calls a background task to upload the documents to S3.
		2. If the number of documents is less than 1000, it fetches the documents and zips them.
		Args:
				course_name (str): The name of the course.
				from_date (str, optional): The start date for the data export. Defaults to ''.
				to_date (str, optional): The end date for the data export. Defaults to ''.
		"""

    response = self.sql.getDocumentsBetweenDates(course_name, from_date, to_date, 'documents')
    # add a condition to route to direct download or s3 download
    if response.count > 500:
      # call background task to upload to s3

      filename = course_name + '_' + str(uuid.uuid4()) + '_documents.zip'
      s3_filepath = f"courses/{course_name}/{filename}"
      # background task of downloading data - map it with above ID
      self.executor.submit(export_data_in_bg, response, "documents", course_name, s3_filepath)
      return {"response": 'Download from S3', "s3_path": s3_filepath}

    else:
      # Fetch data
      if response.count > 0:
        # batch download
        total_doc_count = response.count
        first_id = response.data[0]['id']
        last_id = response.data[-1]['id']

        print("total_doc_count: ", total_doc_count)
        print("first_id: ", first_id)
        print("last_id: ", last_id)

        curr_doc_count = 0
        filename = course_name + '_' + str(uuid.uuid4()) + '_documents.jsonl'
        file_path = os.path.join(os.getcwd(), filename)

        while curr_doc_count < total_doc_count:
          print("Fetching data from id: ", first_id)

          response = self.sql.getDocsForIdsGte(course_name, first_id)
          df = pd.DataFrame(response.data)
          curr_doc_count += len(response.data)

          # writing to file
          if not os.path.isfile(file_path):
            df.to_json(file_path, orient='records', lines=True)
          else:
            df.to_json(file_path, orient='records', lines=True, mode='a')

          if len(response.data) > 0:
            first_id = response.data[-1]['id'] + 1

        # Download file
        try:
          # zip file
          zip_filename = filename.split('.')[0] + '.zip'
          zip_file_path = os.path.join(os.getcwd(), zip_filename)

          with zipfile.ZipFile(zip_file_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(file_path, filename)

          os.remove(file_path)
          return {"response": (zip_file_path, zip_filename, os.getcwd())}
        except Exception as e:
          print(e)
          self.sentry.capture_exception(e)
          return {"response": "Error downloading file."}
      else:
        return {"response": "No data found between the given dates."}

  def export_convo_history_json(self, course_name: str, from_date='', to_date=''):
    """
		This function exports the conversation history to a csv file.
		Args:
				course_name (str): The name of the course.
				from_date (str, optional): The start date for the data export. Defaults to ''.
				to_date (str, optional): The end date for the data export. Defaults to ''.
		"""
    print("Exporting conversation history to json file...")

    response = self.sql.getDocumentsBetweenDates(course_name, from_date, to_date, 'llm-convo-monitor')

    if response.count > 500:
      # call background task to upload to s3
      filename = course_name[0:10] + '-' + str(generate_short_id()) + '_convos.zip'
      s3_filepath = f"courses/{course_name}/{filename}"
      # background task of downloading data - map it with above ID
      self.executor.submit(export_data_in_bg, response, "conversations", course_name, s3_filepath)
      return {"response": 'Download from S3', "s3_path": s3_filepath}

    # Fetch data
    if response.count > 0:
      print("id count greater than zero")
      first_id = response.data[0]['id']
      last_id = response.data[-1]['id']
      total_count = response.count

      filename = course_name[0:10] + '-convos.jsonl'
      file_path = os.path.join(os.getcwd(), filename)
      curr_count = 0
      # Fetch data in batches of 25 from first_id to last_id
      while curr_count < total_count:
        print("Fetching data from id: ", first_id)
        response = self.sql.getAllConversationsBetweenIds(course_name, first_id, last_id)
        # Convert to pandas dataframe
        df = pd.DataFrame(response.data)
        curr_count += len(response.data)

        # Append to csv file
        if not os.path.isfile(file_path):
          df.to_json(file_path, orient='records', lines=True)
        else:
          df.to_json(file_path, orient='records', lines=True, mode='a')

        # Update first_id
        if len(response.data) > 0:
          first_id = response.data[-1]['id'] + 1
          print("updated first_id: ", first_id)

      # Download file
      try:
        # zip file
        zip_filename = filename.split('.')[0] + '.zip'
        zip_file_path = os.path.join(os.getcwd(), zip_filename)

        with zipfile.ZipFile(zip_file_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
          zipf.write(file_path, filename)
        os.remove(file_path)

        return {"response": (zip_file_path, zip_filename, os.getcwd())}
      except Exception as e:
        print(e)
        self.sentry.capture_exception(e)
        return {"response": "Error downloading file!"}
    else:
      return {"response": "No data found between the given dates."}

  def export_conversations(self, course_name: str, from_date: str, to_date: str, emails: list):
    """
    Another function for exporting convos, emails are passed as a string.
    """
    print("Exporting conversation history to json file...")

    response = self.sql.getDocumentsBetweenDates(course_name, from_date, to_date, 'llm-convo-monitor')

    if response.count > 500:
      # call background task to upload to s3
      filename = course_name[0:10] + '-' + str(generate_short_id()) + '-convos.zip'
      s3_filepath = f"courses/{course_name}/{filename}"
      # background task of downloading data - map it with above ID
      self.executor.submit(export_data_in_bg_emails, response, "conversations", course_name, s3_filepath, emails)
      return {"response": 'Download from S3', "s3_path": s3_filepath}

    # Fetch data
    if response.count > 0:
      print("id count greater than zero")
      first_id = response.data[0]['id']
      last_id = response.data[-1]['id']
      total_count = response.count

      filename = course_name[0:10] + '-convos.jsonl'
      file_path = os.path.join(os.getcwd(), filename)
      curr_count = 0
      # Fetch data in batches of 25 from first_id to last_id
      while curr_count < total_count:
        print("Fetching data from id: ", first_id)
        response = self.sql.getAllConversationsBetweenIds(course_name, first_id, last_id)
        # Convert to pandas dataframe
        df = pd.DataFrame(response.data)
        curr_count += len(response.data)

        # Append to csv file
        if not os.path.isfile(file_path):
          df.to_json(file_path, orient='records', lines=True)
        else:
          df.to_json(file_path, orient='records', lines=True, mode='a')

        # Update first_id
        if len(response.data) > 0:
          first_id = response.data[-1]['id'] + 1
          print("updated first_id: ", first_id)

      # Download file
      try:
        # zip file
        zip_filename = filename.split('.')[0] + '.zip'
        zip_file_path = os.path.join(os.getcwd(), zip_filename)

        with zipfile.ZipFile(zip_file_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
          zipf.write(file_path, filename)
        os.remove(file_path)

        return {"response": (zip_file_path, zip_filename, os.getcwd())}
      except Exception as e:
        print(e)
        self.sentry.capture_exception(e)
        return {"response": "Error downloading file!"}
    else:
      return {"response": "No data found between the given dates."}

  def export_convo_history(self, course_name: str, from_date='', to_date=''):
    """
    This function exports the conversation history to a zip file containing markdown files, an Excel file, and a JSONL file.
    Args:
        course_name (str): The name of the course.
        from_date (str, optional): The start date for the data export. Defaults to ''.
        to_date (str, optional): The end date for the data export. Defaults to ''.
    """
    print(
        f"Exporting extended conversation history for course: {course_name}, from_date: {from_date}, to_date: {to_date}"
    )
    error_log = []

    try:
      response = self.sql.getDocumentsBetweenDates(course_name, from_date, to_date, 'llm-convo-monitor')
      responseCount = response.count or 0
      print(f"Received request to export: {responseCount} conversations")
    except Exception as e:
      error_log.append(f"Error fetching documents: {str(e)}")
      print(f"Error fetching documents: {str(e)}")
      return {"response": "Error fetching documents!"}

    if responseCount > 500:
      filename = course_name[0:10] + '-' + str(generate_short_id()) + '_convos_extended.zip'
      s3_filepath = f"courses/{course_name}/{filename}"
      print(
          f"Response count greater than 500, processing in background. Filename: {filename}, S3 filepath: {s3_filepath}"
      )
      self.executor.submit(export_data_in_bg_extended, response, "conversations", course_name, s3_filepath)
      return {"response": 'Download from S3', "s3_path": s3_filepath}

    if responseCount > 0:
      try:
        first_id = response.data[0]['id']
        last_id = response.data[-1]['id']
        total_count = response.count or 0
        print(f"Processing conversations. First ID: {first_id}, Last ID: {last_id}, Total count: {total_count}")

        file_paths = _initialize_file_paths(course_name)
        # print(f"Initialized file paths: {file_paths}")
        workbook, worksheet, wrap_format = _initialize_excel(file_paths['excel'])
        # print(f"Initialized Excel workbook at path: {file_paths['excel']}")
      except Exception as e:
        error_log.append(f"Error initializing file paths or Excel: {str(e)}")
        print(f"Error initializing file paths or Excel: {str(e)}")
        return {"response": "Error initializing file paths or Excel!"}

      curr_count = 0
      row_num = 1

      while curr_count < total_count:
        try:
          print(f"Fetching conversations from ID: {first_id} to {last_id}")
          response = self.sql.getAllConversationsBetweenIds(course_name, first_id, last_id)
          curr_count += len(response.data)
          # print(f"Fetched {len(response.data)} conversations, current count: {curr_count}")

          for convo in response.data:
            # print(f"Processing conversation ID: {convo['convo_id']}")
            _process_conversation(self.s3, convo, course_name, file_paths, worksheet, row_num, error_log, wrap_format)
            row_num += len(convo['convo']['messages'])

          if len(response.data) > 0:
            first_id = response.data[-1]['id'] + 1
            # print(f"Updated first ID to: {first_id}")
        except Exception as e:
          error_log.append(f"Error processing conversations: {str(e)}")
          print(f"Error processing conversations: {str(e)}")
          break

      print(f"Processed {curr_count} conversations, ready to finalize export.")

      try:
        workbook.close()
        print(f"Closed Excel workbook.")
        zip_file_path = _create_zip(file_paths, error_log)
        # print(f"Created zip file at path: {zip_file_path}")
        _cleanup(file_paths)
        # print(f"Cleaned up temporary files.")
      except Exception as e:
        error_log.append(f"Error finalizing export: {str(e)}")
        print(f"Error finalizing export: {str(e)}")
        return {"response": "Error finalizing export!"}

      return {"response": (zip_file_path, file_paths['zip'], os.getcwd())}
    else:
      print("No data found between the given dates.")
      return {"response": "No data found between the given dates."}

  def export_convo_history_user(self, user_email: str, project_name: str):
    """
    This function exports the conversation history for a specific user and project, used on chat page.
    Args:
        user_email (str): The email of the user.
        project_name (str): The name of the project.
    """
    error_log = []
    print(f"Exporting conversation history for user: {user_email}, project: {project_name}")
    try:
      # get all conversations for the user and project
      response = self.sql.getAllConversationsForUserAndProject(user_email, project_name)
      print("response: ", response)
      count = response.count or 0
      print(f"Received request to export: {count} conversations")

      if count > 500:
        filename = f"{user_email}_{project_name}_conversations.zip"
        s3_filepath = f"/conversations/{filename}"
        self.executor.submit(export_convo_history_user_bg, response.data, count, user_email, s3_filepath, project_name)
        return {"response": 'Download from S3', "s3_path": s3_filepath}

    except Exception as e:
      error_log.append(f"Error fetching documents: {str(e)}")
      print(f"Error fetching documents: {str(e)}")
      return {"response": "Error fetching documents!"}

    if count > 0:
      try:
        print(f"Processing {count} conversations for user: {user_email}, project: {project_name}")
        # row_num = 1
        # curr_count = 0
        with tempfile.TemporaryDirectory() as temp_dir:
          # Create directories for markdown and media
          markdown_dir = os.path.join(temp_dir, "markdown")
          media_dir = os.path.join(temp_dir, "media")
          os.makedirs(markdown_dir, exist_ok=True)
          os.makedirs(media_dir, exist_ok=True)

          # Fetch conversations in batches
          # while curr_count < count:
          # try:
          # response = self.sql.getAllConversationsForUserAndProject(user_email, project_name)
          # curr_count += len(response.data)
          # print("curr_count: ", curr_count)
          # Process conversations
          # print("response.data: ", response.data)
          for convo in response.data:
            _process_conversation_for_user_convo_export(self.s3, convo, project_name, markdown_dir, media_dir,
                                                        error_log)

            # row_num += 1

            # except Exception as e:
            #   error_log.append(f"Error fetching conversations: {str(e)}")
            #   print(f"Error fetching conversations: {str(e)}")
            #   return {"response": "Error fetching conversations!"}
            #   break
          # Create zip file
          zip_file_path = _create_zip_for_user_convo_export(markdown_dir, media_dir, error_log)

          return {"response": (zip_file_path, 'user_convo_export.zip', os.getcwd())}
      except Exception as e:
        error_log.append(f"Error creating markdown directory: {str(e)}")
        print(f"Error creating markdown directory: {str(e)}")
        return {"response": "Error creating markdown directory!"}
    else:
      print("No data found for the given user and project.")
      return {"response": "No data found for the given user and project."}


def export_convo_history_user_bg(conversations, count, user_email, s3_path, project_name):
  """
  This function is called in export_convo_history_user() to upload the conversations to S3.
  Args:
      conversations (list): The list of conversations to be uploaded.
      user_email (str): The email of the user.
      s3_path (str): The S3 path where the file will be uploaded.
  """
  s3 = AWSStorage()
  sql = SQLDatabase()

  # create a temporary directory
  with tempfile.TemporaryDirectory() as temp_dir:
    # create directories for markdown and media
    markdown_dir = os.path.join(temp_dir, "markdown")
    media_dir = os.path.join(temp_dir, "media")
    os.makedirs(markdown_dir, exist_ok=True)
    os.makedirs(media_dir, exist_ok=True)

    try:
      # row_num = 1
      curr_count = 0
      error_log = []
      while curr_count < count:
        try:
          response = sql.getAllConversationsForUserAndProject(user_email, project_name, curr_count)
          curr_count += len(response.data)

          for convo in response.data:
            _process_conversation_for_user_convo_export(s3, convo, project_name, markdown_dir, media_dir, error_log)
            # row_num += len(convo)

        except Exception as e:
          error_log.append(f"Error fetching conversations: {str(e)}")
          print(f"Error fetching conversations: {str(e)}")
          return {"response": "Error fetching conversations!"}
          break

      # create zip file
      zip_file_path = _create_zip_for_user_convo_export(markdown_dir, media_dir, error_log)

      # upload to S3
      s3_file = f"conversations/{os.path.basename('user_convo_export.zip')}"
      s3.upload_file(zip_file_path, os.environ['S3_BUCKET_NAME'], s3_file)
      s3_url = s3.generatePresignedUrl('get_object', os.environ['S3_BUCKET_NAME'], s3_file, 172800)

      # send email
      subject = f"UIUC.chat Conversation History Export Complete for {user_email}"
      body_text = f"The data export for {user_email} is complete.\n\nYou can download the file from the following link: \n\n{s3_url}\n\nThis link will expire in 48 hours."
      email_status = send_email(subject, body_text, os.environ['EMAIL_SENDER'], [user_email], [])
      print(f"Email sent to {user_email}: {email_status}")

      return "File uploaded to S3. Email sent to user."
    except Exception as e:
      error_log.append(f"Error finalizing export: {str(e)}")
      print(f"Error finalizing export: {str(e)}")
      return {"response": "Error finalizing export!"}


def export_data_in_bg_extended(response, download_type, course_name, s3_path):
  """
  This function is called to upload the extended conversation history to S3.
  Args:
      response (dict): The response from the Supabase query.
      download_type (str): The type of download - 'documents' or 'conversations'.
      course_name (str): The name of the course.
      s3_path (str): The S3 path where the file will be uploaded.
  """
  print(f"Starting export in background for course: {course_name}, download_type: {download_type}, s3_path: {s3_path}")
  s3 = AWSStorage()
  sql = SQLDatabase()

  total_doc_count = response.count
  first_id = response.data[0]['id']
  curr_doc_count = 0

  file_paths = _initialize_file_paths(course_name)
  workbook, worksheet, wrap_format = _initialize_excel(file_paths['excel'])
  print(f"Initialized Excel workbook at path: {file_paths['excel']}")

  row_num = 1
  error_log = []
  # Process conversations in batches
  while curr_doc_count < total_doc_count:
    try:
      response = sql.getAllFromTableForDownloadType(course_name, download_type, first_id)
      curr_doc_count += len(response.data)

      for convo in response.data:
        print(f"Processing conversation ID: {convo['convo_id']}")
        _process_conversation(s3, convo, course_name, file_paths, worksheet, row_num, error_log, wrap_format)
        row_num += len(convo['convo']['messages'])

      # Update first_id for the next batch
      if len(response.data) > 0:
        first_id = response.data[-1]['id'] + 1
        # print(f"Updated first ID to: {first_id}")
    except Exception as e:
      error_log.append(f"Error processing conversations: {str(e)}")
      print(f"Error processing conversations: {str(e)}")
      break

  print(f"Processed {curr_doc_count} conversations, ready to finalize export.")

  try:
    workbook.close()
    print(f"Closed Excel workbook.")
    zip_file_path = _create_zip(file_paths, error_log)
    print(f"Created zip file at path: {zip_file_path}")
    _cleanup(file_paths)
    print(f"Cleaned up temporary files.")

    # Upload the zip file to S3
    s3.upload_file(zip_file_path, os.environ['S3_BUCKET_NAME'], s3_path)
    os.remove(zip_file_path)
    s3_url = s3.generatePresignedUrl('get_object', os.environ['S3_BUCKET_NAME'], s3_path, 172800)

    # Fetch course metadata to get admin emails
    headers = {"Authorization": f"Bearer {os.environ['VERCEL_READ_ONLY_API_KEY']}", "Content-Type": "application/json"}
    hget_url = str(os.environ['VERCEL_BASE_URL']) + "course_metadatas/" + course_name
    response = requests.get(hget_url, headers=headers)
    course_metadata = response.json()
    course_metadata = json.loads(course_metadata['result'])
    admin_emails = course_metadata['course_admins']
    bcc_emails = []

    # Handle specific email cases
    if 'kvday2@illinois.edu' in admin_emails:
      admin_emails.remove('kvday2@illinois.edu')
      bcc_emails.append('kvday2@illinois.edu')

    admin_emails.append(course_metadata['course_owner'])
    admin_emails = list(set(admin_emails))

    if len(admin_emails) == 0:
      return "No admin emails found. Email not sent."

    # Send email notification to course admins
    subject = "UIUC.chat Conversation History Export Complete for " + course_name
    body_text = "The data export for " + course_name + " is complete.\n\nYou can download the file from the following link: \n\n" + s3_url + "\n\nThis link will expire in 48 hours."
    email_status = send_email(subject, body_text, os.environ['EMAIL_SENDER'], admin_emails, bcc_emails)
    print("email_status: ", email_status)

    return "File uploaded to S3. Email sent to admins."

  except Exception as e:
    error_log.append(f"Error finalizing export: {str(e)}")
    print(f"Error finalizing export: {str(e)}")
    return {"response": "Error finalizing export!"}
    # Encountered pickling error while running the background task. So, moved the function outside the class.


def export_data_in_bg(response, download_type, course_name, s3_path):
  """
	This function is called in export_documents_csv() to upload the documents to S3.
	1. download the documents in batches of 100 and upload them to S3.
	2. generate a pre-signed URL for the S3 file.
	3. send an email to the course admins with the pre-signed URL.

	Args:
		response (dict): The response from the Supabase query.
		download_type (str): The type of download - 'documents' or 'conversations'.
		course_name (str): The name of the course.
	  s3_path (str): The S3 path where the file will be uploaded.
	"""
  s3 = AWSStorage()
  sql = SQLDatabase()

  total_doc_count = response.count
  first_id = response.data[0]['id']
  print("total_doc_count: ", total_doc_count)
  print("pre-defined s3_path: ", s3_path)

  curr_doc_count = 0
  filename = s3_path.split('/')[-1].split('.')[0] + '.jsonl'
  file_path = os.path.join(os.getcwd(), filename)

  # download data in batches of 100
  while curr_doc_count < total_doc_count:
    print("Fetching data from id: ", first_id)
    response = sql.getAllFromTableForDownloadType(course_name, download_type, first_id)
    df = pd.DataFrame(response.data)
    curr_doc_count += len(response.data)

    # writing to file
    if not os.path.isfile(file_path):
      df.to_json(file_path, orient='records', lines=True)
    else:
      df.to_json(file_path, orient='records', lines=True, mode='a')

    if len(response.data) > 0:
      first_id = response.data[-1]['id'] + 1

  # zip file
  zip_filename = filename.split('.')[0] + '.zip'
  zip_file_path = os.path.join(os.getcwd(), zip_filename)

  with zipfile.ZipFile(zip_file_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
    zipf.write(file_path, filename)

  print("zip file created: ", zip_file_path)

  try:
    # upload to S3

    #s3_file = f"courses/{course_name}/exports/{os.path.basename(zip_file_path)}"
    s3_file = f"courses/{course_name}/{os.path.basename(s3_path)}"
    s3.upload_file(zip_file_path, os.environ['S3_BUCKET_NAME'], s3_file)

    # remove local files
    os.remove(file_path)
    os.remove(zip_file_path)

    print("file uploaded to s3: ", s3_file)

    # generate presigned URL
    s3_url = s3.generatePresignedUrl('get_object', os.environ['S3_BUCKET_NAME'], s3_path, 172800)

    # get admin email IDs
    headers = {"Authorization": f"Bearer {os.environ['VERCEL_READ_ONLY_API_KEY']}", "Content-Type": "application/json"}

    hget_url = str(os.environ['VERCEL_BASE_URL']) + "course_metadatas/" + course_name
    response = requests.get(hget_url, headers=headers)
    course_metadata = response.json()
    course_metadata = json.loads(course_metadata['result'])
    admin_emails = course_metadata['course_admins']
    bcc_emails = []

    # check for Kastan's email and move to bcc
    if 'kvday2@illinois.edu' in admin_emails:
      admin_emails.remove('kvday2@illinois.edu')
      bcc_emails.append('kvday2@illinois.edu')

    # add course owner email to admin_emails
    admin_emails.append(course_metadata['course_owner'])
    admin_emails = list(set(admin_emails))
    print("admin_emails: ", admin_emails)
    print("bcc_emails: ", bcc_emails)

    # add a check for emails, don't send email if no admin emails
    if len(admin_emails) == 0:
      return "No admin emails found. Email not sent."

    # send email to admins
    if download_type == "documents":
      subject = "UIUC.chat Documents Export Complete for " + course_name
    elif download_type == "conversations":
      subject = "UIUC.chat Conversation History Export Complete for " + course_name
    else:
      subject = "UIUC.chat Export Complete for " + course_name
    body_text = "The data export for " + course_name + " is complete.\n\nYou can download the file from the following link: \n\n" + s3_url + "\n\nThis link will expire in 48 hours."
    email_status = send_email(subject, body_text, os.environ['EMAIL_SENDER'], admin_emails, bcc_emails)
    print("email_status: ", email_status)

    return "File uploaded to S3. Email sent to admins."

  except Exception as e:
    print(e)
    return "Error: " + str(e)


def export_data_in_bg_emails(response, download_type, course_name, s3_path, emails):
  """
	This function is called in export_documents_csv() to upload the documents to S3.
	1. download the documents in batches of 100 and upload them to S3.
	2. generate a pre-signed URL for the S3 file.
	3. send an email to the course admins with the pre-signed URL.

	Args:
		response (dict): The response from the Supabase query.
		download_type (str): The type of download - 'documents' or 'conversations'.
		course_name (str): The name of the course.
	  s3_path (str): The S3 path where the file will be uploaded.
	"""
  s3 = AWSStorage()
  sql = SQLDatabase()

  total_doc_count = response.count
  first_id = response.data[0]['id']
  print("total_doc_count: ", total_doc_count)
  print("pre-defined s3_path: ", s3_path)

  curr_doc_count = 0
  filename = s3_path.split('/')[-1].split('.')[0] + '.jsonl'
  file_path = os.path.join(os.getcwd(), filename)

  # download data in batches of 100
  while curr_doc_count < total_doc_count:
    print("Fetching data from id: ", first_id)
    response = sql.getAllFromTableForDownloadType(course_name, download_type, first_id)
    df = pd.DataFrame(response.data)
    curr_doc_count += len(response.data)

    # writing to file
    if not os.path.isfile(file_path):
      df.to_json(file_path, orient='records', lines=True)
    else:
      df.to_json(file_path, orient='records', lines=True, mode='a')

    if len(response.data) > 0:
      first_id = response.data[-1]['id'] + 1

  # zip file
  zip_filename = filename.split('.')[0] + '.zip'
  zip_file_path = os.path.join(os.getcwd(), zip_filename)

  with zipfile.ZipFile(zip_file_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
    zipf.write(file_path, filename)

  print("zip file created: ", zip_file_path)

  try:
    # upload to S3

    #s3_file = f"courses/{course_name}/exports/{os.path.basename(zip_file_path)}"
    s3_file = f"courses/{course_name}/{os.path.basename(s3_path)}"
    s3.upload_file(zip_file_path, os.environ['S3_BUCKET_NAME'], s3_file)

    # remove local files
    os.remove(file_path)
    os.remove(zip_file_path)

    print("file uploaded to s3: ", s3_file)

    # generate presigned URL
    s3_url = s3.generatePresignedUrl('get_object', os.environ['S3_BUCKET_NAME'], s3_path, 172800)

    admin_emails = emails
    bcc_emails = []

    print("admin_emails: ", admin_emails)
    print("bcc_emails: ", bcc_emails)

    # add a check for emails, don't send email if no admin emails
    if len(admin_emails) == 0:
      return "No admin emails found. Email not sent."

    # send email to admins
    if download_type == "documents":
      subject = "UIUC.chat Documents Export Complete for " + course_name
    elif download_type == "conversations":
      subject = "UIUC.chat Conversation History Export Complete for " + course_name
    else:
      subject = "UIUC.chat Export Complete for " + course_name
    body_text = "The data export for " + course_name + " is complete.\n\nYou can download the file from the following link: \n\n" + s3_url + "\n\nThis link will expire in 48 hours."
    email_status = send_email(subject, body_text, os.environ['EMAIL_SENDER'], admin_emails, bcc_emails)
    print("email_status: ", email_status)

    return "File uploaded to S3. Email sent to admins."

  except Exception as e:
    print(e)
    return "Error: " + str(e)


def generate_short_id():
  return base64.urlsafe_b64encode(uuid.uuid4().bytes)[:5].decode('utf-8')
