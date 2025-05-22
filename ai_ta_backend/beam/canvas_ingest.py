"""
To deploy: beam deploy canvas_ingest.py:canvas_ingest
Use CAII gmail to auth.
Add secrets with `beam secret create CANVAS_ACCESS_TOKEN <SECRET_HERE>`
Show secrets with `beam secret list`
"""

import json
import os
import re
import shutil
import uuid
from typing import Any, Dict, List

import beam

if beam.env.is_remote():
  # Only import these in the Cloud container, not when building the container.
  import boto3
  import requests
  import sentry_sdk
  import yt_dlp
  from bs4 import BeautifulSoup
  from canvasapi import Canvas
  from posthog import Posthog

image = beam.Image(
    python_version="python3.10",
    python_packages=[
        "boto3==1.28.79",
        "posthog==3.1.0",
        "canvasapi==3.2.0",
        "sentry-sdk==1.39.1",
        "bs4==0.0.2",
        "yt-dlp==2024.7.16",
    ],
)

volume_path = "./canvas_ingest"

ourSecrets = [
    "CANVAS_ACCESS_TOKEN",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "POSTHOG_API_KEY",
    "S3_BUCKET_NAME",
    "BEAM_API_KEY",
]


def loader():
  s3_client = boto3.client(
      's3',
      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
  )
  canvas_client = Canvas("https://canvas.illinois.edu", os.getenv('CANVAS_ACCESS_TOKEN'))
  posthog = Posthog(sync_mode=True, project_api_key=os.environ['POSTHOG_API_KEY'], host='https://app.posthog.com')

  sentry_sdk.init(
      dsn="https://examplePublicKey@o0.ingest.sentry.io/0",
      enable_tracing=True,
  )

  return s3_client, canvas_client, posthog


@beam.endpoint(
    name='canvas_ingest',
    workers=2,
    cpu=1,
    memory="2Gi",
    keep_warm_seconds=0,
    max_pending_tasks=1_000,
    timeout=60 * 15,  # 15 minutes
    on_start=loader,
    autoscaler=beam.QueueDepthAutoscaler(tasks_per_container=2, max_containers=3),
    secrets=ourSecrets,
    image=image,
    volumes=[beam.Volume(name="canvas_ingest", mount_path=volume_path)])
def canvas_ingest(context, **inputs: Dict[str, Any]):
  """
  Main function.
  Params:
    course_name: str
    canvas_url: str
  """
  s3_client, canvas_client, posthog = context.on_start_value  # grab from loader function

  course_name: List[str] | str = inputs.get('course_name', '')
  canvas_url: List[str] | str | None = inputs.get('canvas_url', None)
  # Options:
  files: bool = inputs.get('files', True)
  pages: bool = inputs.get('pages', True)
  modules: bool = inputs.get('modules', True)
  syllabus: bool = inputs.get('syllabus', True)
  assignments: bool = inputs.get('assignments', True)
  discussions: bool = inputs.get('discussions', True)
  options = {
      'files': str(files).lower() == 'true',
      'pages': str(pages).lower() == 'true',
      'modules': str(modules).lower() == 'true',
      'syllabus': str(syllabus).lower() == 'true',
      'assignments': str(assignments).lower() == 'true',
      'discussions': str(discussions).lower() == 'true',
  }
  print("Course Name: ", course_name)
  print("Canvas URL: ", canvas_url)
  print("Download Options: ", options)

  try:
    # canvas.illinois.edu/courses/COURSE_CODE
    match = re.search(r'canvas\.illinois\.edu/courses/([^/]+)', canvas_url)
    canvas_course_id = match.group(1) if match else None

    ingester = CanvasIngest(s3_client, canvas_client, posthog)
    ingester.ingest_course_content(canvas_course_id=canvas_course_id,
                                   course_name=course_name,
                                   content_ingest_dict=options)

    # Can get names, but not emails. e.g. Collected names:  ['UIUC Course AI', 'Shannon Bradley', 'Asmita Vijay Dabholkar', 'Kastan Day', 'Volodymyr Kindratenko', 'Max Lindsey', 'Rohan Marwaha', 'Joshua Min', 'Neha Sheth', 'George Tamas']
    # ingester.add_users(canvas_course_id, course_name)
  except Exception as e:
    print("Top level error:", e)
    sentry_sdk.capture_exception(e)
    return "Failed"


class CanvasIngest():

  def __init__(self, s3_client, canvas_client, posthog):
    self.posthog = posthog
    self.s3_client = s3_client
    self.canvas_client = canvas_client
    self.headers = {"Authorization": "Bearer " + os.getenv('CANVAS_ACCESS_TOKEN')}

  def upload_file(self, file_path: str, bucket_name: str, object_name: str):
    self.s3_client.upload_file(file_path, bucket_name, object_name)

  def add_users(self, canvas_course_id: str, course_name: str):
    """
        Get all users in a course by course ID and add them to uiuc.chat course
        - Student profile does not have access to emails.
        - Currently collecting all names in a list.
        """
    try:
      course = self.canvas_client.get_course(canvas_course_id)
      users = course.get_users()

      user_names = []
      for user in users:
        user_names.append(user.name)

      print("Collected names: ", user_names)

      if len(user_names) > 0:
        return "Success"
      else:
        return "Failed"
    except Exception as e:
      sentry_sdk.capture_exception(e)
      return "Failed to `add users`! Error: " + str(e)

  def download_course_content(self, canvas_course_id: int, dest_folder: str, content_ingest_dict: dict) -> str:
    """
    Downloads all Canvas course materials through the course ID and stores in local directory.
    1. Iterate through content_ingest_dict and download all.
    2. Maintain a list of URLs and convert HTML strings to proper format.
    """
    print("In download_course_content")

    print(f"content_ingest_dict (in final download section) = {content_ingest_dict}")

    try:
      api_path = "https://canvas.illinois.edu/api/v1/courses/" + str(canvas_course_id)

      # Iterate over the content_ingest_dict
      for key, value in content_ingest_dict.items():
        if value is True:
          if key == 'files':
            self.download_files(dest_folder, api_path)
          elif key == 'pages':
            self.download_pages(dest_folder, api_path)
          elif key == 'modules':
            status = self.download_modules(dest_folder, api_path)
            print("status=", status)
          elif key == 'syllabus':
            self.download_syllabus(dest_folder, api_path)
          elif key == 'assignments':
            self.download_assignments(dest_folder, api_path)
          elif key == 'discussions':
            self.download_discussions(dest_folder, api_path)

      # at this point, we have all canvas files in the dest_folder.
      # parse all HTML files in dest_folder and extract URLs
      extracted_urls_from_html = self.extract_urls_from_html(dest_folder)
      #print("extract_urls_from_html=", extract_urls_from_html)

      # links - canvas files, external urls, embedded videos
      file_links = extracted_urls_from_html.get('file_links', [])
      if file_links:
        file_download_status = self.download_files_from_urls(file_links, canvas_course_id, dest_folder)
        print("File download status: ", file_download_status)

      video_links = extracted_urls_from_html.get('video_links', [])
      if video_links:
        video_download_status = self.download_videos_from_urls(video_links, canvas_course_id, dest_folder)
        print("Video download status: ", video_download_status)
      #external_links = extract_urls_from_html.get('external_links', []) --> we can webscrape these later

      data_api_endpoints = extracted_urls_from_html.get('data_api_endpoints', [])
      if data_api_endpoints:
        data_api_endpoints_status = self.download_content_from_api_endpoints(data_api_endpoints, canvas_course_id,
                                                                             dest_folder)
        print("Data API Endpoints download status: ", data_api_endpoints_status)

      return "Success"
    except Exception as e:
      sentry_sdk.capture_exception(e)
      return "Failed! Error: " + str(e)

  def ingest_course_content(self,
                            canvas_course_id: int,
                            course_name: str,
                            content_ingest_dict: Dict[str, bool] = None) -> str:
    """
    Ingests all Canvas course materials through the course ID.
    1. Download zip file from Canvas and store in local directory
    2. Upload all files to S3
    3. Call bulk_ingest() to ingest all files into QDRANT
    4. Delete extracted files from local directory
    """

    print("In ingest_course_content")
    try:
      # a dictionary of all contents we want to ingest - files, pages, modules, syllabus, assignments, discussions.
      if content_ingest_dict is None:
        content_ingest_dict = {
            'files': True,
            'pages': True,
            'modules': True,
            'syllabus': True,
            'assignments': True,
            'discussions': True
        }

      print("Content Dictionary: ", content_ingest_dict)
      # Create a canvas directory with a course folder inside it.
      canvas_dir = os.path.join(volume_path, "canvas_materials")
      folder_name = "canvas_course_" + str(canvas_course_id) + "_ingest"
      folder_path = os.path.join(canvas_dir, folder_name)

      if os.path.exists(canvas_dir):
        print("Canvas directory already exists")
      else:
        os.mkdir(canvas_dir)
        print("Canvas directory created")

      if os.path.exists(folder_path):
        print("Course folder already exists")
      else:
        os.mkdir(folder_path)
        print("Course folder created")

      # Download course content
      self.download_course_content(canvas_course_id, folder_path, content_ingest_dict)

      # Upload files to S3

      # get a list of ALL files (recursive) in folder_path
      all_file_paths = [os.path.join(dp, f) for dp, dn, filenames in os.walk(folder_path) for f in filenames]

      all_s3_paths = []
      all_readable_filenames = []
      # Upload each file to S3
      for file_path in all_file_paths:
        file_name = os.path.basename(file_path)
        extension = os.path.splitext(file_name)[1]
        name_without_extension = re.sub(r'[^a-zA-Z0-9]', '-', os.path.splitext(file_name)[0])
        uid = str(uuid.uuid4()) + '-'

        unique_filename = uid + name_without_extension + extension
        s3_path = "courses/" + course_name + "/" + unique_filename
        readable_filename = name_without_extension + extension
        all_s3_paths.append(s3_path)
        all_readable_filenames.append(readable_filename)
        print(f"Uploading file: {readable_filename}")
        self.upload_file(file_path, os.environ['S3_BUCKET_NAME'], s3_path)

      # Delete files from local directory
      shutil.rmtree(folder_path)

      # Ingest files
      url = 'https://app.beam.cloud/taskqueue/ingest_task_queue/latest'
      headers = {
          'Accept': '*/*',
          'Accept-Encoding': 'gzip, deflate',
          'Authorization': f"Bearer {os.environ['BEAM_API_KEY']}",
          'Content-Type': 'application/json',
      }

      print("Number of docs to ingest: {len(all_s3_paths)}")
      for s3_path, readable_filename in zip(all_s3_paths, all_readable_filenames):
        data = {
            'course_name': course_name,
            'readable_filename': readable_filename,
            's3_paths': s3_path,
            'base_url': "https://canvas.illinois.edu/courses/" + str(canvas_course_id),
        }
        print(f"Posting readable_filename: '{readable_filename}' with S3 path: '{s3_path}'")
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print("Beam Ingest Task Queue response: ", response.json())

      # legacy method
      # ingest = Ingest()
      # canvas_ingest = ingest.bulk_ingest(s3_paths, course_name=course_name)
      return "success"

    except Exception as e:
      print(e)
      sentry_sdk.capture_exception(e)
      return "Failed"

  def download_files(self, dest_folder: str, api_path: str) -> str:
    """
    Downloads all files in a Canvas course into given folder.
    """
    print("In download_files")
    try:
      files_request = requests.get(api_path + "/files", headers=self.headers)
      files = files_request.json()

      if 'status' in files and files['status'] == 'unauthorized':
        # Student user probably blocked for Files access
        print(f"Unauthorized to access files: {files['status']}")
        return "Unauthorized to access files!"
      else:
        # TA user or authorized user will get a JSON of file objects
        for file in files:
          try:
            filename = file['filename']
            download_url = file['url']
            print("Downloading file: ", filename)
            response = requests.get(download_url, headers=self.headers)
            full_path = os.path.join(dest_folder, filename)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, 'wb') as f:
              f.write(response.content)
          except Exception as e:
            print(f"❌❌❌ Error downloading file '{filename}', error: {e}")
            sentry_sdk.capture_exception(e)
            continue

      return "Success"
    except Exception as e:
      sentry_sdk.capture_exception(e)
      return "Failed! Error: " + str(e)

  def download_pages(self, dest_folder: str, api_path: str) -> str:
    """
    Downloads all pages as HTML and stores them in given folder.
    """
    try:
      pages_request = requests.get(api_path + "/pages", headers=self.headers)
      pages = pages_request.json()

      # check if unauthorized
      if 'status' in pages and pages['status'] == 'unauthorized':
        # Student user probably blocked for Pages access
        return "Unauthorized to access pages!"

      for page in pages:
        # check if published and visible to students
        if 'published' in page and not page['published']:
          print(f"Page not published: {page['title']}")
          continue

        if 'hide_from_students' in page and page['hide_from_students']:
          print("Page hidden from students: ", page['title'])
          continue

        # page is visible to students
        if page['html_url'] != '':
          page_name = page['url'] + ".html"
          page_content_request = requests.get(api_path + "/pages/" + str(page['page_id']), headers=self.headers)
          page_body = page_content_request.json()['body']

          with open(dest_folder + "/" + page_name, 'w') as html_file:
            html_file.write(page_body)

      return "Success"
    except Exception as e:
      return "Failed! Error: " + str(e)

  def download_syllabus(self, dest_folder: str, api_path: str) -> str:
    """
    Downloads syllabus as HTML and stores in given folder.
    """
    print("In download_syllabus")
    try:
      course_settings_request = requests.get(api_path + "?include=syllabus_body", headers=self.headers)
      syllabus_body = course_settings_request.json()['syllabus_body']
      syllabus_name = "syllabus.html"

      with open(dest_folder + "/" + syllabus_name, 'w') as html_file:
        html_file.write(syllabus_body)
      return "Success"
    except Exception as e:
      sentry_sdk.capture_exception(e)
      return "Failed! Error: " + str(e)

  def download_modules(self, dest_folder: str, api_path: str) -> str:
    """
    Downloads all content uploaded in modules.
    Modules may contain: assignments, quizzes, files, pages, discussions, external tools and external urls.
    Rest of the things are covered in other functions.
    """
    print("In download_modules")
    # need to parse pages through modules
    try:
      module_request = requests.get(api_path + "/modules?include=items&per_page=50", headers=self.headers)
      print(api_path + "/modules?include=items&per_page=50")
      modules = module_request.json()
      print("Length of modules: ", len(modules))

      for module in modules:
        # check if published
        print("Module: ", module['name'])
        if 'published' in module and not module['published']:
          print("Module not published: ", module['name'])
          continue

        module_number = str(module['position'])
        print("Downloading module: ", module_number)
        module_items = module['items']
        for item in module_items:
          if item['type'] == 'ExternalUrl':  # EXTERNAL LINK
            external_url = item['external_url']
            url_title = item['title']

            # Download external url as HTML
            response = requests.get(external_url)
            if response.status_code == 200:
              html_file_name = "Module_" + module_number + "_external_link_" + url_title.replace(" ", "_") + ".html"
              with open(dest_folder + "/" + html_file_name, 'w') as html_file:
                html_file.write(response.text)

          elif item['type'] == 'Discussion':  # DISCUSSION
            discussion_url = item['url']
            discussion_req = requests.get(discussion_url, headers=self.headers)

            if discussion_req.status_code == 200:
              discussion_data = discussion_req.json()
              discussion_message = discussion_data['message']
              discussion_filename = "Module_" + module_number + "_Discussion_" + discussion_data['title'].replace(
                  " ", "_") + ".html"

              # write the message to a file
              with open(dest_folder + "/" + discussion_filename, 'w') as html_file:
                html_file.write(discussion_message)

          elif item['type'] in ['Assignment', 'Quiz']:
            # Assignments are handled separately
            # Quizzes are not handled yet
            continue

          else:  # OTHER ITEMS - PAGES
            if 'url' not in item:
              continue

            item_url = item['url']
            item_request = requests.get(item_url, headers=self.headers)

            if item_request.status_code == 200:
              item_data = item_request.json()
              # check if published
              if 'published' in item_data and not item_data['published']:
                print(f"Item not published: {item_data['url']}")
                continue

              if 'body' not in item_data:
                continue

              item_body = item_data['body']
              html_file_name = "Module_" + module_number + "_" + item['type'] + "_" + item_data['url'] + ".html"

              # write page body to a file
              with open(dest_folder + "/" + html_file_name, 'w') as html_file:
                html_file.write(item_body)
            else:
              print(f"Item request failed with status code: {item_request.status_code}")

      return "Success"
    except Exception as e:
      sentry_sdk.capture_exception(e)
      return "Failed! Error: " + str(e)

  def download_assignments(self, dest_folder: str, api_path: str) -> str:
    """
    The description attribute has the assignment content in HTML format. Access that and store it as an HTML file.
    """
    print("In download_assignments")
    try:
      assignment_request = requests.get(api_path + "/assignments", headers=self.headers)
      assignments = assignment_request.json()

      for assignment in assignments:
        # check if published
        if 'published' in assignment and not assignment['published']:
          print("Assignment not published: ", assignment['name'])
          continue

        if assignment['description'] is not None and assignment['description'] != "":
          assignment_name = "assignment_" + str(assignment['id']) + ".html"
          assignment_description = assignment['description']

          with open(dest_folder + "/" + assignment_name, 'w') as html_file:
            html_file.write(assignment_description)
      return "Success"
    except Exception as e:
      sentry_sdk.capture_exception(e)
      return "Failed! Error: " + str(e)

  def download_discussions(self, dest_folder: str, api_path: str) -> str:
    """
    Download course discussions as HTML and store in given folder.
    """
    print("In download_discussions")
    try:
      discussion_request = requests.get(api_path + "/discussion_topics", headers=self.headers)
      discussions = discussion_request.json()

      for discussion in discussions:
        # check if published
        if 'published' in discussion and not discussion['published']:
          print("Discussion not published: ", discussion['title'])
          continue

        discussion_content = discussion['message']
        discussion_name = discussion['title'] + ".html"

        with open(dest_folder + "/" + discussion_name, 'w') as html_file:
          html_file.write(discussion_content)
      return "Success"
    except Exception as e:
      sentry_sdk.capture_exception(e)
      return "Failed! Error: " + str(e)

  def extract_urls_from_html(self, dir_path: str) -> Dict[str, List[str]]:
    """
    Extracts URLs from all HTML files in a directory.
    """
    print("In extract_urls_from_html")
    try:
      file_links = []
      video_links = []
      external_links = []
      data_api_endpoints = []
      for file_name in os.listdir(dir_path):
        if file_name.endswith(".html"):
          file_path = os.path.join(dir_path, file_name)
          try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
              content = file.read()
          except UnicodeDecodeError:
            with open(file_path, 'r', encoding='latin-1') as file:
              content = file.read()

          soup = BeautifulSoup(content, 'html.parser')

          # Extracting links from href attributes
          href_links = soup.find_all('a', href=True)
          for link in href_links:
            data_api_endpoint = link.get('data-api-endpoint')
            if data_api_endpoint:
              data_api_endpoints.append(data_api_endpoint)

            href = link['href']
            if re.match(r'https://canvas\.illinois\.edu/courses/\d+/files/.*', href):
              file_links.append(href)
            else:
              external_links.append(href)

          # Extracting video links from src attributes
          src_links = soup.find_all('iframe', src=True)
          for link in src_links:
            src = link['src']
            if re.match(r'https://ensemble\.illinois\.edu/hapi/v1/contents/.*', src):
              video_links.append(src)

      return {
          'file_links': list(set(file_links)),
          'video_links': list(set(video_links)),
          'data_api_endpoints': list(set(data_api_endpoints)),
      }

    except Exception as e:
      sentry_sdk.capture_exception(e)
      return {}

  def download_files_from_urls(self, urls: List[str], course_id: int, dir_path: str):
    """
    This function downloads files from a given Canvas course using the URLs provided.
    input: urls - list of URLs scraped from Canvas HTML pages.
    """
    print("In download_files_from_urls")
    print("Number of URLs: ", len(urls))
    try:
      count = 0
      for url in urls:
        count += 1
        # print("Downloading file from URL: ", url)
        with requests.get(url, stream=True) as r:
          content_type = r.headers.get('Content-Type')
          #print("Content type: ", content_type)
          content_disposition = r.headers.get('Content-Disposition')
          #print("Content disposition: ", content_disposition)
          if content_disposition is None:
            #print("No content disposition")
            continue

          if 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[1].strip('"')
            #print("local filename: ", filename)
          else:
            #print("No filename in content disposition")
            continue

          # write to PDF
          file_path = os.path.join(dir_path, filename)
          with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
              f.write(chunk)
          print("Downloaded file: ", filename)

      return "Success"
    except Exception as e:
      sentry_sdk.capture_exception(e)
      print("Error downloading files from URLs: ", e)
      return "Failed! Error: " + str(e)

  def download_videos_from_urls(self, urls: List[str], course_id: int, dir_path: str):
    """
    This function downloads videos from a given Canvas course using the URLs provided.
    """
    print("In download_videos_from_urls")
    #print("Video URLs: ", len(urls))
    try:
      count = 0
      for url in urls:
        count += 1
        with requests.get(url, stream=True) as r:
          filename = f"{course_id}_video_{count}.mp4"

          # download video
          file_path = os.path.join(dir_path, filename)
          ydl_opts = {
              'outtmpl': f'{dir_path}/{course_id}_video_{count}.%(ext)s',  # Dynamic extension
              'format': 'best',  # Best quality format
          }
          with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(url, download=True)
            ext = info_dict.get('ext', 'mp4')  # Get extension from info, default to mp4
            filename = f"{course_id}_video_{count}.{ext}"

          print(f"Video downloaded successfully: {filename}")

      return "Success"
    except Exception as e:
      sentry_sdk.capture_exception(e)
      print("Error downloading videos from URLs: ", e)
      return "Failed! Error: " + str(e)

  def download_content_from_api_endpoints(self, api_endpoints: List[str], course_id: int, dir_path: str):
    """
    This function downloads files from given Canvas API endpoints. These API endpoints are extracted along with URLs from 
    downloaded HTML files. Extracted as a fix because the main URLs don't always point to a downloadable attachment.
    These endpoints are mostly canvas file links of type - https://canvas.illinois.edu/api/v1/courses/46906/files/12785151
    """
    print("In download_content_from_api_endpoints")
    try:
      for endpoint in api_endpoints:
        try:
          if re.match(r'https:\/\/canvas\.illinois\.edu\/api\/v1\/courses\/\d+\/files\/\d+', endpoint):
            # it is a file endpoint!
            published = False
            api_response = requests.get(endpoint, headers=self.headers)
            if api_response.status_code == 200:
              file_data = api_response.json()

              if 'published' in file_data and not file_data['published']:
                print("File not published: ", file_data['filename'])
                continue

              filename = file_data['filename']
              file_url = file_data['url']
              file_download = requests.get(file_url, headers=self.headers)
              with open(os.path.join(dir_path, filename), 'wb') as f:
                f.write(file_download.content)
              print("Downloaded file: ", filename)
            else:
              print("Failed to download file from API endpoint: ", endpoint)
        except Exception as e:
          sentry_sdk.capture_exception(e)
          print("Error downloading file from API endpoint: ", endpoint)
          continue

      return "Success"
    except Exception as e:
      sentry_sdk.capture_exception(e)
      print("Error downloading files from API endpoints: ", e)
      return "Failed! Error: " + str(e)
