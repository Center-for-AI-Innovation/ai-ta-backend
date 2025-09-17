import os
import re
import uuid
import shutil
import logging
import requests
from typing import Dict, List
from dotenv import load_dotenv

import boto3
from botocore.config import Config
import sentry_sdk
from canvasapi import Canvas

import yt_dlp
from bs4 import BeautifulSoup

from ai_ta_backend.rabbitmq.rmqueue import Queue


load_dotenv()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class IngestCanvas:
    """
    Class for ingesting documents into the vector database.
    """

    def __init__(self):
        self.canvas_url = "https://canvas.illinois.edu"
        self.canvas_access_token = os.getenv('CANVAS_ACCESS_TOKEN')
        self.headers = {"Authorization": f"Bearer {self.canvas_access_token}"}
        self.volume_path = "./canvas_ingest"
        self.minio_url = os.getenv('MINIO_URL')
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        self.s3_client = None
        self.canvas_client = None

    def initialize_resources(self):
        # Connect to AWS S3 file store
        if self.aws_access_key_id and self.aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.minio_url,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                config=Config(s3={'addressing_style': 'path'}),
            )
        else:
            logging.info("AWS ACCESS KEY ID OR SECRET ACCESS KEY NOT FOUND, TRYING WITHOUT CREDENTIALS")
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.minio_url,
                config=Config(s3={'addressing_style': 'path'})
            )

        self.canvas_client = Canvas(self.canvas_url, self.canvas_access_token)

        # TODO: Is this sentry stuff necessary?
        sentry_sdk.init(
              dsn="https://examplePublicKey@o0.ingest.sentry.io/0",
              enable_tracing=True,
        )

    def auto_accept_enrollments(self, target_course_id):
        try:
            # Validate target_course_id
            if target_course_id is None:
                error_msg = "target_course_id cannot be None"
                return f"Failed: {error_msg}"
            user_response = requests.get(
                f"{self.canvas_url}/api/v1/users/self",
                headers=self.headers
            )
            user_response.raise_for_status()
            user_data = user_response.json()
            user_id = user_data.get('id')

            if not user_id:
                return "Failed to get user ID"

            # First, check if user is already enrolled in the target course
            current_enrollments_url = f"{self.canvas_url}/api/v1/users/{user_id}/enrollments?state[]=active"
            current_enrollment_response = requests.get(current_enrollments_url, headers=self.headers)
            current_enrollment_response.raise_for_status()
            current_enrollments = current_enrollment_response.json()

            # Check if already enrolled in target course
            for enrollment in current_enrollments:
                course_id = enrollment.get('course_id')
                if str(course_id) == str(target_course_id):
                    return f"User is already enrolled in course ID {target_course_id}"

            # If not already enrolled, check for pending invitations
            enrollments_url = f"{self.canvas_url}/api/v1/users/{user_id}/enrollments?state[]=invited"
            enrollment_response = requests.get(enrollments_url, headers=self.headers)
            enrollment_response.raise_for_status()
            pending_enrollments = enrollment_response.json()

            # Find the enrollment for the target course
            target_enrollment = None
            for enrollment in pending_enrollments:
                course_id = enrollment.get('course_id')
                if str(course_id) == str(target_course_id):
                    target_enrollment = enrollment
                    break

            # If no enrollment found for the target course, throw error
            if not target_enrollment:
                error_msg = f"User is not enrolled and no pending invitation found for course ID {target_course_id}"
                raise Exception(error_msg)

            # Accept the enrollment for the target course
            course_id = target_enrollment.get('course_id')
            enrollment_id = target_enrollment.get('id')

            if not course_id or not enrollment_id:
                error_msg = f"Missing course_id or enrollment_id in enrollment data: {target_enrollment}"
                raise Exception(error_msg)

            accept_url = f"{self.canvas_url}/api/v1/courses/{course_id}/enrollments/{enrollment_id}/accept"
            accept_response = requests.post(accept_url, headers=self.headers)

            if accept_response.status_code == 200:
                result = accept_response.json()
                if result.get('success'):
                    return f"Successfully accepted enrollment for course ID {course_id}"
                else:
                    error_msg = f"Failed to accept enrollment: {result}"
                    raise Exception(error_msg)
            else:
                error_msg = f"Failed to accept enrollment. Status code: {accept_response.status_code}"
                raise Exception(error_msg)

        except Exception as e:
            sentry_sdk.capture_exception(e)
            return f"Failed! Error: {str(e)}"

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
            user_names = (user['name'] for user in users)
            print("Collected names: ", user_names)

            # TODO: This doesn't do anything... getting names but not emails?

            return len(user_names) > 0
        except Exception as e:
            sentry_sdk.capture_exception(e)
            return "Failed to `add users`! Error: " + str(e)

    def download_course_content(self, canvas_course_id: int, dest_folder: str, content_ingest_dict: dict) -> str:
        """Downloads all Canvas course materials through the course ID and stores in local directory."""
        self.initialize_resources()
        api_path = f"{self.canvas_url}/api/v1/courses/{canvas_course_id}"

        # Iterate over the content_ingest_dict
        for key, value in content_ingest_dict.items():
            if value is True:
                if key == 'files':
                    self.download_files(dest_folder, api_path)
                elif key == 'pages':
                    self.download_pages(dest_folder, api_path)
                elif key == 'modules':
                    self.download_modules(dest_folder, api_path)
                elif key == 'syllabus':
                    self.download_syllabus(dest_folder, api_path)
                elif key == 'assignments':
                    self.download_assignments(dest_folder, api_path)
                elif key == 'discussions':
                    self.download_discussions(dest_folder, api_path)

        extracted_urls_from_html = self.extract_urls_from_html(dest_folder)

        # links - canvas files, external urls, embedded videos
        file_links = extracted_urls_from_html.get('file_links', [])
        if file_links:
            file_download_status = self.download_files_from_urls(file_links, canvas_course_id, dest_folder)
            print("File download status: ", file_download_status)

        video_links = extracted_urls_from_html.get('video_links', [])
        if video_links:
            video_download_status = self.download_videos_from_urls(video_links, canvas_course_id, dest_folder)
            print("Video download status: ", video_download_status)

        data_api_endpoints = extracted_urls_from_html.get('data_api_endpoints', [])
        if data_api_endpoints:
            data_api_endpoints_status = self.download_content_from_api_endpoints(data_api_endpoints,
                                                                                 canvas_course_id,
                                                                                 dest_folder)
            print("Data API Endpoints download status: ", data_api_endpoints_status)

    def ingest_course_content(self,
                              canvas_course_id: int,
                              course_name: str,
                              content_ingest_dict: Dict[str, bool] = None) -> List[str]:
        """
        Ingests all Canvas course materials through the course ID.
        """
        if content_ingest_dict is None:
            content_ingest_dict = {
                'files': True,
                'pages': True,
                'modules': True,
                'syllabus': True,
                'assignments': True,
                'discussions': True
            }

        # Create a canvas directory with a course folder inside it.
        canvas_dir = os.path.join(self.volume_path, "canvas_materials")
        folder_name = f"canvas_course_{canvas_course_id}_ingest"
        folder_path = os.path.join(canvas_dir, folder_name)
        os.makedirs(folder_path, exist_ok=True)

        self.download_course_content(canvas_course_id, folder_path, content_ingest_dict)

        # Upload files to S3
        all_file_paths = [os.path.join(dp, f) for dp, dn, filenames in os.walk(folder_path) for f in filenames]
        all_s3_paths = []
        all_readable_filenames = []
        for file_path in all_file_paths:
            file_name = os.path.basename(file_path)
            extension = os.path.splitext(file_name)[1]
            name_without_extension = re.sub(r'[^a-zA-Z0-9]', '-', os.path.splitext(file_name)[0])
            readable_filename = f"{name_without_extension}{extension}"
            uid = uuid.uuid4()

            unique_filename = f"{uid}-{readable_filename}"
            s3_path = f"courses/{course_name}/{unique_filename}"
            all_s3_paths.append(s3_path)
            all_readable_filenames.append(readable_filename)
            print(f"Uploading file: {readable_filename}")
            self.upload_file(file_path, os.environ['S3_BUCKET_NAME'], s3_path)

        shutil.rmtree(folder_path)

        # Ingest files
        job_ids = []
        active_queue = Queue()  # TODO: Should we post back to /ingest endpoint here?
        for s3_path, readable_filename in zip(all_s3_paths, all_readable_filenames):
            data = {
                'course_name': course_name,
                'readable_filename': readable_filename,
                's3_paths': s3_path,
                'base_url': f"{self.canvas_url}/courses/{canvas_course_id}",
            }
            print(f"Posting readable_filename: '{readable_filename}' with S3 path: '{s3_path}'")
            job_id = active_queue.addJobToIngestQueue(data)
            print("RabbitMQ Ingest Task Queue response: ", job_id)
            job_ids.append(job_id)

        return job_ids

    def download_files(self, dest_folder: str, api_path: str) -> str:
        """
        Downloads all files in a Canvas course into given folder.
        """
        try:
            files_request = requests.get(f"{api_path}/files", headers=self.headers)
            files = files_request.json()
            if 'status' in files and files['status'] == 'unauthorized':
                logging.error(f"Unauthorized to access files: {files['status']}")
                # Student user probably blocked for Files access
                return "Unauthorized to access files!"

            for f in files:
                try:
                    print("Downloading file: ", f['filename'])
                    full_path = os.path.join(dest_folder, f['filename'])
                    os.makedirs(os.path.dirname(full_path), exist_ok=True)
                    response = requests.get(f['url'], headers=self.headers)
                    with open(full_path, 'wb') as dest:
                        dest.write(response.content)
                except Exception as e:
                    logging.error(f"Error downloading file '{f['filename']}', error: {e}")
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
            pages_request = requests.get(f"{api_path}/pages", headers=self.headers)
            pages = pages_request.json()

            if 'status' in pages and pages['status'] == 'unauthorized':
                # Student user probably blocked for Pages access
                return "Unauthorized to access pages!"

            for page in pages:
                if 'published' in page and not page['published']:
                    print(f"Page not published: {page['title']}")
                    continue
                if 'hide_from_students' in page and page['hide_from_students']:
                    print("Page hidden from students: ", page['title'])
                    continue
                # page is visible to students
                if page['html_url'] != '':
                    page_content_request = requests.get(f"{api_path}/pages/{page['page_id']}",
                                                        headers=self.headers)
                    page_body = page_content_request.json()['body']
                    with open(f"{dest_folder}/{page['url']}.html", 'w') as html_file:
                        html_file.write(page_body)
            return "Success"
        except Exception as e:
            return "Failed! Error: " + str(e)

    def download_syllabus(self, dest_folder: str, api_path: str) -> str:
        """
        Downloads syllabus as HTML and stores in given folder.
        """
        try:
            course_settings_request = requests.get(f"{api_path}?include=syllabus_body", headers=self.headers)
            syllabus_body = course_settings_request.json()['syllabus_body']
            with open(f"{dest_folder}/syllabus.html", 'w') as html_file:
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
        try:
            module_request = requests.get(f"{api_path}/modules?include=items&per_page=50", headers=self.headers)
            modules = module_request.json()
            for module in modules:
                if 'published' in module and not module['published']:
                    print("Module not published: ", module['name'])
                    continue

                module_number = str(module['position'])
                print("Downloading module: ", module_number)
                for item in module['items']:

                    if item['type'] == 'ExternalUrl':
                        response = requests.get(item['external_url'])
                        if response.status_code == 200:
                            title_fmt = item['title'].replace(' ', '_')
                            html_file_name = f"Module_{module_number}_external_link_{title_fmt}.html"
                            with open(f"{dest_folder}/{html_file_name}", 'w') as html_file:
                                html_file.write(response.text)

                    elif item['type'] == 'Discussion':
                        discussion_req = requests.get(item['url'], headers=self.headers)
                        if discussion_req.status_code == 200:
                            discussion_data = discussion_req.json()
                            title_fmt = discussion_data['title'].replace(" ", "_")
                            discussion_filename = f"Module_{module_number}_Discussion_{title_fmt}.html"
                            with open(f"{dest_folder}/{discussion_filename}", 'w') as html_file:
                                html_file.write(discussion_data['message'])

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
                            if 'published' in item_data and not item_data['published']:
                                print(f"Item not published: {item_data['url']}")
                                continue
                            if 'body' not in item_data:
                                continue

                            item_body = item_data['body']
                            html_file_name = f"Module_{module_number}_{item['type']}_{item_data['url']}.html"
                            with open(f"{dest_folder}/{html_file_name}", 'w') as html_file:
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
        try:
            assignment_request = requests.get(f"{api_path}/assignments", headers=self.headers)
            assignments = assignment_request.json()
            for assignment in assignments:
                if 'published' in assignment and not assignment['published']:
                    print("Assignment not published: ", assignment['name'])
                    continue

                if assignment['description'] is not None and assignment['description'] != "":
                    assignment_name = f"assignment_{assignment['id']}.html"
                    with open(f"{dest_folder}/{assignment_name}", 'w') as html_file:
                        html_file.write(assignment['description'])
            return "Success"
        except Exception as e:
            sentry_sdk.capture_exception(e)
            return "Failed! Error: " + str(e)

    def download_discussions(self, dest_folder: str, api_path: str) -> str:
        """
        Download course discussions as HTML and store in given folder.
        """
        try:
            discussion_request = requests.get(f"{api_path}/discussion_topics", headers=self.headers)
            discussions = discussion_request.json()
            for discussion in discussions:
                if 'published' in discussion and not discussion['published']:
                    print("Discussion not published: ", discussion['title'])
                    continue

                with open(f"{dest_folder}/{discussion['title']}.html", 'w') as html_file:
                    html_file.write(discussion['message'])
            return "Success"
        except Exception as e:
            sentry_sdk.capture_exception(e)
            return "Failed! Error: " + str(e)

    def extract_urls_from_html(self, dir_path: str) -> Dict[str, List[str]]:
        """
        Extracts URLs from all HTML files in a directory.
        """
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
        try:
            count = 0
            for url in urls:
                count += 1
                with requests.get(url, stream=True) as r:
                    content_disposition = r.headers.get('Content-Disposition')
                    if content_disposition is None:
                        continue

                    if 'filename=' in content_disposition:
                        filename = content_disposition.split('filename=')[1].strip('"')
                    else:
                        continue

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
        try:
            count = 0
            for url in urls:
                count += 1
                with requests.get(url, stream=True) as r:
                    ydl_opts = {
                        'outtmpl': f'{dir_path}/{course_id}_video_{count}.%(ext)s',  # Dynamic extension
                        'format': 'best',
                    }
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info_dict = ydl.extract_info(url, download=True)
                        ext = info_dict.get('ext', 'mp4')
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
        try:
            for endpoint in api_endpoints:
                try:
                    if re.match(r'https:\/\/canvas\.illinois\.edu\/api\/v1\/courses\/\d+\/files\/\d+', endpoint):
                        # it is a file endpoint!
                        api_response = requests.get(endpoint, headers=self.headers)
                        if api_response.status_code == 200:
                            file_data = api_response.json()
                            if 'published' in file_data and not file_data['published']:
                                continue

                            file_download = requests.get(file_data['url'], headers=self.headers)
                            with open(os.path.join(dir_path, file_data['filename']), 'wb') as f:
                                f.write(file_download.content)
                            print("Downloaded file: ", file_data['filename'])
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
