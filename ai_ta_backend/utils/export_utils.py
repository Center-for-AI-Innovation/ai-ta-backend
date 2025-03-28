import json
import os
import zipfile
from urllib.parse import urlparse

import xlsxwriter


def _initialize_base_name(course_name):
  return course_name[0:15] + '-conversation-export'


def _initialize_file_paths(course_name):
  base_name = _initialize_base_name(course_name)
  file_paths = {
      'zip': base_name + '.zip',
      'excel': base_name + '.xlsx',
      'jsonl': base_name + '.jsonl',
      'markdown_dir': os.path.join(os.getcwd(), 'markdown export'),
      'media_dir': os.path.join(os.getcwd(), 'media_files')
  }
  os.makedirs(file_paths['markdown_dir'], exist_ok=True)
  os.makedirs(file_paths['media_dir'], exist_ok=True)
  print(f"Initialized directories: {file_paths['markdown_dir']}, {file_paths['media_dir']}")
  return file_paths


def _initialize_excel(excel_file_path):
  workbook = xlsxwriter.Workbook(excel_file_path)
  worksheet = workbook.add_worksheet()
  worksheet.autofit()
  wrap_format = workbook.add_format()
  wrap_format.set_text_wrap()
  worksheet.set_column('G:G', 100)
  worksheet.set_column('A:A', 35)
  worksheet.set_column('B:E', 20)
  worksheet.set_column('F:F', 10)
  worksheet.set_column('H:H', 15)
  headers = [
      "Conversation ID", "User Email", "Course Name", "Message ID", "Timestamp", "User Role", "Message Content",
      "Had Images(Yes/No)"
  ]
  for col_num, header in enumerate(headers):
    worksheet.write(0, col_num, header)
  print(f"Initialized Excel headers: {headers}")
  return workbook, worksheet, wrap_format


def _process_conversation(s3, convo, course_name, file_paths, worksheet, row_num, error_log, wrap_format):
  try:
    convo_id = convo['convo_id']
    convo_data = convo['convo']
    user_email = convo['user_email']
    timestamp = convo['created_at']
    messages = convo_data['messages']
    if isinstance(messages[0]['content'], list) and messages[0]['role'] == 'user':
      convo_name = messages[0]['content'][0]['text'][:15]
    else:
      convo_name = messages[0]['content'][:15]
    print(f"Processing conversation ID: {convo_id}, User email: {user_email}")

    _create_markdown(s3, convo_id, messages, file_paths['markdown_dir'], file_paths['media_dir'], user_email, error_log,
                     timestamp, convo_name)
    # print(f"Created markdown for conversation ID: {convo_id}")
    _write_to_excel(convo_id, course_name, messages, worksheet, row_num, user_email, timestamp, error_log, wrap_format)
    # print(f"Wrote to Excel for conversation ID: {convo_id}")
    _append_to_jsonl(convo_data, file_paths['jsonl'], error_log)
    # print(f"Appended to JSONL for conversation ID: {convo_id}")
    print(f"Processed conversation ID: {convo_id}")
  except Exception as e:
    print(f"Error processing conversation ID {convo['convo_id']}: {str(e)}")
    error_log.append(f"Error processing conversation ID {convo['convo_id']}: {str(e)}")


def _process_conversation_for_user_convo_export(s3, convo, project_name, markdown_dir, media_dir, error_log):
  try:
    print("processing convo: ", convo)
    convo_id = convo['id']
    name = convo['name']
    user_email = convo['user_email']
    timestamp = convo['created_at']
    messages = convo['messages']

    _create_markdown_for_user_convo_export(s3, convo_id, messages, markdown_dir, media_dir, user_email, error_log,
                                           timestamp, name, project_name)

    print(f"Processed conversation ID {convo_id}")
  except Exception as e:
    print(f"Error processing conversation ID {convo.id}: {str(e)}")
    error_log.append(f"Error processing conversation ID {convo.id}: {str(e)}")


def _create_markdown(s3, convo_id, messages, markdown_dir, media_dir, user_email, error_log, timestamp, convo_name):
  try:
    markdown_filename = f"{timestamp.split('T')[0]}-{convo_name}.md"
    markdown_file_path = os.path.join(markdown_dir, markdown_filename)
    with open(markdown_file_path, 'w') as md_file:
      md_file.write(f"## Conversation ID: {convo_id}\n")
      md_file.write(f"## **User Email**: {user_email}\n\n")
      md_file.write(f"### **Timestamp**: {timestamp}\n\n")

      for message in messages:
        role = "User" if message['role'] == 'user' else "Assistant"
        content = _process_message_content(s3, message['content'], convo_id, media_dir, error_log)
        md_file.write(f"### {role}:\n")
        md_file.write(f"{content}\n\n")
        md_file.write("---\n\n")  # Separator for each message for better readability

    print(f"Created markdown file at path: {markdown_file_path}")
  except Exception as e:
    print(f"Error creating markdown for conversation ID {convo_id}: {str(e)}")
    error_log.append(f"Error creating markdown for conversation ID {convo_id}: {str(e)}")


def _create_markdown_for_user_convo_export(s3, convo_id, messages, markdown_dir, media_dir, user_email, error_log,
                                           timestamp, name, project_name):
  try:
    print(f"Creating markdown file for conversation ID {convo_id}")
    markdown_filename = f"{name}-{timestamp.split('T')}.md"
    markdown_file_path = os.path.join(markdown_dir, markdown_filename)

    with open(markdown_file_path, 'w') as md_file:
      # md_file.write(f"Conversation ID: {convo_id}\n")
      md_file.write(f"## **UIUC Chat Conversation for Project**: {project_name}\n\n")
      md_file.write(f"## **User Email**: {user_email}\n\n")
      md_file.write(f"Date: {timestamp.split('T')[0]}\n\n")

      for message in messages:
        text = message['content_text']
        img_urls = message['content_image_url']
        img_desc = message['image_description']
        print(f"text: {text}")
        print(f"img_urls: {img_urls}")
        role = "User" if message['role'] == 'user' else "Assistant" if message['role'] == 'assistant' else "System"

        # content = _process_message_content(s3, message['content'], convo_id, media_dir, error_log)
        content = _process_message_content_for_user_convo_export(s3, message['content_text'],
                                                                 message['content_image_url'], convo_id, media_dir,
                                                                 error_log)
        md_file.write(f"### {role}:\n")
        md_file.write(f"{content}\n\n")
        if img_desc:
          md_file.write(f"### Image Description:\n")
          md_file.write(f"{img_desc}\n\n")
        md_file.write("---\n\n")  # Separator for each message for better readability

    print(f"Created markdown file at path: {markdown_file_path}")
  except Exception as e:
    print(f"Error creating markdown for conversation ID {convo_id}: {str(e)}")
    error_log.append(f"Error creating markdown for conversation ID {convo_id}: {str(e)}")


def _process_message_content(s3, content, convo_id, media_dir, error_log):
  try:
    if isinstance(content, list):
      flattened_content = []
      for item in content:
        if item['type'] == 'text':
          flattened_content.append(item['text'])
        elif item['type'] == 'image_url':
          # Use only the UUID part of the image URL for the filename
          image_filename = f"{item['image_url']['url'].split('/')[-1].split('?')[0]}"
          image_file_path = os.path.join(media_dir, image_filename)
          image_s3_path = _extract_path_from_url(item['image_url']['url'])
          # Save the image to the media directory
          s3.download_file(image_s3_path, os.environ['S3_BUCKET_NAME'], image_file_path)
          # Adjust the path to be relative from the markdown file's perspective
          relative_image_path = os.path.join('..', media_dir.split('/')[-1], image_filename)
          flattened_content.append(f"![Image]({relative_image_path})")
      print(f"Processed message content for conversation ID: {convo_id}")
      return ' '.join(flattened_content)
    else:
      return content
  except Exception as e:
    print(f"Error processing message content for conversation ID {convo_id}: {str(e)}")
    error_log.append(f"Error processing message content for conversation ID {convo_id}: {str(e)}")
    return content


def _process_message_content_for_user_convo_export(s3, content_text: str, content_image_url: list, convo_id: str,
                                                   media_dir: str, error_log: list) -> str:
  try:
    content = content_text
    for url in content_image_url:
      image_filename = f"{url.split('/')[-1].split('?')[0]}"
      image_file_path = os.path.join(media_dir, image_filename)
      image_s3_path = _extract_path_from_url(url)
      s3.download_file(image_s3_path, os.environ['S3_BUCKET_NAME'], image_file_path)
      relative_image_path = os.path.join('..', media_dir.split('/')[-1], image_filename)
      content += f"\n![Image]({relative_image_path})"
    return content
  except Exception as e:
    print(f"Error processing message content for conversation ID {convo_id}: {str(e)}")
    error_log.append(f"Error processing message content for conversation ID {convo_id}: {str(e)}")
    return content_text


def _extract_path_from_url(url: str) -> str:
  urlObject = urlparse(url)
  path = urlObject.path
  if path.startswith('/'):
    path = path[1:]
  return path


def _write_to_excel(convo_id, course_name, messages, worksheet, row_num, user_email, timestamp, error_log, wrap_format):
  try:
    start_row = row_num
    for message_id, message in enumerate(messages):
      if message_id == 0:
        # if convo_id == '3f1827f5-3d6c-4743-b467-12ef4b2059c5':
        # print(f"Message: {message}")
        worksheet.write(row_num, 0, convo_id)
      worksheet.write(row_num, 1, user_email)
      worksheet.write(row_num, 2, course_name, wrap_format)
      worksheet.write(row_num, 3, message_id)  # Add message ID as the index of the message
      worksheet.write(row_num, 4, timestamp, wrap_format)
      worksheet.write(row_num, 5, message['role'], wrap_format)
      if message['role'] == 'user' and isinstance(message['content'], list):
        # if convo_id == '3f1827f5-3d6c-4743-b467-12ef4b2059c5':
        # print(f"Message content: {message['content']}")
        content = ' '.join([item['text'] for item in message['content'] if item['type'] == 'text'])
        contains_image = any(item['type'] == 'image_url' and 'url' in item['image_url'] for item in message['content'])
        if contains_image:
          worksheet.write(row_num, 7, 'Yes')
        else:
          worksheet.write(row_num, 7, 'No')
      else:
        content = message['content']
      worksheet.write(row_num, 6, content, wrap_format)
      row_num += 1

    # Merge the rows in the first column for the same convo_id
    if row_num > start_row + 1:
      worksheet.merge_range(start_row, 0, row_num - 1, 0, convo_id, wrap_format)

    print(f"Wrote messages to Excel for conversation ID: {convo_id}")
  except Exception as e:
    print(f"Error writing to Excel for conversation ID {convo_id}: {str(e)}")
    error_log.append(f"Error writing to Excel for conversation ID {convo_id}: {str(e)}")


def _append_to_jsonl(convo_data, jsonl_file_path, error_log):
  try:
    with open(jsonl_file_path, 'a') as jsonl_file:
      jsonl_file.write(json.dumps(convo_data) + '\n')
    print(f"Appended conversation data to JSONL file at path: {jsonl_file_path}")
  except Exception as e:
    print(f"Error appending to JSONL for conversation ID {convo_data['convo_id']}: {str(e)}")
    error_log.append(f"Error appending to JSONL for conversation ID {convo_data['convo_id']}: {str(e)}")


def _create_zip(file_paths, error_log):
  zip_file_path = os.path.join(os.getcwd(), file_paths['zip'])
  error_log_path = os.path.join(os.getcwd(), 'error.log')
  with open(error_log_path, 'w') as log_file:
    for error in error_log:
      log_file.write(error + '\n')
  with zipfile.ZipFile(zip_file_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
    for root, _, files in os.walk(file_paths['markdown_dir']):
      for file in files:
        zipf.write(
            os.path.join(root, file),
            os.path.join('markdown export', os.path.relpath(os.path.join(root, file), file_paths['markdown_dir'])))
    for root, _, files in os.walk(file_paths['media_dir']):
      for file in files:
        zipf.write(
            os.path.join(root, file),
            os.path.join(file_paths['media_dir'].split('/')[-1],
                         os.path.relpath(os.path.join(root, file), file_paths['media_dir'])))
    zipf.write(file_paths['excel'], os.path.basename(file_paths['excel']))
    zipf.write(file_paths['jsonl'], os.path.basename(file_paths['jsonl']))
    zipf.write(error_log_path, 'error.log')
  print(f"Created zip file at path: {zip_file_path}")
  return zip_file_path


def _create_zip_for_user_convo_export(markdown_dir, media_dir, error_log):
  zip_file_path = os.path.join(os.getcwd(), 'user_convo_export.zip')
  error_log_path = os.path.join(os.getcwd(), 'error.log')
  with open(error_log_path, 'w') as log_file:
    for error in error_log:
      log_file.write(error + '\n')
  with zipfile.ZipFile(zip_file_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
    for root, _, files in os.walk(markdown_dir):
      for file in files:
        zipf.write(os.path.join(root, file),
                   os.path.join('markdown export', os.path.relpath(os.path.join(root, file), markdown_dir)))
    for root, _, files in os.walk(media_dir):
      for file in files:
        zipf.write(os.path.join(root, file),
                   os.path.join(media_dir.split('/')[-1], os.path.relpath(os.path.join(root, file), media_dir)))
    zipf.write(error_log_path, 'error.log')
  print(f"Created zip file at path: {zip_file_path}")
  return zip_file_path


# def _process_conversation_for_user_convo_export(s3, convo, project_name, markdown_dir, media_dir, error_log):
#   try:
#     convo_id = convo['id']
#     name = convo['name']
#     messages = convo['messages']
#     user_email = convo['user_email']
#     timestamp = convo['created_at']

#     _create_markdown_for_user_convo_export(s3, convo_id, name, messages, user_email, timestamp, project_name, markdown_dir, media_dir, error_log)
#   except Exception as e:
#     print(f"Error processing conversation ID {convo['id']}: {str(e)}")
#     error_log.append(f"Error processing conversation ID {convo['id']}: {str(e)}")


def _cleanup(file_paths):
  os.remove(file_paths['excel'])
  os.remove(file_paths['jsonl'])
  import shutil
  shutil.rmtree(file_paths['markdown_dir'])
  shutil.rmtree(file_paths['media_dir'])
  print(f"Cleaned up files: {file_paths}")
